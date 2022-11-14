use crate::config::Config;
use anyhow::Result as AnyResult;
use common::RuntimeError;
use mysql::prelude::*;
use mysql::*;
use protos::db_server_server::DbServer as Server;
use protos::{AppTables, DbShard};
use std::{fs, io::Write};
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status};
use tracing::{info, trace};

pub type StatusResult<T> = core::result::Result<T, Status>;

pub struct DbServer {
    inner: OnceCell<Inner>,
}

struct Inner {
    _shard: DbShard,
    config: Config,
    connection_pool: Pool,
}

impl DbServer {
    pub fn new() -> Self {
        DbServer {
            inner: OnceCell::new(),
        }
    }

    async fn init(&self, shard: DbShard) -> AnyResult<()> {
        if self.inner.initialized() {
            Err(RuntimeError::Initialized)?;
        }
        let config_path = match shard {
            DbShard::One => std::env::var("SHARD1_CONFIG_PATH")?,
            DbShard::Two => std::env::var("SHARD2_CONFIG_PATH")?,
        };
        let config = Config::new(config_path)?;
        let pool = Pool::new(config.url.as_str())?;
        self.inner
            .get_or_init(move || async move {
                Inner {
                    _shard: shard,
                    config,
                    connection_pool: pool,
                }
            })
            .await;
        Ok(())
    }

    async fn load_tables(&self, table: i32) -> AnyResult<AppTables> {
        let app_table =
            AppTables::from_i32(table).ok_or(Status::invalid_argument("table is invalid"))?;
        info!("start load table: {:?}", app_table);

        let inner = self.inner.get().ok_or(RuntimeError::Uninitialize)?;

        // Step 1: create table
        let sql = &inner.config.create_table_sqls[table as usize];
        let mut conn = inner.connection_pool.get_conn().unwrap();
        trace!("start query: {}", sql);
        conn.query_drop(sql)?;

        // Step 2: set local infile handler
        let local_infile_handler = Some(LocalInfileHandler::new(|file_name, writer| {
            let file_name_str = String::from_utf8_lossy(file_name).to_string();
            let file_content = fs::read_to_string(file_name_str.as_str())?;
            writer.write_all(file_content.as_bytes())
        }));
        conn.set_local_infile_handler(local_infile_handler);

        // Step 3: load data
        let sql_file_path = inner
            .config
            .db_file_dir
            .as_path()
            .join(&inner.config.db_file_names[table as usize]);
        let bulk_query = format!(
            "LOAD DATA LOCAL INFILE '{}' INTO TABLE {}
            FIELDS TERMINATED BY '|'
            LINES TERMINATED BY '\\n' ",
            sql_file_path.to_str().expect("invalid sql file path"),
            inner.config.table_names[table as usize]
        );
        trace!("start query: {}", bulk_query);
        conn.query_drop(bulk_query)?;

        Ok(app_table)
    }
}

#[tonic::async_trait]
impl Server for DbServer {
    /// Ping Server
    async fn ping(&self, _: Request<()>) -> StatusResult<Response<()>> {
        info!("receive ping");
        Ok(Response::new(()))
    }

    async fn init(&self, req: Request<protos::InitServerRequest>) -> StatusResult<Response<()>> {
        let protos::InitServerRequest { shard } = req.into_inner();
        let shard = DbShard::from_i32(shard).ok_or(Status::invalid_argument("shard is invalid"))?;
        info!("init server for shard: {:?}", shard);
        self.init(shard)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(()))
    }

    /// bulk load table from local files.
    async fn bulk_load(
        &self,
        req: Request<protos::BulkLoadRequest>,
    ) -> StatusResult<Response<protos::BulkLoadResponse>> {
        let protos::BulkLoadRequest { table } = req.into_inner();
        self.load_tables(table)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(protos::BulkLoadResponse { result: true }))
    }
}
