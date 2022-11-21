use crate::config::Config;
use common::{Result, RuntimeError, ServerId, StatusResult};
use mysql::prelude::*;
use mysql::*;
use protos::{control_server_client::ControlServerClient, db_server_server::DbServer as Server};
use protos::{AppTables, DbShard, ServerRegisterRequest};
use std::{fs, io::Write};
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Status};
use tracing::{info, trace};

pub struct DbServer {
    control_client: AsyncMutex<ControlServerClient<Channel>>,
    /// state that need init
    inner: OnceCell<Inner>,
}

struct Inner {
    _shard: DbShard,
    config: Config,
    connection_pool: Pool,
}

impl DbServer {
    /// - `control_uri`: Uri of control server
    /// - `uri`: Uri of this server
    pub async fn new(control_uri: Uri, uri: Uri) -> Result<Self> {
        let mut control_client = {
            let ep = Channel::builder(control_uri);
            let client = ControlServerClient::connect(ep)
                .await
                .map_err(|e| RuntimeError::TonicConnectError { source: e })?;
            client
        };
        control_client
            .register(ServerRegisterRequest { uri: uri.to_string() })
            .await?;
        Ok(DbServer {
            control_client: AsyncMutex::new(control_client),
            inner: OnceCell::new(),
        })
    }

    /// This function is a workaround.
    /// Because DBServer cannot get the listenning address of the Real Server.
    pub async fn register(&self, uri: Uri) -> Result<ServerId> {
        let mut client = self.control_client.lock().await;
        let server_id = client
            .register(ServerRegisterRequest {
                uri: uri.to_string(),
            })
            .await?
            .into_inner()
            .server_id;
        Ok(server_id)
    }

    async fn init(&self, shard: DbShard) -> Result<()> {
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

    async fn load_tables(&self, table: i32) -> Result<AppTables> {
        let app_table = AppTables::from_i32(table)
            .ok_or(RuntimeError::RpcInvalidArg("table is invalid".to_owned()))?;
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
        self.init(shard).await?;
        Ok(Response::new(()))
    }

    /// bulk load table from local files.
    async fn bulk_load(
        &self,
        req: Request<protos::BulkLoadRequest>,
    ) -> StatusResult<Response<protos::BulkLoadResponse>> {
        let protos::BulkLoadRequest { table } = req.into_inner();
        self.load_tables(table).await?;
        Ok(Response::new(protos::BulkLoadResponse { result: true }))
    }
}
