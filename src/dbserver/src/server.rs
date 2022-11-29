use crate::config::Config;
use common::{MyRow, Result, RuntimeError, ServerId, StatusResult};
use flexbuffers::FlexbufferSerializer;
use futures::Stream;
use mysql::prelude::*;
use mysql::*;
use protos::{control_server_client::ControlServerClient, db_server_server::DbServer as Server};
use protos::{AppTables, DbShard, ExecSqlFirstResponse, ServerRegisterRequest};
use serde::Serialize;
use std::pin::Pin;
use std::{fs, io::Write};
use tokio::sync::{
    mpsc::{self, Receiver},
    Mutex as AsyncMutex, OnceCell,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Status};
use tracing::{info, trace};

pub struct DbServer {
    control_client: AsyncMutex<ControlServerClient<Channel>>,
    /// state that need init
    inner: OnceCell<Inner>,
}

struct Inner {
    shard: DbShard,
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
            .register(ServerRegisterRequest {
                uri: uri.to_string(),
            })
            .await?;
        Ok(DbServer {
            control_client: AsyncMutex::new(control_client),
            inner: OnceCell::new(),
        })
    }

    /// This function is a workaround,
    /// since DBServer cannot get the listenning address of the Real Server.
    /// - `uri`: uri of this server
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
        match self.inner.get() {
            Some(inner) if inner.shard == shard => return Ok(()),
            Some(_) => return Err(RuntimeError::Initialized),
            None => {}
        }
        let config_path = match shard {
            DbShard::One => std::env::var("SHARD1_CONFIG_PATH")?,
            DbShard::Two => std::env::var("SHARD2_CONFIG_PATH")?,
        };
        let config = Config::new(config_path)?;
        let pool = Pool::new(config.url.as_str())?;
        // load the procedure
        for procedure in super::config::STORE_PROCEDURE {
            pool.get_conn()?.query_drop(procedure)?;
        }
        trace!("load all the procedure");
        self.inner
            .get_or_init(move || async move {
                Inner {
                    shard,
                    config,
                    connection_pool: pool,
                }
            })
            .await;
        Ok(())
    }

    fn get_inner(&self) -> Result<&Inner> {
        self.inner.get().ok_or(RuntimeError::Uninitialize)
    }

    async fn load_tables(&self, table: i32) -> Result<AppTables> {
        let app_table = AppTables::from_i32(table)
            .ok_or(RuntimeError::RpcInvalidArg("table is invalid".to_owned()))?;
        info!("start load table: {:?}", app_table);

        let inner = self.get_inner()?;

        // Step 1: create table
        let sql = &inner.config.create_table_sqls[table as usize];
        let mut conn = inner.connection_pool.get_conn()?;
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

    fn sql_result_stream(&self, sql: String) -> Result<Receiver<Result<Vec<u8>>>> {
        trace!("sql result stream: {sql}");
        let inner = self.get_inner()?;
        let mut conn = inner.connection_pool.get_conn()?;
        let (tx, rx) = mpsc::channel(64);
        tokio::task::spawn_blocking(move || {
            let mut query_result = match conn.exec_iter(sql, ()) {
                Ok(x) => x,
                Err(e) => {
                    tx.blocking_send(Err(e.into())).ok();
                    return;
                }
            };
            let result_set = query_result.iter().unwrap();
            for row in result_set {
                let entry = match row {
                    Ok(row) => {
                        let mut s = FlexbufferSerializer::new();
                        let my_row: MyRow = row.into();
                        my_row.serialize(&mut s).expect("serialize error");
                        Ok(s.take_buffer())
                    }
                    Err(e) => Err(e.into()),
                };
                if tx.blocking_send(entry).is_err() {
                    return;
                }
            }
            debug_assert!(query_result.iter().is_none());
        });
        Ok(rx)
    }

    fn execute_sql_drop(&self, sql: String) -> Result<()> {
        trace!("exec sql drop: {sql}");
        let inner = self.get_inner()?;
        let mut conn = inner.connection_pool.get_conn()?;
        conn.query_drop(sql)?;
        Ok(())
    }

    fn exec_sql_first(&self, sql: String) -> Result<ExecSqlFirstResponse> {
        trace!("exec sql first: {sql}");
        let inner = self.get_inner()?;
        let mut conn = inner.connection_pool.get_conn()?;
        let my_row = conn.exec_first(sql, ())?.map(|row: Row| {
            let mut s = FlexbufferSerializer::new();
            let my_row: MyRow = row.into();
            my_row.serialize(&mut s).expect("serialize error");
            s.take_buffer()
        });
        Ok(ExecSqlFirstResponse { row: my_row })
    }
}

#[tonic::async_trait]
impl Server for DbServer {
    type ExecSqlStream = Pin<Box<dyn Stream<Item = StatusResult<Vec<u8>>> + Send>>;
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

    async fn exec_sql(&self, sql: Request<String>) -> StatusResult<Response<Self::ExecSqlStream>> {
        info!("get exec_sql request");
        let rx = self.sql_result_stream(sql.into_inner())?;
        Ok(Response::new(Box::pin(
            ReceiverStream::new(rx).map(|entry| entry.map_err(|e| e.into())),
        )))
    }

    async fn exec_sql_first(
        &self,
        req: Request<String>,
    ) -> StatusResult<Response<ExecSqlFirstResponse>> {
        info!("get exec_sql_first request");
        let response = self.exec_sql_first(req.into_inner())?;
        Ok(Response::new(response))
    }

    async fn exec_sql_drop(&self, req: Request<String>) -> StatusResult<Response<()>> {
        info!("get execute_sql");
        self.execute_sql_drop(req.into_inner())?;
        Ok(Response::new(()))
    }
}
