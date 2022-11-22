use common::{Result, RuntimeError, StatusResult, ServerId};
use futures::stream::{FuturesOrdered, TryStreamExt};
use protos::{
    control_server_server::ControlServer, db_server_client::DbServerClient, ServerRegisterRequest,
    ServerRegisterResponse,
};
use protos::{
    AppTables, BulkLoadRequest, DbServerMeta, DbStatus, InitServerRequest, ListServerStatusResponse,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response};
use tracing::info;

mod query;


pub struct ControlService {
    inner: Inner,
}

struct Inner {
    db_server_meta: RwLock<HashMap<ServerId, DbServerMeta>>,
    clients: Mutex<HashMap<ServerId, DbServerClient<Channel>>>,
    next_server_id: AtomicU64,
}

impl ControlService {
    pub fn new() -> Self {
        Self {
            inner: Inner {
                db_server_meta: RwLock::new(Default::default()),
                clients: Mutex::new(Default::default()),
                next_server_id: AtomicU64::new(0),
            },
        }
    }

    fn register(&self, req: ServerRegisterRequest) -> Result<ServerId> {
        let ServerRegisterRequest { uri } = req;
        let next_server_id = self.inner.next_server_id.fetch_add(1, Ordering::Relaxed);
        info!("Server {uri} register with id {next_server_id}");
        let mut guard = self.inner.db_server_meta.write().unwrap();
        let meta = DbServerMeta {
            shard: None,
            uri,
            status: DbStatus::Alive as _,
        };
        // the server id must not exist in meta maps
        assert!(matches!(guard.insert(next_server_id, meta), None));
        Ok(next_server_id)
    }

    fn list_server_status(&self) -> Result<ListServerStatusResponse> {
        let server_map = self.inner.db_server_meta.read().unwrap().clone();
        Ok(ListServerStatusResponse { server_map })
    }

    async fn create_client(uri: impl AsRef<str>) -> Result<DbServerClient<Channel>> {
        let ep = Channel::builder(uri.as_ref().parse::<Uri>()?);
        Ok(DbServerClient::connect(ep)
            .await
            .map_err(|e| RuntimeError::TonicConnectError { source: e })?)
    }

    async fn cluster_init(&self) -> Result<()> {
        let mut log_str: String = String::from("cluster init: ");
        // check the server status
        let target_servers = {
            let mut metas = self.inner.db_server_meta.write().unwrap();
            let mut alive_servers = metas
                .iter_mut()
                .filter(|(_, meta)| meta.status() == DbStatus::Alive)
                .collect::<Vec<_>>();
            if alive_servers.len() < 2 {
                return Err(RuntimeError::ServerNotAlive);
            }
            if alive_servers
                .iter()
                .any(|(_, status)| status.shard.is_some())
            {
                return Err(RuntimeError::Initialized);
            }
            // update the shard of the first two server
            for (shard, (_, meta)) in alive_servers.iter_mut().enumerate().take(2) {
                meta.shard = Some(shard as _);
                log_str += &format!("server {} with shard {:?}, ", meta.uri, meta.shard());
            }
            alive_servers
                .into_iter()
                .map(|(sid, meta)| (*sid, meta.clone()))
                // only takes the first two servers
                .take(2)
                .collect::<Vec<_>>()
        };
        // create the tonic clients and bulk load tables
        let futures = target_servers
            .iter()
            .map(|(_, meta)| async move {
                let mut client = Self::create_client(&meta.uri).await?;
                // SAFETY: shard must be assigned
                let shard = meta.shard.unwrap();
                client.init(InitServerRequest { shard }).await?;
                // bulk load the tables
                for table in [AppTables::User, AppTables::Article, AppTables::UserRead] {
                    let req = BulkLoadRequest {
                        table: table as i32,
                    };
                    client.bulk_load(req).await?;
                }
                Ok::<DbServerClient<Channel>, RuntimeError>(client)
            })
            .collect::<FuturesOrdered<_>>();
        let clients = futures.try_collect::<Vec<_>>().await?;
        self.inner.clients.lock().unwrap().extend(
            target_servers
                .into_iter()
                .zip(clients)
                .map(|((sid, _), c)| (sid, c)),
        );
        info!("{log_str}");
        Ok(())
    }
}

#[tonic::async_trait]
impl ControlServer for ControlService {
    async fn ping(&self, _: Request<()>) -> StatusResult<Response<()>> {
        info!("recv ping");
        Ok(Response::new(()))
    }

    async fn register(
        &self,
        req: Request<ServerRegisterRequest>,
    ) -> StatusResult<Response<ServerRegisterResponse>> {
        let server_id = self.register(req.into_inner())?;
        Ok(Response::new(ServerRegisterResponse { server_id }))
    }

    async fn list_server_status(
        &self,
        _: Request<()>,
    ) -> StatusResult<Response<ListServerStatusResponse>> {
        info!("recv list server status");
        let res = self.list_server_status()?;
        Ok(Response::new(res))
    }

    async fn cluster_init(&self, _: Request<()>) -> StatusResult<Response<()>> {
        self.cluster_init().await?;
        Ok(Response::new(()))
    }
}
