use common::{Result, RuntimeError, ServerId, StatusResult, TemporalGranularity};
use futures::stream::{FuturesOrdered, TryStreamExt};
use protos::{
    control_server_server::ControlServer, db_server_client::DbServerClient, ServerRegisterRequest,
    ServerRegisterResponse,
};
use protos::{
    AppTables, BulkLoadRequest, DbServerMeta, DbShard, DbStatus, InitServerRequest,
    ListServerStatusResponse,
};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use tokio::sync::Mutex as AsyncMutex;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response};
use tracing::info;

mod complex;
mod query;

type DbClient = DbServerClient<Channel>;
type SharedDbClient = Arc<AsyncMutex<DbClient>>;

pub struct ControlService {
    inner: Inner,
}

struct Inner {
    db_server_meta: RwLock<HashMap<ServerId, DbServerMeta>>,
    clients: RwLock<HashMap<ServerId, SharedDbClient>>,
    next_server_id: AtomicU64,
}

impl ControlService {
    pub fn new() -> Self {
        Self {
            inner: Inner {
                db_server_meta: RwLock::new(Default::default()),
                clients: RwLock::new(Default::default()),
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
            let metas = self.inner.db_server_meta.read().unwrap();
            let alive_servers = metas
                .iter()
                .filter(|(_, meta)| meta.status() == DbStatus::Alive)
                .take(2)
                .map(|(sid, meta)| (*sid, meta.clone()))
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
            alive_servers
        };
        debug_assert_eq!(target_servers.len(), 2);
        // create the tonic clients and bulk load tables
        let futures = target_servers
            .iter()
            .zip([DbShard::One, DbShard::Two])
            .map(|((sid, meta), shard)| async move {
                let mut client = Self::create_client(&meta.uri).await?;
                client.init(InitServerRequest { shard: shard as _ }).await?;
                // bulk load the tables
                for table in [AppTables::User, AppTables::Article, AppTables::UserRead] {
                    let req = BulkLoadRequest {
                        table: table as i32,
                    };
                    client.bulk_load(req).await?;
                }
                Ok::<(ServerId, SharedDbClient), RuntimeError>((*sid, Arc::new(AsyncMutex::new(client))))
            })
            .collect::<FuturesOrdered<_>>();
        let clients = futures.try_collect::<Vec<_>>().await?;

        // update inner state
        self.inner.clients.write().unwrap().extend(clients);
        let mut metas = self.inner.db_server_meta.write().unwrap();
        for ((id, m), shard) in target_servers.iter().zip([DbShard::One, DbShard::Two]) {
            metas.entry(*id).and_modify(|meta| meta.set_shard(shard));
            log_str += &format!("server {} with shard {:?}, ", m.uri, shard);
        }
        info!("{log_str}");
        Ok(())
    }

    /// check whether the cluster has been initialized
    /// Return the (server id of shard one, server id of shard two)
    fn check_init(&self) -> Result<(ServerId, ServerId)> {
        let metas = self.inner.db_server_meta.read().unwrap();
        let shard_one = metas
            .iter()
            .find(|(_, meta)| {
                meta.status() == DbStatus::Alive && meta.shard == Some(DbShard::One as _)
            })
            .ok_or(RuntimeError::Uninitialize)?;
        let shard_two = metas
            .iter()
            .find(|(_, meta)| {
                meta.status() == DbStatus::Alive && meta.shard == Some(DbShard::Two as _)
            })
            .ok_or(RuntimeError::Uninitialize)?;
        Ok((*shard_one.0, *shard_two.0))
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
        info!("recv list server status req");
        let res = self.list_server_status()?;
        Ok(Response::new(res))
    }

    async fn cluster_init(&self, _: Request<()>) -> StatusResult<Response<()>> {
        info!("recv cluster init req");
        self.cluster_init().await?;
        Ok(Response::new(()))
    }

    async fn generate_be_read_table(&self, _: Request<()>) -> StatusResult<Response<()>> {
        info!("recv generate be read table req");
        self.generate_be_read_table().await?;
        Ok(Response::new(()))
    }

    async fn generate_popular_table(&self, req: Request<i32>) -> StatusResult<Response<()>> {
        let req = req.into_inner();
        let granularity = TemporalGranularity::try_from(req)?;
        info!("recv generate popular table: {}", granularity);
        self.generate_popular_table(granularity).await?;
        Ok(Response::new(()))
    }
}
