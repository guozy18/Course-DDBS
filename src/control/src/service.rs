use crate::DbClient;
use common::{ServerId, StatusResult, TemporalGranularity};
use protos::{
    control_server_server::ControlServer, ExecRequest, ExecResponse, GetArticleTextRequest,
    ServerRegisterRequest, ServerRegisterResponse,
};
use protos::{DbServerMeta, ListServerStatusResponse};
use std::collections::HashMap;
use std::sync::{atomic::AtomicU64, RwLock};
use tokio::fs::read_to_string;
use tonic::{Request, Response};
use tracing::info;

pub struct ControlService {
    pub inner: Inner,
}

pub struct Inner {
    pub db_server_meta: RwLock<HashMap<ServerId, DbServerMeta>>,
    pub clients: RwLock<HashMap<ServerId, DbClient>>,
    pub next_server_id: AtomicU64,
}

impl Default for ControlService {
    fn default() -> Self {
        ControlService::new()
    }
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

    // Query Related
    async fn exec(&self, req: Request<ExecRequest>) -> StatusResult<Response<ExecResponse>> {
        // let ExecRequest { statement } = req.into_inner();
        let result = self.exec(req.into_inner()).await?;
        Ok(Response::new(ExecResponse { result }))
    }

    async fn generate_popular_table(&self, req: Request<i32>) -> StatusResult<Response<()>> {
        let req = req.into_inner();
        let granularity = TemporalGranularity::try_from(req)?;
        info!("recv generate popular table: {}", granularity);
        self.generate_popular_table(granularity).await?;
        Ok(Response::new(()))
    }

    async fn get_article(
        &self,
        req: Request<GetArticleTextRequest>,
    ) -> StatusResult<Response<String>> {
        let aid = req.into_inner().aid;
        let path = format!("/root/Course-DDBS/sql-data/articles/article{aid}/text_a{aid}.txt");
        let text = read_to_string(path).await?;
        Ok(Response::new(text))
    }
}
