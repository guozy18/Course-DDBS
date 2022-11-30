use anyhow::Result as AnyResult;
use common::TemporalGranularity;
use protos::control_server_client::ControlServerClient;
use protos::db_server_client::DbServerClient;
use protos::ListServerStatusResponse;
use protos::{BulkLoadRequest, InitServerRequest};
use tonic::transport::{Channel, Endpoint};

/// A higher-level test-client implementation.
#[derive(Debug, Clone)]
pub struct ControlClient {
    client: ControlServerClient<Channel>,
}

impl ControlClient {
    /// Create a new client
    ///
    /// # Arguments
    /// * `endpoint`: The server endpoint.
    pub async fn new(endpoint: Endpoint) -> AnyResult<Self> {
        let client = ControlServerClient::connect(endpoint).await?;
        Ok(Self { client })
    }

    /// Pings the server.
    pub async fn ping(&mut self) -> AnyResult<()> {
        self.client.ping(()).await?;
        Ok(())
    }

    pub async fn cluster_init(&mut self) -> AnyResult<()> {
        self.client.cluster_init(()).await?;
        Ok(())
    }

    pub async fn list_server_status(&mut self) -> AnyResult<ListServerStatusResponse> {
        Ok(self.client.list_server_status(()).await?.into_inner())
    }

    pub async fn generate_be_read_table(&mut self) -> AnyResult<()> {
        self.client.generate_be_read_table(()).await?;
        Ok(())
    }

    pub async fn generate_popular_table(
        &mut self,
        granularity: TemporalGranularity,
    ) -> AnyResult<()> {
        self.client
            .generate_popular_table(granularity as i32)
            .await?;
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DbClient {
    client: DbServerClient<Channel>,
}

impl DbClient {
    /// Create a new client
    ///
    /// # Arguments
    /// * `endpoint`: The server endpoint.
    pub async fn new(endpoint: Endpoint) -> AnyResult<Self> {
        let client = DbServerClient::connect(endpoint).await?;
        Ok(Self { client })
    }

    /// Pings the server.
    pub async fn ping(&mut self) -> AnyResult<()> {
        self.client.ping(()).await?;
        Ok(())
    }

    /// Pings the server.
    pub async fn init(&mut self, req: InitServerRequest) -> AnyResult<()> {
        self.client.init(req).await?;
        Ok(())
    }

    pub async fn bulk_load(&mut self, req: BulkLoadRequest) -> AnyResult<()> {
        self.client.bulk_load(req).await?;
        Ok(())
    }
}
