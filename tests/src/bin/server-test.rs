use anyhow::Result as AnyResult;
use protos::{db_server_client::DbServerClient, AppTables};
use protos::{BulkLoadRequest, DbShard, InitServerRequest};
use tonic::transport::{Channel, Endpoint, Uri};

/// A higher-level test-client implementation.
#[derive(Debug, Clone)]
struct TestClient {
    client: DbServerClient<Channel>,
}

impl TestClient {
    /// Create a new client
    ///
    /// # Arguments
    /// * `endpoint`: The server endpoint.
    pub async fn new(endpoint: Endpoint) -> AnyResult<Self> {
        let client = DbServerClient::connect(endpoint).await?;
        Ok(TestClient { client })
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

fn format_url(url: &str) -> String {
    let scheme = &url[0..7];
    if scheme != "http://" {
        return format!("http://{}", url);
    }
    String::from(url)
}

#[tokio::main]
pub async fn main() -> AnyResult<()> {
    let address = "http://server1:27023";

    let endpoint = Channel::builder(format_url(address).parse::<Uri>().unwrap());
    let mut store_client = TestClient::new(endpoint).await?;

    // test ping
    store_client.ping().await?;

    // init
    let test_req = InitServerRequest {
        shard: DbShard::One as _,
    };
    store_client.init(test_req).await?;

    for table in [AppTables::User, AppTables::Article, AppTables::UserRead] {
        // test bulk_load
        let test_req = BulkLoadRequest { table: table as _ };
        store_client.bulk_load(test_req).await?;
    }

    Ok(())
}