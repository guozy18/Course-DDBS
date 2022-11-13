use protos::db_server_client::DbServerClient;
use protos::BulkLoadRequest;
use runserver::Result;
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
    pub async fn new(endpoint: Endpoint) -> Result<Self> {
        let client = DbServerClient::connect(endpoint).await?;
        Ok(TestClient { client })
    }

    /// Pings the server.
    pub async fn ping(&mut self) -> Result<()> {
        self.client.ping(()).await?;
        Ok(())
    }

    pub async fn bulk_load(&mut self, req: BulkLoadRequest) -> Result<()> {
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
pub async fn main() -> Result<()> {
    let address = "http://127.0.0.1:27023";

    let endpoint = Channel::builder(format_url(address).parse::<Uri>().unwrap());
    let mut store_client = TestClient::new(endpoint).await?;

    let test_req = BulkLoadRequest {
        data_path: "/Users/oreki/Desktop/Rust/Course-DDBS/article.txt".to_string(),
    };

    // test ping
    store_client.ping().await?;

    // test bulk_load
    store_client.bulk_load(test_req).await?;

    Ok(())
}
