use anyhow::Result as AnyResult;
use protos::{control_server_client::ControlServerClient};
use protos::{DbStatus, ListServerStatusResponse};
use std::time::Instant;
use tonic::transport::{Channel, Endpoint, Uri};

/// A higher-level test-client implementation.
#[derive(Debug, Clone)]
struct TestClient {
    client: ControlServerClient<Channel>,
}

impl TestClient {
    /// Create a new client
    ///
    /// # Arguments
    /// * `endpoint`: The server endpoint.
    pub async fn new(endpoint: Endpoint) -> AnyResult<Self> {
        let client = ControlServerClient::connect(endpoint).await?;
        Ok(TestClient { client })
    }

    /// Pings the server.
    pub async fn ping(&mut self) -> AnyResult<()> {
        self.client.ping(()).await?;
        Ok(())
    }

    /// Pings the server.
    pub async fn cluster_init(&mut self) -> AnyResult<()> {
        self.client.cluster_init(()).await?;
        Ok(())
    }

    pub async fn list_server_status(&mut self) -> AnyResult<ListServerStatusResponse> {
        Ok(self.client.list_server_status(()).await?.into_inner())
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
    let address = "http://control:27022";

    let ep = Channel::builder(format_url(address).parse::<Uri>().unwrap());
    let mut client = TestClient::new(ep).await?;

    // test ping
    client.ping().await?;

    // check the server
    let ListServerStatusResponse { server_map } = client.list_server_status().await?;
    assert!(server_map.len() == 2);
    for (_, meta) in server_map.iter() {
        assert_eq!(meta.status(), DbStatus::Alive);
        assert!(meta.shard.is_none());
    }

    // init the cluster
    let start = Instant::now();
    client.cluster_init().await?;
    let ListServerStatusResponse { server_map } = client.list_server_status().await?;
    assert!(server_map.len() == 2);
    for (sid, meta) in server_map.iter() {
        assert_eq!(meta.status(), DbStatus::Alive);
        assert!(meta.shard.is_some());
        println!(
            "server {sid} ({}) is responsible for shard {:?}",
            meta.uri,
            meta.shard()
        );
    }
    println!("init the cluster elapsed: {:?}", start.elapsed());
    Ok(())
}