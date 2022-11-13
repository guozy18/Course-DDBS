// use protos::control_client::ControlClient;

// A higher-level client implementation.
// #[derive(Debug, Clone)]
// pub struct DDbsClient {
//     client: ControlClient<Channel>,
// }

// impl DDbsClient {

// }

// use protos::db_server_client::DbServerClient;
// use protos::BulkLoadRequest;
// use tonic::transport::{Channel, Endpoint, Uri};

// /// A higher-level test-client implementation.
// #[derive(Debug, Clone)]
// struct TestClient {
//     client: DbServerClient<Channel>,
// }

// impl TestClient {
//     /// Create a new client
//     ///
//     /// # Arguments
//     /// * `endpoint`: The server endpoint.
//     pub async fn new(endpoint: Endpoint) -> Result<Self> {
//         let client = DbServerClient::connect(endpoint).await?;
//         Ok(TestClient { client })
//     }

//     /// Pings the server.
//     pub async fn ping(&mut self) -> Result<()> {
//         self.client.ping(()).await?;
//         Ok(())
//     }

//     pub async fn bulk_load(&mut self, req: BulkLoadRequest) -> Result<()> {
//         self.client.bulk_load(req).await?;
//         Ok(())
//     }
// }

// fn format_url(url: &str) -> String {
//     let scheme = &url[0..7];
//     if scheme != "http://" {
//         return format!("http://{}", url);
//     }
//     String::from(url)
// }
