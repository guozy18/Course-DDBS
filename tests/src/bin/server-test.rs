use anyhow::Result as AnyResult;
use db_tests::DbClient;
use protos::AppTables;
use protos::{BulkLoadRequest, DbShard, InitServerRequest};
use tonic::transport::{Channel, Uri};

/// A higher-level test-client implementation.
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
    let mut store_client = DbClient::new(endpoint).await?;

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
