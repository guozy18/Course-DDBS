use super::*;

pub type StatusResult<T> = core::result::Result<T, Status>;

#[derive(Debug, Clone)]
pub struct DbServer {
    connection_pool: Pool,
}

impl DbServer {
    pub fn new(user: String, password: String, sql_url: String, db_name: String) -> Self {
        let sql_url = format!("mysql://{}:{}@{}/{}", user, password, sql_url, db_name);
        let pool = Pool::new(sql_url.as_str()).unwrap();

        Self {
            connection_pool: pool,
        }
    }
}

#[tonic::async_trait]
impl Server for DbServer {
    /// Ping Server
    async fn ping(&self, _: Request<()>) -> StatusResult<Response<()>> {
        println!("receive ping");
        Ok(Response::new(()))
    }

    /// bulk load table
    async fn bulk_load(
        &self,
        req: Request<protos::BulkLoadRequest>,
    ) -> StatusResult<Response<protos::BulkLoadResponse>> {
        println!("start bulk load");
        let protos::BulkLoadRequest { data_path } = req.into_inner();
        let mut conn = self.connection_pool.get_conn().unwrap();
        let bulk_query = "source ".to_string() + data_path.as_str();
        println!("start query: {}", bulk_query);
        // let _res = conn.query_drop(bulk_query.as_str()).unwrap();
        conn.query_drop(
            r"CREATE TEMPORARY TABLE payment (
                customer_id int not null,
                amount int not null,
                account_name text
            )",
        )
        .unwrap();
        Ok(Response::new(protos::BulkLoadResponse { result: true }))
    }
}
