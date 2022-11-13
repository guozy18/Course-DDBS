use super::*;

use std::{fs, io::Write};

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

    /// bulk load table from local files.
    async fn bulk_load(
        &self,
        req: Request<protos::BulkLoadRequest>,
    ) -> StatusResult<Response<protos::BulkLoadResponse>> {
        println!("start bulk load");
        let protos::BulkLoadRequest { data_path } = req.into_inner();
        let mut conn = self.connection_pool.get_conn().unwrap();

        // Step 1: create table
        conn.query_drop(
            "DROP TABLE IF EXISTS `article`;
        ",
        )
        .unwrap();
        conn.query_drop(
            "CREATE TABLE `article` (
            `timestamp` char(14) DEFAULT NULL,
            `id` char(7) DEFAULT NULL,
            `aid` char(7) DEFAULT NULL,
            `title` char(15) DEFAULT NULL,
            `category` char(11) DEFAULT NULL,
            `abstract` char(30) DEFAULT NULL,
            `articleTags` char(14) DEFAULT NULL,
            `authors` char(13) DEFAULT NULL,
            `language` char(3) DEFAULT NULL,
            `text` char(31) DEFAULT NULL,
            `image` char(32) DEFAULT NULL,
            `video` char(32) DEFAULT NULL
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
        )
        .unwrap();
        // Step 2: set local infile handler
        let local_infile_handler = Some(LocalInfileHandler::new(|file_name, writer| {
            let file_name_str = String::from_utf8_lossy(file_name).to_string();
            let file_content = fs::read_to_string(file_name_str.as_str())?;
            writer.write_all(file_content.as_bytes())
        }));
        conn.set_local_infile_handler(local_infile_handler);
        // Step 3: load data
        let bulk_query = format!(
            "LOAD DATA LOCAL INFILE '{}' INTO TABLE article
            FIELDS TERMINATED BY '|'
            LINES TERMINATED BY '\\n' ",
            data_path
        );
        println!("start query: {}", bulk_query);
        conn.query_drop(bulk_query.as_str()).unwrap();

        Ok(Response::new(protos::BulkLoadResponse { result: true }))
    }
}
