mod driver;

use crate::ControlService;
use common::{Result, ServerId, StatusResult};
use driver::Driver;
use protos::ExecRequest;

impl ControlService {
    // query from client
    pub async fn exec(&self, req: ExecRequest) -> Result<String> {
        fn rewrite_sql(statement: String) -> Vec<(ServerId, String)> {
            let mut driver = Driver::new_with_query(statement);

            // 1. parser sql query and fill context
            driver.parse();

            driver.rewrite()
        }

        let ExecRequest { statement } = req;
        let rewrite_sqls = rewrite_sql(statement);

        let futs = rewrite_sqls
            .into_iter()
            .map(|(server_id, sql)| {
                let mut dbms_client = {
                    let db_clients = self.inner.clients.read().unwrap();
                    db_clients.get(&server_id).unwrap().clone()
                };
                async move {
                    let resp = dbms_client.execute_sql(sql).await?;
                    StatusResult::<_>::Ok(resp.into_inner())
                }
            })
            .collect::<Vec<_>>();
        // drop(db_clients);

        let results = futures::future::join_all(futs).await;
        // for fut in futs {
        //     let x = fut.await;
        // }
        for result in results {
            result?;
            // compute final answer
        }

        Ok(String::new())
    }
}
