mod optimizer;

use std::collections::HashMap;

use crate::ControlService;
use common::{Result, ServerId, StatusResult};
use optimizer::Optimizer;
use protos::{DbServerMeta, DbStatus, ExecRequest};

fn rewrite_sql(
    statement: String,
    shards_info: HashMap<ServerId, DbServerMeta>,
) -> Vec<(ServerId, String)> {
    let shards = shards_info
        .into_iter()
        .filter_map(|(server_id, server_meta)| {
            if server_meta.status() == DbStatus::Alive && server_meta.shard.is_some() {
                Some((server_id, server_meta.shard()))
            } else {
                None
            }
        });
    let mut optimizer = Optimizer::new(statement, shards);

    // 1. parser sql query and fill context
    optimizer.parse();

    optimizer.rewrite()
}

impl ControlService {
    // query from client
    pub async fn exec(&self, req: ExecRequest) -> Result<String> {
        let ExecRequest { statement } = req;
        let shards = self.inner.db_server_meta.read().unwrap().clone();
        let rewrite_sqls = rewrite_sql(statement, shards);

        let futs = rewrite_sqls
            .into_iter()
            .map(|(server_id, sql)| {
                let mut dbms_client = {
                    let db_clients = self.inner.clients.read().unwrap();
                    db_clients.get(&server_id).unwrap().clone()
                };
                async move {
                    let resp = dbms_client.exec_sql_drop(sql).await?;
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
