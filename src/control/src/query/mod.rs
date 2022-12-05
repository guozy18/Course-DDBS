mod optimizer;
mod util;
use std::collections::HashMap;
use util::join_result;

use crate::ControlService;
use common::{Result, ServerId, StatusResult};
use optimizer::Optimizer;
use protos::{DbServerMeta, DbStatus, ExecRequest};
use sqlparser::ast::JoinOperator;

fn rewrite_sql(
    statement: String,
    shards_info: HashMap<ServerId, DbServerMeta>,
) -> (Vec<(ServerId, String)>, Option<JoinOperator>) {
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

    let rewrite_sql = optimizer.rewrite();

    let join_operator = rewrite_sql.1.clone();
    (
        rewrite_sql
            .0
            .into_iter()
            .filter_map(|(server_id, server_sql)| {
                server_sql.map(|server_sql| (server_id, server_sql))
            })
            .collect::<Vec<_>>(),
        join_operator,
    )
}

impl ControlService {
    // query from client
    pub async fn exec(&self, req: ExecRequest) -> Result<String> {
        let ExecRequest { statement } = req;
        let shards = self.inner.db_server_meta.read().unwrap().clone();
        let (rewrite_sqls, join_operator) = rewrite_sql(statement, shards);

        let futs = rewrite_sqls
            .into_iter()
            .map(|(server_id, sql)| {
                let mut dbms_client = {
                    let db_clients = self.inner.clients.read().unwrap();
                    db_clients.get(&server_id).unwrap().clone()
                };
                async move {
                    let resp = dbms_client.exec_sql(sql).await?;
                    StatusResult::<_>::Ok(resp.into_inner())
                }
            })
            .collect::<Vec<_>>();

        let results = futures::future::join_all(futs).await;

        for result in results {
            result?;
            // compute final answer
        }
        // collect the result of the two query to get the final result.
        // execute join operate

        // let left = results.get(0).unwrap().unwrap().clone();
        // let right = results.get(1).unwrap().unwrap().clone();

        // let _final_result = join_result(left, right);

        Ok(String::new())
    }
}
