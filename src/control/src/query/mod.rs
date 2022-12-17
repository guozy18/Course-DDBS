mod optimizer;
mod query_context;
mod util;
use std::collections::HashMap;

use flexbuffers::Reader;
pub use query_context::QueryContext;
pub use util::*;

use crate::ControlService;
use common::{ExecuteResult, MyRow, Profile, Result, ResultSet, ServerId, StatusResult};
use mysql::Value;
use optimizer::Optimizer;
use protos::{DbServerMeta, DbShard, DbStatus, ExecRequest};
use serde::Deserialize;
use sqlparser::ast::{Expr, JoinOperator, OrderByExpr};

type RewriteSqls = Vec<Vec<(ServerId, String)>>;
type OrderByAndLimit = Option<(Vec<OrderByExpr>, Option<Expr>)>;

fn rewrite_sql(
    statement: String,
    shards_info: HashMap<ServerId, DbServerMeta>,
) -> (RewriteSqls, Option<JoinOperator>, OrderByAndLimit) {
    // let shards = shards_info
    //     .into_iter()
    //     .filter_map(|(server_id, server_meta)| {
    //         if server_meta.status() == DbStatus::Alive && server_meta.shard.is_some() {
    //             Some((server_id, server_meta.shard()))
    //         } else {
    //             None
    //         }
    //     });
    let shards = vec![(0u64, DbShard::One), (1u64, DbShard::Two)].into_iter();
    println!("debug: shard_info: {shards:#?}");
    let mut optimizer = Optimizer::new(statement, shards);

    // 1. parser sql query and fill context
    optimizer.parse();

    // 2. get the order by and limit information
    let order_by_and_limit = optimizer.extract_order_by_and_limit();

    let (rewrite_sql, join_operator) = optimizer.rewrite();
    println!("debug: rewrite sql{rewrite_sql:#?}");
    let mut final_sql = Vec::new();
    for single_rewrite_sql in rewrite_sql {
        final_sql.push(
            single_rewrite_sql
                .into_iter()
                .filter_map(|(server_id, server_sql)| {
                    server_sql.map(|server_sql| (server_id, server_sql))
                })
                .collect::<Vec<_>>(),
        );
    }

    (final_sql, join_operator, order_by_and_limit)
}

fn parse_row(row: &MyRow, _header: &[String]) -> Vec<Value> {
    row.get_raw_value().unwrap()
}

impl ControlService {
    // query from client
    pub async fn exec(&self, req: ExecRequest) -> Result<String> {
        // Step1. get the sql query string and get the shards information.
        let ExecRequest { statement } = req;
        let mut result_set = ResultSet::new();
        let shards = self.inner.db_server_meta.read().unwrap().clone();
        // Step2. Refactoring queries and getting distributed query sql.
        let (rewrite_sqls, join_operator, order_by_and_limit) = rewrite_sql(statement, shards);
        println!("Step1: rewrite sqls: {rewrite_sqls:#?}");
        result_set.set_header(vec!["name".to_string()]);
        // Step3. Execute rewrite sqls.
        let final_result = if rewrite_sqls.len() == 1 {
            let shard_sql = rewrite_sqls.get(0).unwrap().clone();
            let futs = shard_sql
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

            let mut final_results = Vec::<u8>::new();
            for (server_id, result) in results.into_iter().enumerate() {
                println!("debug: in {server_id:#?}, get result: {result:#?}");
                final_results.append(&mut result?);
            }

            final_results
        } else if rewrite_sqls.len() == 2 {
            // Query to get the data of the left branch of join
            let left_shard_sql = rewrite_sqls.get(0).unwrap().clone();
            let left_futs = left_shard_sql
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
            let left_results = futures::future::join_all(left_futs).await;

            let mut final_left_results = Vec::<u8>::new();
            for left_result in left_results {
                final_left_results.append(&mut left_result?);
            }

            // Query to get the data of the right branch of join
            let right_shard_sql = rewrite_sqls.get(1).unwrap().clone();
            let right_futs = right_shard_sql
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
            let right_results = futures::future::join_all(right_futs).await;

            let mut final_right_results = Vec::<u8>::new();
            for right_result in right_results {
                final_right_results.append(&mut right_result?);
            }

            // Actual execution of join operation
            do_join(final_left_results, final_right_results, join_operator)
        } else {
            unreachable!()
        };

        // Step 4. Filter the result by the order by and limit information.
        // if let Some((order_by, limit))  = order_by_and_limit {
        //     do_order_by_and limit
        // }
        // let final_result = do_order_by_and_limit(final_result, order_by_and_limit);

        println!("debug: tmp\n {final_result:?}");
        let s = Reader::get_root(final_result.as_slice()).unwrap();
        let rows = Vec::<MyRow>::deserialize(s)?;
        println!("debug: rows \n {rows:#?}");
        let header = &result_set.header;
        let vec_value = rows
            .iter()
            .map(|row| parse_row(row, header))
            .collect::<Vec<_>>();

        println!("debug: defore order_by and limit \n {final_result:#?}");
        let final_result = do_order_by_and_limit(vec_value, order_by_and_limit);
        println!("debug: result_set \n {final_result:#?}");
        result_set.table = final_result;
        println!("debug: after order_by and limit: result_set \n {result_set:#?}");

        // collect the result of the two query to get the final result.
        // execute join operate
        let final_result = serde_json::json!(ExecuteResult {
            result_set: Some(result_set),
            profile: Profile::default(),
        })
        .to_string();

        println!("debug: final result{final_result:#?}");

        Ok(final_result)
    }
}
