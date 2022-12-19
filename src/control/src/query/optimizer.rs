use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common::{Profiler, ServerId};
use protos::DbShard;

use sqlparser::ast::{Expr, JoinOperator, OrderByExpr};

use sqlparser::parser::Parser;

use super::QueryContext;

#[derive(Default)]
pub struct Optimizer {
    query: String,
    ctx: Arc<QueryContext>,
    profiler: Profiler,
    shards: Vec<(ServerId, DbShard)>,
}

impl Optimizer {
    pub fn new(query: String, shards: impl Iterator<Item = (ServerId, DbShard)>) -> Self {
        Optimizer {
            query,
            ctx: Arc::new(QueryContext::default()),
            profiler: Profiler::default(),
            shards: shards.collect::<Vec<_>>(),
        }
    }

    pub fn parse(&mut self) {
        let mut query_context = QueryContext::new();
        let dialect = query_context.get_dialect_ref();
        let ast = Parser::parse_sql(dialect, &self.query).unwrap();
        self.profiler.parse_finished();
        query_context.set_ast(ast.into_iter());
        query_context.set_server_list(self.shards.iter().map(|(x, _)| *x));
        self.ctx = Arc::new(query_context);
    }

    // only rewrite sql::ast::query
    pub fn rewrite(&mut self) -> (Vec<HashMap<ServerId, Option<String>>>, Option<JoinOperator>) {
        let mut rewrite_sql = vec![];
        let join_operator = if let Some(query) = self.ctx.is_query() {
            // 1.
            let (vec_shard_req, join_operator) = self.ctx.extract_join(*query.clone().body);
            for shard_select in vec_shard_req {
                let mut shard_sql = HashMap::new();
                for (server_id, server_select) in shard_select {
                    if let Some(server_select) = server_select {
                        let mut new_query = query.clone();
                        new_query.body = Box::new(server_select);
                        let new_sql_string = new_query.to_string();
                        shard_sql.insert(server_id, Some(new_sql_string));
                    } else {
                        shard_sql.insert(server_id, None);
                    }
                }
                rewrite_sql.push(shard_sql);
            }

            join_operator
        } else {
            let mut shard_sql = HashMap::new();
            // not query, directly forward to all shards.
            for (server_id, _) in self.shards.iter() {
                shard_sql.insert(*server_id, Some(self.query.clone()));
            }
            rewrite_sql.push(shard_sql);
            None
        };
        self.profiler.rewrite_finished();
        (rewrite_sql, join_operator)
    }

    pub fn extract_order_by_and_limit(&self) -> Option<(Vec<OrderByExpr>, Option<Expr>)> {
        self.ctx.is_query().map(|query| {
            (
                self.ctx.extract_order_by(&query),
                self.ctx.extract_limit(&query),
            )
        })
    }

    pub fn extract_header(&self) -> Vec<String> {
        if let Some(query) = self.ctx.is_query() {
            self.ctx.get_header(*query.body)
        } else {
            vec![]
        }
    }
}

#[cfg(test)]
mod test_optimize {
    use super::DbShard;
    use super::{Optimizer, QueryContext};
    use sqlparser::parser::Parser;

    #[test]
    fn test_query_context() {
        let query = "SELECT name, gender FROM User WHERE id < 100 AND region = \"Beijing\"";
        let mut query_context = QueryContext::new();
        let dialect = query_context.get_dialect_ref();
        let ast = Parser::parse_sql(dialect, query).unwrap();
        query_context.set_ast(ast.into_iter());
        query_context.set_server_list([1, 2].into_iter());

        let query = query_context.is_query().unwrap();
        println!("First, get query context: \n{query:#?}\n");

        let order_by = query_context.extract_order_by(&query);
        println!("Second, get order by context: \n{order_by:#?}\n");

        let limit = query_context.extract_limit(&query);
        println!("Third, get limit context: \n{limit:#?}\n");

        let shard_select = query_context.rewrite_selection(*query.body);
        for (server_id, server_select) in shard_select {
            println!(
                "Final, get rewrite select context \nserver_id: {server_id:#?} server_select:\n{server_select:#?}\n"
            );
        }
    }

    #[test]
    fn test_rewrite_join() {
        // let query = "SELECT a.title, b.readNum FROM article AS a INNER JOIN be_read AS b
        //     ON a.id = b.aid
        //     ORDER BY b.readNum DESC
        //     LIMIT 5";
        // let query = "SELECT a.title, b.readNum FROM user AS a INNER JOIN user_read AS b
        //     ON a.uid = b.uid
        //     ORDER BY b.timestamp DESC
        //     LIMIT 5";
        let query = "SELECT a.title, b.readNum FROM user AS a INNER JOIN article AS b 
            ON a.uid = b.aid 
            where a.uid = 100
            ORDER BY b.timestamp DESC 
            LIMIT 5";
        let mut query_context = QueryContext::new();
        let dialect = query_context.get_dialect_ref();
        let ast = Parser::parse_sql(dialect, query).unwrap();
        query_context.set_ast(ast.into_iter());
        query_context.set_server_list([1, 2].into_iter());

        let query = query_context.is_query().unwrap();
        println!("First, get query context: \n{query:#?}\n");

        let order_by = query_context.extract_order_by(&query);
        println!("Second, get order by context: \n{order_by:#?}\n");

        let limit = query_context.extract_limit(&query);
        println!("Third, get limit context: \n{limit:#?}\n");

        let (shard_select, join_operator) = query_context.extract_join(*query.body);
        for iter in shard_select {
            for (server_id, server_select) in iter {
                println!(
                    "Final, get rewrite join \nserver_id: {server_id:#?} server_select:\n{server_select:#?}\n"
                );
            }
        }
        println!("get join operator: \n{join_operator:#?}\n");
        // println!("get symbol table: \n{symbol_table:#?}\n");
    }

    fn construct_optimzier_mock(query: &str) -> Optimizer {
        let query = query.to_string();
        // "SELECT name, gender FROM user WHERE id < 100 AND region = \"Beijing\"".to_string();
        // "SELECT name, gender FROM user WHERE region = \"Beijing\" AND region = \"Beijing\"".to_string();
        // "SELECT name, gender FROM user WHERE region = \"HongKong\" AND region = \"HongKong\"".to_string();
        // "SELECT name, gender FROM user WHERE region = \"HongKong\" AND region = \"Beijing\"".to_string();
        let shards = vec![(0, DbShard::One), (1, DbShard::Two)];
        Optimizer::new(query, shards.into_iter())
    }

    #[test]
    fn test_optimizer() {
        let test_sqls = [
            "SELECT * FROM user AS a INNER JOIN article AS b ON a.uid = b.aid
                where a.uid = 100
                ORDER BY b.timestamp DESC
                LIMIT 5",
            "SELECT name, gender FROM user WHERE id = 100 AND region = \"Beijing\"",
            "SELECT * FROM user AS a INNER JOIN user_read AS b ON a.uid = b.uid
                where a.region = \"Beijing\"
                LIMIT 5",
            // "SELECT a.title, b.readNum FROM user AS a INNER JOIN article AS b ON a.uid = b.aid
            //     where a.uid = 100
            //     ORDER BY b.timestamp DESC
            //     LIMIT 5",
            // "SELECT name, gender FROM user WHERE region = \"Beijing\"",
            // "SELECT name, gender FROM user WHERE region = \"HongKong\" AND region = \"HongKong\"",
            // "SELECT name, gender FROM user WHERE id < 100 AND region = \"Beijing\"",
            "SELECT name, gender FROM user WHERE region = \"HongKong\" AND region = \"Beijing\"",
            "SELECT name, gender FROM user limit 5",
        ];
        for test_sql in test_sqls {
            println!("Origin sql: \n{test_sql:#}\n");
            let mut optimizer = construct_optimzier_mock(test_sql);
            optimizer.parse();
            let result = optimizer.rewrite();
            // println!("Result: get rewrite join operatpr \n: {:#?} \n", &result.1);
            for (number, iter) in result.0.into_iter().enumerate() {
                for (shard_id, shard_sql) in iter {
                    println!(
                                "Result: Iter: iter number {number}, get rewrite select context, server_id: {shard_id:#?} shard_sql:\n{shard_sql:#?}\n"
                            );
                }
            }
        }
    }

    #[test]
    fn test_get_header() {
        let test_sqls = [
            "SELECT * FROM user AS a INNER JOIN article AS b ON a.uid = b.aid
                where a.uid = 100
                ORDER BY b.timestamp DESC
                LIMIT 5",
            "SELECT * FROM user AS a INNER JOIN user_read AS b ON a.uid = b.uid
                where a.region = \"Beijing\"
                ORDER BY b.timestamp DESC
                LIMIT 5",
            "SELECT a.title, b.readNum FROM user AS a INNER JOIN article AS b ON a.uid = b.aid
                where a.uid = 100
                ORDER BY b.timestamp DESC
                LIMIT 5",
            "SELECT name, gender FROM user WHERE region = \"Beijing\"",
            "SELECT name, gender FROM user WHERE region = \"HongKong\" AND region = \"HongKong\"",
            "SELECT name, gender FROM user WHERE id < 100 AND region = \"Beijing\"",
            "SELECT name, gender FROM user WHERE region = \"HongKong\" AND region = \"Beijing\"",
            "SELECT name, gender FROM user limit 5",
            "SELECT name FROM user limit 5",
            "SELECT * FROM user,article limit 5",
        ];
        for test_sql in test_sqls {
            println!("Origin sql: \n{test_sql:#}\n");
            let mut optimizer = construct_optimzier_mock(test_sql);
            optimizer.parse();
            let header = optimizer.extract_header();
            println!("Result header: {header:#?}\n");
        }
    }
}

#[cfg(test)]
mod test {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_sql_parser() {
        let dialect = GenericDialect {};

        let res = Parser::parse_sql(
            &dialect,
            "SELECT a.name, a.gender FROM User As a WHERE a.name = \"user10\"",
        )
        .unwrap();
        println!("{res:#?}");

        let res = Parser::parse_sql(
            &dialect,
            "SELECT name, gender FROM User WHERE id < 100 AND region = \"Beijing\"",
        )
        .unwrap();
        println!("{res:#?}");

        let res = Parser::parse_sql(
            &dialect,
            "SELECT a.title, b.readNum FROM article AS a INNER JOIN be_read AS b
            ON a.id = b.aid ORDER BY b.readNum DESC LIMIT 5",
        )
        .unwrap();
        println!("{res:#?}");
    }

    #[test]
    fn test_select() {
        let dialect = GenericDialect {};

        let res = Parser::parse_sql(&dialect, "SELECT * FROM User, Article").unwrap();
        println!("{res:#?}");

        let res = Parser::parse_sql(&dialect, "SELECT name, gender FROM User, Article").unwrap();
        println!("{res:#?}");

        let x = res[0].clone();
        let sql = x.to_string();
        println!("{sql}");
    }
}
