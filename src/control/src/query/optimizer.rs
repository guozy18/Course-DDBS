use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common::{get_shards_info, Profiler, ServerId};
use protos::DbShard;

use sqlparser::ast::{
    BinaryOperator, Expr, OrderByExpr, Query, Select, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::{Dialect, GenericDialect};

use sqlparser::parser::Parser;

#[derive(Debug)]
pub struct QueryContext {
    dialect: Box<dyn Dialect>,
    ast: Vec<Statement>,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self {
            dialect: Box::new(GenericDialect::default()),
            ast: vec![],
        }
    }
}

impl QueryContext {
    fn new() -> Self {
        Self::default()
    }

    fn get_dialect_ref(&self) -> &dyn Dialect {
        self.dialect.as_ref()
    }

    fn set_ast(&mut self, ast: impl Iterator<Item = Statement>) {
        self.ast = ast.collect();
    }

    fn is_query(&self) -> Option<Query> {
        if self.ast.len() != 1 {
            return None;
        }
        let statement = self.ast.get(0).unwrap().clone();
        match statement {
            Statement::Query(query) => Some(*query),
            _ => None,
        }
    }

    // this function is used to rewrite sql like "SELECT name, gender FROM User WHERE name = \"user10\""
    fn is_single_select(&self, query_body: SetExpr) -> Option<String> {
        fn reslove_from(from: TableWithJoins) -> Option<(String, Option<String>)> {
            // let mut table_alias = HashMap::new();

            let TableWithJoins { relation, joins } = from;
            // 1. not join
            if joins.is_empty() {
                match relation {
                    TableFactor::Table { name, alias, .. } => {
                        let table_name = name.0[0].value.clone();
                        let alias_name = alias.map(|x| x.name.value);
                        Some((table_name, alias_name))
                    }
                    _ => None,
                }
            } else {
                // 2. a join b
                unimplemented!()
            }
        }

        fn return_expr_op(expr: &Expr) -> bool {
            match expr {
                Expr::BinaryOp { op, .. } => {
                    op.eq(&BinaryOperator::Eq) || op.eq(&BinaryOperator::NotEq)
                }
                _ => false,
            }
        }

        // 1. 分片，另一个分片直接取消
        fn reslove_selection(selection: Expr, _recursive: bool) -> HashMap<i32, Expr> {
            fn get_expr_shard(my_expr: &Expr) -> Option<i32> {
                let shards_info = get_shards_info();
                for (shard_id, shard_expr) in shards_info {
                    for expr in shard_expr {
                        if my_expr.eq(&expr) {
                            // 可以分片，则
                            return Some(shard_id);
                        }
                    }
                }
                None
            }

            // let shards_info = get_shards_info();
            let mut res = HashMap::new();
            // 1. 对于每一层selection判断其是否有很多层: 只对于有region划分功能的expr才进行划分
            if return_expr_op(&selection) {
                // 此时是Eq或者NotEq
                if let Some(shard_id) = get_expr_shard(&selection) {
                    res.insert(shard_id, selection);
                } else {
                    for shard_id in [1, 2] {
                        res.insert(shard_id, selection.clone());
                    }
                }
                res
            } else {
                // 此时有两层Expr，需要进行迭代判断

                match selection {
                    Expr::BinaryOp { left, op, right } => {
                        let left_result = reslove_selection(*left, true);
                        let right_result = reslove_selection(*right, true);
                        // all shard id
                        for shard_id in vec![1, 2] {
                            let left_expr = left_result.get(&shard_id);
                            let right_expr = right_result.get(&shard_id);
                            if let (Some(left_expr), Some(right_expr)) = (left_expr, right_expr) {
                                // this shard has result
                                let new_expr = Expr::BinaryOp {
                                    left: Box::new(left_expr.clone()),
                                    op: op.clone(),
                                    right: Box::new(right_expr.clone()),
                                };
                                res.insert(shard_id, new_expr);
                            }
                        }
                        res
                    }
                    _ => {
                        for shard_id in vec![1, 2] {
                            let new_expr = selection.clone();
                            res.insert(shard_id, new_expr);
                        }
                        res
                    }
                }
            }
        }

        // 1. 全表扫描
        // 2. 条件查询
        match query_body {
            SetExpr::Select(select) => {
                // 从from中能够推断出当前查询要访问的目标表，以及其是否是join连接
                // projection 提取要返回的信息表
                // selection 代表的是表的筛选条件的信息
                let Select {
                    from,
                    projection,
                    selection,
                    ..
                } = *select;

                // 1. 提取出要访问的表的名字以及其别名，如果不需要rewrite则返回false
                let x = from.into_iter().map(|x| reslove_from(x));
                // 2. 根据上面表的别名判断selection里是否涉及到跟分片有关的属性，如果有的话直接进行分割
                if let Some(selection) = selection {
                    let res = reslove_selection(selection, true);
                }
                None
            }
            _ => None,
        }
    }

    fn extract_order_by(&self, query: &Query) -> Vec<OrderByExpr> {
        query.order_by.clone()
    }

    fn extract_limit(&self, query: &Query) -> Option<Expr> {
        query.limit.clone()
    }
}

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
        self.ctx = Arc::new(query_context);
    }

    // only rewrite sql::ast::query
    pub fn rewrite(&mut self) -> Vec<(ServerId, String)> {
        let rewrite_sql = if let Some(query) = self.ctx.is_query() {
            let _order_by = self.ctx.extract_order_by(&query);
            let _limit = self.ctx.extract_limit(&query);
            let _select = self.ctx.is_single_select(*query.body);
            String::new()
        } else {
            self.query.clone()
        };
        self.profiler.rewrite_finished();
        vec![(0, rewrite_sql)]
    }
}

#[cfg(test)]
mod test {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::Optimizer;

    #[test]
    fn test_optimizer() {}

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
            "SELECT a.title, b.readNum FROM Article AS a INNER JOIN BeRead AS b
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

        let x = res[0].clone();
        let sql = x.to_string();
        println!("{sql}");
    }
}
