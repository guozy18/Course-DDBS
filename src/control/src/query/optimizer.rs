use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common::{Profiler, ServerId};
use protos::DbShard;

use sqlparser::ast::{
    Expr, OrderByExpr, Query, Select, SetExpr, Statement, TableFactor, TableWithJoins,
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
                        // Some(table_alias)
                    }
                    _ => None,
                }
            } else {
                // 2. a join b
                unimplemented!()
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
