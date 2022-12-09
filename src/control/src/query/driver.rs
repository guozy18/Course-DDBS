use std::sync::Arc;
use std::vec;

use common::{Profiler, ServerId};
// use std::sync::Arc;

use sqlparser::ast::Statement;
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
}

#[derive(Default)]
pub struct Driver {
    query: String,
    ctx: Arc<QueryContext>,
    profiler: Profiler,
}

impl Driver {
    pub fn new_with_query(query: String) -> Self {
        Driver {
            query,
            ctx: Arc::new(QueryContext::default()),
            profiler: Profiler::default(),
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

    pub fn rewrite(mut self) -> Vec<(ServerId, String)> {
        self.profiler.rewrite_finished();
        vec![]
    }
}
