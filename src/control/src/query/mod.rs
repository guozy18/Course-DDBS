mod driver;

use crate::ControlService;
use common::Result;
use protos::ExecRequest;
use sqlparser::ast::{Expr, Ident, Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::{self, Dialect, GenericDialect};
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

impl ControlService {
    // query from client
    pub async fn exec(&self, req: ExecRequest) -> Result<String> {
        let ExecRequest { statement } = req;

        // 1. parser sql query and fill context
        let mut query_context = QueryContext::new();
        let dialect = query_context.get_dialect_ref();
        let ast = Parser::parse_sql(dialect, &statement).unwrap();
        query_context.set_ast(ast.into_iter());

        // 2. 填充到一个QueryContext结构体中，给这个结构体提供一些方法

        // 3. 执行这些结构体
        Ok(statement)
    }
}
