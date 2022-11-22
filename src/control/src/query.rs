use common::{Result, RuntimeError};
use protos::AppTables;
use sqlparser::ast::{Expr, Ident, Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::Parser;

/// Rewrite query and distribute the query.
/// Only support *single statement* for now.
pub struct QueryDistributor {
    dialect: Box<dyn Dialect>,
}

struct ParseOutput {
    sql: Statement,
    tables: Vec<AppTables>,
    selection: Option<Expr>,
}

impl QueryDistributor {
    pub fn new() -> Self {
        let dialect = Box::new(GenericDialect {});
        Self { dialect }
    }

    fn parse(&self, sql: &str) -> Result<ParseOutput> {
        let ast = Parser::parse_sql(self.dialect.as_ref(), sql.as_ref())?;
        assert_eq!(ast.len(), 1, "only support single statement");
        let statement = ast.into_iter().next().unwrap();
        let mut res = ParseOutput {
            sql: statement.clone(),
            tables: Vec::new(),
            selection: None,
        };

        match statement {
            // only support query
            Statement::Query(query) => {
                match *query.body {
                    // only support select
                    SetExpr::Select(mut select) => {
                        res.selection = select.selection;
                        // only support single from
                        if select.from.len() == 1 {
                            let table = &select.from[0];
                            match &table.relation {
                                TableFactor::Table { name, .. } => {
                                    for ident in name.0.iter() {
                                        let t = match ident.value.as_str() {
                                            "user" => AppTables::User,
                                            "article" => AppTables::Article,
                                            "user_read" => AppTables::UserRead,
                                            _ => {
                                                return Err(RuntimeError::UnsupportSql(format!(
                                                    "{}: {}",
                                                    sql.to_owned(),
                                                    "invalid table name"
                                                )))
                                            }
                                        };
                                        res.tables.push(t);
                                    }
                                    return Ok(res);
                                }
                                _ => {}
                            }
                        }
                        return Err(RuntimeError::UnsupportSql(format!(
                            "{}: {}",
                            sql.to_owned(),
                            "only support single from"
                        )));
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        Err(RuntimeError::UnsupportSql(sql.to_owned()))
    }

    /// query only using one table
    fn simple_query(&self, output: ParseOutput) {
        debug_assert_eq!(output.tables.len(), 1);
        // TODO
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
            "SELECT name, gender FROM User WHERE name = \"user10\"",
        )
        .unwrap();
        println!("{res:?}");

        let res = Parser::parse_sql(
            &dialect,
            "SELECT name, gender FROM User WHERE id < 100 AND region = \"Beijing\"",
        )
        .unwrap();
        println!("{res:?}");

        let res = Parser::parse_sql(
            &dialect,
            "SELECT a.title, b.readNum FROM Article AS a INNER JOIN BeRead AS b
            ON a.id = b.aid ORDER BY b.readNum DESC LIMIT 5",
        )
        .unwrap();
        println!("{res:?}");
    }
}
