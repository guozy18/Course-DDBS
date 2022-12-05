use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common::{get_shards_info, join_shard_info, Profiler, ServerId, SymbolTable};
use protos::DbShard;

use sqlparser::ast::{
    BinaryOperator, Expr, Join, JoinOperator, OrderByExpr, Query, Select, SetExpr, Statement,
    TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::{Dialect, GenericDialect};

use sqlparser::parser::Parser;

#[derive(Debug)]
pub struct QueryContext {
    dialect: Box<dyn Dialect>,
    ast: Vec<Statement>,
    server_list: Vec<ServerId>,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self {
            dialect: Box::new(GenericDialect::default()),
            ast: vec![],
            server_list: vec![],
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

    fn set_server_list(&mut self, server_id: impl Iterator<Item = ServerId>) {
        self.server_list = server_id.collect();
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

    // 1. 分片，另一个分片直接取消
    fn reslove_selection(&self, selection: Expr, _recursive: bool) -> HashMap<i32, Expr> {
        fn return_expr_op(expr: &Expr) -> bool {
            match expr {
                Expr::BinaryOp { op, .. } => {
                    op.eq(&BinaryOperator::Eq) || op.eq(&BinaryOperator::NotEq)
                }
                _ => false,
            }
        }

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

        let mut res = HashMap::new();
        // 1. 对于每一层selection判断其是否有很多层: 只对于有region划分功能的expr才进行划分
        if return_expr_op(&selection) {
            // 此时是Eq或者NotEq
            if let Some(shard_id) = get_expr_shard(&selection) {
                // allocate to target shard, and this selection can be none.
                res.insert(
                    shard_id,
                    Expr::Value(Value::HexStringLiteral("placeholder".to_string())),
                );
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
                    let left_result = self.reslove_selection(*left, true);
                    let right_result = self.reslove_selection(*right, true);
                    // all shard id
                    for shard_id in [1, 2] {
                        let left_expr = left_result.get(&shard_id);
                        let right_expr = right_result.get(&shard_id);
                        if let (Some(left_expr), Some(right_expr)) = (left_expr, right_expr) {
                            let placeholder =
                                Expr::Value(Value::HexStringLiteral("placeholder".to_string()));
                            let new_expr =
                                match (left_expr.eq(&placeholder), right_expr.eq(&placeholder)) {
                                    (true, true) => Expr::Value(Value::HexStringLiteral(
                                        "placeholder".to_string(),
                                    )),
                                    (true, false) => right_expr.clone(),
                                    (false, true) => left_expr.clone(),
                                    (false, false) => Expr::BinaryOp {
                                        left: Box::new(left_expr.clone()),
                                        op: op.clone(),
                                        right: Box::new(right_expr.clone()),
                                    },
                                };
                            res.insert(shard_id, new_expr);
                        }
                    }
                    res
                }
                _ => {
                    for shard_id in [1, 2] {
                        let new_expr = selection.clone();
                        res.insert(shard_id, new_expr);
                    }
                    res
                }
            }
        }
    }

    fn rewrite_placeholder(&self, expr: Expr) -> Option<Expr> {
        let placeholder = Expr::Value(Value::HexStringLiteral("placeholder".to_string()));
        if expr.eq(&placeholder) {
            None
        } else {
            Some(expr)
        }
    }

    // this function is used to rewrite sql like "SELECT name, gender FROM User WHERE name = \"user10\""
    fn is_single_select(&self, query_body: SetExpr) -> HashMap<ServerId, Option<SetExpr>> {
        let mut final_query = HashMap::new();
        // 1. 全表扫描
        // 2. 条件查询
        match query_body.clone() {
            SetExpr::Select(select) => {
                // 从from中能够推断出当前查询要访问的目标表，以及其是否是join连接
                // projection 提取要返回的信息表
                // selection 代表的是表的筛选条件的信息
                let Select {
                    from,
                    // projection,
                    selection,
                    ..
                } = *select.clone();

                // 1. 提取出要访问的表的名字以及其别名，如果不需要rewrite则返回false
                let _x = from.into_iter().map(reslove_from);
                // 2. 根据上面表的别名判断selection里是否涉及到跟分片有关的属性，如果有的话直接进行分割
                if let Some(selection) = selection {
                    let res = self.reslove_selection(selection, true);
                    for server_id in self.server_list.clone() {
                        let selection = res.get(&(server_id as i32));
                        // need shard, for no shard, no need query
                        if let Some(selection) = selection {
                            // rewrite placeholder
                            let rewrite_selection = self.rewrite_placeholder(selection.clone());
                            // replace selection expr
                            let mut new_select = *select.clone();
                            new_select.selection = rewrite_selection;
                            let new_query_body = SetExpr::Select(Box::new(new_select));
                            final_query.insert(server_id, Some(new_query_body));
                        } else {
                            final_query.insert(server_id, None);
                        }
                    }
                } else {
                    // no shard partitioning
                    for server_id in self.server_list.clone() {
                        final_query.insert(server_id, Some(query_body.clone()));
                    }
                };
                final_query
            }
            _ => {
                for server_id in self.server_list.clone() {
                    final_query.insert(server_id, Some(query_body.clone()));
                }
                final_query
            }
        }
    }

    fn extract_join(&self, query_body: SetExpr) -> Option<JoinOperator> {
        match query_body {
            SetExpr::Select(select) => {
                // 从from中能够推断出当前查询要访问的目标表，以及其是否是join连接
                // projection 提取要返回的信息表
                // selection 代表的是表的筛选条件的信息
                let Select { from, .. } = *select;

                // 1. 提取出要访问的表的名字以及其别名，如果不需要rewrite则返回false
                let _x = from.into_iter().map(reslove_from);
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

fn reslove_table_factor(relation: TableFactor) -> Option<(String, Option<String>)> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.0[0].value.clone();
            let alias_name = alias.map(|x| x.name.value);
            Some((table_name, alias_name))
        }
        _ => None,
    }
}

fn reslove_from(from: TableWithJoins) -> (SymbolTable, Option<JoinOperator>) {
    // let mut table_alias = HashMap::new();
    let mut symbol_table = SymbolTable::default();

    let TableWithJoins { relation, joins } = from;

    // join from factor
    if let Some((table_name, Some(alias_name))) = reslove_table_factor(relation) {
        symbol_table.insert(table_name, alias_name);
    }

    // 1. not join
    if joins.is_empty() {
        (symbol_table, None)
    } else {
        let join_shard_info = join_shard_info();
        // 2. a join b: currently we only support a join b
        let Join {
            relation,
            join_operator,
        } = joins.get(0).unwrap().clone();

        if let Some((table_name, Some(alias_name))) = reslove_table_factor(relation) {
            symbol_table.insert(table_name, alias_name);
        }
        assert_eq!(symbol_table.len(), 2);
        let join_key = (
            symbol_table.get_index(0).unwrap(),
            symbol_table.get_index(1).unwrap(),
        );
        if let Some(shard_info) = join_shard_info.get(&join_key) {
            // currently we could get the divide join and the we can splitting the condition
        }

        // None
        (symbol_table, Some(join_operator))
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
        query_context.set_server_list(self.shards.iter().map(|(x, _)| *x));
        self.ctx = Arc::new(query_context);
    }

    // only rewrite sql::ast::query
    pub fn rewrite(&mut self) -> HashMap<ServerId, Option<String>> {
        let mut rewrite_sql = HashMap::new();
        if let Some(query) = self.ctx.is_query() {
            let _order_by = self.ctx.extract_order_by(&query);
            let _limit = self.ctx.extract_limit(&query);
            let shard_select = self.ctx.is_single_select(*query.clone().body);
            for (server_id, server_select) in shard_select {
                if let Some(server_select) = server_select {
                    let mut new_query = query.clone();
                    new_query.body = Box::new(server_select);
                    let new_sql_string = new_query.to_string();
                    rewrite_sql.insert(server_id, Some(new_sql_string));
                } else {
                    rewrite_sql.insert(server_id, None);
                }
            }
        } else {
            for (server_id, _) in self.shards.iter() {
                rewrite_sql.insert(*server_id, Some(self.query.clone()));
            }
        }
        self.profiler.rewrite_finished();
        rewrite_sql
    }
}

#[cfg(test)]
mod test_optimize {
    use super::DbShard;
    use super::{Optimizer, QueryContext};
    use sqlparser::parser::Parser;

    fn construct_optimzier_mock() -> Optimizer {
        let query =
            // "SELECT name, gender FROM User WHERE id < 100 AND region = \"Beijing\"".to_string();
            // "SELECT name, gender FROM User WHERE region = \"Beijing\" AND region = \"Beijing\"".to_string();
            "SELECT name, gender FROM User WHERE region = \"HongKong\" AND region = \"HongKong\"".to_string();
        // "SELECT name, gender FROM User WHERE region = \"HongKong\" AND region = \"Beijing\"".to_string();
        let shards = vec![(1, DbShard::One), (2, DbShard::Two)];
        Optimizer::new(query, shards.into_iter())
    }

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

        let shard_select = query_context.is_single_select(*query.body);
        for (server_id, server_select) in shard_select {
            println!(
                "Final, get rewrite select context \nserver_id: {:#?} server_select:\n{:#?}\n",
                server_id, server_select
            );
        }
    }

    #[test]
    fn test_optimizer() {
        let mut optimizer = construct_optimzier_mock();
        optimizer.parse();
        let result = optimizer.rewrite();
        for (shard_id, shard_sql) in result {
            println!(
                "Final, get rewrite select context \nserver_id: {:#?} server_select:\n{:#?}\n",
                shard_id, shard_sql
            );
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
