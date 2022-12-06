use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common::{
    get_join_condition, get_shards_info, join_shard_info, DataShard, Profiler, ServerId,
    SymbolTable,
};
use protos::DbShard;

use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Join, JoinConstraint, JoinOperator, ObjectName, OrderByExpr,
    Query, Select, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value,
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
    fn rewrite_selection(&self, query_body: SetExpr) -> HashMap<ServerId, Option<SetExpr>> {
        let mut final_query = HashMap::new();
        match query_body.clone() {
            SetExpr::Select(select) => {
                // 从from中能够推断出当前查询要访问的目标表，以及其是否是join连接
                // projection 提取要返回的信息表
                // selection 代表的是表的筛选条件的信息
                let Select {
                    // from,
                    // projection,
                    selection,
                    ..
                } = *select.clone();

                // // 1. 提取出要访问的表的名字以及其别名，如果不需要rewrite则返回false
                // let _x = from.into_iter().map(reslove_from);
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

    fn extract_join(
        &self,
        query_body: SetExpr,
    ) -> (
        Vec<HashMap<ServerId, Option<SetExpr>>>,
        Option<JoinOperator>,
    ) {
        fn get_table_factor(table_name: String, alias_name: Option<String>) -> Vec<TableWithJoins> {
            let table_factor = TableFactor::Table {
                name: ObjectName(vec![Ident {
                    value: table_name,
                    quote_style: None,
                }]),
                alias: alias_name.map(|alias| TableAlias {
                    name: Ident {
                        value: alias,
                        quote_style: None,
                    },
                    columns: vec![],
                }),
                args: None,
                with_hints: vec![],
            };
            vec![TableWithJoins {
                relation: table_factor,
                joins: vec![],
            }]
        }

        fn extract_selection(
            left_name: String,
            left_alias_name: Option<String>,
            right_name: String,
            right_alias_name: Option<String>,
            selection: Option<Expr>,
        ) -> (Option<Expr>, Option<Expr>) {
            let join_condition = get_join_condition();
            let left_join = join_condition.get(&left_name).unwrap();
            let right_join = join_condition.get(&right_name).unwrap();

            let left_join = left_alias_name.map_or_else(
                || Expr::Identifier(left_join.clone()),
                |alias_name| {
                    Expr::CompoundIdentifier(vec![
                        Ident {
                            value: alias_name,
                            quote_style: None,
                        },
                        left_join.clone(),
                    ])
                },
            );
            let right_join = right_alias_name.map_or_else(
                || Expr::Identifier(right_join.clone()),
                |alias_name| {
                    Expr::CompoundIdentifier(vec![
                        Ident {
                            value: alias_name,
                            quote_style: None,
                        },
                        right_join.clone(),
                    ])
                },
            );

            if let Some(selection) = selection {
                match selection.clone() {
                    Expr::BinaryOp { left, op, right } => {
                        let left_expr = *left;
                        let right_expr = *right;

                        if left_expr.eq(&left_join) || left_expr.eq(&right_join) {
                            // the slection is relationed to join, we need to rewrite
                            let left_selection = Expr::BinaryOp {
                                left: Box::new(left_join),
                                op: op.clone(),
                                right: Box::new(right_expr.clone()),
                            };
                            let right_selection = Expr::BinaryOp {
                                left: Box::new(right_join),
                                op,
                                right: Box::new(right_expr),
                            };
                            (Some(left_selection), Some(right_selection))
                        } else if right_expr.eq(&left_join) || right_expr.eq(&right_join) {
                            let left_selection = Expr::BinaryOp {
                                left: Box::new(left_join),
                                op: op.clone(),
                                right: Box::new(left_expr.clone()),
                            };
                            let right_selection = Expr::BinaryOp {
                                left: Box::new(right_join),
                                op,
                                right: Box::new(left_expr),
                            };
                            (Some(left_selection), Some(right_selection))
                        } else {
                            (Some(selection.clone()), Some(selection))
                        }
                    }
                    _ => (Some(selection.clone()), Some(selection)),
                }
            } else {
                (None, None)
            }
        }

        let mut final_query = HashMap::new();

        // 1. need rewrite
        match query_body.clone() {
            SetExpr::Select(select) => {
                // 从from中能够推断出当前查询要访问的目标表，以及其是否是join连接
                // projection 提取要返回的信息表
                // selection 代表的是表的筛选条件的信息
                let Select {
                    from, selection, ..
                } = *select.clone();

                // 1. 提取出要访问的表的名字以及其别名，如果不需要rewrite则返回false
                for iter in from {
                    let (symbol_table, join_info) = reslove_from(iter);
                    println!("debug: {join_info:#?}");
                    match join_info {
                        // no need to shard, continue to execute
                        Some((DataShard::NotShard, _)) => {
                            let res = self.rewrite_selection(query_body);
                            return (vec![res], None);
                        }
                        // rewrite sql and compute in control layer
                        Some((DataShard::Shard, join_info)) => {
                            // user join article
                            let mut final_query1 = HashMap::new();
                            let mut final_query2 = HashMap::new();
                            let table_name1 = symbol_table.get_index(0).unwrap();
                            let alias_name1 = symbol_table.get(&table_name1);
                            let from1 = get_table_factor(table_name1.clone(), alias_name1.clone());

                            let table_name2 = symbol_table.get_index(1).unwrap();
                            let alias_name2 = symbol_table.get(&table_name2);
                            let from2 = get_table_factor(table_name2.clone(), alias_name2.clone());
                            println!("debug from: {from1:#?}, {from2:#?}");
                            println!("debug table name: {table_name1:#?}, {table_name2:#?}, {selection:#?}");
                            let (selection1, selection2) = extract_selection(
                                table_name1,
                                alias_name1,
                                table_name2,
                                alias_name2,
                                selection,
                            );
                            println!("debug selection: {selection1:#?}, {selection2:#?}");
                            let mut new_select1 = *select.clone();
                            new_select1.selection = selection1;
                            new_select1.from = from1;
                            let new_query_body1 = SetExpr::Select(Box::new(new_select1));

                            let mut new_select2 = *select;
                            new_select2.selection = selection2;
                            new_select2.from = from2;
                            let new_query_body2 = SetExpr::Select(Box::new(new_select2));
                            println!(
                                "debug query body: {new_query_body1:#?}, {new_query_body2:#?}"
                            );

                            for server_id in self.server_list.clone() {
                                println!("debug server id: {server_id:#?}");
                                final_query1.insert(server_id, Some(new_query_body1.clone()));
                                final_query2.insert(server_id, Some(new_query_body2.clone()));
                            }
                            return (vec![final_query1, final_query2], Some(join_info));
                        }
                        // only to need query in shard2, no need rewrite
                        Some((DataShard::Two, _)) => {
                            final_query.insert(1, None);
                            final_query.insert(2, Some(query_body));
                            return (vec![final_query], None);
                        }
                        _ => unimplemented!(),
                    }
                }
            }
            _ => unimplemented!(),
        }
        (vec![final_query], None)
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

/// Return value: SymbolTable, Option<DataShard, Joinoperator>
///
/// None - not join, don't need devide
///
/// Some(DataShard::Shard, JoinOperator) - rewrite sql and compute in control layer
///
/// Some(DataShard::NotShard, JoinOperator) - no need to shard, continue to execute
///
/// Some(DataShard::Two, JoinOperator) - only to need query in shard2, no need rewrite
fn reslove_from(from: TableWithJoins) -> (SymbolTable, Option<(DataShard, JoinOperator)>) {
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
        let ret = if let Some(shard_info) = join_shard_info.get(&join_key) {
            (symbol_table, Some((shard_info.clone(), join_operator)))
        } else {
            (symbol_table, None)
        };
        ret
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
    pub fn rewrite(&mut self) -> (HashMap<ServerId, Option<String>>, Option<JoinOperator>) {
        let mut rewrite_sql = HashMap::new();
        let join_operator = if let Some(query) = self.ctx.is_query() {
            let _order_by = self.ctx.extract_order_by(&query);
            let _limit = self.ctx.extract_limit(&query);
            // 1.
            let shard_select = self.ctx.rewrite_selection(*query.clone().body);
            let (vec_shard_req, join_operator) = self.ctx.extract_join(*query.clone().body);
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
            join_operator
        } else {
            // not query, directly forward to all shards.
            for (server_id, _) in self.shards.iter() {
                rewrite_sql.insert(*server_id, Some(self.query.clone()));
            }
            None
        };
        self.profiler.rewrite_finished();
        (rewrite_sql, join_operator)
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

        let shard_select = query_context.rewrite_selection(*query.body);
        for (server_id, server_select) in shard_select {
            println!(
                "Final, get rewrite select context \nserver_id: {:#?} server_select:\n{:#?}\n",
                server_id, server_select
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
                    "Final, get rewrite join \nserver_id: {:#?} server_select:\n{:#?}\n",
                    server_id, server_select
                );
            }
        }
        println!("get join operator: \n{join_operator:#?}\n");
    }

    #[test]
    fn test_optimizer() {
        let mut optimizer = construct_optimzier_mock();
        optimizer.parse();
        let result = optimizer.rewrite();
        println!("Final, get rewrite join operatpr \n: {:#?} \n", &result.1);
        for (shard_id, shard_sql) in result.0 {
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

        let x = res[0].clone();
        let sql = x.to_string();
        println!("{sql}");
    }
}
