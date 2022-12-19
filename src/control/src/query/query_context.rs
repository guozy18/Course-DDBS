use std::collections::HashMap;
use std::vec;

use common::{DataShard, ServerId, SymbolTable};

use sqlparser::ast::{
    Expr, Ident, Join, JoinOperator, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::{Dialect, GenericDialect};

use super::{
    extract_selection, get_expr_shard, get_table_factor, get_table_info, get_wild_projection,
    reslove_from, return_expr_op, rewrite_placeholder,
};

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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_dialect_ref(&self) -> &dyn Dialect {
        self.dialect.as_ref()
    }

    pub fn set_ast(&mut self, ast: impl Iterator<Item = Statement>) {
        self.ast = ast.collect();
    }

    pub fn set_server_list(&mut self, server_id: impl Iterator<Item = ServerId>) {
        self.server_list = server_id.collect();
    }

    pub fn is_query(&self) -> Option<Query> {
        if self.ast.len() != 1 {
            return None;
        }
        let statement = self.ast.get(0).unwrap().clone();
        match statement {
            Statement::Query(query) => Some(*query),
            _ => None,
        }
    }

    pub fn is_insert(&self) -> Option<ServerId> {
        if self.ast.len() != 1 {
            return None;
        }
        let statement = self.ast.get(0).unwrap().clone();
        match statement {
            Statement::Insert {
                table_name, source, ..
            } => {
                let Query { body, .. } = *source;
                let body = *body;
                if let SetExpr::Values(values) = body {
                    if table_name
                        == ObjectName(vec![Ident {
                            value: "user".to_string(),
                            quote_style: None,
                        }])
                    {
                        if let Some(x) = values.0.get(0).unwrap().get(10) {
                            if x.eq(&Expr::Value(Value::SingleQuotedString(
                                "Beijing".to_string(),
                            ))) {
                                return Some(0);
                            } else {
                                return Some(1);
                            }
                        } else {
                            return None;
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    pub fn extract_order_by(&self, query: &Query) -> Vec<OrderByExpr> {
        query.order_by.clone()
    }

    pub fn extract_limit(&self, query: &Query) -> Option<Expr> {
        query.limit.clone()
    }

    pub fn get_header(&self, query_body: SetExpr) -> Vec<String> {
        fn reslove_table_name(tables: TableWithJoins) -> Vec<String> {
            let TableWithJoins { relation, joins } = tables;
            let mut table_names = vec![];
            if let TableFactor::Table { name, .. } = relation {
                let table_name = name.0[0].value.clone();
                table_names.push(table_name);
            }
            if joins.len() == 1 {
                let Join { relation, .. } = joins.get(0).unwrap().clone();
                if let TableFactor::Table { name, .. } = relation {
                    let table_name = name.0[0].value.clone();
                    table_names.push(table_name);
                }
            }
            table_names
        }

        let mut symbol_table = vec![];
        if let SetExpr::Select(select) = query_body {
            let Select {
                projection, from, ..
            } = *select;
            let mut table_names = vec![];
            for tables in from {
                let table_name = reslove_table_name(tables);
                let table_info = get_table_info();
                for name in table_name {
                    table_names.append(&mut table_info.get(&name).unwrap().clone());
                }
            }

            for projection_item in projection {
                match projection_item {
                    SelectItem::Wildcard => {
                        symbol_table.append(&mut table_names.clone());
                    }
                    SelectItem::UnnamedExpr(iden) => {
                        symbol_table.push(iden.to_string());
                    }
                    _ => {}
                }
            }
        }
        symbol_table
    }
}

impl QueryContext {
    /// Step1. Parse query_body: SetExpr
    ///
    /// Query {
    ///     query_body: Box<SetExpr>,
    ///     ..
    ///  }
    ///
    /// Return : Vec<HashMap<ServerId, Option<SetExpr>>>
    ///
    /// Vec: multiply query_body
    /// HashMap: <ServerId, new_query_body>
    pub fn extract_join(
        &self,
        query_body: SetExpr,
    ) -> (
        Vec<HashMap<ServerId, Option<SetExpr>>>,
        Option<JoinOperator>,
    ) {
        fn get_shard_set_expr(
            symbol_table: SymbolTable,
            selection: Option<Expr>,
            select: Select,
        ) -> (SetExpr, SetExpr) {
            let table_name1 = symbol_table.get_index(0).unwrap();
            let alias_name1 = symbol_table.get(&table_name1);
            let from1 = get_table_factor(table_name1.clone(), alias_name1.clone());

            let table_name2 = symbol_table.get_index(1).unwrap();
            let alias_name2 = symbol_table.get(&table_name2);
            let from2 = get_table_factor(table_name2.clone(), alias_name2.clone());

            let (selection1, selection2) = extract_selection(
                table_name1,
                alias_name1,
                table_name2,
                alias_name2,
                selection,
            );

            let mut new_select1 = select.clone();
            let new_projection1 = get_wild_projection();
            new_select1.projection = new_projection1;
            new_select1.selection = selection1;
            new_select1.from = from1;
            let new_query_body1 = SetExpr::Select(Box::new(new_select1));

            let mut new_select2 = select;
            let new_projection2 = get_wild_projection();
            new_select2.projection = new_projection2;
            new_select2.selection = selection2;
            new_select2.from = from2;
            let new_query_body2 = SetExpr::Select(Box::new(new_select2));
            (new_query_body1, new_query_body2)
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

                if from.len() == 1 {
                    // 1. 提取出要访问的表的名字以及其别名，如果不需要rewrite则返回false
                    if let Some(iter) = from.into_iter().next() {
                        let (symbol_table, join_info) = reslove_from(iter);
                        // println!("debug: {join_info:#?}");
                        match join_info {
                            // no need to shard, continue to execute
                            Some((DataShard::NotShard, _)) => {
                                final_query = self.rewrite_selection(query_body);
                                return (vec![final_query], None);
                            }
                            // rewrite sql and compute in control layer
                            Some((DataShard::Shard, join_info)) => {
                                // user join article
                                let mut final_query1 = HashMap::new();
                                let mut final_query2 = HashMap::new();

                                let (new_query_body1, new_query_body2) =
                                    get_shard_set_expr(symbol_table, selection, *select);

                                for server_id in self.server_list.clone() {
                                    // println!("debug server id: {server_id:#?}");
                                    final_query1.insert(server_id, Some(new_query_body1.clone()));
                                    final_query2.insert(server_id, Some(new_query_body2.clone()));
                                }
                                return (vec![final_query1, final_query2], Some(join_info));
                            }
                            // only to need query in shard2, no need rewrite
                            Some((DataShard::OnlyTwo, _)) => {
                                final_query.insert(0, None);
                                final_query.insert(1, Some(query_body));
                                return (vec![final_query], None);
                            }
                            _ => final_query = self.rewrite_selection(query_body),
                        }
                    }
                } else {
                    final_query = self.rewrite_selection(query_body);
                }
            }
            _ => final_query = self.rewrite_selection(query_body),
        }
        (vec![final_query], None)
    }

    // this function is used to rewrite sql like "SELECT name, gender FROM User WHERE name = \"user10\""
    /// Step 2. rewrite the seletion
    pub fn rewrite_selection(&self, query_body: SetExpr) -> HashMap<ServerId, Option<SetExpr>> {
        let mut final_query = HashMap::new();
        match query_body.clone() {
            SetExpr::Select(select) => {
                // from       当前查询要访问的目标表，以及其是否是join连接
                // projection 提取要返回的信息表
                // selection  代表的是表的筛选条件的信息
                let Select { selection, .. } = *select.clone();

                // 1. 提取出要访问的表的名字以及其别名，如果不需要rewrite则返回false
                // 2. 根据上面表的别名判断selection里是否涉及到跟分片有关的属性，如果有的话直接进行分割
                if let Some(selection) = selection {
                    let res = self.reslove_selection(selection);
                    for server_id in self.server_list.clone() {
                        let selection = res.get(&(server_id as i32));
                        // need shard, for no shard, no need query
                        if let Some(selection) = selection {
                            // rewrite placeholder
                            let rewrite_selection = rewrite_placeholder(selection.clone());
                            // replace selection expr
                            let mut new_select = *select.clone();
                            new_select.selection = rewrite_selection;
                            // println!("new_selection: {:#?}", new_select);
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

    // 1. 分片，另一个分片直接取消
    pub fn reslove_selection(&self, selection: Expr) -> HashMap<i32, Expr> {
        let mut res = HashMap::new();
        let shards_id = self.server_list.clone();
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
                for shard_id in shards_id {
                    res.insert(shard_id as _, selection.clone());
                }
            }
            res
        } else {
            // 此时有两层Expr，需要进行迭代判断
            match selection {
                Expr::BinaryOp { left, op, right } => {
                    let left_result = self.reslove_selection(*left);
                    let right_result = self.reslove_selection(*right);
                    // all shard id
                    for shard_id in shards_id {
                        let left_expr = left_result.get(&(shard_id as _));
                        let right_expr = right_result.get(&(shard_id as _));
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
                            res.insert(shard_id as _, new_expr);
                        }
                    }
                    res
                }
                _ => {
                    for shard_id in shards_id {
                        let new_expr = selection.clone();
                        res.insert(shard_id as _, new_expr);
                    }
                    res
                }
            }
        }
    }
}
