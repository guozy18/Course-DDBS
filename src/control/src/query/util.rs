// use sqlparser::ast::{Expr, JoinOperator, OrderByExpr};

use std::collections::HashMap;

use common::{
    get_join_condition, get_shards_info, join_shard_info, DataShard, MyRow, Result, SymbolTable,
};
use flexbuffers::Reader;
use serde::Deserialize;
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Join, JoinOperator, ObjectName, OrderByExpr, SelectItem,
    TableAlias, TableFactor, TableWithJoins, Value,
};

pub fn reslove_table_factor(relation: TableFactor) -> Option<(String, Option<String>)> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.0[0].value.clone();
            let alias_name = alias.map(|x| x.name.value);
            Some((table_name, alias_name))
        }
        _ => None,
    }
}

pub fn get_table_info() -> HashMap<String, Vec<String>> {
    let mut table_info = HashMap::new();
    let user_prop = vec![
        "timestamp".to_string(),
        "id".to_string(),
        "uid".to_string(),
        "name".to_string(),
        "gender".to_string(),
        "email".to_string(),
        "phone".to_string(),
        "dept".to_string(),
        "grade".to_string(),
        "language".to_string(),
        "region".to_string(),
        "role".to_string(),
        "preferTags".to_string(),
        "obtainedCredits".to_string(),
    ];
    table_info.insert("user".to_string(), user_prop);

    let article_prop = vec![
        "timestamp".to_string(),
        "id".to_string(),
        "aid".to_string(),
        "title".to_string(),
        "category".to_string(),
        "abstract".to_string(),
        "articleTags".to_string(),
        "authors".to_string(),
        "language".to_string(),
        "text".to_string(),
        "image".to_string(),
        "video".to_string(),
    ];
    table_info.insert("article".to_string(), article_prop);

    let read_prop = vec![
        "timestamp".to_string(),
        "id".to_string(),
        "uid".to_string(),
        "aid".to_string(),
        "readTimeLength".to_string(),
        "aggreeOrNot".to_string(),
        "commentOrNot".to_string(),
        "shareOrNot".to_string(),
        "commentDetail".to_string(),
    ];
    table_info.insert("user_read".to_string(), read_prop);

    let be_read_prop = vec![
        "id".to_string(),
        "aid".to_string(),
        "readNum".to_string(),
        "readUidList".to_string(),
        "commentNum".to_string(),
        "commentUidList".to_string(),
        "agreeNum".to_string(),
        "agreeUidList".to_string(),
        "shareNum".to_string(),
        "shareUidList".to_string(),
    ];
    table_info.insert("be_read".to_string(), be_read_prop);

    let popular_rank_prop = vec![
        "id".to_string(),
        "popularDate".to_string(),
        "temporalGranularity".to_string(),
        "articleAidList".to_string(),
    ];
    table_info.insert("popular_rank".to_string(), popular_rank_prop);

    table_info
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
pub fn reslove_from(from: TableWithJoins) -> (SymbolTable, Option<(DataShard, JoinOperator)>) {
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
        // println!("symbol table: {:?}", symbol_table);
        let ret = if let Some(shard_info) = join_shard_info.get(&join_key) {
            (symbol_table, Some((shard_info.clone(), join_operator)))
        } else {
            (symbol_table, None)
        };
        // println!("ret: {:?}",ret);
        ret
    }
}

pub fn extract_selection(
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

pub fn get_table_factor(table_name: String, alias_name: Option<String>) -> Vec<TableWithJoins> {
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

pub fn return_expr_op(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { op, .. } => op.eq(&BinaryOperator::Eq) || op.eq(&BinaryOperator::NotEq),
        _ => false,
    }
}

pub fn get_expr_shard(my_expr: &Expr) -> Option<i32> {
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

pub fn rewrite_placeholder(expr: Expr) -> Option<Expr> {
    let placeholder = Expr::Value(Value::HexStringLiteral("placeholder".to_string()));
    if expr.eq(&placeholder) {
        None
    } else {
        Some(expr)
    }
}

pub fn get_wild_projection() -> Vec<SelectItem> {
    vec![SelectItem::Wildcard]
}

pub fn do_join(
    left: Vec<u8>,
    right: Vec<u8>,
    join_operator: Option<JoinOperator>,
) -> Result<Vec<MyRow>> {
    let mut final_ans = vec![];
    if let Some(_join_condition) = join_operator {
        if !left.is_empty() && !right.is_empty() {
            let left = Reader::get_root(left.as_slice()).unwrap();
            let left_rows = Vec::<MyRow>::deserialize(left)?;
            let right = Reader::get_root(right.as_slice()).unwrap();
            let right_rows = Vec::<MyRow>::deserialize(right)?;
            for mut left_row in left_rows {
                for right_row in right_rows.clone() {
                    left_row.append(&mut right_row.clone());
                }
                final_ans.push(left_row);
            }
        } else {
            final_ans = vec![];
        }
    } else {
        if !left.is_empty() {
            let left = Reader::get_root(left.as_slice()).unwrap();
            let mut left_rows = Vec::<MyRow>::deserialize(left)?;
            final_ans.append(&mut left_rows);
        }
        if !right.is_empty() {
            let right = Reader::get_root(right.as_slice()).unwrap();
            let mut right_rows = Vec::<MyRow>::deserialize(right)?;
            final_ans.append(&mut right_rows);
        }
    }
    Ok(final_ans)
}

pub fn do_order_by_and_limit(
    results: Vec<Vec<mysql::Value>>,
    order_by_and_limit: Option<(Vec<OrderByExpr>, Option<Expr>)>,
) -> Vec<Vec<mysql::Value>> {
    if let Some((_order_by, Some(limit))) = order_by_and_limit {
        let return_number = match limit {
            Expr::Value(Value::Number(number, _)) => number.parse::<usize>().unwrap(),
            _ => results.len(),
        };
        let return_number = std::cmp::min(return_number, results.len());
        results[0..return_number].to_vec()
    } else {
        results
    }
}
