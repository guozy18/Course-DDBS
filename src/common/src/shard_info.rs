// Database shards info

use sqlparser::ast::{BinaryOperator::Eq, Expr, Ident};
use std::{collections::HashMap, vec};

// Dbshare1: 0, Dbshard2: 0;
pub fn get_shards_info() -> HashMap<i32, Vec<Expr>> {
    let mut shards_info = HashMap::new();
    let mut db1_info = vec![];
    let mut db2_info = vec![];

    // basic shard information
    let region_expr = Box::new(Expr::Identifier(Ident {
        value: "region".to_string(),
        quote_style: None,
    }));
    // let category_expr = Box::new(Expr::Identifier(Ident {
    //     value: "category".to_string(),
    //     quote_style: None,
    // }));
    let beijing_expr = Box::new(Expr::Identifier(Ident {
        value: "Beijing".to_string(),
        quote_style: Some('"'),
    }));
    let hongkong_expr = Box::new(Expr::Identifier(Ident {
        value: "HongKong".to_string(),
        quote_style: Some('"'),
    }));
    // let science_expr = Box::new(Expr::Identifier(Ident {
    //     value: "HongKong".to_string(),
    //     quote_style: None,
    // }));
    // let _expr = Box::new(Expr::Identifier(Ident {
    //     value: "HongKong".to_string(),
    //     quote_style: None,
    // }));

    // DB1:
    let region_beijing = Expr::BinaryOp {
        left: region_expr.clone(),
        op: Eq,
        right: beijing_expr,
    };
    db1_info.push(region_beijing);
    // let category_science = Expr::BinaryOp {
    //     left: category_expr.clone(),
    //     op: Eq,
    //     right: ,
    // };

    // DB2:
    let region_hongkong = Expr::BinaryOp {
        left: region_expr,
        op: Eq,
        right: hongkong_expr,
    };
    db2_info.push(region_hongkong);

    shards_info.insert(1, db1_info);
    shards_info.insert(2, db2_info);
    shards_info
}
