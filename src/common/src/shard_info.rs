// Database shards info

use sqlparser::ast::{BinaryOperator::Eq, Expr, Ident};
use std::{collections::HashMap, vec};

#[derive(Debug, Clone)]
pub enum DataShard {
    Shard,
    OnlyTwo,
    NotShard,
}

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

pub fn get_join_condition() -> HashMap<String, Ident> {
    let mut join_condition = HashMap::new();

    let uid_expr = Ident {
        value: "uid".to_string(),
        quote_style: None,
    };

    let aid_expr = Ident {
        value: "aid".to_string(),
        quote_style: None,
    };

    join_condition.insert("user".to_string(), uid_expr.clone());
    join_condition.insert("read".to_string(), uid_expr);
    join_condition.insert("article".to_string(), aid_expr.clone());
    join_condition.insert("be_read".to_string(), aid_expr);

    join_condition
}

pub fn join_shard_info() -> HashMap<(String, String), DataShard> {
    let mut data_shard = HashMap::new();

    // user join user_read
    data_shard.insert(
        ("user".to_string(), "user_read".to_string()),
        DataShard::NotShard,
    );
    data_shard.insert(
        ("user_read".to_string(), "user".to_string()),
        DataShard::NotShard,
    );
    // user join article: need query on both shard and compute in control layer
    data_shard.insert(
        ("user".to_string(), "article".to_string()),
        DataShard::Shard,
    );
    data_shard.insert(
        ("article".to_string(), "user".to_string()),
        DataShard::Shard,
    );
    // be_read join article: need only in shard2
    data_shard.insert(
        ("article".to_string(), "be_read".to_string()),
        DataShard::OnlyTwo,
    );
    data_shard.insert(
        ("be_read".to_string(), "article".to_string()),
        DataShard::OnlyTwo,
    );

    data_shard
}
