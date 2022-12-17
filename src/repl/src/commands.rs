use mysql::{prelude::Queryable, LocalInfileHandler};
use protos::{AppTables, BulkLoadRequest, InitServerRequest};
use std::{collections::HashSet, fs, io::Write};

use crate::Repl;

pub use async_trait::async_trait;
use common::TemporalGranularity;

/// The command api table.
pub const COMMAND_HANDLERS: [&'static dyn CommandHandler; 7] = [
    &ExitHandler,
    &HelpHandler,
    &ClusterInitHandler,
    &LoadUserReadHandler,
    &BulkLoadHandler,
    &LoadBeReadHandler,
    &LoadPopularTableHandler,
];

#[async_trait]
pub trait CommandHandler {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    async fn exec(
        &self,
        repl: &mut Repl,
        args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

/// Exit the REPL.
pub struct ExitHandler;

#[async_trait]
impl CommandHandler for ExitHandler {
    fn name(&self) -> &'static str {
        ":q"
    }

    fn description(&self) -> &'static str {
        "Quit this application."
    }

    async fn exec(
        &self,
        _repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        std::process::exit(0)
    }
}

/// Print the help message.
pub struct HelpHandler;

#[async_trait]
impl CommandHandler for HelpHandler {
    fn name(&self) -> &'static str {
        ":h"
    }

    fn description(&self) -> &'static str {
        "Show help message."
    }

    async fn exec(
        &self,
        _repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for handler in COMMAND_HANDLERS.iter() {
            println!("{} - {}", handler.name(), handler.description());
        }
        Ok(())
    }
}

/// Cluster Init
pub struct ClusterInitHandler;

#[async_trait]
impl CommandHandler for ClusterInitHandler {
    fn name(&self) -> &'static str {
        ":cluster-init"
    }

    fn description(&self) -> &'static str {
        "Init Database cluster."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        repl.control_client.cluster_init(()).await?;
        Ok(())
    }
}

/// LoadBeRead
pub struct LoadUserReadHandler;

#[async_trait]
impl CommandHandler for LoadUserReadHandler {
    fn name(&self) -> &'static str {
        ":load-user-read"
    }

    fn description(&self) -> &'static str {
        "generator and load the user read table."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        repl.control_client.generate_be_read_table(()).await?;
        let mut conn = repl.conn_pool.get_conn()?;

        let local_infile_handler = Some(LocalInfileHandler::new(|file_name, writer| {
            let file_name_str = String::from_utf8_lossy(file_name).to_string();
            let file_content = fs::read_to_string(file_name_str.as_str())?;
            writer.write_all(file_content.as_bytes())
        }));
        conn.set_local_infile_handler(local_infile_handler);
        conn.query_drop(
            "
        DROP TABLE IF EXISTS `user_read_test`;
            CREATE TABLE `user_read_test` (
            `timestamp` char(14) DEFAULT NULL,
            `id` char(7) DEFAULT NULL,
            `uid` char(5) DEFAULT NULL,
            `aid` char(7) DEFAULT NULL,
            `readTimeLength` char(3) DEFAULT NULL,
            `agreeOrNot` char(2) DEFAULT NULL,
            `commentOrNot` char(2) DEFAULT NULL,
            `shareOrNot` char(2) DEFAULT NULL,
            `commentDetail` char(100) DEFAULT NULL
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
        )?;

        conn.query_drop(
            "
            LOAD DATA LOCAL INFILE '/root/Course-DDBS/sql-data/user_read_shard1.sql' INTO TABLE user_read_test
            FIELDS TERMINATED BY '|'
            LINES TERMINATED BY '\\n' "
        )?;
        conn.query_drop(
            "
            LOAD DATA LOCAL INFILE '/root/Course-DDBS/sql-data/user_read_shard2.sql' INTO TABLE user_read_test
            FIELDS TERMINATED BY '|'
            LINES TERMINATED BY '\\n' "
        )?;

        Ok(())
    }
}

/// LoadBeRead
pub struct BulkLoadHandler;

#[async_trait]
impl CommandHandler for BulkLoadHandler {
    fn name(&self) -> &'static str {
        ":bulk-load"
    }

    fn description(&self) -> &'static str {
        "bulk load."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        repl.control_client.generate_be_read_table(()).await?;

        for (server_id, store_client) in repl.store_client.iter().enumerate() {
            // init
            let test_req = InitServerRequest {
                shard: server_id as _,
            };
            store_client.clone().init(test_req).await?;

            for table in [AppTables::User, AppTables::Article, AppTables::UserRead] {
                // test bulk_load
                let test_req = BulkLoadRequest { table: table as _ };
                store_client.clone().bulk_load(test_req).await?;
            }
        }

        Ok(())
    }
}

/// LoadBeRead
pub struct LoadBeReadHandler;

#[async_trait]
impl CommandHandler for LoadBeReadHandler {
    fn name(&self) -> &'static str {
        ":load-be-read"
    }

    fn description(&self) -> &'static str {
        "generator and load the be read table."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        repl.control_client.generate_be_read_table(()).await?;
        Ok(())
    }
}

/// LoadPopularTable
pub struct LoadPopularTableHandler;

#[async_trait]
impl CommandHandler for LoadPopularTableHandler {
    fn name(&self) -> &'static str {
        ":load-popular-table"
    }

    fn description(&self) -> &'static str {
        "generator and load the popular table."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        {
            let granularity = &TemporalGranularity::Daily;
            repl.control_client
                .generate_popular_table(*granularity as i32)
                .await?;

            let mut conn = repl.conn_pool.get_conn()?;
            let sql = format!(
        "CREATE TEMPORARY TABLE IF NOT EXISTS `popular_rank_test`
        WITH popular_rank_temp AS (
            SELECT aid, {} AS popularDate,
            count(uid) AS readNum
            FROM user_read_test GROUP BY popularDate, aid ORDER BY popularDate, readNum DESC
        )
        SELECT GROUP_CONCAT(aid) as aidList, popularDate FROM (select *, RANK() OVER myw AS n FROM popular_rank_temp
            WINDOW myw AS (PARTITION BY popularDate ORDER BY readNum DESC)) AS temp WHERE n<={} GROUP BY popularDate",
        granularity.to_column_sql("timestamp"),
        granularity.top_num());
            conn.query_drop(sql)?;
            println!("generate golden truth {granularity} popular_rank table succeed!");

            // compare our results with golden truth
            let rows: Vec<(String, String)> = conn.exec(
                format!(
                    r#"SELECT popularDate,articleAidList FROM popular_rank
                WHERE temporalGranularity="{granularity}""#
                ),
                (),
            )?;
            println!("start check popular_rank table of {granularity}...");
            for (date, list) in rows {
                let target_list: Option<String> = conn.exec_first(
                    format!(
                        r#"
            SELECT aidList FROM popular_rank_test WHERE popularDate="{date}""#
                    ),
                    (),
                )?;
                let my_aid = list
                    .split(',')
                    .map(|aid| aid.parse::<u64>().unwrap())
                    .collect::<HashSet<_>>();
                let target_aid = target_list
                    .unwrap()
                    .split(',')
                    .map(|aid| aid.parse::<u64>().unwrap())
                    .collect::<HashSet<_>>();
                assert!(my_aid.is_subset(&target_aid));
            }
            println!("check popular_rank table of {granularity} succeed!");
            conn.query_drop("DROP TABLE `popular_rank_test`")?;
        }
        repl.control_client.generate_be_read_table(()).await?;
        Ok(())
    }
}

/// The main function for handling commands.
pub async fn handle_commands(repl: &mut Repl, args: Vec<String>) {
    let cmd = args[0].clone();
    for handler in COMMAND_HANDLERS.iter() {
        if handler.name() == cmd {
            if let Err(err) = handler.exec(repl, args).await {
                eprintln!("{err:?}");
            }
            return;
        }
    }
    eprintln!("Unknown command {cmd}.");
}
