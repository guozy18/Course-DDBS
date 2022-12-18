use anyhow::Result as AnyResult;
use common::TemporalGranularity;
use db_tests::ControlClient;
use mysql::{prelude::Queryable, LocalInfileHandler};
use protos::{DbStatus, ListServerStatusResponse};
use std::collections::HashSet;
use std::fs;
use std::{io::Write, time::Instant};
use tonic::transport::{Channel, Uri};

fn format_url(url: &str) -> String {
    let scheme = &url[0..7];
    if scheme != "http://" {
        return format!("http://{url}");
    }
    String::from(url)
}

#[tokio::main]
pub async fn main() -> AnyResult<()> {
    let address = "http://control:27022";
    let ep = Channel::builder(format_url(address).parse::<Uri>().unwrap());
    let mut control_client = ControlClient::new(ep).await?;

    let mut db_conns = (1..=2)
        .map(|id| {
            mysql::Conn::new(format!("mysql://root:mysql{id}@mysql{id}/test").as_str())
                .expect("cannot connect to mysql")
        })
        .collect::<Vec<_>>();
    // some helper functions
    fn get_golden_conn(db_conns: &mut [mysql::Conn]) -> &mut mysql::Conn {
        &mut db_conns[0]
    }
    fn get_my_rank_conn(
        db_conns: &mut [mysql::Conn],
        granularity: TemporalGranularity,
    ) -> &mut mysql::Conn {
        if granularity == TemporalGranularity::Daily {
            &mut db_conns[0]
        } else {
            &mut db_conns[1]
        }
    }

    {
        let golden_conn = get_golden_conn(&mut db_conns);
        let local_infile_handler = Some(LocalInfileHandler::new(|file_name, writer| {
            let file_name_str = String::from_utf8_lossy(file_name).to_string();
            let file_content = fs::read_to_string(file_name_str.as_str())?;
            writer.write_all(file_content.as_bytes())
        }));
        golden_conn.set_local_infile_handler(local_infile_handler);
        golden_conn.query_drop(
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

        println!("start init user_read_test table with full tuples...");
        golden_conn.query_drop(
        "
        LOAD DATA LOCAL INFILE '/root/Course-DDBS/sql-data/user_read_shard1.sql' INTO TABLE user_read_test
        FIELDS TERMINATED BY '|'
        LINES TERMINATED BY '\\n' "
    )?;
        golden_conn.query_drop(
        "
        LOAD DATA LOCAL INFILE '/root/Course-DDBS/sql-data/user_read_shard2.sql' INTO TABLE user_read_test
        FIELDS TERMINATED BY '|'
        LINES TERMINATED BY '\\n' "
    )?;

        println!("init user_read_test table with full tuples succeed!");
    }
    // test ping
    control_client.ping().await?;
    println!("ping control success");

    // check the server
    let ListServerStatusResponse { server_map } = control_client.list_server_status().await?;
    assert!(server_map.len() == 2);
    for (_, meta) in server_map.iter() {
        assert_eq!(meta.status(), DbStatus::Alive);
        assert!(meta.shard.is_none());
    }

    // init the cluster
    let start = Instant::now();
    control_client.cluster_init().await?;
    let ListServerStatusResponse { server_map } = control_client.list_server_status().await?;
    assert!(server_map.len() == 2);
    for (sid, meta) in server_map.iter() {
        assert_eq!(meta.status(), DbStatus::Alive);
        assert!(meta.shard.is_some());
        println!(
            "server {sid} ({}) is responsible for shard {:?}",
            meta.uri,
            meta.shard()
        );
    }
    println!("init the cluster elapsed: {:?}", start.elapsed());

    println!("start generating be read table...");
    let start = Instant::now();
    control_client.generate_be_read_table().await?;
    println!("generate be read table elapsed: {:?}", start.elapsed());

    for granularity in [
        TemporalGranularity::Monthly,
        TemporalGranularity::Weekly,
        TemporalGranularity::Daily,
    ] {
        let start = Instant::now();
        println!("start generate {granularity} popular_rank table...");
        control_client.generate_popular_table(granularity).await?;
        println!(
            "generate {granularity} popular_rank table succeed, elapsed: {:?}",
            start.elapsed()
        );

        println!("start generate golden truth {granularity} popular_rank table...");
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
        get_golden_conn(&mut db_conns).query_drop(sql)?;
        println!("generate golden truth {granularity} popular_rank table succeed!");

        // compare our results with golden truth
        let rows: Vec<(String, String)> = get_my_rank_conn(&mut db_conns, granularity).exec(
            format!(
                r#"SELECT popularDate,articleAidList FROM popular_rank
                WHERE temporalGranularity="{}""#,
                granularity
            ),
            (),
        )?;
        println!("start check popular_rank table of {granularity}...");
        for (date, list) in rows {
            let target_list: Option<String> = get_golden_conn(&mut db_conns).exec_first(
                format!(
                    r#"
            SELECT aidList FROM popular_rank_test WHERE popularDate="{}""#,
                    date
                ),
                (),
            )?;
            let my_aid = list
                .split(",")
                .map(|aid| aid.parse::<u64>().unwrap())
                .collect::<HashSet<_>>();
            let target_aid = target_list
                .unwrap()
                .split(",")
                .map(|aid| aid.parse::<u64>().unwrap())
                .collect::<HashSet<_>>();
            assert!(my_aid.is_subset(&target_aid));
        }
        println!("check popular_rank table of {granularity} succeed!");
        get_golden_conn(&mut db_conns).query_drop("DROP TABLE `popular_rank_test`")?;
    }

    Ok(())
}
