use crate::{ControlService, DbClient};
use common::{
    BeRead, MyDate, MyRow, PopularArticle, Result, RuntimeError, ServerId, TemporalGranularity,
};
use flexbuffers::Reader;
use futures::{join, StreamExt};
use itertools::{join, Itertools};
use protos::{DbShard, DbStatus, ExecSqlBatchRequest};
use serde::Deserialize;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::time::Instant;
use tracing::trace;

impl ControlService {
    /// check whether the cluster has been initialized
    /// Return the (server id of shard one, server id of shard two)
    fn check_init(&self) -> Result<(ServerId, ServerId)> {
        let metas = self.inner.db_server_meta.read().unwrap();
        let shard_one = metas
            .iter()
            .find(|(_, meta)| {
                meta.status() == DbStatus::Alive && meta.shard == Some(DbShard::One as _)
            })
            .ok_or(RuntimeError::Uninitialize)?;
        let shard_two = metas
            .iter()
            .find(|(_, meta)| {
                meta.status() == DbStatus::Alive && meta.shard == Some(DbShard::Two as _)
            })
            .ok_or(RuntimeError::Uninitialize)?;
        Ok((*shard_one.0, *shard_two.0))
    }

    // The first complex opeartion to support is to generate the Be-Read table
    pub async fn generate_be_read_table(&self) -> Result<()> {
        // first to check init state
        let (dbms1, dbms2) = self.check_init()?;
        trace!("dbms1 = {dbms1}, dbms2 = {dbms2}");
        let (mut dbms1, mut dbms2) = {
            let clients = self.inner.clients.read().unwrap();
            (
                clients.get(&dbms1).unwrap().clone(),
                clients.get(&dbms2).unwrap().clone(),
            )
        };

        // be_read_table:
        // article.category="science" allocated to DBMS1 and DBMS2
        // article.category="technology" allocated to DBMS2.
        // read table:
        // user.region="Beijing" allocated to DBMS1
        // user.region="Hongkong" allocated to DBMS2

        // first we populate dbms2 since it contains all be_read tuples
        dbms2
            .exec_sql_drop(
                "
            DROP TABLE IF EXISTS `be_read`;

            CREATE TABLE `be_read` (
            `id` int auto_increment,
            `aid` char(5) DEFAULT NULL,
            `readNum` int,
            `readUidList` TEXT,
            `commentNum` int,
            `commentUidList` TEXT,
            `agreeNum` int,
            `agreeUidList` TEXT,
            `shareNum` int,
            `shareUidList` TEXT,
            PRIMARY KEY(id),
            UNIQUE (aid)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

            INSERT INTO be_read (aid, readNum, readUidList, commentNum, commentUidList, agreeNum, agreeUidList, shareNum, shareUidList)
            SELECT aid, count(uid), GROUP_CONCAT(uid), count(IF(commentOrNot=1,1,NULL)), GROUP_CONCAT(IF(commentOrNot = 1, uid, NULL)),
            count(IF(agreeOrNot=1,1,NULL)), GROUP_CONCAT(IF(agreeOrNot = 1, uid, NULL)), count(IF(shareOrNot=1,1,NULL)), GROUP_CONCAT(IF(shareOrNot = 1, uid, NULL))
            FROM user_read GROUP BY aid;"
                    .to_owned(),
            )
            .await?;
        trace!("generate_be_read_table: DBMS2 create the be_read_table");
        let bytes_array_to_sql = |arr: Vec<u8>| -> Result<String> {
            let reader = Reader::get_root(arr.as_slice()).unwrap();
            match Vec::<MyRow>::deserialize(reader) {
                Ok(mut row) => Ok(row
                    .iter_mut()
                    .map(|my_row| {
                        BeRead::from(my_row)
                            .map(|be_read| format!("CALL insert_be_read({be_read});"))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .concat()),
                Err(e) => Err(e.into()),
            }
        };

        let req = ExecSqlBatchRequest {
            sql: "
            SELECT aid, count(uid), GROUP_CONCAT(uid), count(IF(commentOrNot=1, 1, NULL)), GROUP_CONCAT(IF(commentOrNot=1, uid, NULL)),
            count(IF(agreeOrNot=1, 1, NULL)), GROUP_CONCAT(IF(agreeOrNot = 1, uid, NULL)), count(IF(shareOrNot=1, 1, NULL)), GROUP_CONCAT(IF(shareOrNot = 1, uid, NULL))
            FROM user_read GROUP BY aid".to_owned(),
            batch_size: 20
        };

        let mut start = Instant::now();
        let mut be_read_stream = dbms1.exec_sql_batch(req).await?.into_inner();
        while let Some(be_read_arr) = be_read_stream.next().await {
            let sql = bytes_array_to_sql(be_read_arr?)?;
            dbms2.exec_sql_drop(sql).await?;
            trace!(
                "one batch of generating be_read for dbms2 elapsed: {:?}",
                start.elapsed()
            );
            start = Instant::now();
        }
        trace!("generate_be_read_table: DBMS2 finish create the be_read_table");

        // now it is time for dbms1
        dbms1
            .exec_sql_drop(
                "
            DROP TABLE IF EXISTS `be_read`;
            CREATE TABLE `be_read` (
            `id` int auto_increment,
            `aid` char(5) DEFAULT NULL,
            `readNum` int,
            `readUidList` TEXT,
            `commentNum` int,
            `commentUidList` TEXT,
            `agreeNum` int,
            `agreeUidList` TEXT,
            `shareNum` int,
            `shareUidList` TEXT,
            PRIMARY KEY(id),
            UNIQUE (aid)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
                    .to_owned(),
            )
            .await?;
        trace!("generate_be_read_table: dbms1 create be_read table");
        let req = ExecSqlBatchRequest {
            sql: "
            SELECT aid, readNum, readUidList, commentNum, commentUidList, agreeNum, agreeUidList, shareNum, shareUidList
            FROM be_read".to_owned(),
            batch_size: 20
        };
        let mut be_read_stream = dbms2.exec_sql_batch(req).await?.into_inner();

        start = Instant::now();
        while let Some(be_read_arr) = be_read_stream.next().await {
            let sql = bytes_array_to_sql(be_read_arr?)?;
            dbms1.exec_sql_drop(sql).await?;
            trace!(
                "one batch of generating for dbms1 be_read elapsed: {:?}",
                start.elapsed()
            );
            start = Instant::now();
        }
        trace!("generate_be_read_table: DBMS1 finish create the be_read_table");
        Ok(())
    }
}

impl ControlService {
    #[aux_macro::elapsed]
    // The second complex opeartion to support is to generate the popular_table
    pub async fn generate_popular_table(&self, granularity: TemporalGranularity) -> Result<()> {
        // first to check init state
        let (dbms1, dbms2) = self.check_init()?;
        trace!("dbms1 = {dbms1}, dbms2 = {dbms2}");
        let (mut dbms1, mut dbms2) = {
            let clients = self.inner.clients.read().unwrap();
            (
                clients.get(&dbms1).unwrap().clone(),
                clients.get(&dbms2).unwrap().clone(),
            )
        };
        let mut dbs = [&mut dbms1, &mut dbms2];

        Self::generate_popular_rank_temporary_table(&mut dbs, granularity).await?;

        // step1: create a temp table popular_temp
        for dbms in dbs.iter_mut() {
            dbms.exec_sql_drop(
                "
            CREATE TABLE IF NOT EXISTS `popular_rank` (
                `id` INT auto_increment,
                `popularDate` char(12) DEFAULT NULL,
                `temporalGranularity` char(7) DEFAULT NULL,
                `articleAidList` VARCHAR(256) DEFAULT NULL,
                PRIMARY KEY(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
            "
                .to_owned(),
            )
            .await?;
        }

        // step2: collect the different date
        let dates = Self::get_all_date_for(&mut dbs, granularity).await?;

        // step3: populate the top article for each date
        for date in dates {
            let top_k = Self::get_popular_rank_at(&mut dbs, date, granularity).await?;
            let num = top_k.len();
            // now top_k is generated
            let mut top_k = Vec::from_iter(
                top_k
                    .into_iter()
                    .sorted_by_key(|e| std::cmp::Reverse(e.read_num))
                    .map(|e| e.aid),
            );
            let list = top_k.join(",");
            // assert no dedup id
            top_k.sort();
            top_k.dedup();
            debug_assert_eq!(num, top_k.len());
            let dbms = if granularity.to_string() == "daily" {
                &mut dbs[0]
            } else {
                &mut dbs[1]
            };
            trace!("generate popular rank of {granularity} for {date}: {list}");
            dbms.exec_sql_drop(format!(
                r#"
            INSERT INTO `popular_rank` (popularDate, temporalGranularity, articleAidList)
            VALUES ("{date}", "{granularity}", "{list}")
            "#,
            ))
            .await?;
        }

        Ok(())
    }

    pub async fn generate_popular_rank_temporary_table(
        dbs: &mut [&mut DbClient],
        granularity: TemporalGranularity,
    ) -> Result<()> {
        assert_eq!(dbs.len(), 2);
        // step1: create a temp table popular_temp and create two index
        for dbms in dbs {
            dbms.exec_sql_drop(format!(
                "
            CREATE TABLE IF NOT EXISTS `popular_temp_{}`
            SELECT aid,
                {} as popularDate,
                count(uid) as readNum
            FROM user_read GROUP BY popularDate, aid ORDER BY popularDate, readNum DESC;
            CREATE INDEX popular_temp_{}_aid_index ON popular_temp_{} (aid);
            CREATE INDEX popular_temp_{}_date_index ON popular_temp_{} (popularDate)
            ",
                granularity as i32,
                granularity.to_column_sql("timestamp"),
                granularity as i32,
                granularity as i32,
                granularity as i32,
                granularity as i32,
            ))
            .await?;
        }
        Ok(())
    }

    /// Get all distinct date for `granularity`.
    ///
    /// Call [`Self::generate_popular_rank_temporary_table`] before calling this method.
    async fn get_all_date_for(
        dbs: &mut [&mut DbClient],
        granularity: TemporalGranularity,
    ) -> Result<HashSet<MyDate>> {
        assert_eq!(dbs.len(), 2);
        let mut dates = HashSet::new();
        for dbms in dbs {
            let mut date_stream = dbms
                .stream_exec_sql(format!(
                    "SELECT DISTINCT(popularDate) FROM popular_temp_{}",
                    granularity as i32
                ))
                .await?
                .into_inner();
            while let Some(bytes) = date_stream.next().await {
                let row = bytes?;
                let s = Reader::get_root(row.as_slice()).unwrap();
                let mut row = MyRow::deserialize(s)?;
                let date: MyDate = row
                    .get_mut(0)
                    .ok_or_else(|| {
                        RuntimeError::DBTypeParseError(
                            "cannot get index 0 for DISTINCT popularDate".to_owned(),
                        )
                    })?
                    .take()
                    .unwrap()
                    .try_into()?;
                dates.insert(date);
            }
        }
        Ok(dates)
    }

    /// Get the popular rank at `date` for `granularity`.
    /// Returns the popular articles.
    pub async fn get_popular_rank_at(
        dbs: &mut [&mut DbClient],
        date: MyDate,
        granularity: TemporalGranularity,
    ) -> Result<BinaryHeap<PopularArticle>> {
        assert_eq!(dbs.len(), 2);
        let start = Instant::now();
        let k = granularity.top_num();
        let mut top_k: BinaryHeap<PopularArticle> = BinaryHeap::with_capacity(k);
        let (dbms1, dbms2) = dbs.split_at_mut(1);
        let (mut dbms1, mut dbms2) = (&mut dbms1[0], &mut dbms2[0]);
        // This may not correct, but for performance consideration
        let sql = format!(
            r#"SELECT * FROM popular_temp_{} WHERE popularDate = "{}""#,
            granularity as i32, date,
        );
        let req = ExecSqlBatchRequest {
            sql: sql.clone(),
            batch_size: granularity.batch_size() as _,
        };
        let streams = join! {
            dbms1.exec_sql_batch(req.clone()),
            dbms2.exec_sql_batch(req),
        };

        let bytes_to_popular_article_vec = |bytes: Vec<u8>| -> Result<Vec<PopularArticle>> {
            let s = Reader::get_root(bytes.as_slice()).unwrap();
            Vec::<MyRow>::deserialize(s)?
                .into_iter()
                .map(|my_row| my_row.try_into())
                .collect()
        };
        let (stream1, stream2) = (streams.0?.into_inner(), streams.1?.into_inner());
        let mut stream =
            common::utils::interleave(stream1.map(|s| (1, s)), stream2.map(|s| (2, s)));
        // two cursor indicating the current read num of (stream1, stream 2)
        let mut curr_read_num = [u64::MAX >> 1, u64::MAX >> 1];
        let mut batch_idx = 0;
        while let Some((site, bytes)) = stream.next().await {
            if let Ok(bytes) = &bytes {
                trace!("byte length of batch res = {}", bytes.len());
            }
            // early stop
            if top_k.len() == k
                && top_k.peek().unwrap().read_num >= (curr_read_num[0] + curr_read_num[1])
            {
                break;
            }
            batch_idx += 1;
            let mut popular_articles = bytes_to_popular_article_vec(bytes?)?;
            // update the cursor *before the deduplicate*
            curr_read_num[site - 1] = popular_articles
                .last()
                .expect("no articles returned")
                .read_num;
            // deduplicate aid
            popular_articles.retain(|article| top_k.iter().all(|a| a.aid != article.aid));
            if popular_articles.is_empty() {
                continue;
            }
            let aids = join(popular_articles.iter().map(|a| &a.aid), ",");
            // try to get the entry of same aid in the other site
            let other_site = if site == 1 { &mut dbms2 } else { &mut dbms1 };
            let bytes = other_site
                .exec_sql(format!(
                    r#"SELECT * FROM popular_temp_{} WHERE popularDate = "{}" AND aid IN ({})"#,
                    granularity as i32, date, aids
                ))
                .await?
                .into_inner();

            let other_popular_articles: HashMap<String, PopularArticle> =
                bytes_to_popular_article_vec(bytes)?
                    .into_iter()
                    .map(|article| (article.aid.clone(), article))
                    .collect();

            for article in popular_articles.iter_mut() {
                if let Some(other) = other_popular_articles.get(&article.aid) {
                    debug_assert_eq!(article.aid, other.aid);
                    debug_assert_eq!(article.date, other.date);
                    article.read_num += other.read_num;
                }
                trace!(
                    "article {} read num in {date} = {}",
                    article.aid,
                    article.read_num
                );
            }
            top_k.extend(popular_articles.into_iter());
            while top_k.len() > k {
                top_k.pop();
            }
            debug_assert!(top_k.len() <= k);
        }
        trace!(
            "get popular rank for {date}: {:?}, batch_idx = {batch_idx}",
            start.elapsed()
        );
        Ok(top_k)
    }
}
