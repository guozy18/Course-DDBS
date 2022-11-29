use crate::{ControlService, DbClient};
use common::{
    utils::BatchStream, BeRead, MyDate, MyRow, PopularArticle, Result, RuntimeError, StatusResult,
    TemporalGranularity,
};
use flexbuffers::Reader;
use futures::{join, StreamExt};
use serde::Deserialize;
use std::collections::{BinaryHeap, HashSet};
use std::time::Instant;
use tracing::trace;

impl ControlService {
    // The first complex opeartion to support is to generate the Be-Read table
    pub async fn generate_be_read_table(&self) -> Result<()> {
        // first to check init state
        let (dbms1, dbms2) = self.check_init()?;
        trace!("dbms1 = {dbms1}, dbms2 = {dbms2}");
        let (dbms1, dbms2) = {
            let clients = self.inner.clients.read().unwrap();
            (
                clients.get(&dbms1).unwrap().clone(),
                clients.get(&dbms2).unwrap().clone(),
            )
        };
        let mut dbms1 = dbms1.lock().await;
        let mut dbms2 = dbms2.lock().await;

        // be_read_table:
        // article.category="science" allocated to DBMS1 and DBMS2
        // article.category="technology" allocated to DBMS2.
        // read table:
        // user.region="Beijing" allocated to DBMS1
        // user.region="Hongkong" allocated to DBMS2
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
        let bytes_array_to_sql = |arr: Vec<Vec<u8>>| -> Result<String> {
            Ok(arr
                .into_iter()
                .map(|row| {
                    let reader = Reader::get_root(row.as_slice()).unwrap();
                    match MyRow::deserialize(reader) {
                        Ok(mut row) => BeRead::from(&mut row)
                            .map(|be_read| format!("CALL insert_be_read({be_read});")),
                        Err(e) => Err(e.into()),
                    }
                })
                .reduce(|acc, item| match (acc, item) {
                    (Ok(acc), Ok(item)) => Ok(acc + &item),
                    (Err(e), _) | (_, Err(e)) => Err(e),
                })
                .unwrap()?)
        };

        // first we populate dbms2 since it contains all be_read tuples
        let be_read_stream = dbms1.exec_sql( "
            SELECT aid, count(uid), GROUP_CONCAT(uid), count(IF(commentOrNot=1, 1, NULL)), GROUP_CONCAT(IF(commentOrNot=1, uid, NULL)),
            count(IF(agreeOrNot=1, 1, NULL)), GROUP_CONCAT(IF(agreeOrNot = 1, uid, NULL)), count(IF(shareOrNot=1, 1, NULL)), GROUP_CONCAT(IF(shareOrNot = 1, uid, NULL))
            FROM user_read GROUP BY aid".to_owned()
        ).await?.into_inner();
        // batch size is 20
        let mut batch_stream = BatchStream::new(be_read_stream, 20);
        while let Some(be_read_arr) = batch_stream.next().await {
            let be_read_arr = be_read_arr.into_iter().collect::<StatusResult<_>>()?;
            let sql = bytes_array_to_sql(be_read_arr)?;
            dbms2.exec_sql_drop(sql).await?;
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
        let be_read_stream = dbms2.exec_sql("
            SELECT aid, readNum, readUidList, commentNum, commentUidList, agreeNum, agreeUidList, shareNum, shareUidList
            FROM be_read".to_owned()
        ).await?.into_inner();

        let mut batch_stream = BatchStream::new(be_read_stream, 20);
        while let Some(be_read_arr) = batch_stream.next().await {
            let be_read_arr = be_read_arr.into_iter().collect::<StatusResult<_>>()?;
            let sql = bytes_array_to_sql(be_read_arr)?;
            dbms1.exec_sql_drop(sql).await?;
        }
        trace!("generate_be_read_table: DBMS1 finish create the be_read_table");
        Ok(())
    }
}

impl ControlService {
    // The second complex opeartion to support is to generate the popular_table
    pub async fn generate_popular_table(&self, granularity: TemporalGranularity) -> Result<()> {
        // first to check init state
        let (dbms1, dbms2) = self.check_init()?;
        trace!("dbms1 = {dbms1}, dbms2 = {dbms2}");
        let (dbms1, dbms2) = {
            let clients = self.inner.clients.read().unwrap();
            (
                clients.get(&dbms1).unwrap().clone(),
                clients.get(&dbms2).unwrap().clone(),
            )
        };
        let mut dbs = [&mut *dbms1.lock().await, &mut *dbms2.lock().await];

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
            let mut top_k = Vec::from_iter(top_k.into_iter().map(|e| e.aid));
            // dedup with aid
            top_k.sort();
            top_k.dedup();
            debug_assert_eq!(num, top_k.len());
            let list = top_k.join(",");
            let dbms = if granularity.to_string() == "daily" {
                &mut dbs[0]
            } else {
                &mut dbs[1]
            };
            trace!("Generate popular table of {granularity} for {date}: {list}");
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
        // step1: create a temp table popular_temp
        for dbms in dbs {
            dbms.exec_sql_drop(format!(
                "
            CREATE TABLE IF NOT EXISTS `popular_temp_{}`
            SELECT aid,
                {} as popularDate,
                count(uid) as readNum
            FROM user_read GROUP BY popularDate, aid ORDER BY popularDate, readNum DESC",
                granularity as i32,
                granularity.to_column_sql("timestamp")
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
                .exec_sql(format!(
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
                    .ok_or(RuntimeError::DBTypeParseError(
                        "cannot get index 0 for DISTINCT popularDate".to_owned(),
                    ))?
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
        let sql = format!(
            r#"SELECT * FROM popular_temp_{} WHERE popularDate = "{}""#,
            granularity as i32, date,
        );
        let streams = join! {
            dbms1.exec_sql(sql.clone()),
            dbms2.exec_sql(sql),
        };
        let bytes_to_popular_article = |bytes: Vec<u8>| -> Result<PopularArticle> {
            let s = Reader::get_root(bytes.as_slice()).unwrap();
            MyRow::deserialize(s)?.try_into()
        };
        let (stream1, stream2) = (streams.0?.into_inner(), streams.1?.into_inner());
        let mut stream =
            common::utils::interleave(stream1.map(|s| (1, s)), stream2.map(|s| (2, s)));
        // two cursor indicating the current read num of (stream1, stream 2)
        let mut curr_read_num = [0, 0];
        while let Some((site, bytes)) = stream.next().await {
            // early stop
            if top_k.len() == k
                && top_k.peek().unwrap().read_num >= (curr_read_num[0] + curr_read_num[1])
            {
                break;
            }

            let mut popular_article = bytes_to_popular_article(bytes?)?;
            // update the cursor
            curr_read_num[site - 1] = popular_article.read_num;
            // deduplicate aid
            if top_k.iter().any(|article| article.aid == popular_article.aid) {
                continue;
            }
            // try to get the entry of same aid in the other site
            let other_site = if site == 1 { &mut dbms2 } else { &mut dbms1 };
            if let Some(other) = other_site
                .exec_sql_first(format!(
                    r#"SELECT * FROM popular_temp_{} WHERE popularDate = "{}" AND aid = {}"#,
                    granularity as i32, date, popular_article.aid
                ))
                .await?
                .into_inner()
                .row
                .map(bytes_to_popular_article)
                .transpose()?
            {
                debug_assert_eq!(popular_article.aid, other.aid);
                debug_assert_eq!(popular_article.date, other.date);
                popular_article.read_num += other.read_num;
            }
            trace!(
                "new popular_article for {date}: (aid = {}, count = {}), curr_read_num = {:?}",
                popular_article.aid,
                popular_article.read_num,
                curr_read_num
            );
            top_k.push(popular_article);
            if top_k.len() > k {
                top_k.pop();
            }
            debug_assert!(top_k.len() <= k);
        }
        trace!("get popular rank for {date}: {:?}", start.elapsed());
        Ok(top_k)
    }
}
