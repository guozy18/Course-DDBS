use crate::ControlService;
use common::{BeRead, Result};
use flexbuffers::Reader;
use futures::StreamExt;
use serde::Deserialize;
use tracing::trace;

struct FixedArray<T> {
    container: Vec<T>,
    size: usize,
    capacity: usize,
}

impl<T> FixedArray<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            container: Vec::with_capacity(capacity),
            size: 0,
            capacity,
        }
    }

    pub fn push(&mut self, ele: T) -> Option<Vec<T>> {
        if self.size < self.capacity {
            self.container.push(ele);
            self.size += 1;
            None
        } else {
            let mut res = Vec::with_capacity(self.capacity);
            std::mem::swap(&mut res, &mut self.container);
            self.size = 1;
            self.container.push(ele);
            Some(res)
        }
    }

    pub fn pop_all(&mut self) -> Option<Vec<T>> {
        if self.size == 0 {
            return None;
        }
        let mut res = Vec::with_capacity(self.capacity);
        std::mem::swap(&mut res, &mut self.container);
        self.size = 0;
        Some(res)
    }
}

impl ControlService {
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
        dbms2
            .execute_sql(
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
                .map(|be_read| {
                    let reader = Reader::get_root(be_read.as_slice()).unwrap();
                    let be_read = BeRead::deserialize(reader);
                    be_read.map(|be_read| format!("CALL insert_be_read({be_read});"))
                })
                .reduce(|acc, item| match (acc, item) {
                    (Ok(acc), Ok(item)) => Ok(acc + &item),
                    (Err(e), _) | (_, Err(e)) => Err(e),
                })
                .unwrap()?)
        };

        // first we populate dbms2 since it contains all be_read tuples
        let mut be_read_stream = dbms1.generate_be_read(()).await?.into_inner();
        // batch size is 20
        let mut be_read_arr = FixedArray::new(20);
        while let Some(be_read) = be_read_stream.next().await {
            let be_read_1 = be_read?;
            if let Some(arr) = be_read_arr.push(be_read_1) {
                let sql = bytes_array_to_sql(arr)?;
                dbms2.execute_sql(sql).await?;
            }
        }
        if let Some(arr) = be_read_arr.pop_all() {
            let sql = bytes_array_to_sql(arr)?;
            dbms2.execute_sql(sql).await?;
        }
        trace!("generate_be_read_table: DBMS2 finish create the be_read_table");

        // now it is time for dbms1
        dbms1
            .execute_sql(
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
        let mut be_read_stream = dbms2.scan_be_read(()).await?.into_inner();
        while let Some(be_read) = be_read_stream.next().await {
            let be_read_2 = be_read?;
            if let Some(arr) = be_read_arr.push(be_read_2) {
                let sql = bytes_array_to_sql(arr)?;
                dbms1.execute_sql(sql).await?;
            }
        }
        if let Some(arr) = be_read_arr.pop_all() {
            let sql = bytes_array_to_sql(arr)?;
            dbms1.execute_sql(sql).await?;
        }
        trace!("generate_be_read_table: DBMS1 finish create the be_read_table");
        Ok(())
    }
}
