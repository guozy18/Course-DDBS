use common::{Result, RuntimeError};
use serde::Deserialize;
use std::path::{Path, PathBuf};

static DB_FILE_DIR: &'static str = "/root/sql-data";

pub static CREATE_TABLE_SQLS: [&str; 3] = [
    "
        DROP TABLE IF EXISTS `user`;
        CREATE TABLE `user` (
          `timestamp` char(14) DEFAULT NULL,
          `id` char(5) DEFAULT NULL,
          `uid` char(5) DEFAULT NULL,
          `name` char(9) DEFAULT NULL,
          `gender` char(7) DEFAULT NULL,
          `email` char(10) DEFAULT NULL,
          `phone` char(10) DEFAULT NULL,
          `dept` char(9) DEFAULT NULL,
          `grade` char(7) DEFAULT NULL,
          `language` char(3) DEFAULT NULL,
          `region` char(10) DEFAULT NULL,
          `role` char(6) DEFAULT NULL,
          `preferTags` char(7) DEFAULT NULL,
          `obtainedCredits` char(3) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
    "
        DROP TABLE IF EXISTS `article`;
        CREATE TABLE `article` (
        `timestamp` char(14) DEFAULT NULL,
        `id` char(7) DEFAULT NULL,
        `aid` char(7) DEFAULT NULL,
        `title` char(15) DEFAULT NULL,
        `category` char(11) DEFAULT NULL,
        `abstract` char(30) DEFAULT NULL,
        `articleTags` char(14) DEFAULT NULL,
        `authors` char(13) DEFAULT NULL,
        `language` char(3) DEFAULT NULL,
        `text` char(31) DEFAULT NULL,
        `image` char(32) DEFAULT NULL,
        `video` char(32) DEFAULT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
    "
        DROP TABLE IF EXISTS `user_read`;
        CREATE TABLE `user_read` (
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
];

pub static TBLAE_NAMES: [&str; 3] = ["user", "article", "user_read"];

#[derive(Debug, Clone)]
pub struct Config {
    pub url: String,
    pub db_file_dir: PathBuf,
    pub create_table_sqls: Vec<String>,
    pub db_file_names: Vec<String>,
    pub table_names: Vec<String>,
}

#[derive(Deserialize)]
struct TomlConfig {
    /// url of the mysql database (required)
    url: String,
    /// dir of the DB files (optional)
    db_file_dir: Option<String>,
    /// sql of create tables (optional)
    create_table_sqls: Option<Vec<String>>,
    /// name of the DB files (required)
    db_file_names: Vec<String>,
    /// name of the tables (optional)
    table_names: Option<Vec<String>>,
}

impl Config {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let toml_content = std::fs::read(path)?;
        let toml_config: TomlConfig = toml::from_slice(&toml_content)?;
        toml_config.verify()
    }
}

impl TomlConfig {
    fn verify(self) -> Result<Config> {
        let db_file_dir = PathBuf::from(self.db_file_dir.unwrap_or(DB_FILE_DIR.to_string()));
        let create_table_sqls = self
            .create_table_sqls
            .unwrap_or(CREATE_TABLE_SQLS.map(|x| x.to_string()).to_vec());
        let table_names = self
            .table_names
            .unwrap_or(TBLAE_NAMES.map(|x| x.to_string()).to_vec());

        if table_names.len() != create_table_sqls.len() {
            Err(RuntimeError::ConfigError(
                "table names length != create table sqls length".to_string(),
            ))?;
        }
        if table_names.len() != self.db_file_names.len() {
            Err(RuntimeError::ConfigError(
                "table names length != db file names length".to_string(),
            ))?;
        }
        Ok(Config {
            url: self.url,
            db_file_dir: db_file_dir,
            create_table_sqls,
            db_file_names: self.db_file_names,
            table_names,
        })
    }
}
