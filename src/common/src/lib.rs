use std::{env::VarError, fmt::Display, str::Utf8Error};

use thiserror::Error;
use tonic::Status;

mod db_types;
mod profiler;
mod shard_info;
mod symbol_table;
pub mod utils;

pub use db_types::{BeRead, MyDate, MyRow, PopularArticle, ValueAdaptor, ValueDef};
pub use profiler::Profiler;
pub use shard_info::{get_join_condition, get_shards_info, join_shard_info, DataShard};
pub use symbol_table::SymbolTable;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("configuration has errors: {0}")]
    ConfigError(String),
    #[error("service has already been initialized")]
    Initialized,
    #[error("service has not been initialized")]
    Uninitialize,
    #[error("rpc invalid argument: {0}")]
    RpcInvalidArg(String),
    #[error("invalid argument: {0}")]
    InvalidArg(String),
    #[error("number of server is not enough or some server is down")]
    ServerNotAlive,
    #[error(transparent)]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("establish connect error: {source}")]
    TonicConnectError {
        #[from]
        source: tonic::transport::Error,
    },
    #[error(transparent)]
    TonicStatus(#[from] tonic::Status),
    #[error(transparent)]
    SqlParserError(#[from] sqlparser::parser::ParserError),
    #[error("unsupport sql statement {0}")]
    UnsupportSql(String),
    #[error("parse return value from database get error: {0}")]
    DBTypeParseError(String),
    #[error(transparent)]
    DeserializationError(#[from] flexbuffers::DeserializationError),
    #[error("mysql internal error: {0}")]
    MysqlError(#[from] mysql::Error),
    #[error(transparent)]
    VarError(#[from] VarError),
    #[error("error when parsing toml: {0}")]
    TomlParseError(#[from] toml::de::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

pub type StatusResult<T> = std::result::Result<T, Status>;
pub type Result<T> = std::result::Result<T, RuntimeError>;

impl From<RuntimeError> for Status {
    fn from(e: RuntimeError) -> Self {
        Status::internal(e.to_string())
    }
}

impl From<Utf8Error> for RuntimeError {
    fn from(e: Utf8Error) -> Self {
        RuntimeError::DBTypeParseError(e.to_string())
    }
}

pub type ServerId = u64;

#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TemporalGranularity {
    Daily = 0,
    Weekly,
    Monthly,
}

impl TemporalGranularity {
    pub fn top_num(&self) -> usize {
        match self {
            Self::Daily => 3,
            Self::Weekly => 5,
            Self::Monthly => 10,
        }
    }

    /// - `column_name` column_name of timestamp
    pub fn to_column_sql(&self, column_name: &str) -> String {
        match self {
            Self::Daily => format!(
                "DATE(FROM_UNIXTIME(CAST({} as unsigned) DIV 1000))",
                column_name
            ),
            Self::Weekly => format!(
                "YEARWEEK(FROM_UNIXTIME(CAST({} as unsigned) DIV 1000))",
                column_name
            ),
            Self::Monthly => format!(
                "EXTRACT(YEAR_MONTH FROM FROM_UNIXTIME(CAST({} as unsigned) DIV 1000))",
                column_name
            ),
        }
    }

    pub fn batch_size(&self) -> usize {
        match self {
            Self::Daily => 40,
            Self::Weekly => 20,
            Self::Monthly => 20,
        }
    }
}

impl Display for TemporalGranularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        };
        write!(f, "{}", s)
    }
}

impl TryFrom<i32> for TemporalGranularity {
    type Error = RuntimeError;

    fn try_from(v: i32) -> Result<Self> {
        match v {
            x if x == Self::Daily as i32 => Ok(Self::Daily),
            x if x == Self::Weekly as i32 => Ok(Self::Weekly),
            x if x == Self::Monthly as i32 => Ok(Self::Monthly),
            _ => Err(RuntimeError::InvalidArg(format!(
                "cannot convert {v} into TemporalGranularity"
            ))),
        }
    }
}
