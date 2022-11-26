use std::{env::VarError, str::Utf8Error};

use thiserror::Error;
use tonic::Status;

mod db_types;
pub use db_types::BeRead;

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
