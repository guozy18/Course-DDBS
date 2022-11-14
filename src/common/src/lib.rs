use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("configuration has errors: {0}")]
    ConfigError(String),
    #[error("service has already been initialized")]
    Initialized,
    #[error("service has not been initialized")]
    Uninitialize,
    #[error("mysql internal error: {0}")]
    MysqlError(String),
}