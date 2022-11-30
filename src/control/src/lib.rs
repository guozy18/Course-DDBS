use protos::db_server_client::DbServerClient;
use tonic::transport::Channel;

mod cluster;
mod complex;
mod query;
mod service;

pub use service::ControlService;
pub type DbClient = DbServerClient<Channel>;