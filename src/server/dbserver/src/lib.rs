use mysql::prelude::*;
use mysql::*;
use protos::db_server_server::DbServer as Server;
use tonic::{Request, Response, Status};

pub mod server;
pub use server::DbServer;
