use std::net::ToSocketAddrs;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server as TonicServer;
use tracing_subscriber::{filter::EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use dbserver::DbServer;
use runserver::{parse_cli_args, CliArgs, Exception, ServerType};

pub type Result<T> = std::result::Result<T, Exception>;

/// Runs Server.
///
/// # Arguments
/// * `listener`: The TCP listener to run the gRPC server on.
///
/// # Errors
/// This will return an error if the gRPC fails to start on the given
///listener.
async fn run_server(args: CliArgs) -> Result<()> {
    match args.server_type {
        ServerType::Client { .. } => unimplemented!(),
        ServerType::Control { .. } => unimplemented!(),
        ServerType::DbServer { addr, sql_url } => {
            let addr = addr.to_socket_addrs()?.next().unwrap();
            let incoming_listener = TcpListenerStream::new(TcpListener::bind(addr).await?);
            let db_service = DbServer::new(sql_url);
            let service = protos::db_server_server::DbServerServer::new(db_service);
            TonicServer::builder()
                .add_service(service)
                .serve_with_incoming(incoming_listener)
                .await?;
        }
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<()> {
    // Init the tracing subscriber.
    let filter = if let Ok(env_level) = std::env::var(EnvFilter::DEFAULT_ENV) {
        EnvFilter::new(env_level)
    } else {
        EnvFilter::new("info")
    };

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();

    let args = parse_cli_args();

    run_server(args).await?;

    Ok(())
}
