use anyhow::Result;
use clap::{Parser, Subcommand};
use std::fmt::Debug;
use std::net::ToSocketAddrs;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Server as TonicServer, Uri};
use tracing_subscriber::{filter::EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use control::ControlService;
use dbserver::DbServer;

#[derive(Debug, Parser)]
#[clap(name = "Three types server")]
pub struct CliArgs {
    #[clap(subcommand)]
    pub server_type: ServerType,
}

#[derive(Subcommand, Debug)]
pub enum ServerType {
    #[clap(about = "Run as a Client daemon")]
    Client {
        /// Client address
        #[clap(long, default_value = "127.0.0.1:27021", value_name = "HOST:PORT")]
        addr: String,
    },
    #[clap(about = "Run as a Control daemon")]
    Control {
        /// Controler address
        #[clap(short, long, default_value = "0.0.0.0:27022", value_name = "HOST:PORT")]
        addr: String,
    },
    #[clap(about = "Run as a DBMS Server daemon")]
    DbServer {
        #[clap(short, long, help = "the uri of the control uri")]
        control_uri: Uri,
        /// DBServer address
        #[clap(
            short,
            long,
            default_value = "127.0.0.1:27023",
            value_name = "HOST:PORT"
        )]
        addr: String,
    },
}

pub fn parse_cli_args() -> CliArgs {
    let args = CliArgs::parse();
    println!("{:#?}", args);
    args
}

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
        ServerType::Control { addr } => {
            let addr = addr.to_socket_addrs()?.next().unwrap();
            let incoming_listener = TcpListenerStream::new(TcpListener::bind(addr).await?);
            let control_service = ControlService::new();
            let service = protos::control_server_server::ControlServerServer::new(control_service);
            TonicServer::builder()
                .add_service(service)
                .serve_with_incoming(incoming_listener)
                .await?;
        }
        ServerType::DbServer { control_uri, addr } => {
            let addr = addr.to_socket_addrs()?.next().unwrap();
            let uri = format!("http://{addr}")
                .parse::<Uri>()
                .expect("invalid listen address");
            let incoming_listener = TcpListenerStream::new(TcpListener::bind(addr).await?);
            let db_service = DbServer::new(control_uri, uri).await?;
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
