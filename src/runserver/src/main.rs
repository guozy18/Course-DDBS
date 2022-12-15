use anyhow::Result;
use clap::{Parser, Subcommand};
use crossterm::style::Stylize;
use std::fmt::Debug;
use std::net::ToSocketAddrs;
use std::time::SystemTime;
use time::{macros::format_description, OffsetDateTime};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Server as TonicServer, Uri};
use tracing::{subscriber::Subscriber, Event};
use tracing_log::NormalizeEvent;
use tracing_subscriber::{
    fmt::{self, format::Writer, FmtContext, FormatEvent, FormatFields},
    layer::SubscriberExt,
    registry::LookupSpan,
    util::SubscriberInitExt,
    EnvFilter,
};

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
    #[clap(about = "Run as a Control daemon")]
    Control {
        /// Controler address
        #[clap(short, long, default_value = "0.0.0.0:27022", value_name = "HOST:PORT")]
        addr: String,
    },
    #[clap(about = "Run as a DBMS Server daemon")]
    DbServer {
        #[clap(short, long, help = "the uri of the control")]
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
    println!("{args:#?}");
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

struct SimpleFmt;

impl<S, N> FormatEvent<S, N> for SimpleFmt
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        // Create timestamp
        let time_format = format_description!(
            "[month]-[day] [hour repr:24]:[minute]:[second].[subsecond digits:3]"
        );
        let time_now = OffsetDateTime::from(SystemTime::now());
        let time_now = time_now.format(time_format).unwrap();

        // Get line numbers from log crate events
        let normalized_meta = event.normalized_metadata();
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());

        // Write formatted log record
        let message = format!(
            "[{}] {} {}{}{} ",
            time_now.grey(),
            meta.level().to_string().blue(),
            meta.file().unwrap_or("").to_string().yellow(),
            String::from(":").yellow(),
            meta.line().unwrap_or(0).to_string().yellow(),
        );
        write!(writer, "{message}")?;
        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
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
        .with(fmt::layer().event_format(SimpleFmt))
        .with(filter)
        .init();

    let args = parse_cli_args();

    run_server(args).await?;

    Ok(())
}
