use clap::{Parser, Subcommand};
use std::fmt::{Debug, Display, Formatter};

/// An error during execution.
pub struct Exception {
    pub code: u16,
    pub detail: String,
}

impl Exception {
    pub fn new(code: u16, detail: impl Into<String>) -> Self {
        Exception {
            code,
            detail: detail.into(),
        }
    }

    pub fn code(&self) -> u16 {
        self.code
    }

    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }

    pub fn create(code: u16, detail: String) -> Exception {
        Exception { code, detail }
    }

    pub fn from_std_error<T: std::error::Error>(error: T) -> Self {
        Exception {
            code: 1002,
            detail: error.to_string(),
        }
    }
}

impl Debug for Exception {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Code: {}, Detail = {}.", self.code(), self.detail())
    }
}

impl Display for Exception {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Code: {}, displayText = {}.", self.code(), self.detail(),)
    }
}

impl<T: std::error::Error> From<T> for Exception {
    fn from(error: T) -> Self {
        Exception::from_std_error(error)
    }
}

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
        #[clap(long, default_value = "localhost:27021", value_name = "HOST:PORT")]
        addr: String,
    },
    #[clap(about = "Run as a Control daemon")]
    Control {
        /// Controler address
        #[clap(
            short,
            long,
            default_value = "localhost:27022",
            value_name = "HOST:PORT"
        )]
        addr: String,
    },
    #[clap(about = "Run as a DBMS Server daemon")]
    DbServer {
        /// DBServer address
        #[clap(
            short,
            long,
            default_value = "0.0.0.0:27023",
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
