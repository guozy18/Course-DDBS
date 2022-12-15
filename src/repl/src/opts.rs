pub use clap::Parser;

/// REPL command line options
#[derive(Parser)]
pub struct Opts {
    #[clap(
        short,
        long,
        default_value = "localhost:27022",
        value_name = "host:port"
    )]
    pub control_server: String,
}

#[test]
fn test_opts() {
    assert!(Opts::try_parse_from(["127.0.0.1:33024"]).is_ok());
}
