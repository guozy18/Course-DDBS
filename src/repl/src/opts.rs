pub use clap::Parser;

/// REPL command line options
#[derive(Parser, Debug)]
pub struct Opts {
    #[clap(short, long, default_value = "control:27022", value_name = "host:port")]
    pub control_server: String,
    #[clap(short, long, default_value = "server1:27023", value_name = "host:port")]
    pub store_server: Vec<String>,
}

#[cfg(test)]
mod test {
    use super::Opts;
    pub use clap::Parser;

    #[test]
    fn test_opts() {
        assert!(Opts::try_parse_from([
            "./target/debug/ddbs-client",
            "--control-server",
            "127.0.0.1:33024"
        ])
        .is_ok());
    }

    #[test]
    fn test_opts_store() {
        let parse_string = [
            "./target/debug/ddbs-client",
            "-s",
            "localhost:27024",
            "-s",
            "localhost:27026",
        ];
        let res = Opts::try_parse_from(parse_string);
        assert!(res.is_ok());
        println!("{res:#?}");
    }
}
