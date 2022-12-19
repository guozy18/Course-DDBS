mod commands;
mod formatter;
pub mod opts;

use clap::Parser;
use commands::handle_commands;
use opts::Opts;

use common::{Result, RuntimeError};
use std::path::PathBuf;

use rustyline::error::ReadlineError;
use rustyline::Editor;

use protos::{
    control_server_client::ControlServerClient, db_server_client::DbServerClient, ExecRequest,
};
use tonic::transport::{Channel, Uri};

#[derive(Debug)]
pub struct Repl {
    editor: Editor<()>,
    prompt: String,
    control_client: ControlServerClient<Channel>,
    #[allow(unused)]
    store_client: Vec<DbServerClient<Channel>>,
    history_file: PathBuf,
}

impl Repl {
    pub async fn new() -> Result<Self> {
        let opts: Opts = Opts::parse();

        let channel = match format!("http://{}", opts.control_server).parse::<Uri>() {
            Ok(uri) => Channel::builder(uri),
            Err(e) => {
                panic!("Server address format is not parse: {e}");
            }
        };

        let mut editor = Editor::<()>::new();
        let prompt = String::new();
        let control_client = ControlServerClient::connect(channel).await?;

        let mut store_client = vec![];
        for store_server in opts.store_server {
            let channel = match format!("http://{store_server}").parse::<Uri>() {
                Ok(uri) => Channel::builder(uri),
                Err(e) => {
                    panic!("Server address format is not parse: {e}");
                }
            };
            let shard_store_client = DbServerClient::connect(channel).await?;
            store_client.push(shard_store_client);
        }
        // s

        let history_file = Self::get_history_file();
        let _ = editor.load_history(&history_file).is_ok();

        Ok(Repl {
            editor,
            prompt,
            control_client,
            store_client,
            history_file,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        self.prompt = String::from("\x1b[1;32m[Course-DDBS]> \x1b[0m");
        loop {
            let line = self.editor.readline(&self.prompt);
            if self.process(line).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    async fn process(&mut self, line: rustyline::Result<String>) -> Result<()> {
        match line {
            Ok(statement) => {
                let statement = statement.trim();
                if statement.starts_with(':') {
                    // println!("Input control command not sql.");
                    if let Some(args) = shlex::split(statement) {
                        handle_commands(self, args).await;
                    } else {
                        println!("Error on parsing the input command.");
                    }
                } else {
                    println!("[Exec query]: {statement}");
                    let timer = std::time::Instant::now();
                    let result = self
                        .control_client
                        .exec(ExecRequest {
                            statement: statement.to_string(),
                        })
                        .await;
                    let total_time = timer.elapsed();

                    match result {
                        Ok(data) => {
                            let formatted_output =
                                formatter::format_output(data.into_inner().result.as_str());
                            if formatted_output.is_err() {
                                println!(
                                    "format output error {:?}",
                                    formatted_output.err().unwrap()
                                );
                            } else {
                                print!("{}", formatted_output.unwrap());
                            }
                        }
                        Err(status) => println!("{status}"),
                    };

                    println!("Total time: {:.2} ms", total_time.as_secs_f64() * 1000.0);
                }
                self.editor.add_history_entry(statement);
            }
            Err(ReadlineError::Interrupted) => return Err(RuntimeError::Interrupted),
            Err(err) => {
                eprintln!("An error occurred: {err:?}");
                return Err(RuntimeError::ReadlineError);
            }
        }
        Ok(())
    }

    fn get_history_file() -> PathBuf {
        let history_file = ".course_history";
        #[allow(deprecated)]
        match std::env::home_dir() {
            Some(home) => home.join(history_file),
            None => history_file.into(),
        }
    }
}

impl Drop for Repl {
    fn drop(&mut self) {
        let editor = &mut self.editor;
        editor.append_history(&self.history_file).unwrap();
    }
}
