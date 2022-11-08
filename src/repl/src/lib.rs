use std::time::Duration;

use rustyline::error::ReadlineError;
use rustyline::Editor;

// #[derive(Debug)]
// pub struct MyHelper {

// }

// impl Helper for MyHelper {

// }

#[derive(Debug)]
pub struct Repl {
    editor: Editor<()>,
    prompt: String,
}

impl Default for Repl {
    fn default() -> Self {
        Self::new()
    }
}

impl Repl {
    pub fn new() -> Self {
        // let mut rl = ;
        Repl {
            editor: Editor::<()>::new().unwrap(),
            prompt: String::new(),
        }
    }

    pub fn run(&mut self) {
        self.prompt = String::from("\x1b[1;32m[Course-DDBS]> \x1b[0m");
        loop {
            let line = self.editor.readline(&self.prompt);
            if self.process(line).is_err() {
                break;
            }
        }
    }

    fn process(&mut self, line: rustyline::Result<String>) -> Result<(), ()> {
        match line {
            Ok(statement) => {
                if statement.starts_with(':') {
                    println!("Test1: input control command not sql.");
                } else {
                    let timer = std::time::Instant::now();
                    std::thread::sleep(Duration::from_millis(1));
                    println!("Test[Exec query]: {}", statement);
                    let total_time = timer.elapsed();
                    println!("Query time: {:.2} ms", total_time.as_secs_f64() * 1000.0);
                }
            }
            Err(ReadlineError::Interrupted) => return Err(()),
            Err(err) => {
                eprintln!("An error occurred: {:?}", err);
                return Err(());
            }
        }
        Ok(())
    }
}
