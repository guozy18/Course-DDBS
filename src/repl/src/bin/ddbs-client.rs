use repl::Repl;

fn main() -> Result<(), ()> {
    let mut repl = Repl::default();
    repl.run();
    Ok(())
}
