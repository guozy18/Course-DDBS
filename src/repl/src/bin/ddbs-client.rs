use common::Result;
use repl::Repl;

#[tokio::main]
async fn main() -> Result<()> {
    let mut repl = Repl::new().await?;
    repl.run().await?;
    Ok(())
}
