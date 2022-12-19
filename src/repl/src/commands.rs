use crate::Repl;

pub use async_trait::async_trait;
use common::TemporalGranularity;

/// The command api table.
pub const COMMAND_HANDLERS: [&'static dyn CommandHandler; 7] = [
    &ExitHandler,
    &HelpHandler,
    &ClusterInitHandler,
    &LoadBeReadHandler,
    &LoadMonthlyPopularTableHandler,
    &LoadDailyPopularTableHandler,
    &LoadWeeklyPopularTableHandler,
];

#[async_trait]
pub trait CommandHandler {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    async fn exec(
        &self,
        repl: &mut Repl,
        args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

/// Exit the REPL.
pub struct ExitHandler;

#[async_trait]
impl CommandHandler for ExitHandler {
    fn name(&self) -> &'static str {
        ":q"
    }

    fn description(&self) -> &'static str {
        "Quit this application."
    }

    async fn exec(
        &self,
        _repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        std::process::exit(0)
    }
}

/// Print the help message.
pub struct HelpHandler;

#[async_trait]
impl CommandHandler for HelpHandler {
    fn name(&self) -> &'static str {
        ":h"
    }

    fn description(&self) -> &'static str {
        "Show help message."
    }

    async fn exec(
        &self,
        _repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for handler in COMMAND_HANDLERS.iter() {
            println!("{} - {}", handler.name(), handler.description());
        }
        Ok(())
    }
}

/// Cluster Init
///
/// cluster_init rpc will **bulk load** the three tables:
/// user, article and user_read
pub struct ClusterInitHandler;

#[async_trait]
impl CommandHandler for ClusterInitHandler {
    fn name(&self) -> &'static str {
        ":cluster-init"
    }

    fn description(&self) -> &'static str {
        "Init Database cluster."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        repl.control_client.cluster_init(()).await?;
        Ok(())
    }
}

/// LoadBeRead
pub struct LoadBeReadHandler;

#[async_trait]
impl CommandHandler for LoadBeReadHandler {
    fn name(&self) -> &'static str {
        ":load-be-read"
    }

    fn description(&self) -> &'static str {
        "generator and load the be read table."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        repl.control_client.generate_be_read_table(()).await?;
        Ok(())
    }
}

pub struct LoadMonthlyPopularTableHandler;

#[async_trait]
impl CommandHandler for LoadMonthlyPopularTableHandler {
    fn name(&self) -> &'static str {
        ":load-monthly-popular-table"
    }

    fn description(&self) -> &'static str {
        "generator and load the monthly popular table."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let granularity = &TemporalGranularity::Monthly;
        repl.control_client
            .generate_popular_table(*granularity as i32)
            .await?;
        Ok(())
    }
}

pub struct LoadWeeklyPopularTableHandler;

#[async_trait]
impl CommandHandler for LoadWeeklyPopularTableHandler {
    fn name(&self) -> &'static str {
        ":load-weekly-popular-table"
    }

    fn description(&self) -> &'static str {
        "generator and load the weekly popular table."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let granularity = &TemporalGranularity::Weekly;
        repl.control_client
            .generate_popular_table(*granularity as i32)
            .await?;
        Ok(())
    }
}

/// LoadPopularTable
pub struct LoadDailyPopularTableHandler;

#[async_trait]
impl CommandHandler for LoadDailyPopularTableHandler {
    fn name(&self) -> &'static str {
        ":load-daily-popular-table"
    }

    fn description(&self) -> &'static str {
        "generator and load the daily popular table."
    }

    async fn exec(
        &self,
        repl: &mut Repl,
        _args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let granularity = &TemporalGranularity::Daily;
        repl.control_client
            .generate_popular_table(*granularity as i32)
            .await?;
        Ok(())
    }
}

/// The main function for handling commands.
pub async fn handle_commands(repl: &mut Repl, args: Vec<String>) {
    let cmd = args[0].clone();
    for handler in COMMAND_HANDLERS.iter() {
        if handler.name() == cmd {
            if let Err(err) = handler.exec(repl, args).await {
                eprintln!("{err:?}");
            }
            return;
        }
    }
    eprintln!("Unknown command {cmd}.");
}
