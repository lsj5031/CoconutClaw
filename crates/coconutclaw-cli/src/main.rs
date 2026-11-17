use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use coconutclaw_config::{CliOverrides, load_runtime_config};
use std::path::PathBuf;

mod approval;
mod cancel;
mod commands;
mod context;
mod delivery;
mod loops;
mod markers;
mod memory_import;
mod recovery;
mod scheduler;
mod scheduling;
mod service;
mod session;
mod store;
mod telegram;
#[cfg(test)]
mod test_runtime;
mod turn;
mod types;
mod util;

pub(crate) use types::*;

// Re-exports from sub-modules for backward compatibility
#[allow(unused_imports)]
pub(crate) use cancel::{CancelRouter, cancel_marker_path, clear_cancel_marker};
pub(crate) use util::{command_exists, iso_now, resolve_instance_path, shorten_log_text};

use crate::store::Store;

#[derive(Parser, Debug)]
#[command(name = "coconutclaw", version, about = "CoconutClaw Rust CLI")]
struct Cli {
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Once(TurnArgs),
    Run(TurnArgs),
    Doctor(DoctorArgs),
    Service(ServiceArgs),
    ImportOpenclaw(memory_import::ImportArgs),
    Schedule(commands::schedule::ScheduleArgs),
}

#[derive(Args, Debug, Clone)]
struct DoctorArgs {
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct TurnArgs {
    #[arg(long)]
    inject_text: Option<String>,
    #[arg(long)]
    inject_file: Option<PathBuf>,
    #[arg(long)]
    chat_id: Option<String>,
}

#[derive(Args, Debug, Clone)]
struct ServiceArgs {
    #[command(subcommand)]
    action: ServiceAction,
}

#[derive(Subcommand, Debug, Clone)]
enum ServiceAction {
    Install,
    Start,
    Stop,
    Status,
    Uninstall,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let overrides = CliOverrides {
        data_dir: cli.data_dir.clone(),
    };

    if let Commands::ImportOpenclaw(args) = &cli.command {
        let cfg = coconutclaw_config::preview_runtime_config(&overrides)?;
        return memory_import::run(&cfg, args);
    }
    let cfg = load_runtime_config(&overrides)?;
    let command = cli.command;

    // Administrative calendar edits use SQLite transactions, not execution
    // ownership. They must remain available while the single core is running.
    if let Commands::Schedule(args) = &command {
        let store = Store::open(&cfg)?;
        println!(
            "{}",
            commands::schedule::run(&cfg, &store, args, chrono::Utc::now())?
        );
        return Ok(());
    }
    if let Commands::Service(args) = &command {
        return service::run_service(&cfg, &overrides, args.clone());
    }

    let _instance_lock = cfg.acquire_instance_lock()?;
    let mut store = Store::open(&cfg)?;

    match command {
        Commands::Once(args) => commands::run_once(&cfg, &mut store, &args),
        Commands::Run(args) => commands::run_run(&cfg, &mut store, &args),
        Commands::Doctor(args) => commands::run_doctor(&cfg, &args),
        Commands::Service(_) => unreachable!("service command handled before lock/store setup"),
        Commands::ImportOpenclaw(_) => unreachable!("import handled before store setup"),
        Commands::Schedule(_) => unreachable!("calendar command handled before execution lock"),
    }
}
#[cfg(test)]
#[path = "main_integration_test.rs"]
mod tests;
