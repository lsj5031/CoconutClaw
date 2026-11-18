use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use coconutclaw_config::CliOverrides;
use std::path::PathBuf;

mod approval;
mod cancel;
mod commands;
mod context;
mod delivery;
mod loops;
mod markers;
mod memory_import;
mod nodes;
mod recovery;
mod runner_wire;
mod scheduler;
mod scheduling;
mod service;
mod session;
mod store;
mod telegram;
mod telegram_ui;
#[cfg(test)]
mod test_runtime;
mod turn;
mod types;
mod util;
mod wording;

pub(crate) use types::*;

// Re-exports from sub-modules for backward compatibility
#[allow(unused_imports)]
pub(crate) use cancel::{CancelRouter, cancel_marker_path, clear_cancel_marker};
pub(crate) use util::{command_exists, iso_now, resolve_instance_path, shorten_log_text};

use crate::store::Store;

#[derive(Parser, Debug)]
#[command(
    name = "coconutclaw",
    version,
    about = "Run CoconutClaw and manage local tasks"
)]
struct Cli {
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Process one request, then exit.
    Once(TurnArgs),
    /// Listen for Telegram requests and scheduled tasks.
    Run(TurnArgs),
    /// Check local setup without contacting Telegram or Codex.
    Doctor(DoctorArgs),
    /// Manage the background service for this user.
    Service(ServiceArgs),
    /// Import explicitly selected local OpenClaw files.
    ImportOpenclaw(memory_import::ImportArgs),
    /// Create and manage scheduled tasks.
    Schedule(commands::schedule::ScheduleArgs),
    /// Pair execution nodes, inspect saved checks and recover run evidence.
    Nodes(nodes::Args),
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

    if let Commands::Doctor(args) = &cli.command {
        let cfg = coconutclaw_config::preview_runtime_config(&overrides)?;
        return commands::run_doctor(&cfg, args);
    }
    if let Commands::ImportOpenclaw(args) = &cli.command {
        let cfg = coconutclaw_config::preview_runtime_config(&overrides)?;
        return memory_import::run(&cfg, args);
    }
    #[cfg(not(windows))]
    let cfg = coconutclaw_config::load_runtime_config(&overrides)?;
    #[cfg(windows)]
    let cfg = nodes::load_checked(&overrides, &cli.command)?;
    let command = cli.command;
    if let Commands::Nodes(args) = &command {
        return nodes::run(&cfg, args);
    }
    if matches!(&command, Commands::Run(_) | Commands::Once(_)) {
        nodes::require_coordinator(&cfg)?;
    }

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
        Commands::Doctor(_) => unreachable!("doctor handled before layout/lock/store setup"),
        Commands::Service(_) => unreachable!("service command handled before lock/store setup"),
        Commands::ImportOpenclaw(_) => unreachable!("import handled before store setup"),
        Commands::Schedule(_) => unreachable!("calendar command handled before execution lock"),
        Commands::Nodes(_) => unreachable!("nodes handled before execution lock"),
    }
}
#[cfg(test)]
#[path = "main_integration_test.rs"]
mod tests;
