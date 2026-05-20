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
mod recovery;
mod scheduler;
mod scheduling;
mod service;
mod session;
mod slack;
mod store;
mod telegram;
mod turn;
mod types;
mod util;
mod webhook;

pub(crate) use types::*;

// Re-exports from sub-modules for backward compatibility
#[allow(unused_imports)]
pub(crate) use cancel::{
    CancelRouter, cancel_marker_path, clear_cancel_marker, signal_cancel_marker,
};
pub(crate) use coconutclaw_config::parse_on_off as parse_on_like;
pub(crate) use util::{
    asr_feature_enabled, command_exists, iso_now, resolve_instance_path, shorten_log_text,
};

use crate::store::Store;

#[derive(Parser, Debug)]
#[command(name = "coconutclaw", version, about = "CoconutClaw Rust CLI")]
struct Cli {
    #[arg(long, global = true)]
    instance: Option<String>,
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[arg(long = "instance-dir", global = true)]
    instance_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Once(TurnArgs),
    Run(TurnArgs),
    Heartbeat,
    NightlyReflection,
    Doctor(DoctorArgs),
    Service(ServiceArgs),
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
    Install {
        #[arg(long, default_value = "09:00")]
        heartbeat: String,
        #[arg(long, default_value = "22:30")]
        reflection: String,
    },
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
        instance: cli.instance.clone(),
        data_dir: cli.data_dir.clone(),
        instance_dir: cli.instance_dir.clone(),
    };

    let cfg = load_runtime_config(&overrides)?;
    let command = cli.command;

    if let Commands::Service(args) = &command {
        return service::run_service(&cfg, &overrides, args.clone());
    }

    let _instance_lock = cfg.acquire_instance_lock()?;
    let mut store = Store::open(&cfg)?;

    match command {
        Commands::Once(args) => commands::run_once(&cfg, &mut store, &args),
        Commands::Run(args) => commands::run_run(&cfg, &mut store, &args),
        Commands::Heartbeat => commands::run_heartbeat(&cfg, &mut store),
        Commands::NightlyReflection => commands::run_nightly_reflection(&cfg, &mut store),
        Commands::Doctor(args) => commands::run_doctor(&cfg, &args),
        Commands::Service(_) => unreachable!("service command handled before lock/store setup"),
    }
}
#[cfg(test)]
#[path = "main_integration_test.rs"]
mod tests;
