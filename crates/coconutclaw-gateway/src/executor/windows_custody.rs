use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::{self, Command, Stdio};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use coconutclaw_config::windows_security::{present, private_directory, validate_private};

pub const MODE: &str = "--windows-loopback-provider";
pub const HOLD_SECS: u64 = 20;

fn check_parent(path: &Path) -> Result<()> {
    private_directory(
        path.parent().context("missing fixture state parent")?,
        false,
    )
}

fn check_file(path: &Path) -> Result<()> {
    validate_private(path)?;
    ensure!(
        fs::symlink_metadata(path)?.is_file(),
        "fixture state must be a regular file"
    );
    Ok(())
}

pub fn check_input(path: &Path) -> Result<()> {
    check_parent(path)?;
    check_file(path)
}

pub fn check_journal_location(dir: &Path, init: bool) -> Result<()> {
    check_parent(dir)?;
    private_directory(dir, init)
}

pub fn prepare_journal(dir: &Path, init: bool) -> Result<()> {
    check_parent(dir).context("check executor journal parent")?;
    if init {
        fs::create_dir(dir)?;
    }
    private_directory(dir, false).context("check executor journal directory")?;
    for name in [
        "executor.lock",
        "executor.sqlite3",
        "executor.sqlite3-journal",
        "executor.sqlite3-wal",
        "executor.sqlite3-shm",
        "launches",
    ] {
        let path = dir.join(name);
        if present(&path)? {
            check_file(&path).with_context(|| format!("check executor {name} custody"))?;
        }
    }
    Ok(())
}

pub fn command(attempt: i64, hold: bool) -> Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    command.args([
        MODE,
        if hold { "hold" } else { "normal" },
        &attempt.to_string(),
    ]);
    Ok(command)
}

pub fn run(mut args: impl Iterator<Item = OsString>) -> Result<()> {
    let mode = args.next().context("missing fixture provider mode")?;
    if mode == "descendant" {
        ensure!(args.next().is_none(), "unexpected descendant argument");
        thread::sleep(Duration::from_secs(HOLD_SECS * 2));
        return Ok(());
    }
    ensure!(
        mode == "hold" || mode == "normal",
        "unknown fixture provider mode"
    );
    let attempt: u32 = args
        .next()
        .and_then(|arg| arg.to_str()?.parse().ok())
        .context("invalid fixture attempt")?;
    if !(1..=512).contains(&attempt) || args.next().is_some() {
        bail!("invalid fixture provider arguments");
    }
    let mut launches = OpenOptions::new()
        .create(true)
        .append(true)
        .open("launches")?;
    writeln!(launches, "{attempt}")?;
    if mode == "hold" {
        let mut descendant = Command::new(std::env::current_exe()?)
            .args([MODE, "descendant"])
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut tree = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(format!("tree-{attempt}"))?;
        writeln!(tree, "{} {}", process::id(), descendant.id())?;
        drop(tree);
        thread::sleep(Duration::from_secs(HOLD_SECS));
        let _ = descendant.try_wait()?;
    }
    let mut prompt = String::new();
    io::stdin().read_to_string(&mut prompt)?;
    let text: String = prompt
        .lines()
        .next()
        .unwrap_or_default()
        .chars()
        .take(100)
        .collect();
    println!("fixture output: {text}");
    Ok(())
}
