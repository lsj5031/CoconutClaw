use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::num::NonZeroU32;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use coconutclaw_gateway::executor_wire::{CancelEvidence, Event, Grant, Phase, Status};
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::journal::{Intent, Journal};
use super::link::{Link, now_ms};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    origin: String,
    generation: NonZeroU32,
    object_id: String,
    placement: String,
    signing_key: PathBuf,
    incarnation: PathBuf,
    ca: Option<PathBuf>,
    codex: PathBuf,
    codex_home: PathBuf,
    workspace: PathBuf,
    timeout_seconds: u32,
    #[serde(default)]
    provider_env: Vec<String>,
}

impl Config {
    pub fn load(path: &Path, dir: &Path, init: bool) -> Result<(Self, Link, Journal)> {
        let path = path.canonicalize()?;
        let dir = if init {
            let name = dir
                .file_name()
                .context("expected a new journal directory")?;
            dir.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or(Path::new("."))
                .canonicalize()?
                .join(name)
        } else {
            dir.canonicalize()?
        };
        let mut config: Self = serde_json::from_slice(&private_file(&path, &dir)?)
            .map_err(|_| anyhow::anyhow!("invalid private runner configuration"))?;
        ensure!(
            lower_hex(&config.object_id)
                && config.placement == coconutclaw_gateway::executor_wire::DEVICE
                && (1..=3600).contains(&config.timeout_seconds)
                && config.codex.is_absolute()
                && config.codex_home.is_absolute()
                && config.workspace.is_absolute(),
            "invalid production runner placement, duration or paths"
        );
        config.codex = config.codex.canonicalize()?;
        config.workspace = config.workspace.canonicalize()?;
        config.codex_home = config.codex_home.canonicalize()?;
        ensure!(
            config.codex.is_file()
                && config.workspace.is_dir()
                && config.codex_home.is_dir()
                && [
                    &path,
                    &config.signing_key.canonicalize()?,
                    &config.incarnation.canonicalize()?
                ]
                .into_iter()
                .all(|file| !file.starts_with(&config.workspace)
                    && !file.starts_with(&config.codex_home))
                && !config.codex_home.starts_with(&dir),
            "expected separate runner secrets, Codex home and workspace"
        );
        let seed = private_file(&config.signing_key, &dir)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("expected a 32-byte executor signing key"))?;
        let incarnation = private_file(&config.incarnation, &dir)?;
        let incarnation = std::str::from_utf8(&incarnation)?.trim_end_matches('\n');
        ensure!(lower_hex(incarnation), "invalid executor incarnation");
        let instance = format!("bigfur:{}:{}", config.generation, config.object_id);
        let ca = config.ca.as_ref().map(fs::read).transpose()?;
        let link = Link::production(
            &config.origin,
            ca.as_deref(),
            SigningKey::from_bytes(&seed),
            instance.clone(),
            incarnation,
        )?;
        let predecessor = (config.generation.get() > 1)
            .then(|| format!("bigfur:{}:", config.generation.get() - 1));
        let journal = Journal::open_scoped(
            &dir,
            &link.key,
            init,
            instance,
            predecessor.as_deref(),
            true,
        )?;
        Ok((config, link, journal))
    }

    pub fn launch(
        &self,
        link: &Link,
        journal: &Journal,
        dir: &Path,
        intent: Intent,
        grant: &Grant,
        start: Permit,
    ) -> Result<Event> {
        let mut supervisor = self.supervisor(journal.process_lock()?, dir)?;
        let control = supervisor
            .stdin
            .take()
            .context("missing supervisor control")?;
        let output = supervisor
            .stdout
            .take()
            .context("missing supervisor receipt")?;
        let result = (|| {
            let mut control = control;
            let attempt = intent.attempt;
            let status = link.status(attempt)?;
            let permit = Permit::fresh(status.authority_valid_until)?;
            if status.cancel_requested && status.start_until == Some(grant.start_until) {
                permit.check()?;
                return Ok(Event::Cancelled {
                    evidence: CancelEvidence::BeforeStart,
                });
            }
            ensure!(
                status.phase == Phase::Authorized && status.start_until == Some(grant.start_until),
                "fresh status does not authorize the original start grant"
            );
            let deadline = start.shorter(permit);
            deadline.check()?;
            send(
                &mut control,
                json!({ "deadline": deadline, "prompt": intent.prompt }),
            )?;

            let (receipts, received) = mpsc::channel();
            std::thread::spawn(move || {
                for line in BufReader::new(output).lines() {
                    let receipt = line
                        .ok()
                        .and_then(|line| serde_json::from_str::<Receipt>(&line).ok());
                    if receipts.send(receipt).is_err() {
                        break;
                    }
                }
            });
            let mut started = false;
            let interval = Duration::from_millis(250);
            let mut next_poll = Instant::now() + interval;
            loop {
                match received.recv_timeout(next_poll.saturating_duration_since(Instant::now())) {
                    Ok(Some(Receipt::Started)) if !started => {
                        journal.started(&link.key, attempt)?;
                        started = true;
                    }
                    Ok(Some(Receipt::Result { mut text })) if started => {
                        text.retain(|char| {
                            !char.is_control() || matches!(char, '\t' | '\n' | '\r')
                        });
                        let text = text.trim().to_owned();
                        return Ok(if text.is_empty() {
                            Event::Unknown
                        } else {
                            Event::Result { text }
                        });
                    }
                    Ok(Some(Receipt::Cancelled)) if started => {
                        return Ok(Event::Cancelled {
                            evidence: CancelEvidence::ProcessExited,
                        });
                    }
                    Ok(Some(Receipt::Unknown)) => return Ok(Event::Unknown),
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if started
                            && let Ok(status) = link.status(attempt)
                            && status.cancel_requested
                            && status.phase.has_grant()
                            && status.start_until == Some(grant.start_until)
                            && let Ok(deadline) = Permit::fresh(status.authority_valid_until)
                            && deadline.check().is_ok()
                        {
                            let _ = send(&mut control, json!({ "deadline": deadline }));
                        }
                        next_poll = Instant::now() + interval;
                    }
                    _ => bail!("owned Codex supervisor receipt unavailable"),
                }
            }
        })();
        if !supervisor.wait()?.success() {
            return Ok(Event::Unknown);
        }
        result
    }

    fn supervisor(&self, lock: File, dir: &Path) -> Result<std::process::Child> {
        let fd = lock.as_raw_fd();
        let mut command = Command::new("/usr/bin/python3");
        command
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .args(["-I", "-c", include_str!("owned_codex.py")])
            .arg(&self.codex)
            .arg(&self.codex_home)
            .arg(&self.workspace)
            .arg(dir.canonicalize()?)
            .arg(self.timeout_seconds.to_string())
            .arg(fd.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for name in &self.provider_env {
            ensure!(
                !name.starts_with("LD_")
                    && !name.starts_with("PYTHON")
                    && !matches!(
                        name.as_str(),
                        "PATH" | "HOME" | "CODEX_HOME" | "BASH_ENV" | "ENV"
                    ),
                "invalid provider environment variable"
            );
            command.env(
                name,
                std::env::var_os(name).context("provider environment missing")?,
            );
        }
        unsafe {
            command.pre_exec(move || {
                if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(command.spawn()?)
    }
}

#[derive(Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Receipt {
    Started,
    Result { text: String },
    Cancelled,
    Unknown,
}

fn send(control: &mut impl Write, message: serde_json::Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(&message)?;
    bytes.push(b'\n');
    control.write_all(&bytes)?;
    Ok(())
}

pub fn start_permit(grant: &Grant) -> Result<Permit> {
    let permit = Permit::fresh(grant.authority_valid_until)?
        .shorter(Permit::fresh(Some(grant.start_until))?);
    permit.check()?;
    Ok(permit)
}

pub fn check_cancel(status: &Status) -> Result<()> {
    Permit::fresh(status.authority_valid_until)?.check()
}

#[derive(Serialize)]
pub struct Permit {
    wall: i64,
    monotonic: i64,
}

impl Permit {
    fn fresh(wall: Option<i64>) -> Result<Self> {
        let monotonic = monotonic_ms()?;
        let wall = wall.context("missing production authority deadline")?;
        let remaining = wall.saturating_sub(now_ms()?);
        ensure!(
            (1..=2000).contains(&remaining),
            "invalid production authority deadline"
        );
        Ok(Self {
            wall,
            monotonic: monotonic + remaining,
        })
    }

    fn check(&self) -> Result<()> {
        ensure!(
            monotonic_ms()? < self.monotonic && now_ms()? < self.wall,
            "production authority expired"
        );
        Ok(())
    }

    fn shorter(self, other: Self) -> Self {
        Self {
            wall: self.wall.min(other.wall),
            monotonic: self.monotonic.min(other.monotonic),
        }
    }
}

fn monotonic_ms() -> Result<i64> {
    let mut clock = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut clock) } == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    let seconds: i64 = clock.tv_sec;
    let nanos = i128::from(clock.tv_nsec) / 1_000_000;
    Ok(seconds * 1000 + i64::try_from(nanos)?)
}

fn lower_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn private_file(path: &Path, journal: &Path) -> Result<Vec<u8>> {
    let path = path.canonicalize()?;
    let metadata = path.metadata()?;
    ensure!(
        path.is_absolute()
            && !path.starts_with(journal)
            && metadata.is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o077 == 0,
        "runner configuration and keys require owner-only files outside the journal"
    );
    Ok(fs::read(path)?)
}
