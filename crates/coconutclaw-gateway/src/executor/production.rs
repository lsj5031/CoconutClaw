use std::fs;
#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::io::{BufRead, BufReader, Write};
use std::num::NonZeroU32;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(target_os = "linux")]
use std::process::{Child, Stdio};
#[cfg(target_os = "linux")]
use std::sync::mpsc;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use anyhow::bail;
use anyhow::{Context, Result, ensure};
use coconutclaw_gateway::executor_wire::Device;
#[cfg(target_os = "linux")]
use coconutclaw_gateway::executor_wire::{CancelEvidence, Event};
use ed25519_dalek::SigningKey;
use serde::Deserialize;
#[cfg(target_os = "linux")]
use serde::Serialize;
#[cfg(target_os = "linux")]
use serde_json::json;

#[cfg(target_os = "linux")]
use super::journal::Intent;
use super::journal::Journal;
#[cfg(target_os = "linux")]
use super::link::now_ms;
use super::link::{Link, Scope, lower_hex};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    origin: String,
    generation: Option<NonZeroU32>,
    object_id: Option<String>,
    placement: Option<Device>,
    signing_key: PathBuf,
    incarnation: Option<PathBuf>,
    enrollment: Option<PathBuf>,
    ca: Option<PathBuf>,
    codex: PathBuf,
    #[serde(default)]
    codex_sandbox: CodexSandbox,
    codex_home: PathBuf,
    workspace: PathBuf,
    timeout_seconds: u32,
    #[serde(default)]
    provider_env: Vec<String>,
}

#[derive(Default, Deserialize)]
#[serde(try_from = "String")]
enum CodexSandbox {
    #[default]
    WorkspaceWrite,
    DangerFullAccess,
}

impl TryFrom<String> for CodexSandbox {
    type Error = &'static str;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.as_str() {
            "workspace-write" => Ok(Self::WorkspaceWrite),
            "danger-full-access" => Ok(Self::DangerFullAccess),
            _ => Err("invalid Codex sandbox mode"),
        }
    }
}

impl CodexSandbox {
    fn as_arg(&self) -> &'static str {
        match self {
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Enrollment {
    generation: NonZeroU32,
    object_id: String,
    placement: Device,
    incarnation: String,
}

impl Config {
    pub fn load(path: &Path, dir: &Path, init: bool) -> Result<(Self, Link, Journal)> {
        check_private_file(path)?;
        let path = path.canonicalize()?;
        #[cfg(windows)]
        super::windows_custody::check_journal_location(dir, init)?;
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
        let (enrollment, enrolled, enrollment_path) = match (
            &config.enrollment,
            config.generation,
            &config.object_id,
            config.placement,
            &config.incarnation,
        ) {
            (Some(receipt), None, None, None, None) => (
                serde_json::from_slice(&private_file(receipt, &dir)?)
                    .context("invalid private runner enrollment")?,
                true,
                receipt,
            ),
            (None, Some(generation), Some(object_id), Some(Device::Alpha), Some(incarnation))
                if cfg!(target_os = "linux") =>
            {
                let bytes = private_file(incarnation, &dir)?;
                (
                    Enrollment {
                        generation,
                        object_id: object_id.clone(),
                        placement: Device::Alpha,
                        incarnation: std::str::from_utf8(&bytes)?
                            .trim_end_matches('\n')
                            .to_owned(),
                    },
                    false,
                    incarnation,
                )
            }
            _ => anyhow::bail!("expected an exact runner enrollment receipt"),
        };
        ensure!(
            lower_hex(&enrollment.object_id)
                && lower_hex(&enrollment.incarnation)
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
                    &enrollment_path.canonicalize()?
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
        let instance = format!("bigfur:{}:{}", enrollment.generation, enrollment.object_id);
        let scope = if enrolled {
            Scope::Enrolled {
                instance,
                placement: enrollment.placement,
                incarnation: enrollment.incarnation.clone(),
            }
        } else {
            Scope::Singleton(instance)
        };
        #[cfg(windows)]
        if let Some(ca) = &config.ca {
            check_private_file(ca)?;
        }
        let ca = config.ca.as_ref().map(fs::read).transpose()?;
        let link = Link::production(
            &config.origin,
            ca.as_deref(),
            SigningKey::from_bytes(&seed),
            scope,
            &enrollment.incarnation,
        )?;
        let predecessor = (!enrolled && enrollment.generation.get() > 1)
            .then(|| format!("bigfur:{}:", enrollment.generation.get() - 1));
        let journal = Journal::open_scoped(
            &dir,
            &link.key,
            init,
            link.scope.clone(),
            predecessor.as_deref(),
            !enrolled,
            enrolled,
        )?;
        Ok((config, link, journal))
    }

    #[cfg(target_os = "linux")]
    pub fn launch(
        &self,
        link: &Link,
        journal: &Journal,
        dir: &Path,
        intent: Intent,
        start_until: i64,
        start: Permit,
    ) -> Result<Event> {
        let supervisor = self.supervisor(journal.process_lock()?, dir)?;
        let attempt = intent.attempt;
        supervise(supervisor, link, &intent, start_until, start, || {
            journal.started(&link.key, attempt)
        })
    }

    #[cfg(target_os = "linux")]
    fn supervisor(&self, lock: File, dir: &Path) -> Result<Child> {
        let mut command = supervisor_command(&lock);
        command
            .arg(&self.codex)
            .arg(&self.codex_home)
            .arg(&self.workspace)
            .arg(dir.canonicalize()?)
            .arg(self.timeout_seconds.to_string())
            .arg(lock.as_raw_fd().to_string())
            .arg(self.codex_sandbox.as_arg());
        self.provider_environment(&mut command)?;
        Ok(command.spawn()?)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn command(&self, output: &Path) -> Result<(Command, u64)> {
        let mut command = Command::new(&self.codex);
        self.provider_environment(&mut command)?;
        command
            .env("HOME", &self.codex_home)
            .env("CODEX_HOME", &self.codex_home)
            .current_dir(&self.workspace)
            .args([
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "--sandbox",
                self.codex_sandbox.as_arg(),
                "-c",
                "approval_policy=\"never\"",
                "-c",
                "shell_environment_policy.inherit=\"none\"",
                "--output-last-message",
            ])
            .arg(output)
            .arg("-");
        #[cfg(target_os = "macos")]
        command.env("PATH", "/usr/local/bin:/usr/bin:/bin");
        #[cfg(windows)]
        {
            let system =
                PathBuf::from(std::env::var_os("SystemRoot").context("missing SystemRoot")?);
            ensure!(system.is_absolute(), "invalid SystemRoot");
            command
                .env("SystemRoot", &system)
                .env("PATH", system.join("System32"))
                .env("USERPROFILE", &self.codex_home);
        }
        Ok((command, u64::from(self.timeout_seconds)))
    }

    fn provider_environment(&self, command: &mut Command) -> Result<()> {
        for name in &self.provider_env {
            let upper = name.to_ascii_uppercase();
            ensure!(
                !upper.starts_with("LD_")
                    && !upper.starts_with("DYLD_")
                    && !upper.starts_with("PYTHON")
                    && !matches!(
                        upper.as_str(),
                        "PATH"
                            | "HOME"
                            | "CODEX_HOME"
                            | "BASH_ENV"
                            | "ENV"
                            | "SYSTEMROOT"
                            | "USERPROFILE"
                    ),
                "invalid provider environment variable"
            );
            command.env(
                name,
                std::env::var_os(name).context("provider environment missing")?,
            );
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub fn fixture_supervisor(lock: File, dir: &Path, fixture: &Command) -> Result<Child> {
    let dir = dir.canonicalize()?;
    Ok(supervisor_command(&lock)
        .arg(fixture.get_program())
        .arg(&dir)
        .arg(&dir)
        .arg(&dir)
        .arg("8")
        .arg(lock.as_raw_fd().to_string())
        .arg("--fixture")
        .args(fixture.get_args())
        .spawn()?)
}

#[cfg(target_os = "linux")]
fn supervisor_command(lock: &File) -> Command {
    let fd = lock.as_raw_fd();
    let mut command = Command::new("/usr/bin/python3");
    command
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .args(["-I", "-c", include_str!("owned_codex.py")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command
}

#[cfg(target_os = "linux")]
pub fn supervise(
    mut supervisor: Child,
    link: &Link,
    intent: &Intent,
    start_until: i64,
    deadline: Permit,
    record_started: impl Fn() -> Result<()>,
) -> Result<Event> {
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
                    record_started()?;
                    started = true;
                }
                Ok(Some(Receipt::Result { mut text })) if started => {
                    text.retain(|char| !char.is_control() || matches!(char, '\t' | '\n' | '\r'));
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
                        && status.start_until == Some(start_until)
                        && let Ok(deadline) = Permit::authority(status.authority_valid_until)
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

#[cfg(target_os = "linux")]
#[derive(Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Receipt {
    Started,
    Result { text: String },
    Cancelled,
    Unknown,
}

#[cfg(target_os = "linux")]
fn send(control: &mut impl Write, message: serde_json::Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(&message)?;
    bytes.push(b'\n');
    control.write_all(&bytes)?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Serialize)]
pub struct Permit {
    wall: i64,
    monotonic: i64,
}

#[cfg(target_os = "linux")]
impl Permit {
    pub fn authority(wall: Option<i64>) -> Result<Self> {
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

    pub fn check(&self) -> Result<()> {
        ensure!(
            monotonic_ms()? < self.monotonic && now_ms()? < self.wall,
            "production authority expired"
        );
        Ok(())
    }

    pub fn shorter(self, other: Self) -> Self {
        Self {
            wall: self.wall.min(other.wall),
            monotonic: self.monotonic.min(other.monotonic),
        }
    }
}

#[cfg(target_os = "linux")]
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

fn private_file(path: &Path, journal: &Path) -> Result<Vec<u8>> {
    check_private_file(path)?;
    let path = path.canonicalize()?;
    ensure!(
        !path.starts_with(journal),
        "runner configuration and keys require owner-only files outside the journal"
    );
    Ok(fs::read(path)?)
}

#[cfg(unix)]
fn check_private_file(path: &Path) -> Result<()> {
    let metadata = path.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o077 == 0,
        "runner configuration and keys require owner-only files"
    );
    Ok(())
}

#[cfg(windows)]
fn check_private_file(path: &Path) -> Result<()> {
    super::windows_custody::check_input(path)
}
