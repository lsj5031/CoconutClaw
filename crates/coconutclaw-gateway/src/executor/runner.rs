use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use coconutclaw_gateway::executor_wire::{
    CancelEvidence, Device, Event, Grant, Phase, Status, fixture_instance,
};
use coconutclaw_provider::{ChildBoundary, ChildExit, run_single_child};
use ed25519_dalek::SigningKey;
use rand::Rng;
use serde::Deserialize;
use serde_json::{Value, json};

use super::journal::{Intent, Journal};
use super::link::{Link, PilotPaused, Scope, Unavailable, lower_hex, now_ms};
#[cfg(target_os = "linux")]
use super::production::{self, Permit as AuthorityDeadline};
#[cfg(windows)]
use super::windows_custody as custody;

#[cfg(not(target_os = "linux"))]
type AuthorityDeadline = Deadline;

#[cfg(target_os = "macos")]
#[path = "macos_custody.rs"]
mod custody;

#[derive(Clone, Copy, PartialEq)]
enum Fixture {
    Normal,
    Hold,
    CrashBeforeSpawn,
    CrashAfterSpawn,
}

struct Executor {
    link: Link,
    journal: Journal,
    dir: PathBuf,
    execution: Execution,
}

enum Execution {
    Fixture(Fixture),
    #[cfg(target_os = "linux")]
    Production(Box<production::Config>),
}

pub fn run() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next();
    #[cfg(any(target_os = "macos", windows))]
    if mode.as_deref().is_some_and(|arg| arg == custody::MODE) {
        return custody::run(args);
    }
    #[cfg(target_os = "linux")]
    if mode.as_deref().is_some_and(|arg| arg == "--production") {
        let usage = "--production init|resume <private runner JSON> <journal directory>";
        let init = match args.next().as_deref().and_then(|arg| arg.to_str()) {
            Some("init") => true,
            Some("resume") => false,
            _ => bail!("{usage}"),
        };
        let config = PathBuf::from(args.next().context(usage)?);
        let dir = PathBuf::from(args.next().context(usage)?);
        ensure!(args.next().is_none(), "{usage}");
        let (config, link, journal) = production::Config::load(&config, &dir, init)?;
        return Executor {
            link,
            journal,
            dir,
            execution: Execution::Production(Box::new(config)),
        }
        .serve();
    }
    let usage = "--loopback-fixture init|resume <https-origin> <CA PEM> <32-byte key file> <journal directory> [--generation N | --enrollment N OBJECT_ID PLACEMENT INCARNATION] [hold|crash-before-spawn|crash-after-spawn]";
    ensure!(
        mode.is_some_and(|arg| arg == "--loopback-fixture"),
        "{usage}"
    );
    let init = match args.next().as_deref().and_then(|arg| arg.to_str()) {
        Some("init") => true,
        Some("resume") => false,
        _ => bail!("{usage}"),
    };
    let origin = args.next().context(usage)?;
    let cert = args.next().context(usage)?;
    let key_path = args.next().context(usage)?;
    let dir = PathBuf::from(args.next().context(usage)?);
    let mut behavior = args.next();
    let mut predecessor = None;
    let scope = match behavior.as_deref().and_then(|arg| arg.to_str()) {
        Some("--generation" | "--enrollment") => {
            let generation = args
                .next()
                .and_then(|arg| arg.to_str()?.parse::<NonZeroU32>().ok())
                .context(usage)?;
            let scope = if behavior.as_deref().is_some_and(|arg| arg == "--enrollment") {
                let object = args
                    .next()
                    .and_then(|arg| arg.into_string().ok())
                    .context(usage)?;
                let placement = args
                    .next()
                    .and_then(|arg| Device::named(arg.to_str()?))
                    .context(usage)?;
                let incarnation = args
                    .next()
                    .and_then(|arg| arg.into_string().ok())
                    .context(usage)?;
                ensure!(
                    lower_hex(&object) && lower_hex(&incarnation),
                    "invalid fixture enrollment"
                );
                Scope::Enrolled {
                    instance: format!("bigfur:{generation}:{object}"),
                    placement,
                    incarnation,
                }
            } else {
                predecessor =
                    (generation.get() > 1).then(|| fixture_instance(generation.get() - 1));
                Scope::Singleton(fixture_instance(generation.get()))
            };
            behavior = args.next();
            scope
        }
        _ => Scope::Singleton(fixture_instance(1)),
    };
    let fixture = match behavior {
        None => Fixture::Normal,
        Some(arg) if arg == "hold" => Fixture::Hold,
        Some(arg) if arg == "crash-before-spawn" => Fixture::CrashBeforeSpawn,
        Some(arg) if arg == "crash-after-spawn" => Fixture::CrashAfterSpawn,
        _ => bail!("{usage}"),
    };
    ensure!(args.next().is_none(), "{usage}");
    #[cfg(windows)]
    {
        ensure!(
            scope.placement().is_some(),
            "Windows loopback requires an explicit enrollment"
        );
        custody::check_input(Path::new(&key_path)).context("check fixture seed custody")?;
        custody::check_input(Path::new(&cert)).context("check fixture CA custody")?;
    }
    let seed = fs::read(key_path)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected a disposable 32-byte device key"))?;
    let key = SigningKey::from_bytes(&seed);
    let link = Link::new(
        origin.to_str().context(usage)?,
        &fs::read(cert)?,
        key,
        scope,
    )?;
    let journal = Journal::open_scoped(
        &dir,
        &link.key,
        init,
        link.scope.clone(),
        predecessor.as_deref(),
        false,
    )?;
    Executor {
        link,
        journal,
        dir,
        execution: Execution::Fixture(fixture),
    }
    .serve()
}

impl Executor {
    fn serve(&mut self) -> Result<()> {
        self.journal.recover(&self.link.key)?;
        let mut connected = false;
        let mut backoff_ms = 250;
        loop {
            let step = (|| {
                if !connected {
                    self.link.hello(&self.journal)?;
                    connected = true;
                }
                self.step()
            })();
            match step {
                Ok(idle) => {
                    backoff_ms = 250;
                    if idle {
                        thread::sleep(jitter(10_000));
                    }
                }
                Err(error) if error.is::<Unavailable>() => {
                    connected = false;
                    thread::sleep(jitter(backoff_ms));
                    backoff_ms = (backoff_ms * 2).min(6400);
                }
                Err(error) if error.is::<PilotPaused>() => {
                    connected = false;
                    let pause = error.downcast::<PilotPaused>()?;
                    thread::sleep(pause.0);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn step(&mut self) -> Result<bool> {
        if let Some(pending) = self.journal.pending()? {
            let reply: Value = self
                .link
                .post(
                    json!({"op":"report", "proof":pending.proof}),
                    self.journal.next_sequence()?,
                )?
                .context("signed evidence conflicts with the ledger")?;
            ensure!(
                reply["event"] == pending.event,
                "unexpected evidence acknowledgement"
            );
            self.journal.acknowledge(pending.id)?;
            return Ok(false);
        }
        if let Some(intent) = self.journal.intent()? {
            return self.launch(intent);
        }
        #[derive(Deserialize)]
        struct Next {
            task: Option<String>,
        }
        #[derive(Deserialize)]
        struct Claim {
            task: String,
            attempt: i64,
            prompt: String,
            placement: Option<String>,
        }
        let next: Next = self.link.read(json!({"op":"next"}))?;
        let Some(task) = next.task else {
            return Ok(true);
        };
        let Some(claim): Option<Claim> = self.link.post(
            json!({"op":"claim", "task":task}),
            self.journal.next_sequence()?,
        )?
        else {
            return Ok(true);
        };
        ensure!(claim.task == task, "claim bound to another task");
        let expected = self
            .link
            .scope
            .placement()
            .or_else(|| self.link.is_production().then_some(Device::Alpha));
        ensure!(
            expected.is_none_or(|placement| claim.placement.as_deref() == Some(placement.name())),
            "claim lacks the enrolled runner placement"
        );
        self.journal.remember(&Intent {
            attempt: claim.attempt,
            prompt: claim.prompt,
        })?;
        Ok(false)
    }

    fn launch(&mut self, intent: Intent) -> Result<bool> {
        let attempt = intent.attempt;
        let status = self.link.status(attempt)?;
        if self.resolve_without_launch(&status, false)? {
            return Ok(false);
        }
        let sequence = self.journal.next_sequence()?;
        let Some(grant): Option<Grant> = self
            .link
            .post(json!({"op":"start", "attempt":attempt}), sequence)?
        else {
            return Ok(true);
        };
        if grant.instance != self.link.scope.instance()
            || grant.attempt != attempt
            || grant.request_sequence != sequence
            || self
                .link
                .scope
                .placement()
                .is_some_and(|placement| grant.placement != Some(placement))
        {
            return Err(Unavailable.into());
        }
        #[cfg(target_os = "linux")]
        if let Execution::Production(config) = &self.execution {
            let permit = production::start_permit(&grant).map_err(|_| Unavailable)?;
            ensure!(
                self.journal.consume(attempt)?,
                "grant has already been consumed"
            );
            let event = config
                .launch(&self.link, &self.journal, &self.dir, intent, &grant, permit)
                .unwrap_or(Event::Unknown);
            self.journal.finish(&self.link.key, attempt, [event])?;
            return Ok(false);
        }
        let enrolled = self.link.scope.placement().is_some();
        let enrolled_deadline = if enrolled {
            Some(
                AuthorityDeadline::authority(Some(grant.start_until))
                    .map_err(|_| Unavailable)?
                    .shorter(
                        AuthorityDeadline::authority(grant.authority_valid_until)
                            .map_err(|_| Unavailable)?,
                    ),
            )
        } else {
            None
        };
        let status = self.link.status(attempt)?;
        if self.resolve_without_launch(&status, enrolled)? {
            return Ok(false);
        }
        ensure!(
            status.phase == Phase::Authorized && status.start_until == Some(grant.start_until),
            "grant conflicts with current authorization"
        );
        let deadline = if let Some(start) = enrolled_deadline {
            let deadline = start.shorter(
                AuthorityDeadline::authority(status.authority_valid_until)
                    .map_err(|_| Unavailable)?,
            );
            deadline.check().map_err(|_| Unavailable)?;
            #[cfg(target_os = "linux")]
            return self.launch_enrolled_fixture(intent, grant.start_until, deadline);
            #[cfg(not(target_os = "linux"))]
            deadline
        } else {
            let Some(deadline) = Deadline::new(grant.start_until, 5000)? else {
                return Ok(true);
            };
            deadline
        };
        ensure!(
            self.journal.consume(attempt)?,
            "grant has already been consumed"
        );
        #[cfg(target_os = "linux")]
        let Execution::Fixture(fixture) = self.execution else {
            unreachable!();
        };
        #[cfg(not(target_os = "linux"))]
        let Execution::Fixture(fixture) = self.execution;
        let events = run_child(
            &self.link,
            &self.dir,
            intent,
            grant.start_until,
            deadline,
            fixture,
        );
        self.journal.finish(&self.link.key, attempt, events)?;
        Ok(false)
    }

    #[cfg(target_os = "linux")]
    fn launch_enrolled_fixture(
        &mut self,
        intent: Intent,
        start_until: i64,
        deadline: AuthorityDeadline,
    ) -> Result<bool> {
        let Execution::Fixture(fixture) = self.execution else {
            unreachable!();
        };
        let attempt = intent.attempt;
        ensure!(
            self.journal.consume(attempt)?,
            "grant has already been consumed"
        );
        if fixture == Fixture::CrashBeforeSpawn {
            std::process::exit(72);
        }
        let mut command = fixture_command(attempt, fixture == Fixture::Hold);
        command.arg("detached");
        let event = (|| {
            let supervisor =
                production::fixture_supervisor(self.journal.process_lock()?, &self.dir, &command)?;
            production::supervise(
                supervisor,
                &self.link,
                &intent,
                start_until,
                || Ok(Some(deadline)),
                || {
                    if fixture == Fixture::CrashAfterSpawn {
                        std::process::exit(73);
                    }
                    self.journal.started(&self.link.key, attempt)
                },
            )
        })()
        .unwrap_or(Event::Unknown);
        self.journal.finish(&self.link.key, attempt, [event])?;
        Ok(false)
    }

    fn resolve_without_launch(
        &mut self,
        status: &Status,
        received_enrolled_grant: bool,
    ) -> Result<bool> {
        let terminal = if status.cancel_requested || status.phase == Phase::CancelledBeforeStart {
            #[cfg(target_os = "linux")]
            if self.link.is_production() {
                production::check_cancel(status).map_err(|_| Unavailable)?;
            }
            if self.link.scope.placement().is_some() {
                AuthorityDeadline::authority(status.authority_valid_until)
                    .map_err(|_| Unavailable)?
                    .check()
                    .map_err(|_| Unavailable)?;
            }
            Some(Event::Cancelled {
                evidence: CancelEvidence::BeforeStart,
            })
        } else {
            match status.phase {
                Phase::Claimed => return Ok(false),
                Phase::Authorized if received_enrolled_grant => return Ok(false),
                Phase::Authorized
                    if !self.link.is_production()
                        && self.link.scope.placement().is_none()
                        && status
                            .start_until
                            .is_some_and(|until| now_ms().is_ok_and(|now| now < until)) =>
                {
                    return Ok(false);
                }
                Phase::Authorized => Some(Event::Unknown),
                Phase::Unknown if status.start_until.is_some() => Some(Event::Unknown),
                Phase::Unknown | Phase::Expired => None,
                _ => bail!("unexpected ledger phase for unconsumed attempt"),
            }
        };
        self.journal
            .finish(&self.link.key, status.attempt, terminal)?;
        Ok(true)
    }
}

fn jitter(base_ms: u64) -> Duration {
    Duration::from_millis(base_ms + rand::rng().random_range(0..=base_ms / 4))
}

struct Deadline {
    wall: i64,
    monotonic: Instant,
}

impl Deadline {
    fn new(wall: i64, max_ms: i64) -> Result<Option<Self>> {
        let monotonic_now = Instant::now();
        let remaining = wall.saturating_sub(now_ms()?);
        Ok((1..=max_ms).contains(&remaining).then(|| Self {
            wall,
            monotonic: monotonic_now + Duration::from_millis(remaining as u64),
        }))
    }

    #[cfg(not(target_os = "linux"))]
    fn authority(wall: Option<i64>) -> Result<Self> {
        Self::new(wall.ok_or(Unavailable)?, 2000)?.ok_or_else(|| Unavailable.into())
    }

    #[cfg(not(target_os = "linux"))]
    fn shorter(self, other: Self) -> Self {
        Self {
            wall: self.wall.min(other.wall),
            monotonic: self.monotonic.min(other.monotonic),
        }
    }

    fn check(&self) -> Result<()> {
        ensure!(
            Instant::now() < self.monotonic && now_ms()? < self.wall,
            "start grant expired"
        );
        Ok(())
    }
}

#[cfg(unix)]
fn fixture_command(attempt: i64, hold: bool) -> Command {
    let mut command = Command::new("/bin/sh");
    command.args([
        "-c",
        r#"
            printf '%s\n' "$1" >> launches || exit 1
            if [ "$2" = hold ]; then
                if [ "$3" = detached ]; then
                    /usr/bin/python3 -I -c 'import os; os.setsid(); os.execl("/bin/sleep", "sleep", "6")' &
                    wait
                else
                    /bin/sleep 6
                fi
            fi
            IFS= read -r prompt || :
            printf 'fixture output: %.100s\n' "$prompt"
        "#,
        "fixture",
        &attempt.to_string(),
        if hold { "hold" } else { "" },
    ]);
    command
}

fn run_child(
    link: &Link,
    dir: &Path,
    intent: Intent,
    start_until: i64,
    deadline: Deadline,
    fixture: Fixture,
) -> Vec<Event> {
    let started = AtomicBool::new(false);
    let cancel = Arc::new(AtomicBool::new(false));
    let attempt = intent.attempt;
    let exit = thread::scope(|scope| {
        let child = scope.spawn(|| {
            let hold = fixture == Fixture::Hold;
            #[cfg(unix)]
            let mut command = fixture_command(attempt, hold);
            #[cfg(target_os = "macos")]
            if link.scope.placement().is_some() {
                command = custody::command(attempt, hold, &deadline)?;
            }
            #[cfg(windows)]
            let mut command = custody::command(attempt, hold)?;
            command.current_dir(dir);
            #[cfg(windows)]
            let timeout = custody::HOLD_SECS + 5;
            #[cfg(unix)]
            let timeout = 8;
            run_single_child(
                command,
                &intent.prompt,
                &cancel,
                timeout,
                ChildBoundary {
                    before_spawn: &|| {
                        if fixture == Fixture::CrashBeforeSpawn {
                            std::process::exit(72);
                        }
                        deadline.check()
                    },
                    after_spawn: &|| {
                        if fixture == Fixture::CrashAfterSpawn {
                            std::process::exit(73);
                        }
                        started.store(true, Ordering::Release);
                    },
                },
            )
        });
        let mut next_poll = Instant::now() + Duration::from_millis(250);
        while !child.is_finished() {
            if Instant::now() >= next_poll && !cancel.load(Ordering::Acquire) {
                let delay = match link.status(attempt) {
                    Ok(status)
                        if status.cancel_requested
                            && (link.scope.placement().is_none()
                                || (status.phase.has_grant()
                                    && status.start_until == Some(start_until)
                                    && AuthorityDeadline::authority(
                                        status.authority_valid_until,
                                    )
                                    .is_ok_and(|permit| permit.check().is_ok()))) =>
                    {
                        cancel.store(true, Ordering::Release);
                        Duration::from_millis(250)
                    }
                    Err(error) if error.is::<PilotPaused>() => error
                        .downcast_ref::<PilotPaused>()
                        .map_or(Duration::from_secs(60), |pause| pause.0),
                    _ => Duration::from_millis(250),
                };
                next_poll = Instant::now() + delay;
            }
            thread::sleep(Duration::from_millis(25));
        }
        child.join().ok().and_then(Result::ok)
    });
    let mut events = Vec::new();
    if started.load(Ordering::Acquire) {
        events.push(Event::Started);
    }
    events.push(match exit {
        Some(ChildExit::Success(text)) => Event::Result {
            text: text.trim_end().to_owned(),
        },
        Some(ChildExit::Cancelled) => Event::Cancelled {
            evidence: CancelEvidence::ProcessExited,
        },
        _ => Event::Unknown,
    });
    events
}
