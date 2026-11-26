use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use coconutclaw_gateway::executor_wire::{
    CancelEvidence, Event, Grant, Phase, Status, fixture_instance,
};
use coconutclaw_provider::{ChildBoundary, ChildExit, run_single_child};
use ed25519_dalek::SigningKey;
use rand::Rng;
use serde::Deserialize;
use serde_json::{Value, json};

use super::journal::{Intent, Journal};
use super::link::{Link, PilotPaused, Unavailable, now_ms};
#[cfg(target_os = "linux")]
use super::production;

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
    let usage = "--loopback-fixture init|resume <https-origin> <CA PEM> <32-byte key file> <journal directory> [--generation N] [hold|crash-before-spawn|crash-after-spawn]";
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
    let generation = if behavior.as_deref().is_some_and(|arg| arg == "--generation") {
        let generation = args
            .next()
            .and_then(|arg| arg.to_str()?.parse::<u32>().ok())
            .filter(|generation| *generation > 0)
            .context(usage)?;
        behavior = args.next();
        generation
    } else {
        1
    };
    let fixture = match behavior {
        None => Fixture::Normal,
        Some(arg) if arg == "hold" => Fixture::Hold,
        Some(arg) if arg == "crash-before-spawn" => Fixture::CrashBeforeSpawn,
        Some(arg) if arg == "crash-after-spawn" => Fixture::CrashAfterSpawn,
        _ => bail!("{usage}"),
    };
    ensure!(args.next().is_none(), "{usage}");
    let seed = fs::read(key_path)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected a disposable 32-byte device key"))?;
    let key = SigningKey::from_bytes(&seed);
    let link = Link::new(
        origin.to_str().context(usage)?,
        &fs::read(cert)?,
        key,
        fixture_instance(generation),
    )?;
    let journal = Journal::open(&dir, &link.key, init, generation)?;
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
        ensure!(
            !self.link.is_production()
                || claim.placement.as_deref() == Some(coconutclaw_gateway::executor_wire::DEVICE),
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
        if self.resolve_without_launch(&status)? {
            return Ok(false);
        }
        let sequence = self.journal.next_sequence()?;
        let Some(grant): Option<Grant> = self
            .link
            .post(json!({"op":"start", "attempt":attempt}), sequence)?
        else {
            return Ok(true);
        };
        if grant.instance != self.link.instance
            || grant.attempt != attempt
            || grant.request_sequence != sequence
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
        let status = self.link.status(attempt)?;
        if self.resolve_without_launch(&status)? {
            return Ok(false);
        }
        ensure!(
            status.phase == Phase::Authorized && status.start_until == Some(grant.start_until),
            "grant conflicts with current authorization"
        );
        let Some(deadline) = Deadline::from_grant(&grant)? else {
            return Ok(true);
        };
        ensure!(
            self.journal.consume(attempt)?,
            "grant has already been consumed"
        );
        let Execution::Fixture(fixture) = self.execution else {
            unreachable!();
        };
        let events = run_child(&self.link, &self.dir, intent, deadline, fixture);
        self.journal.finish(&self.link.key, attempt, events)?;
        Ok(false)
    }

    fn resolve_without_launch(&mut self, status: &Status) -> Result<bool> {
        let terminal = if status.cancel_requested || status.phase == Phase::CancelledBeforeStart {
            #[cfg(target_os = "linux")]
            if self.link.is_production() {
                production::check_cancel(status).map_err(|_| Unavailable)?;
            }
            Some(Event::Cancelled {
                evidence: CancelEvidence::BeforeStart,
            })
        } else {
            match status.phase {
                Phase::Claimed => return Ok(false),
                Phase::Authorized
                    if !self.link.is_production()
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
    fn from_grant(grant: &Grant) -> Result<Option<Self>> {
        let monotonic_now = Instant::now();
        let remaining = grant.start_until.saturating_sub(now_ms()?);
        Ok((1..=5000).contains(&remaining).then(|| Self {
            wall: grant.start_until,
            monotonic: monotonic_now + Duration::from_millis(remaining as u64),
        }))
    }

    fn check(&self) -> Result<()> {
        ensure!(
            Instant::now() < self.monotonic && now_ms()? < self.wall,
            "start grant expired"
        );
        Ok(())
    }
}

fn run_child(
    link: &Link,
    dir: &Path,
    intent: Intent,
    deadline: Deadline,
    fixture: Fixture,
) -> Vec<Event> {
    let started = AtomicBool::new(false);
    let cancel = Arc::new(AtomicBool::new(false));
    let attempt = intent.attempt;
    let exit = thread::scope(|scope| {
        let child = scope.spawn(|| {
            let mut command = Command::new("/bin/sh");
            command.env_clear().current_dir(dir).args([
                "-c",
                r#"
                    printf '%s\n' "$1" >> launches || exit 1
                    if [ "$2" = hold ]; then /bin/sleep 6; fi
                    IFS= read -r prompt || :
                    printf 'fixture output: %.100s\n' "$prompt"
                "#,
                "fixture",
                &attempt.to_string(),
                if fixture == Fixture::Hold { "hold" } else { "" },
            ]);
            run_single_child(
                command,
                &intent.prompt,
                &cancel,
                8,
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
                    Ok(status) if status.cancel_requested => {
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
