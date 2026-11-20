//! Run the pinned NSH package with one private local request slot. The UI
//! supplies public bytes and a reviewed digest. Process configuration stays here.
use super::{Card, Local, REVISION, Role, read_json};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Component, Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, TryRecvError},
    thread,
    time::{Duration, Instant},
};

const POLL: Duration = Duration::from_millis(75);
const DEADLINE: Duration = Duration::from_secs(20);
const CARD_LIMIT: usize = 65536;
const REQUEST_LIMIT: usize = CARD_LIMIT * 4 + 128;
const CAPABILITIES: &str =
    r#"{"picker":true,"fs":{"read":["${dataDir}/outgoing"],"write":["${dataDir}/incoming"]}}"#;

#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    /// Absolute path to the reviewed executable in the pinned NSH package.
    #[arg(long)]
    companion: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    sequence: u32,
    action: Action,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Review { bytes: Vec<u8> },
    Confirm { digest: String },
    Start,
    Stop,
}

#[derive(Serialize)]
struct Preview {
    card: Card,
    digest: String,
}

#[derive(Serialize)]
struct Reply {
    sequence: u32,
    runner: RunnerState,
    #[serde(skip_serializing_if = "Option::is_none")]
    preview: Option<Preview>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'static str>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RunnerState {
    Idle,
    Starting,
    Running,
    Stopping,
    StartupTimeout,
    StopTimeout,
    StartupFailed,
    Exited,
    Stopped,
    ExitUnverified,
    LockUnverified,
}

struct RunnerChild {
    process: Child,
    ready: Receiver<Option<String>>,
    deadline: Instant,
}

struct Host<'a> {
    cli: PathBuf,
    root: &'a Path,
    identity: Card,
    session: &'a Path,
    review: Option<(Vec<u8>, String)>,
    child: Option<RunnerChild>,
    reply: Reply,
}

pub(crate) fn run(root: Option<&Path>, args: &Args) -> Result<()> {
    let root = root.context("provide the existing absolute --data-dir for the local runner")?;
    let identity = runner_card(root)?;
    let cli = std::env::current_exe()?;
    ensure!(
        args.companion.is_absolute() && args.companion.is_file(),
        "provide the absolute packaged companion executable"
    );
    let package = args
        .companion
        .parent()
        .context("missing companion directory")?
        .join("nsh");

    let tmp = root.join("tmp");
    check_directory(&tmp, false)?;
    let session = tempfile::Builder::new()
        .prefix("desktop-")
        .tempdir_in(tmp)?;
    check_directory(session.path(), true)?;
    let inbox = session.path().join("exchange");
    fs::create_dir(&inbox)?;
    check_directory(&inbox, false)?;
    fs::create_dir(inbox.join("incoming"))?;
    fs::create_dir(inbox.join("outgoing"))?;
    let app = session.path().join("app");
    fs::create_dir(&app)?;
    let mut metadata: serde_json::Value =
        serde_json::from_reader(File::open(package.join("app.json"))?)?;
    let metadata = metadata
        .as_object_mut()
        .context("invalid companion package metadata")?;
    ensure!(
        metadata.get("id").and_then(serde_json::Value::as_str) == Some("dev.coconutclaw.companion"),
        "expected the reviewed CoconutClaw companion package"
    );
    let inbox_path = inbox
        .to_str()
        .filter(|path| !path.contains("${"))
        .context("desktop channel requires a UTF-8 path")?;
    metadata.insert("dataDir".into(), inbox_path.into());
    fs::write(app.join("app.json"), serde_json::to_vec(metadata)?)?;
    fs::copy(package.join("bundle.nshc"), app.join("bundle.nshc"))?;
    fs::write(app.join("capabilities.json"), CAPABILITIES)?;
    let icon = package.join("icon.ico");
    if icon.is_file() {
        fs::copy(icon, app.join("icon.ico"))?;
    }

    let mut host = Host {
        cli,
        root,
        identity,
        session: session.path(),
        review: None,
        child: None,
        reply: Reply {
            sequence: 0,
            runner: RunnerState::Idle,
            preview: None,
            message: None,
        },
    };
    host.publish(&inbox)?;
    let mut companion = Command::new(&args.companion)
        .env("NSH_APP_DIR", app)
        .spawn()?;
    let result = host.serve(&inbox, &mut companion);
    if result.is_err() && companion.try_wait()?.is_none() {
        companion.kill()?;
        companion.wait()?;
    }
    host.close()?;
    result
}

impl Host<'_> {
    fn serve(&mut self, inbox: &Path, companion: &mut Child) -> Result<()> {
        let path = inbox.join("incoming/request.json");
        loop {
            if let Some(exit) = companion.try_wait()? {
                ensure!(exit.success(), "companion exited with {exit}");
                return Ok(());
            }
            let previous = self.reply.runner;
            self.observe()?;
            if let Some(request) = read_request(&path)? {
                ensure!(
                    self.reply.sequence.checked_add(1) == Some(request.sequence),
                    "invalid desktop request sequence"
                );
                self.reply.sequence = request.sequence;
                self.reply.preview = None;
                self.reply.message = None;
                self.handle(request.action);
                self.publish(inbox)?;
            } else if previous != self.reply.runner {
                self.publish(inbox)?;
            }
            thread::sleep(POLL);
        }
    }

    fn publish(&self, inbox: &Path) -> Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(self.session)?;
        serde_json::to_writer(&mut file, &self.reply)?;
        file.persist(inbox.join("outgoing/reply.json"))?;
        Ok(())
    }

    fn handle(&mut self, action: Action) {
        match action {
            Action::Review { bytes } => {
                self.review = None;
                match self.preview(&bytes) {
                    Ok(preview) => {
                        self.review = Some((bytes, preview.digest.clone()));
                        self.reply.preview = Some(preview);
                    }
                    Err(_) => {
                        self.reply.message = Some(
                            "Pairing preview rejected. Check the coordinator card and the local runner’s identity, revision and permissions.",
                        )
                    }
                }
            }
            Action::Confirm { digest } => {
                self.reply.message = Some(
                    if let Some((bytes, reviewed)) = self.review.take()
                        && digest == reviewed
                        && self.pair(&bytes, Some(&reviewed)).is_ok()
                    {
                        "Coordinator paired."
                    } else {
                        "Pairing rejected. Review the coordinator card again. Any existing admission was preserved."
                    },
                );
            }
            Action::Start if self.child.is_none() => {
                if self.start().is_err() {
                    self.reply.runner = RunnerState::StartupFailed;
                }
            }
            Action::Stop if self.reply.runner == RunnerState::Running => {
                if let Some(child) = &mut self.child {
                    // Closing the child's only writer is the shutdown request.
                    child.process.stdin.take();
                    child.deadline = Instant::now() + DEADLINE;
                    self.reply.runner = RunnerState::Stopping;
                }
            }
            Action::Start | Action::Stop => {
                self.reply.message =
                    Some("That action is unavailable for the current runner state.");
            }
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.cli);
        command
            .args(["--data-dir"])
            .arg(self.root)
            .arg("nodes")
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;
            command.creation_flags(CREATE_NO_WINDOW);
        }
        command
    }

    fn same_runner(&self) -> Result<()> {
        ensure!(
            runner_card(self.root)?.id == self.identity.id,
            "configured local runner changed"
        );
        Ok(())
    }

    fn pair(&self, bytes: &[u8], digest: Option<&str>) -> Result<Vec<u8>> {
        self.same_runner()?;
        ensure!(bytes.len() <= CARD_LIMIT, "public card exceeds bound");
        let mut card = tempfile::NamedTempFile::new_in(self.session)?;
        card.write_all(bytes)?;
        let mut command = self.command();
        command.arg("pair").arg(card.path());
        match digest {
            Some(digest) => command.args(["--confirm", digest]),
            None => command.arg("--preview"),
        };
        let output = command.output()?;
        ensure!(output.status.success(), "pair command rejected the card");
        Ok(output.stdout)
    }

    fn preview(&self, bytes: &[u8]) -> Result<Preview> {
        let output = self.pair(bytes, None)?;
        let output = std::str::from_utf8(&output)?;
        let (card, digest) = output
            .trim_end()
            .split_once('\n')
            .context("expected pair preview")?;
        let digest = digest
            .strip_prefix("Confirmation digest ")
            .context("expected confirmation digest")?;
        ensure!(
            digest == blake3::hash(bytes).to_hex().as_str(),
            "unexpected confirmation digest"
        );
        Ok(Preview {
            card: serde_json::from_str(card)?,
            digest: digest.into(),
        })
    }

    fn start(&mut self) -> Result<()> {
        self.same_runner()?;
        let mut process = self
            .command()
            .args(["start", "--desktop-control"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = process.stdout.take().expect("piped runner stdout");
        let (send, ready) = mpsc::channel();
        thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).take(128).read_line(&mut line);
            let _ = send.send(result.ok().filter(|_| line.ends_with('\n')).map(|_| line));
        });
        self.child = Some(RunnerChild {
            process,
            ready,
            deadline: Instant::now() + DEADLINE,
        });
        self.reply.runner = RunnerState::Starting;
        Ok(())
    }

    fn observe(&mut self) -> Result<()> {
        let Some(child) = &mut self.child else {
            return Ok(());
        };
        if let Some(exit) = child.process.try_wait()? {
            self.child = None;
            self.reply.runner = match self.reply.runner {
                RunnerState::Starting | RunnerState::StartupTimeout => RunnerState::StartupFailed,
                RunnerState::Stopping | RunnerState::StopTimeout if exit.success() => {
                    if verify_lock_release(self.root).is_ok() {
                        RunnerState::Stopped
                    } else {
                        RunnerState::LockUnverified
                    }
                }
                RunnerState::Stopping | RunnerState::StopTimeout => RunnerState::ExitUnverified,
                _ => RunnerState::Exited,
            };
            return Ok(());
        }
        match self.reply.runner {
            RunnerState::Starting => match child.ready.try_recv() {
                Ok(Some(line))
                    if line.trim_end() == format!("Runner ready {}", self.identity.id)
                        && child.process.try_wait()?.is_none() =>
                {
                    self.reply.runner = RunnerState::Running
                }
                Err(TryRecvError::Empty) if Instant::now() < child.deadline => (),
                _ => {
                    child.process.stdin.take();
                    self.reply.runner = RunnerState::StartupTimeout;
                }
            },
            RunnerState::Stopping if Instant::now() >= child.deadline => {
                self.reply.runner = RunnerState::StopTimeout;
            }
            _ => (),
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            child.process.stdin.take();
            let deadline = Instant::now() + DEADLINE;
            loop {
                if let Some(exit) = child.process.try_wait()? {
                    ensure!(exit.success(), "runner exited with {exit}");
                    return verify_lock_release(self.root);
                }
                ensure!(
                    Instant::now() < deadline,
                    "runner exit was not observed after closing its control pipe"
                );
                thread::sleep(POLL);
            }
        }
        Ok(())
    }
}

fn read_request(path: &Path) -> Result<Option<Request>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        metadata.is_file() && metadata.len() <= REQUEST_LIMIT as u64,
        "invalid desktop request file"
    );
    #[cfg(windows)]
    coconutclaw_config::windows_security::validate_private(path)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut bytes = Vec::new();
    options
        .open(path)?
        .take(REQUEST_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)?;
    fs::remove_file(path)?;
    ensure!(
        bytes.len() <= REQUEST_LIMIT,
        "desktop request exceeds bound"
    );
    Ok(Some(serde_json::from_slice(&bytes)?))
}

pub(super) fn runner_card(root: &Path) -> Result<Card> {
    ensure!(
        root.is_absolute() && !root.components().any(|part| part == Component::ParentDir),
        "provide the existing absolute runner directory"
    );
    check_directory(root, true)?;
    check_directory(&root.join("runtime"), false)?;
    check_directory(&root.join("runtime/nodes"), true)?;
    for name in [
        "config.toml",
        "runtime/nodes/local.json",
        "runtime/nodes/device-key",
    ] {
        check_file(&root.join(name))?;
    }
    let lock = root.join("runtime/instance.lock");
    match fs::symlink_metadata(&lock) {
        Ok(_) => check_file(&lock)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    let local: Local = read_json(&root.join("runtime/nodes/local.json"))?;
    ensure!(
        local.card.role == Role::Runner
            && local.card.version == 1
            && local.card.revision == REVISION,
        "desktop controls require a local runner enrolled at the reviewed CLI revision"
    );
    Ok(local.card)
}

fn check_directory(path: &Path, private: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        for ancestor in path.ancestors() {
            ensure!(
                fs::symlink_metadata(ancestor)?.is_dir(),
                "runner custody path is not a real directory"
            );
        }
        let metadata = fs::symlink_metadata(path)?;
        let mask = if private { 0o077 } else { 0o022 };
        // SAFETY: geteuid takes no arguments and has no memory effects.
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & mask == 0,
            "runner directory permissions or owner do not match the current account"
        );
    }
    #[cfg(windows)]
    {
        let _ = private;
        coconutclaw_config::windows_security::private_directory(path, false)?;
    }
    Ok(())
}

fn check_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file(),
        "runner custody path is not a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid takes no arguments and has no memory effects.
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o022 == 0,
            "runner file permissions or owner do not match the current account"
        );
    }
    #[cfg(windows)]
    coconutclaw_config::windows_security::validate_private(path)?;
    Ok(())
}

fn verify_lock_release(root: &Path) -> Result<()> {
    check_directory(root, true)?;
    let runtime = root.join("runtime");
    check_directory(&runtime, false)?;
    let path = runtime.join("instance.lock");
    check_file(&path)?;
    OpenOptions::new()
        .write(true)
        .open(path)?
        .try_lock_exclusive()
        .context("runner exited but the instance lock could not be acquired")?;
    Ok(())
}

pub(super) fn listen_for_stop(stop: tokio::sync::oneshot::Sender<()>) -> Result<()> {
    #[cfg(unix)]
    {
        // SAFETY: the file descriptor and fcntl arguments are constants.
        ensure!(
            unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_SETFD, libc::FD_CLOEXEC) } != -1,
            "could not protect desktop control pipe from inheritance"
        );
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::{
            Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation},
            System::Console::{GetStdHandle, STD_INPUT_HANDLE},
        };
        // SAFETY: GetStdHandle returns a borrowed standard handle. Neither call
        // transfers ownership or dereferences caller memory.
        ensure!(
            unsafe { SetHandleInformation(GetStdHandle(STD_INPUT_HANDLE), HANDLE_FLAG_INHERIT, 0) }
                != 0,
            "could not protect desktop control pipe from inheritance"
        );
    }
    thread::Builder::new()
        .name("desktop-control".into())
        .spawn(move || {
            // One-way ownership pipe. Closing the writer also handles owner exit.
            let _ = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
            let _ = stop.send(());
        })?;
    Ok(())
}
