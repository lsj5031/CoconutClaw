//! Unix disposable subprocess harness. No executable/runtime admission flag.
use super::*;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            fs::OpenOptionsExt,
            net::{UnixListener, UnixStream},
        },
    },
    path::PathBuf,
    process::{Child, Command as OsCommand, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

const MAX_FRAME: usize = 256 * 1024;
const ENTRY: &str = "store::task_adapter::proposals::process::child_process_entry";
const MARKER: &str = "CoconutClaw disposable mock process v1\n";
pub(in crate::store::task_adapter) mod provider;
pub(in crate::store::task_adapter) mod remote;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::store::task_adapter) enum Mode {
    Cooperative,
    Late,
    Provider,
    ProviderFail,
    ProviderResume,
    ProviderLimit,
    ProviderOversize,
    RealCodex,
    HostCodex,
}
impl Mode {
    pub(in crate::store::task_adapter) fn is_provider(self) -> bool {
        matches!(
            self,
            Self::RealCodex
                | Self::HostCodex
                | Self::Provider
                | Self::ProviderFail
                | Self::ProviderResume
                | Self::ProviderLimit
                | Self::ProviderOversize
        )
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::store::task_adapter) enum Crash {
    Start,
    Report,
    Signature,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct WorkStat {
    pub assignment: u64,
    pub pid: u32,
    pub run: i64,
    pub status: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum Command {
    Init {
        origin: BoundOrigin,
        session: String,
    },
    Sign {
        history: Vec<Vec<u8>>,
        action: Action,
    },
    Disposition {
        assignment: u64,
        outcome: Outcome,
        output: String,
    },
    Saved,
    Proposal {
        assignment: u64,
    },
    Begin {
        assignment: u64,
        mode: Mode,
    },
    Finish {
        assignment: u64,
    },
    Cancel {
        assignment: u64,
    },
    Stats,
    ReleaseProvider {
        assignment: u64,
    },
    AbandonProvider {
        assignment: u64,
    },
    ProviderEvidence,
    Arm {
        crash: Crash,
    },
    Stop,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    boundary: String,
    origin: String,
    command: Command,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hello {
    public: EndpointId,
    pid: u32,
    boundary: String,
}
#[derive(Serialize, Deserialize)]
pub(super) enum Reply {
    Unit,
    Bytes(Vec<u8>),
    Events(Vec<Vec<u8>>),
    Proposal(Option<Vec<u8>>),
    Stats(Vec<WorkStat>),
    ProviderEvidence(Vec<provider::Evidence>),
    Error(String),
}
impl Reply {
    pub(super) fn bytes(self) -> Result<Vec<u8>> {
        match self {
            Self::Bytes(v) => Ok(v),
            _ => anyhow::bail!("unexpected process response"),
        }
    }
    pub(super) fn events(self) -> Result<Vec<Vec<u8>>> {
        match self {
            Self::Events(v) => Ok(v),
            _ => anyhow::bail!("unexpected process response"),
        }
    }
    pub(super) fn proposal(self) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Proposal(v) => Ok(v),
            _ => anyhow::bail!("unexpected process response"),
        }
    }
}

fn write_frame(stream: &mut UnixStream, bytes: &[u8]) -> Result<()> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= MAX_FRAME,
        "invalid process frame size"
    );
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)?;
    Ok(())
}
fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let size = u32::from_be_bytes(header) as usize;
    ensure!(size > 0 && size <= MAX_FRAME, "invalid process frame size");
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}
fn poll(fd: i32, millis: i32) -> Result<()> {
    let mut descriptor = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // A finite kernel wait, not sleep/poll retries.
    let count = unsafe { libc::poll(&mut descriptor, 1, millis) };
    ensure!(count > 0, "process boundary wait failed or expired");
    Ok(())
}
fn reap(child: &mut Child) -> Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::FromRawFd;
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, child.id(), 0) } as i32;
        let exited = if fd >= 0 {
            let file = unsafe { File::from_raw_fd(fd) };
            poll(file.as_raw_fd(), 2000).is_ok()
        } else {
            false
        };
        if !exited {
            let _ = child.kill();
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = child.kill();
    child.wait()?;
    Ok(())
}

pub(in crate::store::task_adapter) struct Bootstrap {
    client: Arc<ProcessClient>,
}
pub(super) struct ProcessClient {
    root: PathBuf,
    rpc: Mutex<(UnixStream, String, String)>,
    child: Mutex<Child>,
    slots: Arc<Semaphore>,
    pub(in crate::store::task_adapter) pid: u32,
    public: EndpointId,
}
struct SpawnGuard(Option<Child>);
impl Drop for SpawnGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl Drop for ProcessClient {
    fn drop(&mut self) {
        if let Ok(child) = self.child.get_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl Bootstrap {
    pub(in crate::store::task_adapter) fn launch(root: &Path) -> Result<Self> {
        Self::launch_with(root, false)
    }
    pub(in crate::store::task_adapter) fn launch_real(root: &Path) -> Result<Self> {
        ensure!(
            std::env::var("COCONUTCLAW_REAL_CODEX_ACCEPTANCE").as_deref() == Ok("1"),
            "real acceptance requires explicit opt-in"
        );
        Self::launch_with(root, true)
    }
    fn launch_with(root: &Path, real: bool) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        let marker = root.join("disposable");
        if marker.exists() {
            ensure!(
                std::fs::read_to_string(&marker)? == MARKER,
                "not a disposable process directory"
            );
        } else {
            ensure!(
                std::fs::read_dir(root)?.next().is_none(),
                "refuse unmarked nonempty process directory"
            );
            std::fs::write(&marker, MARKER)?;
        }
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let socket = root.join(format!(
            "rpc-{}-{}.sock",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let listener = UnixListener::bind(&socket)?;
        std::fs::create_dir_all(root.join("home"))?;
        std::fs::create_dir_all(root.join("codex-home"))?;
        let mut command = OsCommand::new(std::env::current_exe()?);
        command
            .args(["--exact", ENTRY, "--ignored", "--nocapture"])
            .env_clear();
        if real {
            command
                .env("HOME", "/home/leo")
                .env("CODEX_HOME", root.join("codex-home"))
                .env(
                    "PATH",
                    "/home/leo/.local/share/mise/installs/node/24.20.0/bin:/usr/bin:/bin",
                )
                .env("COCONUTCLAW_REAL_CODEX_ACCEPTANCE", "1");
        } else {
            command
                .env("HOME", root.join("home"))
                .env("CODEX_HOME", root.join("codex-home"));
        }
        let child = command
            .env("FACTORY_AIRGAP_ENABLED", "1")
            .env("COCONUTCLAW_MOCK_PROCESS_SOCKET", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut child = SpawnGuard(Some(child));
        let result = (|| -> Result<ProcessClient> {
            poll(listener.as_raw_fd(), 10000)?;
            let (mut stream, _) = listener.accept()?;
            stream.set_read_timeout(Some(Duration::from_secs(10)))?;
            stream.set_write_timeout(Some(Duration::from_secs(10)))?;
            let hello: Hello = serde_json::from_slice(&read_frame(&mut stream)?)?;
            let pid = child.0.as_ref().context("child absent")?.id();
            ensure!(
                hello.pid == pid && hello.pid != std::process::id() && hello.boundary.len() == 64,
                "wrong process hello"
            );
            Ok(ProcessClient {
                root: root.into(),
                rpc: Mutex::new((stream, hello.boundary, String::new())),
                pid,
                child: Mutex::new(child.0.take().context("child absent")?),
                slots: Arc::new(Semaphore::new(4)),
                public: hello.public,
            })
        })();
        let _ = std::fs::remove_file(socket);
        // The child is moved into ProcessClient only after hello validation.
        Ok(Self {
            client: Arc::new(result?),
        })
    }
    pub(in crate::store::task_adapter) fn public(&self) -> EndpointId {
        self.client.public
    }
    pub(in crate::store::task_adapter) async fn bind(
        self,
        origin: &BoundOrigin,
        session: &SessionKey,
    ) -> Result<RunnerClient> {
        ensure!(
            origin.origin.runners.contains(&self.public()),
            "process runner role mismatch"
        );
        self.client
            .call(Command::Init {
                origin: origin.clone(),
                session: session.id(),
            })
            .await?;
        self.client
            .rpc
            .lock()
            .map_err(|_| anyhow::anyhow!("process client poisoned"))?
            .2 = origin.hash()?;
        Ok(RunnerClient {
            public: self.public(),
            origin: origin.hash()?,
            route: Route::Process(self.client),
        })
    }
}
impl ProcessClient {
    pub(super) async fn call(self: &Arc<Self>, command: Command) -> Result<Reply> {
        let permit = self.slots.clone().acquire_owned().await?;
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut rpc = this
                .rpc
                .lock()
                .map_err(|_| anyhow::anyhow!("process client poisoned"))?;
            let bytes = serde_json::to_vec(&Envelope {
                version: 1,
                boundary: rpc.1.clone(),
                origin: rpc.2.clone(),
                command,
            })?;
            write_frame(&mut rpc.0, &bytes)?;
            let reply: Reply = serde_json::from_slice(&read_frame(&mut rpc.0)?)?;
            match reply {
                Reply::Error(error) => anyhow::bail!("mock process refused: {error}"),
                reply => Ok(reply),
            }
        })
        .await?
    }
    pub(super) async fn shutdown(self: &Arc<Self>) -> Result<()> {
        let response = self.call(Command::Stop).await;
        let this = self.clone();
        let cleanup = tokio::task::spawn_blocking(move || {
            let mut child = this
                .child
                .lock()
                .map_err(|_| anyhow::anyhow!("child lock poisoned"))?;
            reap(&mut child)
        })
        .await?;
        cleanup?;
        response?;
        Ok(())
    }
}
impl RunnerClient {
    pub(super) async fn control(&self, command: Command) -> Result<Reply> {
        match &self.route {
            Route::Process(p) => p.call(command).await,
            Route::Remote(p) => p.call(command).await,
            _ => anyhow::bail!("not a process runner"),
        }
    }
    fn process(&self) -> Result<&Arc<ProcessClient>> {
        match &self.route {
            Route::Process(p) => Ok(p),
            _ => anyhow::bail!("not a process runner"),
        }
    }
    pub(in crate::store::task_adapter) fn child_pid(&self) -> Result<u32> {
        Ok(self.process()?.pid)
    }
    pub(in crate::store::task_adapter) async fn begin(
        &self,
        assignment: u64,
        mode: Mode,
    ) -> Result<()> {
        self.control(Command::Begin { assignment, mode }).await?;
        Ok(())
    }
    pub(in crate::store::task_adapter) async fn finish(&self, assignment: u64) -> Result<Vec<u8>> {
        self.control(Command::Finish { assignment }).await?.bytes()
    }
    pub(in crate::store::task_adapter) async fn cancel_work(&self, assignment: u64) -> Result<()> {
        self.control(Command::Cancel { assignment }).await?;
        Ok(())
    }
    pub(in crate::store::task_adapter) async fn stats(&self) -> Result<Vec<WorkStat>> {
        match self.control(Command::Stats).await? {
            Reply::Stats(v) => Ok(v),
            _ => anyhow::bail!("unexpected stats"),
        }
    }
    pub(in crate::store::task_adapter) async fn arm(&self, crash: Crash) -> Result<()> {
        self.control(Command::Arm { crash }).await?;
        Ok(())
    }
    pub(in crate::store::task_adapter) fn kill_child(&self) -> Result<()> {
        let mut child = self
            .process()?
            .child
            .lock()
            .map_err(|_| anyhow::anyhow!("child lock poisoned"))?;
        child.kill()?;
        child.wait()?;
        Ok(())
    }
    pub(in crate::store::task_adapter) async fn malformed(&self, kind: &str) -> Result<()> {
        let this = self.process()?.clone();
        let kind = kind.to_owned();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut rpc = this
                .rpc
                .lock()
                .map_err(|_| anyhow::anyhow!("process client poisoned"))?;
            if kind == "oversized" {
                rpc.0.write_all(&((MAX_FRAME + 1) as u32).to_be_bytes())?;
            } else {
                let mut value = serde_json::to_value(Envelope {
                    version: 1,
                    boundary: rpc.1.clone(),
                    origin: rpc.2.clone(),
                    command: Command::Stats,
                })?;
                match kind.as_str() {
                    "boundary" => value["boundary"] = "wrong".into(),
                    "origin" => value["origin"] = "wrong".into(),
                    "version" => value["version"] = 2.into(),
                    "malformed" => {}
                    _ => anyhow::bail!("unknown frame fixture"),
                }
                let bytes = if kind == "malformed" {
                    b"{".to_vec()
                } else {
                    serde_json::to_vec(&value)?
                };
                write_frame(&mut rpc.0, &bytes)?;
            }
            match serde_json::from_slice::<Reply>(&read_frame(&mut rpc.0)?)? {
                Reply::Error(_) => Ok(()),
                _ => anyhow::bail!("invalid frame unexpectedly accepted"),
            }
        })
        .await?
    }
}

struct State {
    root: PathBuf,
    providers: std::collections::BTreeMap<u64, provider::Work>,
    runner: Runner,
    store: Store,
    session: SessionKey,
    crash: Option<Crash>,
}
impl State {
    fn open(root: &Path, origin: BoundOrigin, key: SecretKey, session: String) -> Result<Self> {
        ensure!(
            session.len() <= 256 && session.starts_with("local:"),
            "invalid child session"
        );
        let session = SessionKey::from_id(session);
        let runner = Runner::open(origin, key, &root.join("runner.sqlite"))?;
        let store = Store::open(&RuntimeConfig::test_config_at(root.join("work")))?;
        TaskAdapter::open(&store, &runner.origin, &session)?;
        store.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS process_claim(
            assignment INTEGER PRIMARY KEY,run INTEGER NOT NULL UNIQUE REFERENCES task_runs(id),
            pid INTEGER NOT NULL,late INTEGER NOT NULL,provider INTEGER NOT NULL);",
        )?;
        store.conn.execute_batch("CREATE TABLE IF NOT EXISTS provider_evidence(
            assignment INTEGER PRIMARY KEY REFERENCES process_claim(assignment),evidence TEXT NOT NULL);")?;
        let state = Self {
            root: root.into(),
            providers: std::collections::BTreeMap::new(),
            runner,
            store,
            session,
            crash: None,
        };
        state.interrupt()?;
        Ok(state)
    }
    fn interrupt(&self) -> Result<()> {
        for row in self.stats()? {
            let run = self
                .store
                .get_task_run(row.run)?
                .context("child task absent")?;
            if run.finished_at.is_none() {
                self.store.finish_task_run(
                    row.run,
                    TaskRunStatus::Failed,
                    &chrono::Utc::now().to_rfc3339(),
                    Some("mock outcome unknown; child interrupted; no automatic retry"),
                    None,
                )?;
            }
        }
        Ok(())
    }
    fn stats(&self) -> Result<Vec<WorkStat>> {
        let rows: Vec<(u64, i64, u32)> = self
            .store
            .conn
            .prepare("SELECT assignment,run,pid FROM process_claim ORDER BY assignment")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        ensure!(rows.len() <= 4, "child task bound exceeded");
        rows.into_iter()
            .map(|(assignment, run, pid)| {
                Ok(WorkStat {
                    assignment,
                    run,
                    pid,
                    status: self
                        .store
                        .get_task_run(run)?
                        .context("child task absent")?
                        .status,
                })
            })
            .collect()
    }
    fn begin(&mut self, assignment: u64, mode: Mode) -> Result<()> {
        if mode.is_provider() {
            provider::validate(&self.root, mode)?;
        }
        let (start, proposal, final_response): (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>) =
            self.runner.sql.query_row(
                "SELECT start,proposal,final FROM responses WHERE assignment=?1",
                [assignment],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
        ensure!(
            proposal.is_none() && final_response.is_none(),
            "child outcome already observed; never invoke"
        );
        let prefix: String = self.runner.sql.query_row(
            "SELECT prefix FROM runner_prefix WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        let mut events: Vec<Vec<u8>> = serde_json::from_str(&prefix)?;
        events.push(start);
        let history = self.runner.history(events)?;
        let (_, bytes, delegation) = {
            let bytes = history
                .export()
                .into_iter()
                .find(|b| SignedEvent::parse(b).is_ok_and(|s| s.event.sequence == assignment))
                .context("child assignment absent")?;
            let Action::Assign { request, .. } = SignedEvent::parse(&bytes)?.event.action else {
                anyhow::bail!("not child assignment")
            };
            ((), bytes, serde_json::from_str::<Delegation>(&request)?)
        };
        ensure!(
            history.jobs[&assignment].runner == self.runner.key.public(),
            "wrong child runner"
        );
        let adapter = TaskAdapter::open(&self.store, &self.runner.origin, &self.session)?;
        let mut request = adapter.request(&delegation.payload)?;
        ensure!(
            SignedEvent::parse(&bytes)?.event.sequence == assignment,
            "child assignment mismatch"
        );
        let tx = self.store.conn.unchecked_transaction()?;
        let exists: bool = self.store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM process_claim WHERE assignment=?1)",
            [assignment],
            |r| r.get(0),
        )?;
        ensure!(!exists, "child invocation already claimed; never retry");
        ensure!(
            self.stats()?
                .iter()
                .filter(|r| matches!(r.status.as_str(), "running" | "cancel_requested"))
                .count()
                < 2,
            "child admission full"
        );
        request.session = SessionKey::local(&format!(
            "process-mock:{}/{}",
            self.runner.origin.hash()?,
            assignment
        ));
        request.update_id = Some(format!("process-mock:{assignment}"));
        let run = self.store.insert_task_run(InsertTaskRunParams {
            session_id: request.session.id(),
            channel: "local".into(),
            source_chat_id: None,
            source_user_id: None,
            update_id: request.update_id.clone(),
            prompt: request.input.user_text.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            progress_message_id: None,
            scheduled_task_id: None,
        })?;
        self.store.conn.execute(
            "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
            params![run, build_resume_payload(&request, &request.input)?],
        )?;
        self.store
            .update_task_run_started(run, &chrono::Utc::now().to_rfc3339())?;
        self.store.conn.execute(
            "INSERT INTO process_claim VALUES(?1,?2,?3,?4,?5)",
            params![
                assignment,
                run,
                std::process::id(),
                mode == Mode::Late,
                mode.is_provider()
            ],
        )?;
        tx.commit()?;
        if mode.is_provider() {
            self.start_provider(assignment, mode)?;
        }
        Ok(())
    }
    fn finish(&mut self, assignment: u64) -> Result<Vec<u8>> {
        let (id, late, is_provider): (i64, bool, bool) = self.store.conn.query_row(
            "SELECT run,late,provider FROM process_claim WHERE assignment=?1",
            [assignment],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let run = self.store.get_task_run(id)?.context("child task absent")?;
        ensure!(
            run.finished_at.is_none(),
            "child work ended; never invoke again"
        );
        let (outcome, output) = if is_provider {
            match self.finish_provider(assignment) {
                Ok(result) => result,
                Err(error) => {
                    let failure = self.evidence(assignment)?.failure;
                    let message = match &failure {
                        Some(failure) => format!(
                            "provider outcome unknown/refused; no automatic retry; {}: {}",
                            failure.phase, failure.detail
                        ),
                        None => "provider outcome unknown/refused; no automatic retry".into(),
                    };
                    if !self.providers.contains_key(&assignment) {
                        self.store.finish_task_run(
                            id,
                            TaskRunStatus::Failed,
                            &chrono::Utc::now().to_rfc3339(),
                            Some(&message),
                            None,
                        )?;
                    }
                    // Only the bounded safe failure crosses the control boundary.
                    return Err(if failure.is_some() {
                        anyhow::anyhow!(message)
                    } else {
                        error
                    });
                }
            }
        } else if run.cancel_requested_at.is_some() && !late {
            (
                Outcome::Cancelled,
                format!(
                    "child {} cooperatively cancelled assignment {assignment}",
                    std::process::id()
                ),
            )
        } else {
            // The only mock computation. No parent-produced outcome is accepted.
            let checksum = blake3::hash(run.prompt.as_bytes());
            (
                Outcome::Completed,
                format!(
                    "child {} assignment {assignment} mock digest {checksum}",
                    std::process::id()
                ),
            )
        };
        let bytes = self.runner.report(assignment, outcome, output.clone())?;
        let tx = self.store.conn.unchecked_transaction()?;
        self.store.finish_task_run(
            id,
            if outcome == Outcome::Cancelled {
                TaskRunStatus::Cancelled
            } else if outcome == Outcome::Completed {
                TaskRunStatus::Completed
            } else {
                TaskRunStatus::Failed
            },
            &chrono::Utc::now().to_rfc3339(),
            None,
            Some(&output),
        )?;
        self.store.conn.execute(
            "UPDATE task_payloads SET output=?2 WHERE task_run_id=?1",
            params![id, output],
        )?;
        tx.commit()?;
        Ok(bytes)
    }
    fn handle(&mut self, command: Command) -> Result<Reply> {
        let mut point = None;
        let reply = match command {
            Command::Sign { history, action } => {
                point = match action {
                    Action::Start { .. } => Some(Crash::Start),
                    Action::Result { .. } => Some(Crash::Signature),
                    _ => None,
                };
                Reply::Bytes(self.runner.sign(history, action)?)
            }
            Command::Disposition {
                assignment,
                outcome,
                output,
            } => {
                ensure!(
                    matches!(outcome, Outcome::Unknown | Outcome::Cancelled),
                    "parent cannot report process completion"
                );
                Reply::Bytes(self.runner.report(assignment, outcome, output)?)
            }
            Command::Saved => Reply::Events(self.runner.saved()?),
            Command::Proposal { assignment } => {
                Reply::Proposal(self.runner.stored_proposal(assignment)?)
            }
            Command::Begin { assignment, mode } => {
                self.begin(assignment, mode)?;
                Reply::Unit
            }
            Command::Finish { assignment } => {
                let bytes = self.finish(assignment)?;
                point = Some(Crash::Report);
                Reply::Bytes(bytes)
            }
            Command::Cancel { assignment } => {
                let id: i64 = self.store.conn.query_row(
                    "SELECT run FROM process_claim WHERE assignment=?1",
                    [assignment],
                    |r| r.get(0),
                )?;
                ensure!(
                    self.store
                        .get_task_run(id)?
                        .context("child task absent")?
                        .finished_at
                        .is_none(),
                    "child task already settled"
                );
                self.store
                    .mark_task_run_cancel_requested(id, &chrono::Utc::now().to_rfc3339())?;
                self.cancel_provider(assignment);
                Reply::Unit
            }
            Command::ReleaseProvider { assignment } => {
                self.release_provider(assignment)?;
                Reply::Unit
            }
            Command::AbandonProvider { assignment } => {
                self.finish_provider(assignment)?;
                let id: i64 = self.store.conn.query_row(
                    "SELECT run FROM process_claim WHERE assignment=?1",
                    [assignment],
                    |r| r.get(0),
                )?;
                self.store.finish_task_run(
                    id,
                    TaskRunStatus::Failed,
                    &chrono::Utc::now().to_rfc3339(),
                    Some("provider control abandoned; outcome unknown; no retry"),
                    None,
                )?;
                Reply::Unit
            }
            Command::ProviderEvidence => Reply::ProviderEvidence(self.provider_evidence()?),
            Command::Stats => Reply::Stats(self.stats()?),
            Command::Arm { crash } => {
                self.crash = Some(crash);
                Reply::Unit
            }
            Command::Stop => {
                self.interrupt()?;
                Reply::Unit
            }
            Command::Init { .. } => anyhow::bail!("process already bound"),
        };
        if point.is_some() && point == self.crash {
            std::process::exit(86);
        }
        Ok(reply)
    }
}

fn serve(socket: PathBuf) -> Result<()> {
    let root = socket.parent().context("child socket directory absent")?;
    ensure!(
        std::fs::read_to_string(root.join("disposable"))? == MARKER,
        "not a disposable child"
    );
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("writer.lock"))?;
    lock.try_lock().context("process runner already owned")?;
    let key_path = root.join("mock-key");
    let mut key = [0; 32];
    if key_path.exists() {
        File::open(key_path)?.read_exact(&mut key)?;
    } else {
        File::open("/dev/urandom")?.read_exact(&mut key)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .mode(0o600)
            .write(true)
            .open(key_path)?;
        file.write_all(&key)?;
        file.sync_all()?;
    }
    let key = SecretKey::from(key);
    let mut nonce = [0; 32];
    File::open("/dev/urandom")?.read_exact(&mut nonce)?;
    let boundary = blake3::hash(&nonce).to_string();
    let mut stream = UnixStream::connect(&socket)?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    write_frame(
        &mut stream,
        &serde_json::to_vec(&Hello {
            public: key.public(),
            pid: std::process::id(),
            boundary: boundary.clone(),
        })?,
    )?;
    let mut state: Option<State> = None;
    loop {
        poll(stream.as_raw_fd(), -1)?; // Idle until input/EOF; owner Drop kills/reaps.
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        let bytes = match read_frame(&mut stream) {
            Ok(bytes) => bytes,
            Err(_) => {
                let _ = write_frame(
                    &mut stream,
                    &serde_json::to_vec(&Reply::Error("invalid process frame".into()))?,
                );
                return Ok(());
            }
        };
        stream.set_read_timeout(None)?;
        let response = (|| -> Result<(Reply, bool)> {
            let envelope: Envelope = serde_json::from_slice(&bytes)?;
            ensure!(
                serde_json::to_vec(&envelope)? == bytes
                    && envelope.version == 1
                    && envelope.boundary == boundary,
                "wrong process boundary/version/encoding"
            );
            if let Some(state) = state.as_mut() {
                ensure!(
                    envelope.origin == state.runner.origin.hash()?,
                    "wrong process origin"
                );
                let stop = matches!(envelope.command, Command::Stop);
                Ok((state.handle(envelope.command)?, stop))
            } else {
                ensure!(envelope.origin.is_empty(), "unexpected initial origin");
                let Command::Init { origin, session } = envelope.command else {
                    anyhow::bail!("process not initialized")
                };
                state = Some(State::open(root, origin, key.clone(), session)?);
                Ok((Reply::Unit, false))
            }
        })();
        let (reply, stop) = match response {
            Ok(value) => value,
            Err(error) => (Reply::Error(error.to_string()), false),
        };
        write_frame(&mut stream, &serde_json::to_vec(&reply)?)?;
        if stop {
            return Ok(());
        }
    }
}

#[test]
#[ignore = "disposable child entry point, launched explicitly by process tests"]
fn child_process_entry() {
    let socket =
        std::env::var_os("COCONUTCLAW_MOCK_PROCESS_SOCKET").expect("child harness socket required");
    serve(PathBuf::from(socket)).unwrap();
}
