//! Explicit fixture-only provider wiring. Never resolve an ambient codex binary.
use super::*;
use std::sync::{atomic::AtomicBool, mpsc};

const SCRIPT: &str = include_str!("fake_codex.py");
pub(in crate::store::task_adapter) mod host;
mod real;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    executable: PathBuf,
    script: PathBuf,
    workspace: PathBuf,
    home: PathBuf,
    codex_home: PathBuf,
}
impl Config {
    fn expected(root: &Path) -> Self {
        Self {
            executable: "/usr/bin/python3".into(),
            script: root.join("fake-codex.py"),
            workspace: root.join("workspace"),
            home: root.join("home"),
            codex_home: root.join("codex-home"),
        }
    }
    fn load(root: &Path) -> Result<Self> {
        let cfg: Self = serde_json::from_slice(&std::fs::read(root.join("provider.json"))?)?;
        let expected = Self::expected(root);
        ensure!(
            cfg.executable == expected.executable
                && cfg.executable.is_file()
                && cfg.script == expected.script
                && cfg.workspace == expected.workspace
                && cfg.home == expected.home
                && cfg.codex_home == expected.codex_home,
            "only the explicit disposable fake provider paths are admitted"
        );
        ensure!(
            std::fs::read_to_string(&cfg.script)? == SCRIPT,
            "fake executable missing or changed"
        );
        ensure!(
            std::env::var_os("HOME").as_deref() == Some(cfg.home.as_os_str())
                && std::env::var_os("CODEX_HOME").as_deref() == Some(cfg.codex_home.as_os_str()),
            "provider homes must be isolated"
        );
        Ok(cfg)
    }
}

pub(in crate::store::task_adapter) fn provision(root: &Path) -> Result<()> {
    ensure!(
        std::fs::read_to_string(root.join("disposable"))? == MARKER,
        "not disposable"
    );
    let cfg = Config::expected(root);
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&cfg.script)?
        .write_all(SCRIPT.as_bytes())?;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join("provider.json"))?
        .write_all(&serde_json::to_vec(&cfg)?)?;
    Ok(())
}
pub(super) fn validate(root: &Path, mode: Mode) -> Result<()> {
    if mode == Mode::HostCodex {
        host::validate(root)
    } else if mode == Mode::RealCodex {
        real::validate(root)
    } else {
        Config::load(root).map(|_| ())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct Evidence {
    pub assignment: u64,
    pub runner_pid: u32,
    pub observed: Option<Observed>,
    pub session: Option<String>,
    pub progress: Vec<String>,
    pub exit_code: Option<i32>,
    pub output: Option<String>,
    #[serde(default)]
    pub failure: Option<Failure>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct Failure {
    pub phase: String,
    pub detail: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct Observed {
    pub pid: u32,
    pub parent: u32,
    pub group: u32,
    pub descendant: u32,
    pub prompt: String,
    pub cwd: String,
    pub home: String,
    pub codex_home: String,
    pub args: Vec<String>,
    pub environment: Vec<String>,
    pub claim_run: i64,
    pub durable_start: bool,
}
pub(super) struct Work {
    flag: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<Result<coconutclaw_provider::ProviderOutput>>>,
    control: Option<UnixStream>,
    progress: mpsc::Receiver<String>,
}
impl Work {
    fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Parent owns only a completion-read endpoint, never provider output computation.
pub(in crate::store::task_adapter) struct Notice {
    listener: UnixListener,
    path: PathBuf,
    real: bool,
}
impl Drop for Notice {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
impl Notice {
    pub(in crate::store::task_adapter) fn bind(
        root: &Path,
        assignment: u64,
        real: bool,
    ) -> Result<Self> {
        let path = root.join(format!("done-{assignment}.sock"));
        Ok(Self {
            listener: UnixListener::bind(&path)?,
            path,
            real,
        })
    }
    pub(in crate::store::task_adapter) async fn wait(self) -> Result<()> {
        tokio::task::spawn_blocking(move || -> Result<()> {
            poll(
                self.listener.as_raw_fd(),
                if self.real { -1 } else { 10000 },
            )?;
            let (mut stream, _) = self.listener.accept()?;
            stream.set_read_timeout(if self.real {
                None
            } else {
                Some(Duration::from_secs(10))
            })?;
            let mut ready = [0];
            stream.read_exact(&mut ready)?;
            ensure!(ready == [1], "invalid provider completion notice");
            Ok(())
        })
        .await?
    }
}
impl RunnerClient {
    pub(in crate::store::task_adapter) fn provider_notice(
        &self,
        assignment: u64,
        real: bool,
    ) -> Result<Notice> {
        Notice::bind(&self.process()?.root, assignment, real)
    }
    pub(in crate::store::task_adapter) async fn release_provider(
        &self,
        assignment: u64,
    ) -> Result<()> {
        self.control(Command::ReleaseProvider { assignment })
            .await?;
        Ok(())
    }
    pub(in crate::store::task_adapter) async fn abandon_provider(
        &self,
        assignment: u64,
    ) -> Result<()> {
        self.control(Command::AbandonProvider { assignment })
            .await?;
        Ok(())
    }
    pub(in crate::store::task_adapter) async fn provider_evidence(&self) -> Result<Vec<Evidence>> {
        match self.control(Command::ProviderEvidence).await? {
            Reply::ProviderEvidence(v) => Ok(v),
            _ => anyhow::bail!("wrong provider evidence"),
        }
    }
}

impl State {
    pub(super) fn start_provider(&mut self, assignment: u64, mode: Mode) -> Result<()> {
        if mode == Mode::HostCodex {
            return self.start_host(assignment);
        }
        if mode == Mode::RealCodex {
            return self.start_real(assignment);
        }
        let cfg = Config::load(&self.root)?;
        let job = self.root.join(format!("provider-{assignment}"));
        std::fs::create_dir(&job)?;
        let workspace = cfg.workspace.join(assignment.to_string());
        std::fs::create_dir_all(&workspace)?;
        let listener = UnixListener::bind(job.join("control.sock"))?;
        let mut notice = UnixStream::connect(self.root.join(format!("done-{assignment}.sock")))?;
        let mut config = RuntimeConfig::test_config_at(job.join("config"));
        std::fs::create_dir_all(&config.tmp_dir)?;
        config.instance_dir = workspace.clone();
        config.codex.bin = format!(
            "{} -I '{}' '{}' {} {}",
            cfg.executable.display(),
            cfg.script.display(),
            job.display(),
            assignment,
            std::process::id()
        );
        config.exec_policy = "safe".into(); // No compatibility retry of a rejected dangerous flag.
        config.telegram_bot_token = None;
        config.codex.model = Some("explicit-fake-only".into());
        config.provider_timeout_secs = 10;
        let run: i64 = self.store.conn.query_row(
            "SELECT run FROM process_claim WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        let prompt = self
            .store
            .get_task_run(run)?
            .context("child task absent")?
            .prompt;
        // Only known fixture flags are passed; no caller-supplied command or session token.
        if mode == Mode::ProviderFail {
            config.codex.bin.push_str(" fail");
        }
        if mode == Mode::ProviderLimit {
            config.codex.bin.push_str(" limit");
        }
        if mode == Mode::ProviderOversize {
            config.codex.bin.push_str(" oversized");
        }
        let resume =
            (mode == Mode::ProviderResume).then(|| format!("fixture-session-{assignment}"));
        let flag = Arc::new(AtomicBool::new(false));
        let cancel = flag.clone();
        let done = Arc::new(AtomicBool::new(false));
        let completed = done.clone();
        let (progress_tx, progress) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let result = coconutclaw_provider::run_provider_session(
                None,
                &config,
                &prompt,
                Some(&cancel),
                Some(&progress_tx),
                None,
                resume.as_deref(),
            );
            // Preserve startup failures from this explicit, isolated fixture;
            // otherwise the parent only sees an expired handshake socket.
            let diagnostic = match &result {
                Ok(output) if !output.success => Some(output.raw_output.clone()),
                Err(error) => Some(format!("{error:#}")),
                _ => None,
            };
            if let Some(diagnostic) = diagnostic {
                let _ = std::fs::write(job.join("startup-error.txt"), diagnostic);
            }
            completed.store(true, Ordering::SeqCst);
            let _ = notice.write_all(&[1]);
            result
        });
        // Own the worker before any fallible handshake. Drop cancels/joins on refusal.
        let mut work = Work {
            flag,
            done,
            handle: Some(handle),
            control: None,
            progress,
        };
        poll(listener.as_raw_fd(), 10000)?;
        let (mut control, _) = listener.accept()?;
        control.set_read_timeout(Some(Duration::from_secs(10)))?;
        let observed: Observed = serde_json::from_slice(&read_frame(&mut control)?)?;
        ensure!(
            observed.parent == std::process::id()
                && observed.group == observed.pid
                && observed.pid != std::process::id()
                && observed.cwd == workspace.to_str().context("workspace encoding")?
                && observed.home == cfg.home.to_str().context("home encoding")?
                && observed.codex_home == cfg.codex_home.to_str().context("codex home encoding")?,
            "wrong fake provider execution boundary"
        );
        work.control = Some(control);
        let evidence = Evidence {
            assignment,
            runner_pid: std::process::id(),
            observed: Some(observed),
            session: None,
            progress: Vec::new(),
            exit_code: None,
            output: None,
            failure: None,
        };
        self.store.conn.execute(
            "INSERT INTO provider_evidence VALUES(?1,?2)",
            params![assignment, serde_json::to_string(&evidence)?],
        )?;
        self.providers.insert(assignment, work);
        Ok(())
    }
    pub(super) fn evidence(&self, assignment: u64) -> Result<Evidence> {
        let json: String = self.store.conn.query_row(
            "SELECT evidence FROM provider_evidence WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        Ok(serde_json::from_str(&json)?)
    }
    fn save_evidence(&self, e: &Evidence) -> Result<()> {
        self.store.conn.execute(
            "UPDATE provider_evidence SET evidence=?2 WHERE assignment=?1",
            params![e.assignment, serde_json::to_string(e)?],
        )?;
        Ok(())
    }
    fn failure(&self, assignment: u64, phase: &str, error: &anyhow::Error) -> Result<()> {
        let mut evidence = self.evidence(assignment)?;
        // Do not persist arbitrary SDK error bodies, commands or credential
        // values. Validation labels are our own constants; I/O retains its kind.
        let text = error.to_string();
        let detail = match text.as_str() {
            "actual native tool call missing"
            | "native Terra/max/yolo context missing"
            | "native session missing"
            | "native tool output did not prove the fixture"
            | "tool ran on wrong host"
            | "shared native config/auth integrity changed"
            | "fixture artifact must be a bounded regular file"
            | "artifact manifest exceeds signed text bound"
            | "provider output exceeds signed proposal limit"
            | "provider worker panic" => text,
            _ => error
                .downcast_ref::<std::io::Error>()
                .map(|e| format!("I/O failure: {:?}", e.kind()))
                .unwrap_or_else(|| {
                    "provider lifecycle failure; inspect scoped native evidence".into()
                }),
        };
        evidence.failure = Some(Failure {
            phase: phase.into(),
            detail,
        });
        self.save_evidence(&evidence)
    }
    pub(super) fn provider_evidence(&self) -> Result<Vec<Evidence>> {
        let ids: Vec<u64> = self
            .store
            .conn
            .prepare("SELECT assignment FROM provider_evidence ORDER BY assignment")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        ensure!(ids.len() <= 4, "provider evidence bound");
        ids.into_iter()
            .map(|id| {
                let mut e = self.evidence(id)?;
                if let Some(work) = self.providers.get(&id) {
                    e.progress.extend(work.progress.try_iter());
                    ensure!(
                        e.progress.len() <= 16 && e.progress.iter().all(|p| p.len() <= 512),
                        "fixture progress bound"
                    );
                    self.save_evidence(&e)?;
                }
                Ok(e)
            })
            .collect()
    }
    pub(super) fn release_provider(&mut self, assignment: u64) -> Result<()> {
        let work = self
            .providers
            .get_mut(&assignment)
            .context("provider absent")?;
        if let Some(control) = work.control.as_mut() {
            control.write_all(&[1])?;
        }
        Ok(())
    }
    pub(super) fn cancel_provider(&self, assignment: u64) {
        if let Some(work) = self.providers.get(&assignment) {
            work.cancel();
        }
    }
    pub(super) fn finish_provider(&mut self, assignment: u64) -> Result<(Outcome, String)> {
        ensure!(
            self.providers
                .get(&assignment)
                .context("provider absent")?
                .done
                .load(Ordering::SeqCst),
            "provider still running; completion RPC never waits for work"
        );
        if let Err(error) = self.provider_evidence() {
            self.failure(assignment, "completion-evidence", &error)?;
            return Err(error);
        }
        let mut work = self
            .providers
            .remove(&assignment)
            .context("provider absent")?;
        let joined = work.handle.take().context("provider joined")?.join();
        let output = match joined {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                self.failure(assignment, "provider-api", &error)?;
                return Err(error);
            }
            Err(_) => {
                let error = anyhow::anyhow!("provider worker panic");
                self.failure(assignment, "provider-worker", &error)?;
                return Err(error);
            }
        };
        // Provider returns only after its process wait and pipe processing.
        let mut evidence = self.evidence(assignment)?;
        evidence.progress.extend(work.progress.try_iter());
        evidence.session = output.session_id;
        evidence.exit_code = Some(output.exit_code);
        evidence.output = Some(output.raw_output.clone());
        self.save_evidence(&evidence)?;
        if self.root.join("host-provider.json").is_file() && output.success {
            return match host::manifest(&self.root, assignment, &evidence) {
                Ok(manifest) => Ok((Outcome::Completed, manifest)),
                Err(error) => {
                    self.failure(assignment, "artifact-validation", &error)?;
                    Err(error)
                }
            };
        }
        if output.raw_output.len() > 2048 {
            let error = anyhow::anyhow!("provider output exceeds signed proposal limit");
            self.failure(assignment, "output-bound", &error)?;
            return Err(error);
        }
        Ok((
            if output.exit_code == 130 {
                Outcome::Cancelled
            } else if output.success {
                Outcome::Completed
            } else {
                Outcome::Failed
            },
            output.raw_output,
        ))
    }
}
