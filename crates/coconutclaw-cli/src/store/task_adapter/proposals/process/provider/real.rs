//! Owner-opted-in local real provider acceptance, never enabled by default tests.
use super::*;
const WRAPPER: &str = "/home/leo/.local/lib/coconutclaw/codex-safe";
pub(super) fn validate(root: &Path) -> Result<()> {
    ensure!(
        std::env::var("COCONUTCLAW_REAL_CODEX_ACCEPTANCE").as_deref() == Ok("1"),
        "real Codex requires explicit owner acceptance opt-in"
    );
    ensure!(
        std::env::var("HOME").as_deref() == Ok("/home/leo")
            && std::env::var_os("CODEX_HOME").as_deref()
                == Some(root.join("codex-home").as_os_str())
            && Path::new(WRAPPER).is_file(),
        "real wrapper/auth location unavailable"
    );
    Ok(())
}
impl State {
    pub(super) fn start_real(&mut self, assignment: u64) -> Result<()> {
        validate(&self.root)?;
        let job = self.root.join(format!("provider-{assignment}"));
        std::fs::create_dir(&job)?;
        let workspace = self.root.join(format!("workspace/{assignment}"));
        ensure!(workspace.is_dir(), "explicit real fixture workspace absent");
        let mut notice = UnixStream::connect(self.root.join(format!("done-{assignment}.sock")))?;
        let mut config = RuntimeConfig::test_config_at(job.join("config"));
        std::fs::create_dir_all(&config.tmp_dir)?;
        config.instance_dir = workspace;
        // Do not assign the shared config directory to the native CLI: it
        // persists project trust even when invocation overrides supply trust.
        let home = self.root.join("codex-home");
        std::fs::write(
            home.join("auth-in-place.py"),
            include_str!("auth_in_place.py"),
        )?;
        let provision = OsCommand::new("/usr/bin/python3")
            .args(["-I", "-c", include_str!("real_routing.py")])
            .arg(&home)
            .status()?;
        ensure!(
            provision.success(),
            "isolated real routing configuration failed"
        );
        config.codex.bin = WRAPPER.into();
        config.codex.model = Some("gpt-6.1-terra".into());
        config.exec_policy = "yolo".into();
        config.telegram_bot_token = None;
        // Real acceptance follows existing activity without an arbitrary short cutoff.
        config.provider_timeout_secs = 0;
        // Retain existing 30-minute activity / 2-hour pending-tool limits.
        // Zero idle means immediate inactivity, unlike the optional total limit.
        let run: i64 = self.store.conn.query_row(
            "SELECT run FROM process_claim WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        let prompt = self
            .store
            .get_task_run(run)?
            .context("child claim absent")?
            .prompt;
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
                None,
            );
            completed.store(true, Ordering::SeqCst);
            let _ = notice.write_all(&[1]);
            result
        });
        let work = Work {
            flag,
            done,
            handle: Some(handle),
            control: None,
            progress,
        };
        let evidence = Evidence {
            assignment,
            runner_pid: std::process::id(),
            observed: None,
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
}
