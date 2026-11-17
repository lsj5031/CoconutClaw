//! One explicitly provisioned native Codex file fixture, portable across Unix.
use super::*;

pub(in crate::store::task_adapter) const ARTIFACT: &str =
    "CoconutClaw multihost fixture artifact v1";
pub(in crate::store::task_adapter) const PROMPT: &str = "Authorized harmless multihost acceptance. Use an actual shell/Python tool to read fixture-input.json in this current workspace and write fixture-output.json with exactly nonce, sum=a+b, tool_pid=os.getpid(), tool_parent=os.getppid(), hostname=socket.gethostname(). Only read/write these two fixture files. No network, home/auth/config/repository access, delegation or other side effects. Execute the tool, do not merely describe it. Final answer only nonce=<nonce> sum=<sum>.";
pub(in crate::store::task_adapter) const CONTROLLED_PROMPT: &str = "Authorized harmless controlled acceptance. Execute exactly one Python tool, no retries. Read fixture-input.json in this workspace. Write fixture-tool-started.json with nonce, tool_pid=os.getpid(), tool_parent=os.getppid(), pgid=os.getpgrp(), hostname=socket.gethostname(), started_ns=time.time_ns(). In that SAME Python process poll every 0.05s for at most 600 seconds, only in this workspace: if fixture-cancel.json exists, write fixture-tool-ended.json with nonce, tool_pid, cancelled=true, ended_ns=time.time_ns() and exit130; if fixture-release.json exists, write fixture-output.json with nonce, sum=a+b, tool_pid, tool_parent, hostname, started_ns and finished_ns=time.time_ns(), then write fixture-tool-ended.json with nonce, tool_pid, cancelled=false and ended_ns. If neither arrives before deadline, exit with failure, do not compute output. Only these six fixture files may be accessed. No network, home/auth/config/repository access, delegation or other effects. Execute, do not describe. Final answer only nonce=<nonce> sum=<sum> after success, otherwise cancelled or timed out.";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct Config {
    pub executable: PathBuf,
    pub python: PathBuf,
    /// Read by the local wrapper only, never copied into the fixture.
    pub auth_json: PathBuf,
    pub shared_config: PathBuf,
    pub provider_name: String,
    pub base_url: String,
    pub wire_api: String,
    #[serde(default)]
    pub auth_command_in_place: bool,
    #[serde(default)]
    pub controlled_fixture: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct Input {
    pub nonce: String,
    pub a: i64,
    pub b: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::store::task_adapter) struct Manifest {
    pub version: u8,
    pub assignment: u64,
    pub nonce: String,
    pub sum: i64,
    pub hash: String,
    pub size: usize,
    pub session: String,
    pub runner_pid: u32,
    pub hostname: String,
}

fn load(root: &Path) -> Result<Config> {
    Ok(serde_json::from_slice(&std::fs::read(
        root.join("host-provider.json"),
    )?)?)
}

fn regular(path: &Path) -> Result<Vec<u8>> {
    let stat = std::fs::symlink_metadata(path)?;
    ensure!(
        stat.file_type().is_file() && stat.len() > 0 && stat.len() <= 4096,
        "fixture artifact must be a bounded regular file"
    );
    Ok(std::fs::read(path)?)
}

pub(in crate::store::task_adapter) fn validate(root: &Path) -> Result<()> {
    let cfg = load(root)?;
    for p in [
        &cfg.executable,
        &cfg.python,
        &cfg.auth_json,
        &cfg.shared_config,
    ] {
        ensure!(
            p.is_absolute() && p.is_file() && !p.to_string_lossy().contains(['\'', '\n', '\r']),
            "explicit host provider dependency missing or unsupported path"
        );
    }
    let url = reqwest::Url::parse(&cfg.base_url)?;
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && matches!(url.scheme(), "http" | "https"),
        "routing URL must contain no credentials/query/fragment"
    );
    ensure!(
        !cfg.provider_name.is_empty() && cfg.wire_api == "responses",
        "explicit native responses routing required"
    );
    Ok(())
}

fn integrity(cfg: &Config) -> Result<serde_json::Value> {
    use std::os::unix::fs::MetadataExt;
    let auth = std::fs::metadata(&cfg.auth_json)?;
    let shared = std::fs::metadata(&cfg.shared_config)?;
    Ok(serde_json::json!({
        "config_hash":blake3::hash(&std::fs::read(&cfg.shared_config)?).to_string(),
        "auth_hash":blake3::hash(&std::fs::read(&cfg.auth_json)?).to_string(),
        "config_metadata":[shared.len(),shared.ino(),shared.dev(),shared.mode() as u64],
        "config_mtime":shared.mtime(),"config_ctime":shared.ctime(),
        "config_mtime_nsec":shared.mtime_nsec(),"config_ctime_nsec":shared.ctime_nsec(),
        "auth_metadata":[auth.len(),auth.ino(),auth.dev(),auth.mode() as u64],
        "auth_ctime":auth.ctime(),"auth_ctime_nsec":auth.ctime_nsec(),
        "auth_size":auth.len(),
        "auth_modified":auth.modified()?.duration_since(std::time::UNIX_EPOCH)?.as_nanos().to_string()
    }))
}

fn integrity_report(root: &Path) -> Result<serde_json::Value> {
    let before: serde_json::Value = serde_json::from_slice(&std::fs::read(
        root.join("provider-1/integrity-before.json"),
    )?)?;
    ensure!(
        before["config_hash"]
            .as_str()
            .is_some_and(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
            && before["auth_size"].as_u64().is_some()
            && before["auth_modified"]
                .as_str()
                .is_some_and(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit())),
        "invalid integrity-before record"
    );
    let after = integrity(&load(root)?)?;
    Ok(serde_json::json!({
        "assignment":1,"read_only":true,"config_hash_algorithm":"BLAKE3",
        "config_hash_before":before["config_hash"],"config_hash_after":after["config_hash"],
        "config_unchanged":before["config_hash"]==after["config_hash"],
        "auth_size_unchanged":before["auth_size"]==after["auth_size"],
        "auth_mtime_unchanged":before["auth_modified"]==after["auth_modified"],
        "auth_bytes_unchanged":before["auth_hash"]==after["auth_hash"],
        "metadata_unchanged":before["auth_metadata"]==after["auth_metadata"]
            && before["config_metadata"]==after["config_metadata"]
            && before["auth_ctime"]==after["auth_ctime"]
            && before["auth_ctime_nsec"]==after["auth_ctime_nsec"]
            && before["config_mtime"]==after["config_mtime"]
            && before["config_mtime_nsec"]==after["config_mtime_nsec"]
            && before["config_ctime"]==after["config_ctime"]
            && before["config_ctime_nsec"]==after["config_ctime_nsec"],
        "unchanged":before==after
    }))
}

#[test]
#[ignore = "explicit read-only fixture integrity diagnostic; never invokes a provider"]
fn integrity_diagnostic_entry() {
    let root = PathBuf::from(
        std::env::var_os("COCONUTCLAW_HOST_INTEGRITY_ROOT")
            .expect("explicit fixture root required"),
    );
    assert!(root.is_absolute());
    assert_eq!(
        std::fs::read_to_string(root.join("MULTIHOST")).unwrap(),
        "CoconutClaw disposable multihost acceptance v1\n"
    );
    println!("COCONUTCLAW_INTEGRITY={}", integrity_report(&root).unwrap());
}

impl State {
    pub(super) fn start_host(&mut self, assignment: u64) -> Result<()> {
        validate(&self.root)?;
        let cfg = load(&self.root)?;
        let job = self.root.join(format!("provider-{assignment}"));
        std::fs::create_dir(&job)?;
        let workspace = self.root.join(format!("workspace/{assignment}"));
        ensure!(workspace.is_dir(), "explicit multihost workspace absent");
        let before = integrity(&cfg)?;
        std::fs::write(
            job.join("integrity-before.json"),
            serde_json::to_vec(&before)?,
        )?;
        let home = job.join("codex-home");
        std::fs::create_dir(&home)?;
        // Only public routing is projected. Native auth is supplied in memory
        // by the owned local wrapper from this host's existing auth file.
        let name = serde_json::to_string(&cfg.provider_name)?;
        let mut routing = format!(
            "model = \"gpt-6.1-terra\"\nmodel_reasoning_effort = \"max\"\nproject_doc_max_bytes = 0\nmodel_provider = {name}\n[model_providers.{name}]\nname = {name}\nbase_url = {}\nwire_api = \"responses\"\nenv_key = \"COCONUTCLAW_ACCEPTANCE_API_KEY\"\nrequires_openai_auth = false\n[shell_environment_policy]\ninherit = \"none\"\n[shell_environment_policy.set]\nPATH = \"/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin\"\n",
            serde_json::to_string(&cfg.base_url)?
        );
        if cfg.auth_command_in_place {
            // The shim rereads the original selected provider in place. Neither
            // command arguments nor returned credentials are projected here.
            let shim = job.join("host-auth-in-place.py");
            std::fs::write(&shim, include_str!("host_auth_in_place.py"))?;
            routing = format!(
                "model = \"gpt-6.1-terra\"\nmodel_reasoning_effort = \"max\"\nproject_doc_max_bytes = 0\nmodel_provider = {name}\n[model_providers.{name}]\nname = {name}\nbase_url = {}\nwire_api = \"responses\"\n[model_providers.{name}.auth]\ncommand = {}\nargs = {}\ntimeout_ms = 10000\n[shell_environment_policy]\ninherit = \"none\"\n[shell_environment_policy.set]\nPATH = \"/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin\"\n",
                serde_json::to_string(&cfg.base_url)?,
                serde_json::to_string(&cfg.python)?,
                serde_json::to_string(&[
                    "-I".to_owned(),
                    shim.to_string_lossy().into_owned(),
                    self.root
                        .join("host-provider.json")
                        .to_string_lossy()
                        .into_owned(),
                ])?,
            );
        }
        std::fs::write(home.join("config.toml"), routing)?;
        let wrapper = job.join("native-exec.py");
        std::fs::write(&wrapper, include_str!("native_exec.py"))?;
        let guardian = UnixListener::bind(job.join("guardian.sock"))?;
        let mut notice = UnixStream::connect(self.root.join(format!("done-{assignment}.sock")))?;
        let mut config = RuntimeConfig::test_config_at(job.join("config"));
        std::fs::create_dir_all(&config.tmp_dir)?;
        config.instance_dir = workspace;
        config.codex.bin = format!(
            "'{}' -I '{}' '{}' '{}'",
            cfg.python.display(),
            wrapper.display(),
            self.root.join("host-provider.json").display(),
            job.display()
        );
        config.codex.model = Some("gpt-6.1-terra".into());
        config.exec_policy = "yolo".into();
        config.telegram_bot_token = None;
        config.provider_timeout_secs = 0; // Existing activity limits remain.
        let run: i64 = self.store.conn.query_row(
            "SELECT run FROM process_claim WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        let prompt = self
            .store
            .get_task_run(run)?
            .context("host claim absent")?
            .prompt;
        ensure!(
            prompt
                == if cfg.controlled_fixture {
                    CONTROLLED_PROMPT
                } else {
                    PROMPT
                },
            "only the owned harmless file fixture is admitted"
        );
        let flag = Arc::new(AtomicBool::new(false));
        let cancel = flag.clone();
        let done = Arc::new(AtomicBool::new(false));
        let completed = done.clone();
        let (progress_tx, progress) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            // Keeping this listener alive is a kernel parent-liveness lease.
            // Abrupt runner death closes it, triggering the wrapper's group
            // cleanup without trusting a stale recorded PID on restart.
            let _guardian = guardian;
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
        self.providers.insert(
            assignment,
            Work {
                flag,
                done,
                handle: Some(handle),
                control: None,
                progress,
            },
        );
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
        Ok(())
    }
}

pub(super) fn manifest(root: &Path, assignment: u64, evidence: &Evidence) -> Result<String> {
    let cfg = load(root)?;
    let job = root.join(format!("provider-{assignment}"));
    let before: serde_json::Value =
        serde_json::from_slice(&std::fs::read(job.join("integrity-before.json"))?)?;
    ensure!(
        integrity(&cfg)? == before,
        "shared native config/auth integrity changed"
    );
    let session = evidence.session.clone().context("native session missing")?;
    let workspace = root.join(format!("workspace/{assignment}"));
    let input: Input = serde_json::from_slice(&regular(&workspace.join("fixture-input.json"))?)?;
    let bytes = regular(&workspace.join("fixture-output.json"))?;
    let output: serde_json::Value = serde_json::from_slice(&bytes)?;
    ensure!(
        output["nonce"] == input.nonce
            && output["sum"] == input.a + input.b
            && output["tool_pid"].as_u64().is_some_and(|p| p > 1)
            && output["tool_parent"].as_u64().is_some_and(|p| p > 1),
        "native tool output did not prove the fixture"
    );
    let hostname = super::super::remote::hostname()?;
    ensure!(output["hostname"] == hostname, "tool ran on wrong host");
    verify_tool_artifact(&input, &hostname, &bytes, cfg.controlled_fixture)?;
    let mut contexts = Vec::new();
    let mut calls = Vec::new();
    let mut outputs = std::collections::BTreeSet::new();
    fn scan(
        root: &Path,
        session: &str,
        contexts: &mut Vec<serde_json::Value>,
        calls: &mut Vec<serde_json::Value>,
        outputs: &mut std::collections::BTreeSet<(String, String)>,
    ) -> Result<()> {
        for entry in std::fs::read_dir(root)? {
            let path = entry?.path();
            if path.is_dir() {
                scan(&path, session, contexts, calls, outputs)?;
            } else if path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().contains(session))
            {
                for line in std::fs::read_to_string(path)?.lines() {
                    let event: serde_json::Value = serde_json::from_str(line)?;
                    let p = &event["payload"];
                    if event["type"] == "turn_context" {
                        contexts.push(serde_json::json!({
                            "cwd":p["cwd"],"model":p["model"],"effort":p["effort"],
                            "approval_policy":p["approval_policy"],"sandbox_policy":p["sandbox_policy"]
                        }));
                    } else if event["type"] == "response_item" {
                        match p["type"].as_str() {
                            Some(kind @ ("function_call" | "custom_tool_call")) => {
                                let arguments = if kind == "function_call" {
                                    &p["arguments"]
                                } else {
                                    &p["input"]
                                };
                                calls.push(serde_json::json!({
                                    "type":kind,"name":p["name"],"call_id":p["call_id"],
                                    "status":p["status"],"arguments":arguments
                                }));
                            }
                            Some(kind @ ("function_call_output" | "custom_tool_call_output")) => {
                                if let Some(id) = p["call_id"]
                                    .as_str()
                                    .filter(|id| !id.is_empty() && id.len() <= 256)
                                    && p.get("output").is_some_and(|v| !v.is_null())
                                {
                                    outputs.insert((kind.to_owned(), id.to_owned()));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }
    scan(
        &job.join("codex-home/sessions"),
        &session,
        &mut contexts,
        &mut calls,
        &mut outputs,
    )?;
    ensure!(
        contexts.iter().any(|c| c["model"] == "gpt-6.1-terra"
            && c["effort"] == "max"
            && c["approval_policy"] == "never"),
        "native Terra/max/yolo context missing"
    );
    let complete_calls: Vec<_> = calls
        .into_iter()
        .filter_map(|mut c| {
            let output_type = match c["type"].as_str()? {
                "function_call" => "function_call_output",
                "custom_tool_call" => "custom_tool_call_output",
                _ => return None,
            };
            let id = c["call_id"]
                .as_str()
                .filter(|id| !id.is_empty() && id.len() <= 256)?;
            if !outputs.contains(&(output_type.to_owned(), id.to_owned())) {
                return None;
            }
            c["output_type"] = output_type.into();
            Some(c)
        })
        .collect();
    ensure!(
        complete_calls.iter().any(|c| matches!(
            c["name"].as_str(),
            Some("exec" | "exec_command" | "shell" | "shell_command")
        ) && c["arguments"]
            .as_str()
            .is_some_and(|a| a.contains("fixture-output.json"))),
        "actual native tool call missing"
    );
    std::fs::write(
        job.join("scoped-native-events.json"),
        serde_json::to_vec_pretty(
            &serde_json::json!({"session":session,"contexts":contexts,"tool_calls":complete_calls}),
        )?,
    )?;
    let manifest = Manifest {
        version: 1,
        assignment,
        nonce: input.nonce,
        sum: input.a + input.b,
        hash: blake3::hash(&bytes).to_string(),
        size: bytes.len(),
        session,
        runner_pid: std::process::id(),
        hostname,
    };
    let text = serde_json::to_string(&manifest)?;
    ensure!(
        text.len() <= 2048,
        "artifact manifest exceeds signed text bound"
    );
    Ok(text)
}

pub(in crate::store::task_adapter) fn verify_artifact(
    manifest: &Manifest,
    assignment: u64,
    input: &Input,
    host: &str,
    bytes: &[u8],
    controlled: bool,
) -> Result<()> {
    ensure!(
        manifest.version == 1
            && manifest.assignment == assignment
            && manifest.nonce == input.nonce
            && manifest.sum == input.a + input.b
            && manifest.hostname == host
            && !manifest.session.is_empty()
            && manifest.size > 0
            && manifest.size <= 4096
            && manifest.size == bytes.len()
            && manifest.hash == blake3::hash(bytes).to_string(),
        "artifact binding mismatch"
    );
    verify_tool_artifact(input, host, bytes, controlled)
}

fn verify_tool_artifact(input: &Input, host: &str, bytes: &[u8], controlled: bool) -> Result<()> {
    let v: serde_json::Value = serde_json::from_slice(bytes)?;
    ensure!(
        v.as_object()
            .is_some_and(|o| o.len() == if controlled { 7 } else { 5 })
            && v["nonce"] == input.nonce
            && v["sum"] == input.a + input.b
            && v["hostname"] == host
            && v["tool_pid"].as_u64().is_some_and(|p| p > 1)
            && v["tool_parent"].as_u64().is_some_and(|p| p > 1),
        "invalid tool artifact"
    );
    if controlled {
        let start = v["started_ns"]
            .as_i64()
            .context("invalid start timestamp")?;
        let finish = v["finished_ns"]
            .as_i64()
            .context("invalid finish timestamp")?;
        ensure!(start > 0 && finish >= start, "invalid tool timestamp order");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
