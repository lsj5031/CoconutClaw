use super::*;

/// Loopback-only stateful UDP path: reverse traffic is dropped until a gateway
/// packet opens it. No firewall, host configuration, external endpoint or clock
/// change. Closing the gate models idle loss without a minute-long test sleep.
struct ColdPath {
    core: std::net::SocketAddr,
    runner: std::net::SocketAddr,
    warm: Arc<std::sync::atomic::AtomicBool>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    task: Option<std::thread::JoinHandle<()>>,
}

impl ColdPath {
    fn new(core: std::net::SocketAddr, runner: std::net::SocketAddr) -> Self {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        let front_core = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let front_runner = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        front_core.set_nonblocking(true).unwrap();
        front_runner.set_nonblocking(true).unwrap();
        let mut gate = Self {
            core: front_core.local_addr().unwrap(),
            runner: front_runner.local_addr().unwrap(),
            warm: Arc::new(AtomicBool::new(false)),
            dropped: Arc::new(AtomicUsize::new(0)),
            stop: Arc::new(AtomicBool::new(false)),
            task: None,
        };
        let (warm, dropped, stop) = (gate.warm.clone(), gate.dropped.clone(), gate.stop.clone());
        gate.task = Some(std::thread::spawn(move || {
            let mut bytes = [0; 65536];
            while !stop.load(Ordering::SeqCst) {
                if let Ok((size, from)) = front_runner.recv_from(&mut bytes) {
                    assert_eq!(from, core);
                    warm.store(true, Ordering::SeqCst);
                    front_core.send_to(&bytes[..size], runner).unwrap();
                }
                if let Ok((size, from)) = front_core.recv_from(&mut bytes) {
                    assert_eq!(from, runner);
                    if warm.load(Ordering::SeqCst) {
                        front_runner.send_to(&bytes[..size], core).unwrap();
                    } else {
                        dropped.fetch_add(1, Ordering::SeqCst);
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }));
        gate
    }
}

impl Drop for ColdPath {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.task.take().unwrap().join().unwrap();
    }
}

#[test]
fn signed_cancellation_receipt_opens_cold_reverse_data_path_before_import() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            use crate::store::task_adapter::TaskAdapter;
            use coconutclaw_sync::synthetic::membership::domain::Origin;
            let temp = tempfile::tempdir().unwrap();
            let core_cfg = config(temp.path().join("core"), "coordinator");
            let _lock = check_root(&core_cfg).unwrap();
            let owner = local_key(&core_cfg.root, "owner-key").unwrap();
            let core = local_key(&core_cfg.root, "role-key").unwrap();
            let mut publisher = peer(&core_cfg, "data", owner.public()).await.unwrap();
            let endpoint = endpoint(&core_cfg.network, core.clone()).await.unwrap();
            let mut runner_cfg = config(temp.path().join("runner"), "runner");
            runner_cfg.owner = Some(owner.public());
            runner_cfg.core = Some(core.public());
            let mut runner = launch(runner_cfg);
            let mut ready = runner.ready.clone();
            ready.role = "coordinator".into();
            ready.control = endpoint.addr();
            ready.data = publisher.addr();
            ready.author = publisher.author().await.unwrap();
            let origin = BoundOrigin::sign(
                Origin {
                    owner: owner.public(),
                    store: blake3::hash(core_cfg.run_id.as_bytes()).to_string(),
                    namespace: namespace(&core_cfg).id(),
                    scope: "synthetic/chat-a".into(),
                    core: core.public(),
                    runners: BTreeSet::from([runner.ready.control.id]),
                },
                &owner,
            )
            .unwrap();
            let policy = SignedPolicy::sign(
                Policy {
                    generation: 1,
                    expires_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        + 86400,
                    namespace: namespace(&core_cfg).id(),
                    scope: origin.origin.scope.clone(),
                    members: vec![
                        Member {
                            endpoint: ready.data.id,
                            authors: BTreeSet::from([ready.author]),
                        },
                        Member {
                            endpoint: runner.ready.data.id,
                            authors: BTreeSet::from([runner.ready.author]),
                        },
                    ],
                },
                &owner,
            )
            .unwrap();
            publisher.install_policy(&policy).unwrap();
            let mut client = Client {
                endpoint: endpoint.clone(),
                ready: runner.ready.clone(),
                origin: origin.clone(),
                slots: Arc::new(Semaphore::new(4)),
            };
            client
                .initialize(
                    policy.clone(),
                    ready,
                    provider::host::Input {
                        nonce: core_cfg.run_id.clone(),
                        a: 17,
                        b: 25,
                    },
                )
                .await
                .unwrap();
            let session = SessionKey::local("cold-reverse-receipt");
            let source =
                Store::open(&RuntimeConfig::test_config_at(core_cfg.root.join("source"))).unwrap();
            let id = super::super::super::super::tests::enqueue_text(
                &source,
                &session,
                0,
                "harmless fixture",
            );
            let adapter = TaskAdapter::open(&source, &origin, &session).unwrap();
            let assign = adapter
                .export(&mut publisher, id, &core, runner.ready.control.id)
                .await
                .unwrap();
            let core_data = publisher.addr().id;
            let initial_addr = publisher.addr();
            client
                .import(
                    &mut publisher,
                    Locator {
                        addr: initial_addr.clone(),
                        author: assign.origin.parse().unwrap(),
                        record: assign,
                    },
                )
                .await
                .unwrap();
            // Lose the old source data path, retaining the same scoped authority
            // and original assignment. The runner's cached direct address is now
            // dead, as after an idle NAT mapping is lost. The new reverse path
            // exposes only the cold proxy, never the source's new direct port.
            publisher.shutdown().await.unwrap();
            publisher = peer(&core_cfg, "data", owner.public()).await.unwrap();
            publisher.install_policy(&policy).unwrap();
            assert_ne!(publisher.addr(), initial_addr);
            assert_eq!(publisher.addr().id, core_data);
            let path = ColdPath::new(
                *publisher.addr().ip_addrs().next().unwrap(),
                *runner.ready.data.ip_addrs().next().unwrap(),
            );
            client.ready.data = EndpointAddr::new(client.ready.data.id).with_ip_addr(path.runner);
            let cancel = adapter
                .request_cancel(&mut publisher, 1, &core)
                .await
                .unwrap();
            let began = std::time::Instant::now();
            let received = client
                .import(
                    &mut publisher,
                    Locator {
                        addr: EndpointAddr::new(core_data).with_ip_addr(path.core),
                        author: cancel.origin.parse().unwrap(),
                        record: cancel,
                    },
                )
                .await;
            assert!(
                received.is_ok(),
                "signed cancellation receipt failed after {:?}, dropped {} packets: {received:?}",
                began.elapsed(),
                path.dropped.load(Ordering::SeqCst)
            );
            assert!(
                began.elapsed() < Duration::from_secs(3),
                "cancellation data transfer was not prompt"
            );
            assert!(
                path.warm.load(Ordering::SeqCst),
                "gateway never opened reverse data path"
            );
            assert!(matches!(
                client.call(Command::Stop).await.unwrap(),
                Reply::Unit
            ));
            assert!(runner.child.wait().unwrap().success());
            endpoint.close().await;
            publisher.shutdown().await.unwrap();
        });
}

#[test]
fn actual_remote_host_control_cancellation_settles_without_output_or_reinvocation() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let exe = root.join("native-fixture");
    std::fs::write(&exe, r#"#!/usr/bin/python3
import json,os,signal,subprocess,sys
from pathlib import Path
signal.signal(signal.SIGTERM,lambda *_:None)
sys.stdin.read()
print(json.dumps({'type':'thread.started','thread_id':'fixture-cancel-session'}),flush=True)
print(json.dumps({'type':'turn.started'}),flush=True)
Path('native-started').write_text(str(os.getpid()))
tool=subprocess.Popen([sys.executable,'-c',"""
import json,os,socket,time
from pathlib import Path
nonce=json.loads(Path('fixture-input.json').read_text())['nonce']
Path('fixture-tool-started.json').write_text(json.dumps(dict(nonce=nonce,tool_pid=os.getpid(),
    tool_parent=os.getppid(),pgid=os.getpgrp(),hostname=socket.gethostname(),started_ns=time.time_ns())))
while not Path('fixture-cancel.json').exists(): time.sleep(0.01)
Path('fixture-tool-ended.json').write_text(json.dumps(dict(nonce=nonce,tool_pid=os.getpid(),
    cancelled=True,ended_ns=time.time_ns())))
raise SystemExit(130)
"""],start_new_session=True)
raise SystemExit(tool.wait())
"#).unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o700)).unwrap();
    let auth = root.join("auth.json");
    std::fs::write(&auth, b"{\"OPENAI_API_KEY\":\"synthetic-only\"}").unwrap();
    let shared = root.join("config.toml");
    std::fs::write(&shared, b"model='fixture'\n").unwrap();
    let core_cfg = config(root.join("core"), "coordinator");
    let mut core = launch(core_cfg.clone());
    let mut remote_cfg = config(root.join("runner"), "runner");
    remote_cfg.owner = Some(core.ready.owner);
    remote_cfg.core = Some(core.ready.control.id);
    remote_cfg.provider = Some(provider::host::Config {
        executable: exe,
        python: "/usr/bin/python3".into(),
        auth_json: auth,
        shared_config: shared,
        provider_name: "fixture".into(),
        base_url: "http://127.0.0.1:1/v1".into(),
        wire_api: "responses".into(),
        auth_command_in_place: false,
        controlled_fixture: true,
    });
    let mut runner = launch(remote_cfg.clone());
    let ready = runner.ready.clone();
    let core_root = core_cfg.root.clone();
    let work = std::thread::spawn(move || control(&core_root, "run-cancel", Some(&ready)));
    let workspace = remote_cfg.root.join("workspace/1");
    let started = workspace.join("fixture-tool-started.json");
    for _ in 0..1000 {
        if started.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        started.exists(),
        "owned deterministic native fixture not started"
    );
    let tool: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&started).unwrap()).unwrap();
    assert_eq!(tool["nonce"], core_cfg.run_id);
    // A real tool can outlive the per-fetch 10s deadline without expiring an
    // otherwise live scope. Cancel must not wait for the provider to finish.
    std::thread::sleep(Duration::from_secs(11));
    let requested_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let began = std::time::Instant::now();
    let cancelled = control(&core_cfg.root, "cancel", None);
    let acknowledgment_time = began.elapsed();
    println!("cancellation custody acknowledgment: {acknowledgment_time:?}");
    if cancelled["cancel_requested"] != true {
        drop(core);
        drop(runner);
        let _ = work.join();
        panic!("real cancel control refused: {cancelled}");
    }
    let receipt = work.join().unwrap();
    assert_eq!(receipt["ok"], true, "{receipt}");
    assert_eq!(receipt["source_status"], "cancelled");
    assert_eq!(receipt["invocations"], 1);
    assert!(
        acknowledgment_time < Duration::from_secs(3),
        "cancellation custody acknowledgment took {acknowledgment_time:?}"
    );
    let native_pid: i32 = std::fs::read_to_string(workspace.join("native-started"))
        .unwrap()
        .parse()
        .unwrap();
    let observed_launch: serde_json::Value = serde_json::from_slice(
        &std::fs::read(remote_cfg.root.join("provider-1/launch.json")).unwrap(),
    )
    .unwrap();
    for pid in [
        native_pid,
        tool["tool_pid"].as_i64().unwrap() as i32,
        observed_launch["pid"].as_i64().unwrap() as i32,
        -(tool["pgid"].as_i64().unwrap() as i32),
        -(observed_launch["group"].as_i64().unwrap() as i32),
    ] {
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "owned fixture process/group {pid} survived cancellation"
        );
    }
    let ended: serde_json::Value =
        serde_json::from_slice(&std::fs::read(workspace.join("fixture-tool-ended.json")).unwrap())
            .unwrap();
    assert_eq!(ended["nonce"], tool["nonce"]);
    assert_eq!(ended["tool_pid"], tool["tool_pid"]);
    assert_eq!(ended["cancelled"], true);
    let tool_cancel_ns = u128::from(ended["ended_ns"].as_u64().unwrap()) - requested_ns;
    assert!(
        tool_cancel_ns < Duration::from_secs(3).as_nanos(),
        "tool cancellation took {tool_cancel_ns}ns"
    );
    eprintln!(
        "signed cancellation: custody_ack={acknowledgment_time:?}, tool_stop={tool_cancel_ns}ns, full_receipt={:?}",
        began.elapsed()
    );
    assert!(
        !remote_cfg
            .root
            .join("workspace/1/fixture-output.json")
            .exists()
    );
    assert_eq!(control(&core_cfg.root, "stop", None)["stopped"], true);
    assert!(core.child.wait().unwrap().success());
    assert!(runner.child.wait().unwrap().success());
    archive_role_log(root, "coordinator");
    archive_role_log(root, "runner");
    let mut restarted_core = launch(core_cfg.clone());
    let mut restarted_runner = launch(remote_cfg);
    let replay = control(
        &core_cfg.root,
        "replay-cancel",
        Some(&restarted_runner.ready),
    );
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["source_status"], "cancelled");
    assert_eq!(replay["invocations"], 1);
    assert_eq!(replay["event_hashes"], receipt["event_hashes"]);
    assert_eq!(own_probe(&core_cfg.root)["exact_duplicate"], true);
    assert_eq!(control(&core_cfg.root, "stop", None)["stopped"], true);
    assert!(restarted_core.child.wait().unwrap().success());
    assert!(restarted_runner.child.wait().unwrap().success());
}

#[test]
fn macos_hardware_identity_uses_only_valid_unique_ioreg_uuid() {
    let sample = br#"+-o Root <class IOPlatformExpertDevice>
    {
      "IOPlatformSerialNumber" = "synthetic-not-an-identity"
      "IOPlatformUUID" = "12345678-9ABC-4DEF-8123-123456789ABC"
    }
"#;
    let expected = "12345678-9abc-4def-8123-123456789abc";
    assert_eq!(macos_identity(true, sample).unwrap(), expected);
    assert!(macos_identity(false, sample).is_err());
    for malformed in [
        "",
        "\"IOPlatformSerialNumber\" = \"12345678-9ABC-4DEF-8123-123456789ABC\"",
        "\"IOPlatformUUID\" = \"\"",
        "\"IOPlatformUUID\" = 42",
        "\"IOPlatformUUID\" = \"12345678-9abc-4def-8123-123456789abz\"",
        "\"IOPlatformUUID\" = \"123456789abc4def8123123456789abc\"",
        "\"IOPlatformUUID\" = \"00000000-0000-0000-0000-000000000000\"",
        "\"IOPlatformUUID\" = \"ffffffff-ffff-ffff-ffff-ffffffffffff\"",
    ] {
        assert!(macos_identity(true, malformed.as_bytes()).is_err());
    }
    assert!(macos_identity(true, &[255]).is_err());
    let mut maximum = sample.to_vec();
    maximum.resize(65536, b' ');
    assert_eq!(macos_identity(true, &maximum).unwrap(), expected);
    assert!(macos_identity(true, &vec![b' '; 65537]).is_err());
    let duplicate =
        format!("\"IOPlatformUUID\" = \"{expected}\"\n\"IOPlatformUUID\" = \"{expected}\"");
    assert!(macos_identity(true, duplicate.as_bytes()).is_err());
    let other = b"\"IOPlatformUUID\" = \"12345678-9abc-4def-8123-123456789abd\"";
    assert_ne!(
        blake3::hash(macos_identity(true, sample).unwrap().as_bytes()),
        blake3::hash(macos_identity(true, other).unwrap().as_bytes())
    );
    let lower = format!("\"IOPlatformUUID\" = \"{expected}\"");
    assert_eq!(macos_identity(true, lower.as_bytes()).unwrap(), expected);
    #[cfg(target_os = "linux")]
    assert_eq!(
        machine_hash().unwrap(),
        blake3::hash(&std::fs::read("/etc/machine-id").unwrap()).to_string()
    );
}

struct Role {
    child: Child,
    ready: Ready,
}

impl Drop for Role {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn launch(mut config: RoleConfig) -> Role {
    let root = config.root.parent().unwrap();
    let ack = root.join(format!("{}-ack.sock", config.role));
    let listener = UnixListener::bind(&ack).unwrap();
    config.ready_socket = Some(ack.clone());
    let file = root.join(format!("{}-config.json", config.role));
    std::fs::write(&file, serde_json::to_vec(&config).unwrap()).unwrap();
    let log = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join(format!(
            "{}-{}.log",
            config.role,
            local_key(root, "log-id").unwrap().public()
        )))
        .unwrap();
    let child = OsCommand::new(std::env::current_exe().unwrap())
        .args(["--exact", ENTRY, "--ignored", "--nocapture"])
        .env_clear()
        .env("COCONUTCLAW_MULTIHOST_CONFIG", &file)
        .env("FACTORY_AIRGAP_ENABLED", "1")
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    poll(listener.as_raw_fd(), 10000).unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let ready = serde_json::from_slice(&read_frame(&mut stream).unwrap()).unwrap();
    stream.write_all(&[1]).unwrap();
    std::fs::remove_file(ack).unwrap();
    Role { child, ready }
}

fn control(root: &Path, command: &str, ready: Option<&Ready>) -> serde_json::Value {
    let mut stream = UnixStream::connect(root.join("control.sock")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let value = serde_json::json!({
        "command":command,"runner_ready":ready,"allow_same_host":true
    });
    write_frame(&mut stream, &serde_json::to_vec(&value).unwrap()).unwrap();
    let response = read_frame(&mut stream).unwrap_or_else(|error| {
        for _ in 0..100 {
            if root.join("failure.json").exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "control {command}: {error}; role failure: {}",
            std::fs::read_to_string(root.join("failure.json")).unwrap_or_default()
        )
    });
    serde_json::from_slice(&response).unwrap()
}

fn own_probe(root: &Path) -> serde_json::Value {
    let command = std::process::Command::new("python3")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/multihost-acceptance.py"))
        .args(["control", "--root"])
        .arg(root)
        .arg("probe")
        .arg("--proposal")
        .arg(root.join("proposal.json"))
        .output()
        .unwrap();
    assert!(command.status.success(), "{command:?}");
    serde_json::from_slice(&command.stdout).unwrap()
}

fn proposal_probe(root: &Path, proposal: Vec<u8>) -> serde_json::Value {
    let mut socket = UnixStream::connect(root.join("control.sock")).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write_frame(
        &mut socket,
        &serde_json::to_vec(&serde_json::json!({
            "command":"probe","bytes":proposal
        }))
        .unwrap(),
    )
    .unwrap();
    serde_json::from_slice(&read_frame(&mut socket).unwrap()).unwrap()
}

fn config(root: PathBuf, role: &str) -> RoleConfig {
    RoleConfig {
        root,
        role: role.into(),
        run_id: "portable-role-regression-123456".into(),
        network: AcceptanceNetwork {
            bind: "127.0.0.1:0".parse().unwrap(),
            relay: None,
        },
        owner: None,
        core: None,
        provider: None,
        ready_socket: None,
    }
}

#[test]
fn portable_roles_actual_scoped_transfer_duplicate_and_full_process_restart() {
    let root = tempfile::tempdir().unwrap();
    let core_cfg = config(root.path().join("core"), "coordinator");
    let mut core = launch(core_cfg.clone());
    let mut runner_cfg = config(root.path().join("runner"), "runner");
    runner_cfg.owner = Some(core.ready.owner);
    runner_cfg.core = Some(core.ready.control.id);
    let mut runner = launch(runner_cfg.clone());
    let receipt = control(&core_cfg.root, "run", Some(&runner.ready));
    assert_eq!(receipt["ok"], true, "{receipt}");
    assert_eq!(receipt["fixture_only"], true);
    assert_eq!(receipt["multihost"], false);
    assert_eq!(receipt["source_status"], "completed");
    assert_eq!(receipt["invocations"], 1);
    assert_ne!(core.ready.pid, runner.ready.pid);
    assert_ne!(std::process::id(), runner.ready.pid);
    assert_eq!(receipt["event_hashes"].as_array().unwrap().len(), 3);
    assert_eq!(control(&core_cfg.root, "status", None)["receipt"], receipt);
    assert_eq!(control(&core_cfg.root, "stop", None)["stopped"], true);
    assert!(core.child.wait().unwrap().success());
    assert!(runner.child.wait().unwrap().success());
    // No runtime, connection, Store, key actor or OS process survives this
    // boundary. Only the marked durable roots are reopened.
    std::fs::remove_file(root.path().join("coordinator-config.json")).unwrap();
    std::fs::remove_file(root.path().join("runner-config.json")).unwrap();
    std::fs::remove_file(root.path().join(format!(
        "coordinator-{}.log",
        local_key(root.path(), "log-id").unwrap().public()
    )))
    .unwrap();
    std::fs::remove_file(root.path().join(format!(
        "runner-{}.log",
        local_key(root.path(), "log-id").unwrap().public()
    )))
    .unwrap();
    let mut core = launch(core_cfg.clone());
    let mut runner = launch(runner_cfg);
    let replay = control(&core_cfg.root, "replay", Some(&runner.ready));
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["replay"], true);
    assert_eq!(replay["event_hashes"], receipt["event_hashes"]);
    assert_eq!(replay["invocations"], 1);
    let own = own_probe(&core_cfg.root);
    assert_eq!(own["exact_duplicate"], true, "{own}");
    assert_eq!(own["rejected"], false, "{own}");
    assert_eq!(own["history_tasks_invocations_unchanged"], true);
    assert_eq!(own["outbox_unchanged"], true);
    assert_eq!(control(&core_cfg.root, "stop", None)["stopped"], true);
    assert!(core.child.wait().unwrap().success());
    assert!(runner.child.wait().unwrap().success());
}

fn archive_role_log(root: &Path, role: &str) {
    let path = root.join(format!(
        "{}-{}.log",
        role,
        local_key(root, "log-id").unwrap().public()
    ));
    std::fs::rename(&path, path.with_extension("before-restart.log")).unwrap();
}

#[test]
fn coordinator_only_result_cut_reconnect_preserves_remote_and_original_store() {
    let root = tempfile::tempdir().unwrap();
    let core_cfg = config(root.path().join("core"), "coordinator");
    let mut core = launch(core_cfg.clone());
    let mut runner_cfg = config(root.path().join("runner"), "runner");
    runner_cfg.owner = Some(core.ready.owner);
    runner_cfg.core = Some(core.ready.control.id);
    let mut runner = launch(runner_cfg);
    let cut = control(&core_cfg.root, "run-cut", Some(&runner.ready));
    assert_eq!(cut["owned_cut"], true, "{cut}");
    assert_eq!(cut["ok"], false);
    let cfg = RuntimeConfig::test_config_at(core_cfg.root.join("source"));
    let store = Store::open(&cfg).unwrap();
    assert_eq!(store.get_task_run(2).unwrap().unwrap().status, "running");
    assert_eq!(control(&core_cfg.root, "detach", None)["stopped"], true);
    assert!(core.child.wait().unwrap().success());
    assert!(runner.child.try_wait().unwrap().is_none());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(core_cfg.root.join("stopped.json")).unwrap()
        )
        .unwrap()["runner_left_alive"],
        true
    );
    archive_role_log(root.path(), "coordinator");
    let mut restarted = launch(core_cfg.clone());
    let replay = control(&core_cfg.root, "replay", Some(&runner.ready));
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["invocations"], 1);
    assert_eq!(replay["source_status"], "completed");
    assert_eq!(store.get_task_run(2).unwrap().unwrap().status, "completed");
    assert_eq!(replay["runner"]["pid"], runner.ready.pid);
    assert_eq!(
        replay["tampered_fields_refused"].as_array().unwrap().len(),
        3
    );
    assert_eq!(control(&core_cfg.root, "stop", None)["stopped"], true);
    assert!(restarted.child.wait().unwrap().success());
    assert!(runner.child.wait().unwrap().success());
}

#[test]
fn remote_transport_preflight_allocates_no_task_or_provider() {
    let root = tempfile::tempdir().unwrap();
    let core_cfg = config(root.path().join("core"), "coordinator");
    let mut core = launch(core_cfg.clone());
    let mut runner_cfg = config(root.path().join("runner"), "runner");
    runner_cfg.owner = Some(core.ready.owner);
    runner_cfg.core = Some(core.ready.control.id);
    let mut runner = launch(runner_cfg);
    let checked = control(&core_cfg.root, "check", Some(&runner.ready));
    assert_eq!(checked["transport_initialized"], true, "{checked}");
    assert_eq!(checked["scoped_data_roundtrip"], true, "{checked}");
    assert_eq!(checked["invocations"], 0);
    let store = Store::open(&RuntimeConfig::test_config_at(core_cfg.root.join("source"))).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(control(&core_cfg.root, "stop", None)["stopped"], true);
    assert!(core.child.wait().unwrap().success());
    assert!(runner.child.wait().unwrap().success());
}

#[test]
fn foreign_signed_route_is_rejected_at_actual_driver_boundary() {
    let root = tempfile::tempdir().unwrap();
    let mut pairs = Vec::new();
    for i in 0..2 {
        let folder = root.path().join(i.to_string());
        std::fs::create_dir(&folder).unwrap();
        let core_cfg = config(folder.join("core"), "coordinator");
        let core = launch(core_cfg.clone());
        let mut runner_cfg = config(folder.join("runner"), "runner");
        runner_cfg.owner = Some(core.ready.owner);
        runner_cfg.core = Some(core.ready.control.id);
        let runner = launch(runner_cfg);
        let receipt = control(&core_cfg.root, "run", Some(&runner.ready));
        assert_eq!(receipt["ok"], true, "{receipt}");
        pairs.push((core_cfg, core, runner));
    }
    let proposal = std::fs::read(pairs[0].0.root.join("proposal.json")).unwrap();
    let own = own_probe(&pairs[0].0.root);
    assert_eq!(own["exact_duplicate"], true, "{own}");
    assert_eq!(own["rejected"], false, "{own}");
    assert_eq!(own["history_tasks_invocations_unchanged"], true);
    assert_eq!(own["outbox_unchanged"], true);
    let checked = proposal_probe(&pairs[1].0.root, proposal.clone());
    assert_eq!(checked["rejected"], true, "{checked}");
    assert!(
        checked["error"].as_str().unwrap().contains("binding"),
        "{checked}"
    );
    assert_eq!(checked["history_tasks_invocations_unchanged"], true);
    assert_eq!(checked["outbox_unchanged"], true);
    let key = local_key(
        &pairs[0].0.root.parent().unwrap().join("runner"),
        "role-key",
    )
    .unwrap();
    let mut stale: SignedProposal = serde_json::from_slice(&proposal).unwrap();
    stale.proposal.attempt = 0;
    let signed_stale = SignedProposal::sign(stale.proposal, &key).unwrap();
    let mut tampered: SignedProposal = serde_json::from_slice(&proposal).unwrap();
    tampered.proposal.source_run += 1;
    let mut malformed = proposal.clone();
    malformed.push(b'\n');
    for (bytes, expected) in [
        (signed_stale, "binding"),
        (serde_json::to_vec(&tampered).unwrap(), "signature"),
        (malformed, "noncanonical"),
    ] {
        let refused = proposal_probe(&pairs[0].0.root, bytes);
        assert_eq!(refused["rejected"], true, "{refused}");
        assert!(
            refused["error"].as_str().unwrap().contains(expected),
            "{refused}"
        );
        assert_eq!(refused["exact_duplicate"], false);
        assert_eq!(refused["history_tasks_invocations_unchanged"], true);
        assert_eq!(refused["outbox_unchanged"], true);
        let valid = own_probe(&pairs[0].0.root);
        assert_eq!(valid["rejected"], false, "{valid}");
        assert_eq!(valid["exact_duplicate"], true, "{valid}");
    }
    for (cfg, mut core, mut runner) in pairs {
        assert_eq!(control(&cfg.root, "stop", None)["stopped"], true);
        assert!(core.child.wait().unwrap().success());
        assert!(runner.child.wait().unwrap().success());
    }
}

#[test]
fn remote_frames_and_signed_artifact_bounds_are_exact() {
    let bytes = encode(&Response::Process(Reply::Unit)).unwrap();
    assert!(decode::<Response>(&bytes).is_ok());
    assert!(decode::<Response>(&bytes[..bytes.len() - 1]).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode::<Response>(&trailing).is_err());
    assert!(decode::<Response>(&[0, 0, 0, 0]).is_err());
    assert!(encode(&"x".repeat(MAX_FRAME)).is_err());
    let noncanonical = b"{ \"Process\" : \"Unit\" }";
    let mut frame = (noncanonical.len() as u32).to_be_bytes().to_vec();
    frame.extend(noncanonical);
    assert!(decode::<Response>(&frame).is_err());
    use provider::host::{Input, Manifest, verify_artifact};
    let input = Input {
        nonce: "independent-nonce".into(),
        a: 17,
        b: 25,
    };
    let mut artifact = br#"{"nonce":"independent-nonce","sum":42,"tool_pid":123,"tool_parent":122,"hostname":"fixture-host"}"#.to_vec();
    artifact.resize(4096, b' ');
    let manifest = Manifest {
        version: 1,
        assignment: 1,
        nonce: input.nonce.clone(),
        sum: 42,
        hash: blake3::hash(&artifact).to_string(),
        size: 4096,
        session: "fixture-session".into(),
        runner_pid: 122,
        hostname: "fixture-host".into(),
    };
    verify_artifact(&manifest, 1, &input, "fixture-host", &artifact, false).unwrap();
    artifact.push(b' ');
    let mut oversized = manifest.clone();
    oversized.size = 4097;
    oversized.hash = blake3::hash(&artifact).to_string();
    assert!(verify_artifact(&oversized, 1, &input, "fixture-host", &artifact, false).is_err());
    artifact.pop();
    let mut wrong = manifest.clone();
    wrong.nonce = "other".into();
    assert!(verify_artifact(&wrong, 1, &input, "fixture-host", &artifact, false).is_err());
    let mut wrong = manifest.clone();
    wrong.sum = 43;
    assert!(verify_artifact(&wrong, 1, &input, "fixture-host", &artifact, false).is_err());
    let mut wrong = manifest.clone();
    wrong.hash = "00".repeat(32);
    assert!(verify_artifact(&wrong, 1, &input, "fixture-host", &artifact, false).is_err());
    assert!(verify_artifact(&manifest, 2, &input, "fixture-host", &artifact, false).is_err());
    assert!(verify_artifact(&manifest, 1, &input, "other-host", &artifact, false).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn native_guardian_closes_owned_process_group_after_runner_parent_dies() {
    use std::io::BufRead;
    use std::os::fd::FromRawFd;
    let root = tempfile::tempdir().unwrap();
    let job = root.path().join("job");
    std::fs::create_dir(&job).unwrap();
    let auth = root.path().join("fixture-auth.json");
    std::fs::write(
        &auth,
        br#"{"OPENAI_API_KEY":"explicit-noncredential-fixture"}"#,
    )
    .unwrap();
    let profile = root.path().join("profile.json");
    std::fs::write(
        &profile,
        serde_json::to_vec(&serde_json::json!({
            "auth_json":auth,"executable":"/usr/bin/python3"
        }))
        .unwrap(),
    )
    .unwrap();
    let wrapper = root.path().join("native-exec.py");
    std::fs::write(&wrapper, include_str!("../provider/native_exec.py")).unwrap();
    let code = r#"
import json,os,socket,subprocess,sys
from pathlib import Path
wrapper,profile,job=sys.argv[1:]
listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
listener.bind(job+'/guardian.sock');listener.listen(1)
native="import json,os,socket,sys; s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); s.bind(sys.argv[1]); s.listen(1); print(json.dumps({'pid':os.getpid(),'group':os.getpgrp()}),flush=True); s.accept()"
child=subprocess.Popen(['/usr/bin/python3','-I',wrapper,profile,job,'-I','-c',native,job+'/native.sock'],stdout=subprocess.PIPE,start_new_session=True)
owned,_=listener.accept()
line=child.stdout.readline()
data=json.loads(line);data['wrapper']=child.pid
print(json.dumps(data),flush=True)
owned.recv(1)
"#;
    let mut parent = OsCommand::new("/usr/bin/python3")
        .args(["-I", "-c", code])
        .arg(wrapper)
        .arg(profile)
        .arg(&job)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    std::io::BufReader::new(parent.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let ids: serde_json::Value = serde_json::from_str(&line).unwrap();
    let wrapper = ids["wrapper"].as_u64().unwrap();
    assert_eq!(ids["group"].as_u64().unwrap(), wrapper);
    let mut exits = Vec::new();
    for pid in [ids["pid"].as_u64().unwrap(), wrapper] {
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
        assert!(fd >= 0);
        exits.push(unsafe { File::from_raw_fd(fd) });
    }
    parent.kill().unwrap();
    parent.wait().unwrap();
    for fd in exits {
        poll(fd.as_raw_fd(), 5000).unwrap();
    }
    let launch: serde_json::Value =
        serde_json::from_slice(&std::fs::read(job.join("launch.json")).unwrap()).unwrap();
    assert_eq!(launch["group"].as_u64(), Some(wrapper));
    assert_eq!(launch["pid"].as_u64(), Some(wrapper));
    assert!(!job.join("codex-home/auth.json").exists());
}
