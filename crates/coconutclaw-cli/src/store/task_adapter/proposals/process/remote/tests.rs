use super::*;

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
    serde_json::from_slice(&read_frame(&mut stream).unwrap()).unwrap()
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
    assert_eq!(control(&core_cfg.root, "stop", None)["stopped"], true);
    assert!(core.child.wait().unwrap().success());
    assert!(runner.child.wait().unwrap().success());
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
    verify_artifact(&manifest, 1, &input, "fixture-host", &artifact).unwrap();
    artifact.push(b' ');
    let mut oversized = manifest.clone();
    oversized.size = 4097;
    oversized.hash = blake3::hash(&artifact).to_string();
    assert!(verify_artifact(&oversized, 1, &input, "fixture-host", &artifact).is_err());
    artifact.pop();
    let mut wrong = manifest.clone();
    wrong.nonce = "other".into();
    assert!(verify_artifact(&wrong, 1, &input, "fixture-host", &artifact).is_err());
    let mut wrong = manifest.clone();
    wrong.sum = 43;
    assert!(verify_artifact(&wrong, 1, &input, "fixture-host", &artifact).is_err());
    let mut wrong = manifest.clone();
    wrong.hash = "00".repeat(32);
    assert!(verify_artifact(&wrong, 1, &input, "fixture-host", &artifact).is_err());
    assert!(verify_artifact(&manifest, 2, &input, "fixture-host", &artifact).is_err());
    assert!(verify_artifact(&manifest, 1, &input, "other-host", &artifact).is_err());
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
