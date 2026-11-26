use super::*;
use crate::{TimeoutPolicy, run_provider_process};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};
use windows_sys::Win32::{
    Foundation::WAIT_OBJECT_0,
    System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    },
};

fn command(root: &Path, mode: &str) -> Command {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", "windows::tests::process_fixture", "--nocapture"])
        .env("COCONUTCLAW_JOB_TEST_ROOT", root)
        .env("COCONUTCLAW_JOB_TEST_MODE", mode)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}
fn wait_file(path: &Path) -> String {
    let until =
        Instant::now() + Duration::from_secs(if path.ends_with("release") { 75 } else { 15 });
    loop {
        if let Ok(value) = fs::read_to_string(path)
            && !value.is_empty()
        {
            return value;
        }
        assert!(Instant::now() < until, "owned fixture did not become ready");
        thread::sleep(Duration::from_millis(20));
    }
}
fn process(root: &Path, name: &str) -> Handle {
    let pid: u32 = wait_file(&root.join(name)).parse().unwrap();
    // SYNCHRONIZE, not PROCESS_ALL_ACCESS; retain the actual process object,
    // so exit assertions cannot confuse PID reuse with cleanup.
    let raw = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            0,
            pid,
        )
    };
    assert!(!raw.is_null());
    Handle(raw)
}
fn exited(handle: &Handle) {
    assert_eq!(
        unsafe { WaitForSingleObject(handle.0, 15000) },
        WAIT_OBJECT_0
    );
}

#[test]
fn process_fixture() {
    let Ok(mode) = std::env::var("COCONUTCLAW_JOB_TEST_MODE") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("COCONUTCLAW_JOB_TEST_ROOT").unwrap());
    match mode.as_str() {
        "leaf" => {
            fs::write(root.join("leaf"), std::process::id().to_string()).unwrap();
            thread::sleep(Duration::from_secs(90));
        }
        "leader" => {
            let mut leaf = command(&root, "leaf").spawn().unwrap();
            fs::write(root.join("leader"), std::process::id().to_string()).unwrap();
            wait_file(&root.join("release"));
            // Explicitly do not wait for leaf: it has detached standard I/O
            // and must still die when the normal leader returns.
            let _ = leaf.try_wait();
        }
        "owner" => {
            let (_child, _job) = spawn(&command(&root, "leader"), false, true).unwrap();
            thread::sleep(Duration::from_secs(90));
        }
        "shutdown" => {
            let (_child, _job) = spawn(&command(&root, "leader"), false, true).unwrap();
            wait_file(&root.join("release"));
            crate::kill_active_process_groups();
            fs::write(root.join("shutdown-ready"), "ready").unwrap();
            wait_file(&root.join("owner-release"));
        }
        _ => panic!("unknown fixture mode"),
    }
}

#[test]
fn cancellation_timeout_and_normal_exit_kill_detached_descendants() {
    for mode in ["cancel", "timeout", "normal"] {
        let root = coconutclaw_config::RuntimeConfig::test_config().data_dir;
        fs::create_dir_all(&root).unwrap();
        let flag = Arc::new(AtomicBool::new(false));
        let flag2 = flag.clone();
        let root2 = root.clone();
        let worker = thread::spawn(move || {
            run_provider_process(
                command(&root2, "leader"),
                None,
                Some(&flag2),
                None,
                None,
                "owned-test",
                TimeoutPolicy::new(Some(30), 60, 60).into(),
            )
        });
        let leader = process(&root, "leader");
        let leaf = process(&root, "leaf");
        match mode {
            "cancel" => flag.store(true, Ordering::SeqCst),
            "normal" => fs::write(root.join("release"), "release").unwrap(),
            // Exercise the actual timeout path without depending on process
            // startup fitting inside a one-second clock.
            "timeout" => (),
            _ => unreachable!(),
        }
        let result = worker.join().unwrap().unwrap();
        assert_eq!(result.cancelled, mode == "cancel");
        assert_eq!(result.timed_out, mode == "timeout");
        exited(&leader);
        exited(&leaf);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn abrupt_owner_death_and_owned_shutdown_leave_no_tree_on_restart() {
    for mode in ["owner", "shutdown"] {
        let root = coconutclaw_config::RuntimeConfig::test_config().data_dir;
        fs::create_dir_all(&root).unwrap();
        let mut owner = command(&root, mode).spawn().unwrap();
        let leader = process(&root, "leader");
        let leaf = process(&root, "leaf");
        if mode == "owner" {
            owner.kill().unwrap();
        } else {
            fs::write(root.join("release"), "release").unwrap();
            wait_file(&root.join("shutdown-ready"));
            // Prove registry shutdown itself works while its owner and job
            // handle remain alive, not just eventual kill-on-close at exit.
            exited(&leader);
            exited(&leaf);
            assert!(owner.try_wait().unwrap().is_none());
            fs::write(root.join("owner-release"), "release").unwrap();
        }
        owner.wait().unwrap();
        exited(&leader);
        exited(&leaf);
        // A second independent invocation must not inherit a leaked old job.
        fs::remove_file(root.join("leaf")).unwrap();
        fs::remove_file(root.join("leader")).unwrap();
        let _ = fs::remove_file(root.join("release"));
        let (restarted, job) = spawn(&command(&root, "leader"), false, true).unwrap();
        let mut member = 0;
        checked(unsafe {
            windows_sys::Win32::System::JobObjects::IsProcessInJob(
                restarted.process.0,
                job.0.0,
                &mut member,
            )
        })
        .unwrap();
        assert_ne!(member, 0);
        let new_leaf = process(&root, "leaf");
        job.terminate().unwrap();
        restarted.wait().unwrap();
        exited(&new_leaf);
        fs::remove_dir_all(root).unwrap();
    }
}
