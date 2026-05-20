use super::*;
use coconutclaw_config::RuntimeConfig;
use std::path::PathBuf;
use std::sync::Mutex;

fn env_lock() -> &'static Mutex<()> {
    use std::sync::OnceLock;
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn sanitize_identifier_matches_shell_behavior_for_basic_cases() {
    assert_eq!(sanitize_identifier("Work_Prod"), "work_prod");
    assert_eq!(sanitize_identifier("x y z"), "x-y-z");
    assert_eq!(sanitize_identifier("##"), "");
    assert_eq!(sanitize_identifier("___"), "___");
}

#[test]
fn service_names_uses_env_selected_instance_name() {
    let _lock = env_lock().lock().expect("env lock");
    let cfg = RuntimeConfig::test_config();
    let overrides = CliOverrides {
        instance: None,
        data_dir: None,
        instance_dir: None,
    };

    unsafe { std::env::set_var("COCONUTCLAW_INSTANCE", "work") };
    let names = service_names(&cfg, &overrides).expect("service names");
    unsafe { std::env::remove_var("COCONUTCLAW_INSTANCE") };

    assert_eq!(names.instance_key, "work");
    assert_eq!(names.run_label, "io.coconutclaw.run.work");
}

#[test]
fn service_names_uses_env_selected_instance_dir() {
    let _lock = env_lock().lock().expect("env lock");
    let mut cfg = RuntimeConfig::test_config();
    cfg.root_dir = PathBuf::from("/repo");
    cfg.instance_dir = PathBuf::from("/tmp/coconut/work");
    let overrides = CliOverrides {
        instance: None,
        data_dir: None,
        instance_dir: None,
    };

    unsafe { std::env::set_var("INSTANCE_DIR", "/tmp/coconut/work") };
    let names = service_names(&cfg, &overrides).expect("service names");
    unsafe { std::env::remove_var("INSTANCE_DIR") };

    assert_eq!(
        names.instance_key,
        format!("dir-work-{}", cksum_identifier("/tmp/coconut/work"))
    );
    assert!(names.run_label.starts_with("io.coconutclaw.run.dir-work-"));
}
