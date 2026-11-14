use super::*;

#[test]
fn configured_launchd_definition_uses_resolved_log_directory() {
    let mut cfg = RuntimeConfig::test_config();
    cfg.root_dir = PathBuf::from("/unwritable/checkout");
    cfg.log_dir = cfg.instance_dir.join("custom-logs");
    let names = service_names(&cfg, &CliOverrides { data_dir: None }).unwrap();
    let plist = configured_run_plist(&cfg, &names, Path::new("/bin/coconutclaw"), "/bin");
    assert!(
        plist.contains(
            &cfg.log_dir
                .join("io.coconutclaw.run.log")
                .display()
                .to_string()
        )
    );
    assert!(
        plist.contains(
            &cfg.log_dir
                .join("io.coconutclaw.run.err.log")
                .display()
                .to_string()
        )
    );
    assert!(!plist.contains("/unwritable/checkout/LOGS"));
    assert!(plist.contains(&cfg.instance_dir.display().to_string()));
}
#[test]
fn service_has_only_one_run_unit_and_pins_data_directory() {
    let cfg = RuntimeConfig::test_config();
    let names = service_names(&cfg, &CliOverrides { data_dir: None }).unwrap();
    assert_eq!(names.run_task, "coconutclaw.service");
    assert_eq!(names.run_label, "io.coconutclaw.run");
    assert_eq!(
        service_command_args(&cfg, "run"),
        vec![
            "--data-dir".to_string(),
            cfg.instance_dir.display().to_string(),
            "run".to_string()
        ]
    );
}

#[test]
fn launchd_run_definition_has_no_calendar_schedule() {
    let text = render_run_plist(
        "io.coconutclaw.run",
        &["/tmp/a & b".into(), "run".into()],
        Path::new("/tmp/work"),
        "/usr/bin",
        Path::new("/tmp/out"),
        Path::new("/tmp/err"),
    );
    assert!(text.contains("KeepAlive"));
    assert!(text.contains("a &amp; b"));
    assert!(!text.contains("StartCalendarInterval"));
}
