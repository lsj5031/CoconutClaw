use super::*;

struct Fixture {
    root: tempfile::TempDir,
    evidence: Evidence,
    context: serde_json::Value,
    call: serde_json::Value,
    output: serde_json::Value,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let auth = root.path().join("fixture-auth.json");
        std::fs::write(&auth, br#"{"OPENAI_API_KEY":"noncredential-fixture"}"#).unwrap();
        let shared_config = root.path().join("shared-config.toml");
        std::fs::write(&shared_config, "model = \"fixture\"\n").unwrap();
        let cfg = Config {
            executable: "/bin/true".into(),
            python: "/usr/bin/python3".into(),
            auth_json: auth,
            shared_config,
            provider_name: "fixture".into(),
            base_url: "http://127.0.0.1:1/v1".into(),
            wire_api: "responses".into(),
        };
        std::fs::write(
            root.path().join("host-provider.json"),
            serde_json::to_vec(&cfg).unwrap(),
        )
        .unwrap();
        let job = root.path().join("provider-1");
        std::fs::create_dir(&job).unwrap();
        std::fs::write(
            job.join("integrity-before.json"),
            serde_json::to_vec(&integrity(&cfg).unwrap()).unwrap(),
        )
        .unwrap();
        let workspace = root.path().join("workspace/1");
        std::fs::create_dir_all(&workspace).unwrap();
        let input = Input {
            nonce: "cc-real-multihost-20261116-1201".into(),
            a: 17,
            b: 25,
        };
        std::fs::write(
            workspace.join("fixture-input.json"),
            serde_json::to_vec(&input).unwrap(),
        )
        .unwrap();
        std::fs::write(
            workspace.join("fixture-output.json"),
            serde_json::to_vec(&serde_json::json!({
                "nonce":input.nonce, "sum":42, "tool_pid":44694, "tool_parent":44609,
                "hostname":super::super::super::remote::hostname().unwrap()
            }))
            .unwrap(),
        )
        .unwrap();
        let evidence = Evidence {
            assignment: 1,
            runner_pid: std::process::id(),
            observed: None,
            session: Some("01a1e174-adfe-7140-bd48-a7e8a82e7fc2".into()),
            progress: Vec::new(),
            exit_code: Some(0),
            output: Some(format!("nonce={} sum=42", input.nonce)),
            failure: None,
        };
        let context = serde_json::json!({"type":"turn_context","payload":{
            "cwd":workspace, "model":"gpt-6.1-terra", "effort":"max",
            "approval_policy":"never","sandbox_policy":{"type":"danger-full-access"}
        }});
        // Shape independently observed by the parent on real Mars native0.167.
        let call = serde_json::json!({"type":"response_item","payload":{
            "type":"custom_tool_call","name":"exec","call_id":"call_fixture",
            "status":"completed","input":"python3 -c \"read fixture-input.json; write fixture-output.json\""
        }});
        let output = serde_json::json!({"type":"response_item","payload":{
            "type":"custom_tool_call_output","call_id":"call_fixture","output":"Process exited with code 0"
        }});
        Self {
            root,
            evidence,
            context,
            call,
            output,
        }
    }

    fn write_events(&self, events: &[serde_json::Value]) {
        let folder = self
            .root
            .path()
            .join("provider-1/codex-home/sessions/2026/11/16");
        std::fs::create_dir_all(&folder).unwrap();
        let path = folder.join(format!(
            "rollout-{}.jsonl",
            self.evidence.session.as_ref().unwrap()
        ));
        let text = events
            .iter()
            .map(|v| serde_json::to_string(v).unwrap() + "\n")
            .collect::<String>();
        std::fs::write(path, text).unwrap();
    }
}

#[test]
fn actual_native_custom_tool_call_fixture_is_accepted() {
    let f = Fixture::new();
    f.write_events(&[f.context.clone(), f.call.clone(), f.output.clone()]);
    let text = manifest(f.root.path(), 1, &f.evidence)
        .expect("real custom exec call/output must prove the fixture");
    let manifest: Manifest = serde_json::from_str(&text).unwrap();
    assert_eq!(manifest.sum, 42);
    assert_eq!(manifest.session, f.evidence.session.unwrap());
}

#[test]
fn legacy_function_execution_keeps_correlated_native_proof() {
    let mut f = Fixture::new();
    f.call["payload"]["type"] = "function_call".into();
    f.call["payload"]["name"] = "exec_command".into();
    f.call["payload"]["arguments"] = "{\"cmd\":\"write fixture-output.json\"}".into();
    f.call["payload"].as_object_mut().unwrap().remove("input");
    f.output["payload"]["type"] = "function_call_output".into();
    f.write_events(&[f.context.clone(), f.call.clone(), f.output.clone()]);
    manifest(f.root.path(), 1, &f.evidence).unwrap();
    let proof: serde_json::Value = serde_json::from_slice(
        &std::fs::read(f.root.path().join("provider-1/scoped-native-events.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(proof["tool_calls"][0]["type"], "function_call");
    assert_eq!(
        proof["tool_calls"][0]["output_type"],
        "function_call_output"
    );
    assert_eq!(proof["tool_calls"][0]["call_id"], "call_fixture");
}

#[test]
fn native_execution_requires_real_call_input_and_matching_output() {
    for invalid in [
        "absent-output",
        "foreign-call",
        "wrong-output-type",
        "null-output",
        "absent-input",
        "foreign-input-field",
        "nonexec",
        "no-fixture",
        "empty-id",
        "oversized-id",
        "absent-call",
    ] {
        let mut f = Fixture::new();
        match invalid {
            "foreign-call" => f.output["payload"]["call_id"] = "other".into(),
            "wrong-output-type" => f.output["payload"]["type"] = "function_call_output".into(),
            "null-output" => f.output["payload"]["output"] = serde_json::Value::Null,
            "absent-input" => {
                f.call["payload"].as_object_mut().unwrap().remove("input");
            }
            "foreign-input-field" => {
                let input = f.call["payload"]
                    .as_object_mut()
                    .unwrap()
                    .remove("input")
                    .unwrap();
                f.call["payload"]["arguments"] = input;
            }
            "nonexec" => f.call["payload"]["name"] = "describe".into(),
            "no-fixture" => f.call["payload"]["input"] = "echo done".into(),
            "empty-id" | "oversized-id" => {
                let id = if invalid == "empty-id" {
                    String::new()
                } else {
                    "x".repeat(257)
                };
                f.call["payload"]["call_id"] = id.clone().into();
                f.output["payload"]["call_id"] = id.into();
            }
            _ => {}
        }
        let mut events = vec![f.context.clone()];
        if invalid != "absent-call" {
            events.push(f.call.clone());
        }
        if invalid != "absent-output" {
            events.push(f.output.clone());
        }
        f.write_events(&events);
        assert_eq!(
            manifest(f.root.path(), 1, &f.evidence)
                .unwrap_err()
                .to_string(),
            "actual native tool call missing",
            "{invalid}"
        );
    }
    // Exercise the inclusive call-id bound rather than only small fixtures.
    let mut f = Fixture::new();
    f.call["payload"]["call_id"] = "x".repeat(256).into();
    // A paired completion is decisive, not the earlier call's status snapshot.
    f.call["payload"]["status"] = "in_progress".into();
    f.output["payload"]["call_id"] = "x".repeat(256).into();
    f.write_events(&[f.context.clone(), f.call.clone(), f.output.clone()]);
    manifest(f.root.path(), 1, &f.evidence).unwrap();
}

#[test]
fn native_format_support_preserves_context_file_host_session_and_integrity_gates() {
    for invalid in [
        "model",
        "effort",
        "approval",
        "nonce",
        "sum",
        "host",
        "missing-session",
        "wrong-session",
        "shared-config",
        "auth-metadata",
        "oversized-artifact",
    ] {
        let mut f = Fixture::new();
        match invalid {
            "model" => f.context["payload"]["model"] = "wrong".into(),
            "effort" => f.context["payload"]["effort"] = "wrong".into(),
            "approval" => f.context["payload"]["approval_policy"] = "wrong".into(),
            "shared-config" => {
                std::fs::write(f.root.path().join("shared-config.toml"), "changed").unwrap()
            }
            "auth-metadata" => {
                std::fs::write(f.root.path().join("fixture-auth.json"), "changed").unwrap()
            }
            "oversized-artifact" => std::fs::write(
                f.root.path().join("workspace/1/fixture-output.json"),
                vec![b' '; 4097],
            )
            .unwrap(),
            "nonce" | "sum" | "host" => {
                let path = f.root.path().join("workspace/1/fixture-output.json");
                let mut value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                match invalid {
                    "nonce" => value["nonce"] = "foreign".into(),
                    "sum" => value["sum"] = 43.into(),
                    _ => value["hostname"] = "foreign-host".into(),
                }
                std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
            }
            _ => {}
        }
        f.write_events(&[f.context.clone(), f.call.clone(), f.output.clone()]);
        if invalid == "missing-session" {
            f.evidence.session = None;
        }
        if invalid == "wrong-session" {
            f.evidence.session = Some("other-session".into());
        }
        assert!(
            manifest(f.root.path(), 1, &f.evidence).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn integrity_diagnostic_uses_blake3_and_is_read_only_through_parent_cli() {
    let f = Fixture::new();
    let marker = f.root.path().join("MULTIHOST");
    std::fs::write(&marker, "CoconutClaw disposable multihost acceptance v1\n").unwrap();
    fn snapshot(
        path: &Path,
        files: &mut std::collections::BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)>,
    ) {
        for item in std::fs::read_dir(path).unwrap() {
            let p = item.unwrap().path();
            if p.is_dir() {
                snapshot(&p, files);
            } else {
                files.insert(
                    p.clone(),
                    (
                        std::fs::read(&p).unwrap(),
                        std::fs::metadata(p).unwrap().modified().unwrap(),
                    ),
                );
            }
        }
    }
    let mut before = std::collections::BTreeMap::new();
    snapshot(f.root.path(), &mut before);
    let output = std::process::Command::new("/usr/bin/python3")
        .env("RUST_TEST_THREADS", "1")
        .arg("-I")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/multihost-acceptance.py"))
        .args(["integrity", "--root"])
        .arg(f.root.path())
        .arg("--executable")
        .arg(std::env::current_exe().unwrap())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["config_hash_algorithm"], "BLAKE3");
    assert_eq!(value["unchanged"], true);
    assert_eq!(value["read_only"], true);
    assert_eq!(
        value["config_hash_after"],
        blake3::hash(b"model = \"fixture\"\n").to_string()
    );
    let mut after = std::collections::BTreeMap::new();
    snapshot(f.root.path(), &mut after);
    assert_eq!(before, after);
    std::fs::write(f.root.path().join("shared-config.toml"), "changed").unwrap();
    let report = integrity_report(f.root.path()).unwrap();
    assert_eq!(report["unchanged"], false);
    assert_eq!(report["config_unchanged"], false);
    assert_eq!(report["auth_size_unchanged"], true);
    assert_eq!(report["auth_mtime_unchanged"], true);
}

#[test]
fn existing_durable_provider_evidence_without_failure_still_decodes() {
    let f = Fixture::new();
    let mut old = serde_json::to_value(&f.evidence).unwrap();
    old.as_object_mut().unwrap().remove("failure");
    let evidence: Evidence = serde_json::from_value(old).unwrap();
    assert!(evidence.failure.is_none());
    assert_eq!(evidence.session, f.evidence.session);
    assert_eq!(evidence.output, f.evidence.output);
}
