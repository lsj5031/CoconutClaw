use super::*;
use crate::store::task_adapter::proposals::process::provider::provision;
use std::os::fd::{AsRawFd, FromRawFd};

#[tokio::test]
#[ignore = "requires explicit owner-authorized real Codex/auth acceptance"]
async fn real_codex_process_scoped_acceptance() {
    assert_eq!(
        std::env::var("COCONUTCLAW_REAL_CODEX_ACCEPTANCE").as_deref(),
        Ok("1")
    );
    let receipt = std::path::PathBuf::from(std::env::var("COCONUTCLAW_REAL_RECEIPT_DIR").unwrap());
    std::fs::create_dir(&receipt).unwrap();
    let config_before = blake3::hash(&std::fs::read("/home/leo/.codex/config.toml").unwrap());
    let auth_before = std::fs::metadata("/home/leo/.codex/auth.json").unwrap();
    let (f, mut d) = ProcessFixture::new_real().await;
    let runner_pid = d.runner.child_pid().unwrap();
    assert_ne!(runner_pid, std::process::id());
    let workspace = f.root.path().join("workspace/1");
    std::fs::create_dir_all(&workspace).unwrap();
    let nonce = blake3::hash(
        format!("{}:{}", std::process::id(), chrono::Utc::now().to_rfc3339()).as_bytes(),
    )
    .to_string();
    let input = serde_json::json!({"nonce":nonce,"a":19,"b":23});
    std::fs::write(
        workspace.join("fixture-input.json"),
        serde_json::to_vec(&input).unwrap(),
    )
    .unwrap();
    let prompt = "Authorized harmless acceptance. Use an actual shell/Python tool to read fixture-input.json in the current workspace, and write fixture-output.json with the same nonce, sum=a+b, tool_pid=os.getpid(), tool_parent=os.getppid(). Only read/write these two fixture files in this workspace. No network, auth/config/home/repository access, delegation or other side effects. Do not merely describe commands: execute the tool. Final answer only nonce=<nonce> sum=<sum>.";
    let id = enqueue_text(&d.source, &d.session, 0, prompt);
    d.reconcile().await.unwrap();
    let a = d.queue(id).await.unwrap();
    assert_eq!(a, 1);
    let start = std::time::Instant::now();
    d.admit_process(a, Mode::RealCodex)
        .await
        .unwrap()
        .release()
        .unwrap();
    applied(&mut d, a).await;
    let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
    std::fs::write(
        receipt.join("provider-evidence.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    let status = d.source.get_task_run(id).unwrap().unwrap().status;
    let signed = result(&d, a).unwrap();
    std::fs::write(receipt.join("signed-result.json"), &signed).unwrap();
    let initial = serde_json::json!({"host":std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap().trim(),
        "runner_pid":runner_pid,"coordinator_pid":std::process::id(),"source_run":id,"assignment":a,
        "seconds":start.elapsed().as_secs_f64(),"source_status":status,"model":"gpt-6.1-terra","effort":"max","workspace":workspace});
    std::fs::write(
        receipt.join("outcome.json"),
        serde_json::to_vec_pretty(&initial).unwrap(),
    )
    .unwrap();
    if status != "completed" {
        d.shutdown().await.unwrap();
        panic!("real provider did not complete; retained provider-evidence/outcome receipts");
    }
    let file: serde_json::Value =
        serde_json::from_slice(&std::fs::read(workspace.join("fixture-output.json")).unwrap())
            .unwrap();
    assert_eq!(file["nonce"], nonce);
    assert_eq!(file["sum"], 42);
    assert!(file["tool_pid"].as_u64().unwrap() > 0);
    assert_ne!(file["tool_pid"], runner_pid);
    assert_eq!(evidence.exit_code, Some(0));
    assert!(evidence.session.as_ref().is_some_and(|s| !s.is_empty()));
    assert!(
        evidence.progress.iter().any(|p| p.starts_with("✓ ")),
        "no actual completed tool progress"
    );
    assert!(output(&d.source, id).unwrap().contains(&nonce));
    assert!(output(&d.source, id).unwrap().contains("42"));
    assert!(matches!(
        SignedEvent::parse(&signed).unwrap().event.action,
        Action::Result {
            outcome: Outcome::Completed,
            ..
        }
    ));
    let before = history(&mut d);
    assert_eq!(
        d.accept_proposal(d.runner.stored_proposal(a).await.unwrap().unwrap())
            .await
            .unwrap(),
        Step::Duplicate(a)
    );
    assert_eq!(history(&mut d), before);
    assert_eq!(d.runner.stats().await.unwrap().len(), 1);
    assert!(d.admit_process(a, Mode::RealCodex).await.is_err());
    std::fs::write(
        receipt.join("fixture-input.json"),
        serde_json::to_vec_pretty(&input).unwrap(),
    )
    .unwrap();
    std::fs::write(
        receipt.join("fixture-output.json"),
        serde_json::to_vec_pretty(&file).unwrap(),
    )
    .unwrap();
    d.shutdown().await.unwrap();
    dead(runner_pid);
    let mut d = f.reopen().await;
    assert!(d.reconcile().await.is_err());
    f.base.authorize(&mut d);
    d.reconcile().await.unwrap();
    assert_eq!(history(&mut d), before);
    assert_eq!(result(&d, a).unwrap(), signed);
    assert_eq!(
        d.source.get_task_run(id).unwrap().unwrap().status,
        "completed"
    );
    assert_eq!(d.runner.stats().await.unwrap().len(), 1);
    assert!(d.runner.begin(a, Mode::RealCodex).await.is_err());
    d.shutdown().await.unwrap();
    assert_eq!(
        blake3::hash(&std::fs::read("/home/leo/.codex/config.toml").unwrap()),
        config_before,
        "native exec changed global config"
    );
    let auth_after = std::fs::metadata("/home/leo/.codex/auth.json").unwrap();
    assert_eq!(auth_after.len(), auth_before.len());
    assert_eq!(
        auth_after.modified().unwrap(),
        auth_before.modified().unwrap()
    );
    let capture=std::process::Command::new("/usr/bin/python3").args(["-I","-c",r#"
import json,sys
from pathlib import Path
home,session,target=map(Path,sys.argv[1:])
files=list((home/'sessions').rglob('*'+str(session)+'*.jsonl'))
assert len(files)==1
contexts=[];tools=[]
for line in files[0].open():
    event=json.loads(line);v=event.get('payload',{})
    if event.get('type')=='turn_context':
        contexts.append({k:v.get(k) for k in ['cwd','model','effort','approval_policy','sandbox_policy']})
    if event.get('type')=='response_item' and v.get('type')=='function_call':
        tools.append({k:v.get(k) for k in ['name','call_id','arguments']})
assert any(c['model']=='gpt-6.1-terra' and c['effort']=='max' for c in contexts)
assert any('fixture-output.json' in (t.get('arguments') or '') for t in tools)
target.write_text(json.dumps(dict(session=str(session),contexts=contexts,tool_calls=tools),indent=2)+'\n')
"#]).arg(f.root.path().join("codex-home")).arg(evidence.session.as_ref().unwrap())
        .arg(receipt.join("scoped-native-events.json")).status().unwrap();
    assert!(capture.success(), "scoped native model/tool capture failed");
    std::fs::write(
        receipt.join("verified.json"),
        b"{\"verified\":true,\"exact_duplicate\":true,\"restart\":true,\"invocations\":1,\"config_unchanged\":true,\"auth_metadata_unchanged\":true}\n",
    )
    .unwrap();
    println!(
        "Real Codex scoped acceptance completed; durable receipts at {}",
        receipt.display()
    );
}

fn exit_fd(pid: u32) -> std::fs::File {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
    assert!(fd >= 0, "owned fixture must be live at acknowledgment");
    unsafe { std::fs::File::from_raw_fd(fd) }
}
fn exited(fd: &std::fs::File) {
    let mut event = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    assert_eq!(
        unsafe { libc::poll(&mut event, 1, 10000) },
        1,
        "owned fixture failed to exit"
    );
    assert_ne!(event.revents & libc::POLLIN, 0);
}

#[tokio::test]
async fn provider_process_actual_api_prompt_workspace_session_progress_and_failure() {
    bounded(async {
        let (f, mut d) = ProcessFixture::new().await;
        provision(f.root.path()).unwrap();
        d.reconcile().await.unwrap();
        for (i, mode) in [
            Mode::Provider,
            Mode::ProviderResume,
            Mode::ProviderFail,
            Mode::ProviderLimit,
        ]
        .into_iter()
        .enumerate()
        {
            let id = enqueue(&d.source, &d.session, i as u32);
            let a = d.queue(id).await.unwrap();
            let work = d.admit_process(a, mode).await.unwrap();
            assert!(
                d.runner.finish(a).await.is_err(),
                "completion cannot wait behind control RPC"
            );
            let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
            let observed = evidence.observed.as_ref().unwrap();
            assert_eq!(evidence.assignment, a);
            assert_eq!(observed.parent, d.runner.child_pid().unwrap());
            assert_ne!(observed.pid, std::process::id());
            assert_ne!(observed.pid, observed.parent);
            assert_eq!(observed.prompt, "artificial delegated request");
            assert_eq!(
                observed.cwd,
                f.root
                    .path()
                    .join(format!("workspace/{a}"))
                    .to_str()
                    .unwrap()
            );
            assert_eq!(observed.home, f.root.path().join("home").to_str().unwrap());
            assert_eq!(
                observed.codex_home,
                f.root.path().join("codex-home").to_str().unwrap()
            );
            assert_eq!(
                observed.claim_run,
                d.runner.stats().await.unwrap().last().unwrap().run
            );
            assert!(observed.durable_start);
            assert!(observed.args.contains(&"--json".into()));
            assert_eq!(
                observed.args.get(1).map(String::as_str),
                Some(if mode == Mode::ProviderResume {
                    "resume"
                } else {
                    "--cd"
                })
            );
            let leader = exit_fd(observed.pid);
            let descendant = exit_fd(observed.descendant);
            work.release().unwrap();
            applied(&mut d, a).await;
            assert_eq!(d.step().await.unwrap(), Step::Stopped(a));
            exited(&leader);
            exited(&descendant);
            dead(observed.pid);
            dead(observed.descendant);
            let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
            assert_eq!(evidence.assignment, a);
            assert!(evidence.progress.contains(&"Processing...".into()));
            if mode == Mode::ProviderFail {
                assert_eq!(evidence.exit_code, Some(7));
                assert!(evidence.session.is_none());
                assert_eq!(d.source.get_task_run(id).unwrap().unwrap().status, "failed");
                assert!(
                    output(&d.source, id)
                        .unwrap()
                        .contains("explicit fake failure")
                );
            } else {
                assert_eq!(evidence.exit_code, Some(0));
                assert_eq!(evidence.session, Some(format!("fixture-session-{a}")));
                assert_eq!(
                    output(&d.source, id).unwrap(),
                    if mode == Mode::ProviderLimit {
                        "B".repeat(2048)
                    } else {
                        format!("fake assignment {a}: artificial delegated request")
                    }
                );
                assert_eq!(
                    d.source.get_task_run(id).unwrap().unwrap().status,
                    "completed"
                );
            }
            assert!(d.runner.begin(a, mode).await.is_err());
        }
        let before = history(&mut d);
        d.shutdown().await.unwrap();
        let mut d = f.reopen().await;
        f.base.authorize(&mut d);
        d.reconcile().await.unwrap();
        assert_eq!(history(&mut d), before);
        assert_eq!(d.runner.provider_evidence().await.unwrap().len(), 4);
        assert_eq!(d.runner.stats().await.unwrap().len(), 4);
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn provider_process_cancel_before_and_during_reaps_fake_and_descendant() {
    bounded(async {
        let (f, mut d) = ProcessFixture::new().await;
        provision(f.root.path()).unwrap();
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        d.cancel(a).await.unwrap();
        d.admit_process(a, Mode::Provider)
            .await
            .unwrap()
            .release()
            .unwrap();
        assert!(d.runner.stats().await.unwrap().is_empty());
        assert!(d.runner.provider_evidence().await.unwrap().is_empty());
        assert_eq!(
            d.source.get_task_run(id).unwrap().unwrap().status,
            "cancelled"
        );
        let id = enqueue(&d.source, &d.session, 1);
        let b = d.queue(id).await.unwrap();
        let mut work = d.admit_process(b, Mode::Provider).await.unwrap();
        let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
        let leader = exit_fd(evidence.observed.as_ref().unwrap().pid);
        let descendant = exit_fd(evidence.observed.as_ref().unwrap().descendant);
        d.cancel(b).await.unwrap();
        work.cancelled().await.unwrap();
        applied(&mut d, b).await;
        assert_eq!(d.step().await.unwrap(), Step::Stopped(b));
        drop(work);
        exited(&leader);
        exited(&descendant);
        dead(evidence.observed.as_ref().unwrap().pid);
        dead(evidence.observed.as_ref().unwrap().descendant);
        assert_eq!(
            d.source.get_task_run(id).unwrap().unwrap().status,
            "cancelled"
        );
        assert_eq!(
            d.runner.provider_evidence().await.unwrap()[0].exit_code,
            Some(130)
        );
        // Dropping release control stops effects but is not a known successful result.
        let id = enqueue(&d.source, &d.session, 2);
        let c = d.queue(id).await.unwrap();
        let work = d.admit_process(c, Mode::Provider).await.unwrap();
        let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
        let leader = exit_fd(evidence.observed.as_ref().unwrap().pid);
        let descendant = exit_fd(evidence.observed.as_ref().unwrap().descendant);
        drop(work);
        applied(&mut d, c).await;
        unknown(&d.source, id);
        exited(&leader);
        exited(&descendant);
        dead(evidence.observed.as_ref().unwrap().pid);
        dead(evidence.observed.as_ref().unwrap().descendant);
        assert_eq!(
            d.runner.stats().await.unwrap().last().unwrap().status,
            "failed"
        );
        // Shutdown while another provider is blocked also owns its process cleanup.
        let id = enqueue(&d.source, &d.session, 3);
        let e = d.queue(id).await.unwrap();
        let work = d.admit_process(e, Mode::Provider).await.unwrap();
        let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
        let leader = exit_fd(evidence.observed.as_ref().unwrap().pid);
        let descendant = exit_fd(evidence.observed.as_ref().unwrap().descendant);
        d.shutdown().await.unwrap();
        drop(work);
        exited(&leader);
        exited(&descendant);
        dead(evidence.observed.as_ref().unwrap().pid);
        dead(evidence.observed.as_ref().unwrap().descendant);
    })
    .await;
}

#[tokio::test]
async fn provider_process_crash_after_invocation_report_or_signature_never_invokes_again() {
    bounded(async {
        for point in ["active", "report", "signature"] {
            let (f, mut d) = ProcessFixture::new().await;
            provision(f.root.path()).unwrap();
            d.reconcile().await.unwrap();
            let id = enqueue(&d.source, &d.session, 0);
            let a = d.queue(id).await.unwrap();
            let work = d.admit_process(a, Mode::Provider).await.unwrap();
            let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
            let leader = exit_fd(evidence.observed.as_ref().unwrap().pid);
            let descendant = exit_fd(evidence.observed.as_ref().unwrap().descendant);
            if point == "active" {
                d.runner.kill_child().unwrap();
                drop(work);
            } else {
                d.runner
                    .arm(if point == "report" {
                        Crash::Report
                    } else {
                        Crash::Signature
                    })
                    .await
                    .unwrap();
                work.release().unwrap();
            }
            assert!(d.step().await.is_err());
            assert!(d.shutdown().await.is_err());
            exited(&leader);
            exited(&descendant);
            let journal = Connection::open(f.root.path().join("runner.sqlite")).unwrap();
            let exact: Option<Vec<u8>> = journal
                .query_row(
                    "SELECT final FROM responses WHERE assignment=?1",
                    [a],
                    |r| r.get(0),
                )
                .unwrap();
            let mut d = f.reopen().await;
            assert!(d.reconcile().await.is_err());
            f.base.authorize(&mut d);
            d.reconcile().await.unwrap();
            unknown(&d.source, id);
            unknown(&d.target, 1);
            assert_eq!(d.runner.stats().await.unwrap().len(), 1);
            let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
            assert_eq!(evidence.assignment, a);
            if point != "active" {
                assert_eq!(evidence.exit_code, Some(0));
                assert_eq!(
                    output(&d.source, id).unwrap(),
                    "fake assignment 1: artificial delegated request"
                );
            } else {
                assert!(evidence.exit_code.is_none());
            }
            if let Some(exact) = exact {
                assert_eq!(result(&d, a).unwrap(), exact);
            }
            assert!(d.admit_process(a, Mode::Provider).await.is_err());
            assert!(d.runner.begin(a, Mode::Provider).await.is_err());
            let before = history(&mut d);
            d.shutdown().await.unwrap();
            let mut d = f.reopen().await;
            f.base.authorize(&mut d);
            d.reconcile().await.unwrap();
            assert_eq!(history(&mut d), before);
            assert_eq!(d.runner.stats().await.unwrap().len(), 1);
            assert_eq!(d.runner.provider_evidence().await.unwrap().len(), 1);
            d.shutdown().await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn provider_process_explicit_config_and_output_bounds_refuse_without_fallback() {
    bounded(async {
        for invalid in ["missing", "ambient"] {
            let (f, mut d) = ProcessFixture::new().await;
            provision(f.root.path()).unwrap();
            d.reconcile().await.unwrap();
            if invalid == "missing" {
                std::fs::remove_file(f.root.path().join("fake-codex.py")).unwrap();
            } else {
                let path = f.root.path().join("provider.json");
                let mut json: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                json["executable"] = "codex".into();
                std::fs::write(path, serde_json::to_vec(&json).unwrap()).unwrap();
            }
            let id = enqueue(&d.source, &d.session, 0);
            let a = d.queue(id).await.unwrap();
            assert!(d.admit_process(a, Mode::Provider).await.is_err());
            assert!(d.runner.stats().await.unwrap().is_empty());
            assert!(d.runner.provider_evidence().await.unwrap().is_empty());
            d.shutdown().await.unwrap();
            let mut d = f.reopen().await;
            f.base.authorize(&mut d);
            d.reconcile().await.unwrap();
            unknown(&d.source, id);
            assert!(d.runner.stats().await.unwrap().is_empty());
            assert!(d.admit_process(a, Mode::Provider).await.is_err());
            d.shutdown().await.unwrap();
        }
        let (f, mut d) = ProcessFixture::new().await;
        provision(f.root.path()).unwrap();
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        d.admit_process(a, Mode::ProviderOversize)
            .await
            .unwrap()
            .release()
            .unwrap();
        // Refused text is retained locally, never truncated, retried or replaced by a digest.
        applied(&mut d, a).await;
        unknown(&d.source, id);
        let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
        assert_eq!(evidence.output.unwrap().len(), 2049);
        assert_eq!(evidence.exit_code, Some(0));
        let failure = evidence.failure.unwrap();
        assert_eq!(failure.phase, "output-bound");
        assert_eq!(
            failure.detail,
            "provider output exceeds signed proposal limit"
        );
        assert_eq!(d.runner.stats().await.unwrap()[0].status, "failed");
        assert!(d.runner.finish(a).await.is_err());
        assert!(d.runner.begin(a, Mode::ProviderOversize).await.is_err());
        d.shutdown().await.unwrap();
        let mut d = f.reopen().await;
        f.base.authorize(&mut d);
        d.reconcile().await.unwrap();
        unknown(&d.source, id);
        let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
        assert_eq!(evidence.failure.unwrap().phase, "output-bound");
        assert_eq!(evidence.output.unwrap().len(), 2049);
        assert_eq!(d.runner.stats().await.unwrap().len(), 1);
        assert!(d.runner.begin(a, Mode::ProviderOversize).await.is_err());
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn provider_process_manifest_refusal_keeps_safe_failure_unknown_and_no_retry() {
    bounded(async {
        let (f, mut d) = ProcessFixture::new().await;
        provision(f.root.path()).unwrap();
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        let work = d.admit_process(a, Mode::Provider).await.unwrap();
        // Existing fake API worker, no new provider backend. Inject a host
        // validation refusal after its actual successful output, before Finish.
        // An arbitrary error body must not enter the durable safe diagnostic.
        let private_error_body = "noncredential-private-fixture-".repeat(10000);
        std::fs::write(
            f.root.path().join("host-provider.json"),
            serde_json::to_vec(&serde_json::json!({private_error_body.clone():true})).unwrap(),
        )
        .unwrap();
        work.release().unwrap();
        applied(&mut d, a).await;
        unknown(&d.source, id);
        let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
        assert_eq!(evidence.exit_code, Some(0));
        assert_eq!(evidence.session.as_deref(), Some("fixture-session-1"));
        let failure = evidence.failure.unwrap();
        assert_eq!(failure.phase, "artifact-validation");
        assert!(failure.phase.len() <= 32 && failure.detail.len() <= 128);
        assert!(!failure.detail.contains("noncredential-private-fixture"));
        assert_eq!(d.runner.stats().await.unwrap()[0].status, "failed");
        assert!(d.runner.begin(a, Mode::Provider).await.is_err());
        let before = history(&mut d);
        d.shutdown().await.unwrap();
        let mut d = f.reopen().await;
        f.base.authorize(&mut d);
        d.reconcile().await.unwrap();
        unknown(&d.source, id);
        assert_eq!(history(&mut d), before);
        let evidence = d.runner.provider_evidence().await.unwrap().pop().unwrap();
        assert_eq!(evidence.failure.unwrap().phase, "artifact-validation");
        assert_eq!(d.runner.stats().await.unwrap().len(), 1);
        assert!(d.runner.begin(a, Mode::Provider).await.is_err());
        assert!(d.admit_process(a, Mode::Provider).await.is_err());
        d.shutdown().await.unwrap();
    })
    .await;
}
