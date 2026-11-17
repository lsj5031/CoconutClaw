use super::super::super::proposals::process::{Bootstrap, Crash, Mode};
use super::*;
mod provider;

struct ProcessFixture {
    base: Fixture,
    root: tempfile::TempDir,
    real: bool,
}
impl ProcessFixture {
    async fn new() -> (Self, RuntimeDriver) {
        Self::build(false).await
    }
    #[cfg(target_os = "linux")]
    async fn new_real() -> (Self, RuntimeDriver) {
        Self::build(true).await
    }
    async fn build(real: bool) -> (Self, RuntimeDriver) {
        let root = tempfile::tempdir().unwrap();
        let bootstrap = if real {
            Bootstrap::launch_real(root.path())
        } else {
            Bootstrap::launch(root.path())
        }
        .unwrap();
        let n = Network::with_runner(Some(bootstrap.public())).await;
        let source = n.config("source");
        let target = n.config("target");
        let session = SessionKey::local("process-fixture");
        let client = bootstrap.bind(&n.origin, &session).await.unwrap();
        let d = RuntimeDriver::new(
            n.origin.clone(),
            session.clone(),
            (Store::open(&source).unwrap(), Store::open(&target).unwrap()),
            (n.a, n.b),
            (n.core.clone(), client),
        )
        .unwrap();
        let base = Fixture {
            root: n.root,
            origin: n.origin,
            core: n.core,
            runner: n.runner,
            ka: n.ka,
            kb: n.kb,
            ns: n.ns,
            policy: n.policy,
            source,
            target,
            session,
        };
        (Self { base, root, real }, d)
    }
    async fn reopen(&self) -> RuntimeDriver {
        let b = &self.base;
        let bootstrap = if self.real {
            Bootstrap::launch_real(self.root.path())
        } else {
            Bootstrap::launch(self.root.path())
        }
        .unwrap();
        let client = bootstrap.bind(&b.origin, &b.session).await.unwrap();
        let a = ScopedPeer::reopen(
            &b.root.path().join("a"),
            b.ka.clone(),
            b.origin.origin.owner,
            Capability::Write(b.ns.clone()),
        )
        .await
        .unwrap();
        let mirror = ScopedPeer::reopen(
            &b.root.path().join("b"),
            b.kb.clone(),
            b.origin.origin.owner,
            Capability::Write(b.ns.clone()),
        )
        .await
        .unwrap();
        RuntimeDriver::new(
            b.origin.clone(),
            b.session.clone(),
            (
                Store::open(&b.source).unwrap(),
                Store::open(&b.target).unwrap(),
            ),
            (a, mirror),
            (b.core.clone(), client),
        )
        .unwrap()
    }
}
fn dead(pid: u32) {
    assert_eq!(
        unsafe { libc::kill(pid as i32, 0) },
        -1,
        "mock child survived"
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}

#[tokio::test]
async fn runner_process_different_pid_roundtrip_overlaps_and_computes_in_child() {
    bounded(async {
        let (_f, mut d) = ProcessFixture::new().await;
        let pid = d.runner.child_pid().unwrap();
        assert_ne!(pid, std::process::id());
        d.reconcile().await.unwrap();
        let ids: Vec<_> = (0..4).map(|i| enqueue(&d.source, &d.session, i)).collect();
        let a = d.queue(ids[0]).await.unwrap();
        let first = d.admit_process(a, Mode::Cooperative).await.unwrap();
        let b = d.queue(ids[1]).await.unwrap();
        let second = d.admit_process(b, Mode::Cooperative).await.unwrap();
        let c = d.queue(ids[2]).await.unwrap();
        let e = d.queue(ids[3]).await.unwrap();
        assert!(d.admit_process(c, Mode::Cooperative).await.is_err());
        let stats = d.runner.stats().await.unwrap();
        assert_eq!(stats.len(), 2);
        assert!(stats.iter().all(|s| s.pid == pid && s.status == "running"));
        assert!(
            d.runner
                .report(a, Outcome::Completed, "parent invented output".into())
                .await
                .is_err()
        );
        second.release().unwrap();
        applied(&mut d, b).await;
        assert_eq!(d.step().await.unwrap(), Step::Stopped(b));
        for assignment in [c, e] {
            d.admit_process(assignment, Mode::Cooperative)
                .await
                .unwrap()
                .release()
                .unwrap();
            applied(&mut d, assignment).await;
            assert_eq!(d.step().await.unwrap(), Step::Stopped(assignment));
        }
        first.release().unwrap();
        applied(&mut d, a).await;
        assert_eq!(d.step().await.unwrap(), Step::Stopped(a));
        let digest = blake3::hash(b"artificial delegated request").to_string();
        for (id, assignment) in ids.iter().zip([a, b, c, e]) {
            assert_eq!(
                d.source.get_task_run(*id).unwrap().unwrap().status,
                "completed"
            );
            assert_eq!(
                output(&d.source, *id).unwrap(),
                format!("child {pid} assignment {assignment} mock digest {digest}")
            );
            assert!(d.runner.begin(assignment, Mode::Cooperative).await.is_err());
        }
        let stats = d.runner.stats().await.unwrap();
        assert_eq!(stats.len(), 4);
        assert!(
            stats
                .iter()
                .all(|s| s.pid == pid && s.status == "completed")
        );
        let shape: Vec<_> = history(&mut d)
            .iter()
            .map(
                |bytes| match SignedEvent::parse(bytes).unwrap().event.action {
                    Action::Assign { job, .. } => ('A', job),
                    Action::Start { assignment } => ('S', assignment),
                    Action::Result { assignment, .. } => ('R', assignment),
                    _ => panic!("unexpected event"),
                },
            )
            .collect();
        assert_eq!(
            shape,
            vec![
                ('A', 1),
                ('S', 1),
                ('A', 3),
                ('S', 3),
                ('A', 5),
                ('A', 6),
                ('R', 3),
                ('S', 5),
                ('R', 5),
                ('S', 6),
                ('R', 6),
                ('R', 1)
            ]
        );
        d.shutdown().await.unwrap();
        dead(pid);
    })
    .await;
}

#[tokio::test]
async fn runner_process_crash_points_reopen_exact_journal_without_duplicate_invocation() {
    bounded(async {
        for point in ["start", "active", "report", "signature"] {
            let (f, mut d) = ProcessFixture::new().await;
            d.reconcile().await.unwrap();
            let pid = d.runner.child_pid().unwrap();
            let id = enqueue(&d.source, &d.session, 0);
            let a = d.queue(id).await.unwrap();
            if point == "start" {
                d.runner.arm(Crash::Start).await.unwrap();
                assert!(d.admit_process(a, Mode::Cooperative).await.is_err());
            } else {
                let work = d.admit_process(a, Mode::Cooperative).await.unwrap();
                assert_eq!(d.runner.stats().await.unwrap().len(), 1);
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
            }
            if point == "start" {
                let rows: i64 = d
                    .target
                    .conn
                    .query_row("SELECT count(*) FROM adapter_execution", [], |r| r.get(0))
                    .unwrap();
                assert_eq!(rows, 0); // Signature was durable, response never reached Store.
            } else {
                assert!(result(&d, a).is_none());
            }
            assert!(d.shutdown().await.is_err());
            dead(pid);
            // Read public signed evidence, never the child's generated key file.
            let journal = Connection::open(f.root.path().join("runner.sqlite")).unwrap();
            let exact: Option<Vec<u8>> = journal
                .query_row(
                    "SELECT final FROM responses WHERE assignment=?1",
                    [a],
                    |r| r.get(0),
                )
                .unwrap();
            let mut d = f.reopen().await;
            assert_ne!(d.runner.child_pid().unwrap(), pid);
            assert!(d.reconcile().await.is_err()); // Quarantine remains.
            f.base.authorize(&mut d);
            d.reconcile().await.unwrap();
            unknown(&d.source, id);
            unknown(&d.target, 1);
            if let Some(exact) = exact {
                assert_eq!(result(&d, a).unwrap(), exact);
            }
            if matches!(point, "report" | "signature") {
                assert!(
                    output(&d.source, id)
                        .unwrap()
                        .starts_with(&format!("child {pid} "))
                );
                assert!(matches!(
                    SignedEvent::parse(&result(&d, a).unwrap())
                        .unwrap()
                        .event
                        .action,
                    Action::Result {
                        outcome: Outcome::Completed,
                        ..
                    }
                ));
            }
            let stats = d.runner.stats().await.unwrap();
            assert_eq!(stats.len(), usize::from(point != "start"));
            assert!(stats.iter().all(|s| s.pid == pid));
            if point == "active" {
                assert_eq!(stats[0].status, "failed");
            }
            assert!(d.admit_process(a, Mode::Cooperative).await.is_err());
            assert!(d.runner.begin(a, Mode::Cooperative).await.is_err());
            assert_eq!(d.runner.stats().await.unwrap().len(), stats.len());
            let before = history(&mut d);
            d.shutdown().await.unwrap();
            let mut d = f.reopen().await;
            f.base.authorize(&mut d);
            d.reconcile().await.unwrap();
            assert_eq!(history(&mut d), before);
            assert_eq!(d.runner.stats().await.unwrap().len(), stats.len());
            d.shutdown().await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn runner_process_cancellation_before_during_and_late_completion_is_conservative() {
    bounded(async {
        let (_f, mut d) = ProcessFixture::new().await;
        d.reconcile().await.unwrap();
        let pid = d.runner.child_pid().unwrap();
        let ids: Vec<_> = (0..3).map(|i| enqueue(&d.source, &d.session, i)).collect();
        let a = d.queue(ids[0]).await.unwrap();
        d.cancel(a).await.unwrap();
        d.admit_process(a, Mode::Cooperative)
            .await
            .unwrap()
            .release()
            .unwrap();
        assert!(d.runner.stats().await.unwrap().is_empty());
        assert_eq!(
            d.source.get_task_run(ids[0]).unwrap().unwrap().status,
            "cancelled"
        );
        let b = d.queue(ids[1]).await.unwrap();
        let mut work = d.admit_process(b, Mode::Cooperative).await.unwrap();
        d.cancel(b).await.unwrap();
        work.cancelled().await.unwrap();
        applied(&mut d, b).await;
        assert_eq!(d.step().await.unwrap(), Step::Stopped(b));
        drop(work);
        assert_eq!(
            d.source.get_task_run(ids[1]).unwrap().unwrap().status,
            "cancelled"
        );
        let e = d.queue(ids[2]).await.unwrap();
        let mut work = d.admit_process(e, Mode::Late).await.unwrap();
        d.cancel(e).await.unwrap();
        work.cancelled().await.unwrap();
        assert!(
            d.runner
                .stats()
                .await
                .unwrap()
                .iter()
                .any(|s| s.assignment == e && s.status == "cancel_requested")
        );
        work.release().unwrap();
        applied(&mut d, e).await;
        unknown(&d.source, ids[2]);
        unknown(&d.target, 3);
        assert!(
            output(&d.source, ids[2])
                .unwrap()
                .starts_with(&format!("child {pid} "))
        );
        assert!(matches!(
            SignedEvent::parse(&result(&d, e).unwrap())
                .unwrap()
                .event
                .action,
            Action::Result {
                outcome: Outcome::Completed,
                ..
            }
        ));
        assert_eq!(d.runner.stats().await.unwrap().len(), 2);
        d.shutdown().await.unwrap();
        dead(pid);
    })
    .await;
}

#[tokio::test]
async fn runner_process_framing_boundary_refusal_and_shutdown_drop_cleanup() {
    bounded(async {
        let unmarked = tempfile::tempdir().unwrap();
        std::fs::write(unmarked.path().join("unrelated"), b"preserve").unwrap();
        assert!(Bootstrap::launch(unmarked.path()).is_err());
        assert_eq!(
            std::fs::read(unmarked.path().join("unrelated")).unwrap(),
            b"preserve"
        );
        let (f, mut d) = ProcessFixture::new().await;
        d.reconcile().await.unwrap();
        for kind in ["malformed", "version", "boundary", "origin"] {
            d.runner.malformed(kind).await.unwrap();
            assert!(d.runner.stats().await.unwrap().is_empty());
            assert!(history(&mut d).is_empty());
        }
        let pid = d.runner.child_pid().unwrap();
        d.runner.malformed("oversized").await.unwrap();
        assert!(d.shutdown().await.is_err());
        dead(pid);
        let mut d = f.reopen().await;
        f.base.authorize(&mut d);
        d.reconcile().await.unwrap();
        assert!(d.runner.stats().await.unwrap().is_empty());
        d.shutdown().await.unwrap();
        let bootstrap = Bootstrap::launch(f.root.path()).unwrap();
        let client = bootstrap
            .bind(&f.base.origin, &f.base.session)
            .await
            .unwrap();
        let pid = client.child_pid().unwrap();
        drop(client);
        dead(pid);
        let bootstrap = Bootstrap::launch(f.root.path()).unwrap();
        let client = bootstrap
            .bind(&f.base.origin, &f.base.session)
            .await
            .unwrap();
        client.shutdown().await.unwrap();
    })
    .await;
}
