use super::super::driver::{MockWork, RuntimeDriver, Step};
use super::super::proposals::RunnerClient;
use super::roundtrip::{output, unknown};
use super::*;
use coconutclaw_sync::synthetic::membership::domain::Outcome;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::oneshot;
#[cfg(target_os = "linux")]
mod process;
mod proposals;

struct Fixture {
    root: tempfile::TempDir,
    origin: BoundOrigin,
    core: SecretKey,
    runner: SecretKey,
    ka: SecretKey,
    kb: SecretKey,
    ns: NamespaceSecret,
    policy: SignedPolicy,
    source: RuntimeConfig,
    target: RuntimeConfig,
    session: SessionKey,
}

impl Fixture {
    async fn new() -> (Self, RuntimeDriver) {
        let n = Network::new().await;
        let source = n.config("source");
        let target = n.config("target");
        let session = SessionKey::local("async-driver-fixture");
        let d = RuntimeDriver::new(
            n.origin.clone(),
            session.clone(),
            (Store::open(&source).unwrap(), Store::open(&target).unwrap()),
            (n.a, n.b),
            (
                n.core.clone(),
                RunnerClient::spawn(
                    n.origin.clone(),
                    n.runner.clone(),
                    &n.root.path().join("runner-proposals.sqlite"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let f = Self {
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
        (f, d)
    }

    async fn reopen(&self) -> RuntimeDriver {
        let a = ScopedPeer::reopen(
            &self.root.path().join("a"),
            self.ka.clone(),
            self.origin.origin.owner,
            Capability::Write(self.ns.clone()),
        )
        .await
        .unwrap();
        let b = ScopedPeer::reopen(
            &self.root.path().join("b"),
            self.kb.clone(),
            self.origin.origin.owner,
            Capability::Write(self.ns.clone()),
        )
        .await
        .unwrap();
        RuntimeDriver::new(
            self.origin.clone(),
            self.session.clone(),
            (
                Store::open(&self.source).unwrap(),
                Store::open(&self.target).unwrap(),
            ),
            (a, b),
            (
                self.core.clone(),
                RunnerClient::spawn(
                    self.origin.clone(),
                    self.runner.clone(),
                    &self.root.path().join("runner-proposals.sqlite"),
                )
                .unwrap(),
            ),
        )
        .unwrap()
    }

    fn publisher_sql(&self) -> Connection {
        Connection::open(self.root.path().join("a/projection.sqlite")).unwrap()
    }

    fn authorize(&self, d: &mut RuntimeDriver) {
        d.publisher.install_policy(&self.policy).unwrap();
        d.mirror.install_policy(&self.policy).unwrap();
    }
}

async fn bounded(future: impl std::future::Future<Output = ()>) {
    // Windows CI runners take several times longer than a workstation; the
    // bound only exists to stop a hung test, not to pace healthy execution.
    tokio::time::timeout(Duration::from_secs(120), future)
        .await
        .unwrap();
}

fn result(d: &RuntimeDriver, assignment: u64) -> Option<Vec<u8>> {
    d.target
        .conn
        .query_row(
            "SELECT result FROM adapter_execution WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )
        .unwrap()
}

fn history(d: &mut RuntimeDriver) -> Vec<Vec<u8>> {
    let events = d.publisher.domain_history(&d.origin).unwrap().export();
    assert_eq!(events, d.mirror.domain_history(&d.origin).unwrap().export());
    for (i, bytes) in events.iter().enumerate() {
        assert_eq!(
            SignedEvent::parse(bytes).unwrap().event.sequence,
            i as u64 + 1
        );
    }
    events
}

async fn waiting(
    work: MockWork,
    calls: Arc<AtomicUsize>,
    started: oneshot::Sender<()>,
    gate: oneshot::Receiver<()>,
    text: &'static str,
) {
    assert_eq!(work.prompt, "artificial delegated request");
    calls.fetch_add(1, Ordering::SeqCst);
    started.send(()).unwrap();
    gate.await.unwrap();
    work.result.finish(Outcome::Completed, text).await.unwrap();
}

async fn applied(d: &mut RuntimeDriver, expected: u64) {
    // Legitimate callback-stop events can arrive between outcomes.
    for _ in 0..5 {
        match d.step().await.unwrap() {
            Step::Applied(a) => {
                assert_eq!(a, expected);
                return;
            }
            Step::Stopped(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    panic!("no accepted outcome");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_driver_overlapping_callbacks_use_current_head_and_bounded_admission() {
    bounded(async {
        let (_f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let ids: Vec<_> = (0..5).map(|i| enqueue(&d.source, &d.session, i)).collect();
        let calls = Arc::new(AtomicUsize::new(0));
        let a = d.queue(ids[0]).await.unwrap();
        let (started1, ready1) = oneshot::channel();
        let (release1, gate1) = oneshot::channel();
        let c = calls.clone();
        d.admit(a, move |w| waiting(w, c, started1, gate1, "first"))
            .await
            .unwrap();
        ready1.await.unwrap();
        let b = d.queue(ids[1]).await.unwrap();
        let (started2, ready2) = oneshot::channel();
        let (release2, gate2) = oneshot::channel();
        let c = calls.clone();
        d.admit(b, move |w| waiting(w, c, started2, gate2, "second"))
            .await
            .unwrap();
        ready2.await.unwrap();
        let c = d.queue(ids[2]).await.unwrap();
        let e = d.queue(ids[3]).await.unwrap();
        assert!(
            d.admit(c, |_| async { panic!("full admission invoked mock") })
                .await
                .is_err()
        );
        assert!(d.queue(ids[4]).await.is_err());
        assert_eq!(
            d.source.get_task_run(ids[4]).unwrap().unwrap().status,
            "queued"
        );
        assert_eq!(count(&d.target), 4);
        let claimed: i64 = d
            .target
            .conn
            .query_row("SELECT sum(claimed) FROM adapter_execution", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(claimed, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        release2.send(()).unwrap();
        applied(&mut d, b).await;
        assert_eq!(d.step().await.unwrap(), Step::Stopped(b));
        for (assignment, text) in [(c, "third"), (e, "fourth")] {
            let calls = calls.clone();
            d.admit(assignment, move |w| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                w.result.finish(Outcome::Completed, text).await.unwrap();
            })
            .await
            .unwrap();
            applied(&mut d, assignment).await;
            assert_eq!(d.step().await.unwrap(), Step::Stopped(assignment));
        }
        release1.send(()).unwrap();
        applied(&mut d, a).await;
        assert_eq!(d.step().await.unwrap(), Step::Stopped(a));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        let events = history(&mut d);
        assert_eq!(events.len(), 12); // Worst allowed workload: four complete tasks.
        let shape: Vec<_> = events
            .iter()
            .map(|b| match SignedEvent::parse(b).unwrap().event.action {
                Action::Assign { job, .. } => ('A', job),
                Action::Start { assignment } => ('S', assignment),
                Action::Result { assignment, .. } => ('R', assignment),
                _ => panic!("unexpected event"),
            })
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
        for (id, text) in ids.iter().zip(["first", "second", "third", "fourth"]) {
            assert_eq!(
                d.source.get_task_run(*id).unwrap().unwrap().status,
                "completed"
            );
            assert_eq!(output(&d.source, *id).as_deref(), Some(text));
        }
        assert_eq!(d.queue(ids[0]).await.unwrap(), a);
        d.reconcile().await.unwrap();
        assert_eq!(history(&mut d), events);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runtime_driver_cancellation_before_and_during_work_preserves_late_unknown() {
    bounded(async {
        let (_f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let ids: Vec<_> = (0..3).map(|i| enqueue(&d.source, &d.session, i)).collect();
        let calls = Arc::new(AtomicUsize::new(0));
        let a = d.queue(ids[0]).await.unwrap();
        d.cancel(a).await.unwrap();
        let c = calls.clone();
        d.admit(a, move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            async { panic!("pre-cancel callback") }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            d.source.get_task_run(ids[0]).unwrap().unwrap().status,
            "cancelled"
        );
        let b = d.queue(ids[1]).await.unwrap();
        let (started, ready) = oneshot::channel();
        let c = calls.clone();
        d.admit(b, move |mut w| async move {
            c.fetch_add(1, Ordering::SeqCst);
            started.send(()).unwrap();
            w.cancellation.changed().await.unwrap();
            assert!(*w.cancellation.borrow());
            w.result
                .finish(Outcome::Cancelled, "cooperatively cancelled mock")
                .await
                .unwrap();
        })
        .await
        .unwrap();
        ready.await.unwrap();
        let e = d.queue(ids[2]).await.unwrap();
        let (started, ready) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let c = calls.clone();
        d.admit(e, move |w| {
            waiting(w, c, started, gate, "late completion evidence")
        })
        .await
        .unwrap();
        ready.await.unwrap();
        d.cancel(b).await.unwrap();
        d.cancel(e).await.unwrap();
        applied(&mut d, b).await;
        assert_eq!(d.step().await.unwrap(), Step::Stopped(b));
        release.send(()).unwrap();
        applied(&mut d, e).await;
        assert_eq!(d.step().await.unwrap(), Step::Stopped(e));
        assert_eq!(
            d.source.get_task_run(ids[1]).unwrap().unwrap().status,
            "cancelled"
        );
        unknown(&d.source, ids[2]);
        unknown(&d.target, 3);
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
        assert_eq!(
            output(&d.source, ids[2]).as_deref(),
            Some("late completion evidence")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(history(&mut d).len(), 9);
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runtime_driver_duplicate_and_conflicting_channel_outcomes_allocate_no_extra_events() {
    bounded(async {
        let (_f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        d.admit(a, move |w| async move {
            c.fetch_add(1, Ordering::SeqCst);
            w.result.finish(Outcome::Completed, "stable").await.unwrap();
            w.result.finish(Outcome::Completed, "stable").await.unwrap();
            w.result
                .finish(Outcome::Failed, "conflicting outcome")
                .await
                .unwrap();
        })
        .await
        .unwrap();
        applied(&mut d, a).await;
        let bytes = result(&d, a).unwrap();
        let before = history(&mut d);
        assert_eq!(d.step().await.unwrap(), Step::Duplicate(a));
        assert!(
            d.step()
                .await
                .unwrap_err()
                .to_string()
                .contains("conflicting mock outcome")
        );
        assert_eq!(d.step().await.unwrap(), Step::Stopped(a));
        assert_eq!(result(&d, a).unwrap(), bytes);
        assert_eq!(history(&mut d), before);
        assert_eq!(output(&d.source, id).as_deref(), Some("stable"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        d.reconcile().await.unwrap();
        assert_eq!(history(&mut d), before);
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runtime_driver_pending_publication_transport_and_source_commit_block_allocation() {
    bounded(async {
        let (f,mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let ids:Vec<_> = (0..3).map(|i|enqueue(&d.source,&d.session,i)).collect();
        let mut releases = Vec::new();
        let mut assignments = Vec::new();
        for id in &ids[..2] {
            let a = d.queue(*id).await.unwrap();
            assignments.push(a);
            let (started,ready) = oneshot::channel();
            let (release,gate) = oneshot::channel();
            releases.push(release);
            let c = calls.clone();
            d.admit(a,move|w|waiting(w,c,started,gate,"durable async result")).await.unwrap();
            ready.await.unwrap();
        }
        let sql = f.publisher_sql();
        sql.execute_batch("CREATE TRIGGER abort_publication BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT,'injected publication failure'); END;").unwrap();
        releases.remove(0).send(()).unwrap();
        assert!(d.step().await.is_err());
        let exact = result(&d,assignments[0]).unwrap();
        assert_eq!(d.source.get_task_run(ids[0]).unwrap().unwrap().status,"running");
        assert!(output(&d.source,ids[0]).is_none());
        assert!(d.queue(ids[2]).await.is_err());
        releases.remove(0).send(()).unwrap();
        assert!(d.step().await.is_err()); // Queued outcome remains unconsumed.
        assert!(result(&d,assignments[1]).is_none());
        sql.execute_batch("DROP TRIGGER abort_publication").unwrap();
        let mirror = Connection::open(f.root.path().join("b/projection.sqlite")).unwrap();
        mirror.execute_batch("CREATE TRIGGER abort_transfer BEFORE INSERT ON received BEGIN SELECT RAISE(ABORT,'injected delayed transfer'); END;").unwrap();
        assert!(d.reconcile().await.is_err());
        assert!(d.queue(ids[2]).await.is_err());
        assert!(result(&d,assignments[1]).is_none());
        mirror.execute_batch("DROP TRIGGER abort_transfer").unwrap();
        d.source.conn.execute_batch("CREATE TRIGGER abort_source BEFORE UPDATE OF result ON adapter_returns BEGIN SELECT RAISE(ABORT,'injected source commit failure'); END;").unwrap();
        assert!(d.reconcile().await.is_err());
        assert!(output(&d.source,ids[0]).is_none());
        assert!(d.queue(ids[2]).await.is_err());
        d.source.conn.execute_batch("DROP TRIGGER abort_source").unwrap();
        d.reconcile().await.unwrap();
        assert_eq!(result(&d,assignments[0]).unwrap(),exact);
        assert!(history(&mut d).contains(&exact));
        assert!(result(&d,assignments[1]).is_none()); // Still-live claim wasn't recovered Unknown.
        applied(&mut d,assignments[1]).await;
        assert_eq!(calls.load(Ordering::SeqCst),2);
        for id in &ids[..2] {
            assert_eq!(d.source.get_task_run(*id).unwrap().unwrap().status,"completed");
            assert_eq!(output(&d.source,*id).as_deref(),Some("durable async result"));
        }
        assert_eq!(history(&mut d).len(),6);
        assert_eq!(d.queue(ids[2]).await.unwrap(),7);
        d.shutdown().await.unwrap();
    }).await;
}

#[tokio::test]
async fn runtime_driver_restart_replays_durable_result_and_orphaned_start_unknown_after_quarantine()
{
    bounded(async {
        let (f,mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let ids:Vec<_> = (0..2).map(|i|enqueue(&d.source,&d.session,i)).collect();
        let mut releases = Vec::new();
        let mut assignments = Vec::new();
        for id in &ids {
            let a = d.queue(*id).await.unwrap();
            assignments.push(a);
            let (started,ready) = oneshot::channel();
            let (release,gate) = oneshot::channel();
            releases.push(release);
            let c = calls.clone();
            d.admit(a,move|w|waiting(w,c,started,gate,"restart durable result")).await.unwrap();
            ready.await.unwrap();
        }
        let sql = f.publisher_sql();
        sql.execute_batch("CREATE TRIGGER abort_publication BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT,'injected publication failure'); END;").unwrap();
        releases.remove(0).send(()).unwrap();
        assert!(d.step().await.is_err());
        let exact = result(&d,assignments[0]).unwrap();
        sql.execute_batch("DROP TRIGGER abort_publication").unwrap();
        d.shutdown().await.unwrap(); // Other mock is aborted, never resumed.
        assert!(releases.remove(0).send(()).is_err());
        let mut d = f.reopen().await;
        assert!(d.reconcile().await.is_err()); // Persistent peers reopen quarantined.
        assert!(d.queue(ids[0]).await.is_err());
        assert!(result(&d,assignments[1]).is_none());
        assert!(output(&d.source,ids[0]).is_none());
        f.authorize(&mut d);
        d.reconcile().await.unwrap();
        assert_eq!(result(&d,assignments[0]).unwrap(),exact);
        assert_eq!(d.source.get_task_run(ids[0]).unwrap().unwrap().status,"completed");
        unknown(&d.source,ids[1]);
        unknown(&d.target,2);
        assert!(matches!(SignedEvent::parse(&result(&d,assignments[1]).unwrap()).unwrap().event.action,
            Action::Result {outcome:Outcome::Unknown,..}));
        let c = calls.clone();
        assert!(d.admit(assignments[1],move|_|{c.fetch_add(1,Ordering::SeqCst);async {}}).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst),2);
        let before = history(&mut d);
        d.shutdown().await.unwrap();
        let mut d = f.reopen().await;
        f.authorize(&mut d);
        d.reconcile().await.unwrap();
        assert_eq!(history(&mut d),before);
        assert_eq!(calls.load(Ordering::SeqCst),2);
        d.shutdown().await.unwrap();
    }).await;
}

#[tokio::test]
async fn runtime_driver_assignment_start_and_result_failure_boundaries_never_retry_execution() {
    bounded(async {
        for boundary in ["assignment","start","result","panic"] {
            let (f,mut d) = Fixture::new().await;
            d.reconcile().await.unwrap();
            let id = enqueue(&d.source,&d.session,0);
            let sql = f.publisher_sql();
            let calls = Arc::new(AtomicUsize::new(0));
            if boundary == "assignment" {
                sql.execute_batch("CREATE TRIGGER abort_publication BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT,'injected assignment failure'); END;").unwrap();
                assert!(d.queue(id).await.is_err());
                assert_eq!(count(&d.target),0);
                sql.execute_batch("DROP TRIGGER abort_publication").unwrap();
                d.reconcile().await.unwrap();
            } else {
                assert_eq!(d.queue(id).await.unwrap(),1);
            }
            if boundary == "start" {
                sql.execute_batch("CREATE TRIGGER abort_publication BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT,'injected start failure'); END;").unwrap();
            } else if boundary == "result" {
                d.target.conn.execute_batch("CREATE TRIGGER abort_result BEFORE UPDATE OF result ON adapter_execution BEGIN SELECT RAISE(ABORT,'injected result persistence failure'); END;").unwrap();
            }
            let c = calls.clone();
            let admitted = d.admit(1,move|w|async move {
                c.fetch_add(1,Ordering::SeqCst);
                assert_ne!(boundary,"panic","injected async mock panic");
                if boundary == "result" { w.result.finish(Outcome::Completed,"not durably committed").await.unwrap(); }
                // Otherwise end without a result: conservative Unknown.
            }).await;
            if boundary == "start" {
                assert!(admitted.is_err());
                assert_eq!(calls.load(Ordering::SeqCst),0);
                sql.execute_batch("DROP TRIGGER abort_publication").unwrap();
                d.reconcile().await.unwrap();
            } else {
                admitted.unwrap();
                if matches!(boundary,"result"|"panic") {
                    assert!(d.step().await.is_err());
                    assert!(result(&d,1).is_none());
                    assert_eq!(d.target.get_task_run(1).unwrap().unwrap().status,"running");
                    if boundary == "result" {
                        d.target.conn.execute_batch("DROP TRIGGER abort_result").unwrap();
                    }
                    d.reconcile().await.unwrap();
                } else { applied(&mut d,1).await; }
                assert_eq!(calls.load(Ordering::SeqCst),1);
            }
            unknown(&d.source,id);
            unknown(&d.target,1);
            let prior = history(&mut d);
            d.reconcile().await.unwrap();
            assert_eq!(history(&mut d),prior);
            let c = calls.clone();
            assert!(d.admit(1,move|_|{c.fetch_add(1,Ordering::SeqCst);async {}}).await.is_err());
            assert_eq!(calls.load(Ordering::SeqCst),usize::from(boundary != "start"));
            d.shutdown().await.unwrap();
        }
    }).await;
}

#[tokio::test]
async fn runtime_driver_lost_cancel_publication_replays_intent_and_signals_live_mock() {
    bounded(async {
        let (f,mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source,&d.session,0);
        let a = d.queue(id).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let (started,ready) = oneshot::channel();
        d.admit(a,move|mut w|async move {
            c.fetch_add(1,Ordering::SeqCst);
            started.send(()).unwrap();
            w.cancellation.changed().await.unwrap();
            assert!(*w.cancellation.borrow());
            w.result.finish(Outcome::Cancelled,"cancel intent repaired").await.unwrap();
        }).await.unwrap();
        ready.await.unwrap();
        let sql = f.publisher_sql();
        sql.execute_batch("CREATE TRIGGER abort_publication BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT,'injected cancel publication failure'); END;").unwrap();
        assert!(d.cancel(a).await.is_err());
        assert!(d.source.get_task_run(id).unwrap().unwrap().cancel_requested_at.is_some());
        assert!(d.target.get_task_run(1).unwrap().unwrap().cancel_requested_at.is_none());
        let command:String = d.source.conn.query_row("SELECT command FROM adapter_cancel",[],|r|r.get(0)).unwrap();
        sql.execute_batch("DROP TRIGGER abort_publication").unwrap();
        d.reconcile().await.unwrap();
        assert!(result(&d,a).is_none()); // Active claim was preserved through reconnect.
        applied(&mut d,a).await;
        assert_eq!(d.source.get_task_run(id).unwrap().unwrap().status,"cancelled");
        let saved:String = d.target.conn.query_row("SELECT command FROM adapter_cancel",[],|r|r.get(0)).unwrap();
        assert_eq!(saved,command);
        assert_eq!(calls.load(Ordering::SeqCst),1);
        let before = history(&mut d);
        d.cancel(a).await.unwrap();
        assert_eq!(history(&mut d),before);
        d.shutdown().await.unwrap();
    }).await;
}

#[tokio::test]
async fn runtime_driver_persisted_conflicting_signed_outcome_refuses_entire_reconciliation() {
    bounded(async {
        let (f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        d.admit(a, |w| async move {
            w.result
                .finish(Outcome::Completed, "canonical")
                .await
                .unwrap();
        })
        .await
        .unwrap();
        applied(&mut d, a).await;
        let before = history(&mut d);
        let mut event = SignedEvent::parse(&result(&d, a).unwrap()).unwrap().event;
        event.action = Action::Result {
            assignment: a,
            outcome: Outcome::Failed,
            output: "validly signed fork".into(),
        };
        let fork = SignedEvent::sign(event, &f.runner)
            .unwrap()
            .bytes()
            .unwrap();
        let mut conflicting = before.clone();
        conflicting.push(fork.clone());
        assert!(
            History::replay(
                &d.origin,
                d.origin.origin.owner,
                d.origin.origin.namespace,
                conflicting
            )
            .is_err()
        );
        d.target
            .conn
            .execute(
                "UPDATE adapter_execution SET result=?1 WHERE assignment=?2",
                params![fork, a],
            )
            .unwrap();
        assert!(d.reconcile().await.is_err());
        assert_eq!(history(&mut d), before); // Refused before publishing the alternative.
        assert_eq!(output(&d.source, id).as_deref(), Some("canonical"));
        assert!(d.queue(id).await.is_err());
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runtime_driver_lost_store_correlations_refuse_recovery_and_new_admission() {
    bounded(async {
        for missing in ["source-map", "execution"] {
            let (f, mut d) = Fixture::new().await;
            d.reconcile().await.unwrap();
            let id = enqueue(&d.source, &d.session, 0);
            let a = d.queue(id).await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let c = calls.clone();
            let (started, ready) = oneshot::channel();
            let (release, gate) = oneshot::channel();
            d.admit(a, move |w| {
                waiting(w, c, started, gate, "never recovered by executing")
            })
            .await
            .unwrap();
            ready.await.unwrap();
            let before = history(&mut d);
            d.shutdown().await.unwrap();
            assert!(release.send(()).is_err());
            let mut d = f.reopen().await;
            f.authorize(&mut d);
            if missing == "source-map" {
                d.source
                    .conn
                    .execute("DELETE FROM adapter_outgoing WHERE run=?1", [id])
                    .unwrap();
            } else {
                d.target
                    .conn
                    .execute("DELETE FROM adapter_execution WHERE assignment=?1", [a])
                    .unwrap();
            }
            assert!(d.reconcile().await.is_err());
            assert_eq!(history(&mut d), before);
            let other = enqueue(&d.source, &d.session, 1);
            assert!(d.queue(other).await.is_err());
            let c = calls.clone();
            assert!(
                d.admit(a, move |_| {
                    c.fetch_add(1, Ordering::SeqCst);
                    async {}
                })
                .await
                .is_err()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(output(&d.source, id).is_none());
            d.shutdown().await.unwrap();
        }
    })
    .await;
}
