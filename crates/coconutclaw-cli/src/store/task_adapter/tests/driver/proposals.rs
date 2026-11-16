use super::super::super::proposals::{MockSigner, SignedProposal};
use super::*;

#[tokio::test]
async fn runner_proposals_hostile_bindings_signatures_and_bounds_refuse_before_acceptance() {
    bounded(async {
        let (f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let (started, ready) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        d.admit(a, move |w| waiting(w, c, started, gate, "canonical"))
            .await
            .unwrap();
        ready.await.unwrap();
        let good = d
            .runner
            .report(a, Outcome::Completed, "canonical".into())
            .await
            .unwrap();
        let signed: SignedProposal = serde_json::from_slice(&good).unwrap();
        let original = signed.proposal.clone();
        let mut candidates = vec![b"{}".to_vec(), vec![b'x'; 8193]];
        let mut noncanonical = good.clone();
        noncanonical.push(b'\n');
        candidates.push(noncanonical);
        let mut unknown: serde_json::Value = serde_json::from_slice(&good).unwrap();
        unknown["extra"] = true.into();
        candidates.push(serde_json::to_vec(&unknown).unwrap());
        for field in [
            "origin",
            "scope",
            "assignment",
            "digest",
            "source",
            "parent",
            "index",
            "attempt0",
            "attempt2",
            "empty",
            "oversize",
            "control",
        ] {
            let mut p = original.clone();
            match field {
                "origin" => p.origin = "00".repeat(32),
                "scope" => p.scope = "foreign/scope".into(),
                "assignment" => p.assignment += 1,
                "digest" => p.assignment_hash = "00".repeat(32),
                "source" => p.source_run += 1,
                "parent" => p.parent += 1,
                "index" => p.index += 1,
                "attempt0" => p.attempt = 0,
                "attempt2" => p.attempt = 2,
                "empty" => p.output.clear(),
                "oversize" => p.output = "x".repeat(2049),
                "control" => p.output = "invalid\0output".into(),
                _ => unreachable!(),
            }
            candidates.push(SignedProposal::sign(p, &f.runner).unwrap());
        }
        let wrong = key();
        let mut p = original.clone();
        p.runner = wrong.public();
        candidates.push(SignedProposal::sign(p, &wrong).unwrap());
        let invalid = SignedProposal {
            proposal: original,
            signature: wrong.sign(b"wrong signature"),
        };
        candidates.push(serde_json::to_vec(&invalid).unwrap());
        let before = history(&mut d);
        for bytes in candidates {
            assert!(d.accept_proposal(bytes).await.is_err());
            assert_eq!(history(&mut d), before);
            assert!(result(&d, a).is_none());
            assert!(output(&d.source, id).is_none());
            assert_eq!(
                d.source.get_task_run(id).unwrap().unwrap().status,
                "running"
            );
        }
        assert_eq!(
            d.accept_proposal(good.clone()).await.unwrap(),
            Step::Applied(a)
        );
        assert_eq!(d.accept_proposal(good).await.unwrap(), Step::Duplicate(a));
        assert_eq!(output(&d.source, id).as_deref(), Some("canonical"));
        assert_eq!(
            d.source.get_task_run(id).unwrap().unwrap().status,
            "completed"
        );
        release.send(()).unwrap();
        assert_eq!(d.step().await.unwrap(), Step::Duplicate(a));
        assert_eq!(d.step().await.unwrap(), Step::Stopped(a));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runner_proposals_signing_handshake_rejects_unobserved_or_changed_outcomes() {
    bounded(async {
        let (_f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (started, ready) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        d.admit(a, move |w| waiting(w, calls, started, gate, "observed"))
            .await
            .unwrap();
        ready.await.unwrap();
        let prefix = d.publisher.domain_history(&d.origin).unwrap();
        assert!(
            d.runner
                .sign(
                    &d.origin,
                    &prefix,
                    Action::Result {
                        assignment: a,
                        outcome: Outcome::Completed,
                        output: "invented".into()
                    }
                )
                .await
                .is_err()
        );
        let bytes = d
            .runner
            .report(a, Outcome::Completed, "observed".into())
            .await
            .unwrap();
        for action in [
            Action::Result {
                assignment: a,
                outcome: Outcome::Failed,
                output: "observed".into(),
            },
            Action::Result {
                assignment: a,
                outcome: Outcome::Completed,
                output: "different".into(),
            },
            Action::Result {
                assignment: a + 1,
                outcome: Outcome::Completed,
                output: "observed".into(),
            },
            Action::Source {
                reference: "not runner authority".into(),
            },
        ] {
            assert!(d.runner.sign(&d.origin, &prefix, action).await.is_err());
        }
        assert!(result(&d, a).is_none());
        assert_eq!(d.accept_proposal(bytes).await.unwrap(), Step::Applied(a));
        release.send(()).unwrap();
        assert_eq!(d.step().await.unwrap(), Step::Duplicate(a));
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runner_proposals_lost_final_signing_response_replays_exact_and_never_reexecutes() {
    bounded(async {
        let (f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let ids: Vec<_> = (0..2).map(|i| enqueue(&d.source, &d.session, i)).collect();
        let mut assignments = Vec::new();
        let mut releases = Vec::new();
        for id in &ids {
            let a = d.queue(*id).await.unwrap();
            assignments.push(a);
            let (started, ready) = oneshot::channel();
            let (release, gate) = oneshot::channel();
            releases.push(release);
            let c = calls.clone();
            d.admit(a, move |w| waiting(w, c, started, gate, "unreceived"))
                .await
                .unwrap();
            ready.await.unwrap();
        }
        let a = assignments[0];
        d.runner
            .report(a, Outcome::Completed, "durable signed evidence".into())
            .await
            .unwrap();
        let prefix = d.publisher.domain_history(&d.origin).unwrap();
        d.runner
            .lose_signing_response(
                &prefix,
                Action::Result {
                    assignment: a,
                    outcome: Outcome::Completed,
                    output: "durable signed evidence".into(),
                },
            )
            .await
            .unwrap();
        let exact = d
            .runner
            .saved()
            .await
            .unwrap()
            .into_iter()
            .find(|b| {
                SignedEvent::parse(b)
                    .is_ok_and(|s| matches!(s.event.action,Action::Result{assignment:x,..} if x==a))
            })
            .unwrap();
        assert!(result(&d, a).is_none());
        assert_eq!(history(&mut d).len(), 4); // Signed response isn't publication.
        assert!(
            d.runner
                .sign(
                    &d.origin,
                    &prefix,
                    Action::Result {
                        assignment: assignments[1],
                        outcome: Outcome::Unknown,
                        output: "stale head challenge".into(),
                    }
                )
                .await
                .is_err()
        ); // Cannot sign over the same reserved global sequence.
        assert!(
            d.runner
                .stored_proposal(assignments[1])
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            d.runner
                .sign(
                    &d.origin,
                    &prefix,
                    Action::Result {
                        assignment: a,
                        outcome: Outcome::Completed,
                        output: "durable signed evidence".into(),
                    }
                )
                .await
                .unwrap(),
            exact
        );
        d.shutdown().await.unwrap();
        for release in releases {
            assert!(release.send(()).is_err());
        }
        let mut d = f.reopen().await;
        assert!(d.reconcile().await.is_err());
        f.authorize(&mut d);
        d.reconcile().await.unwrap();
        assert_eq!(result(&d, a).unwrap(), exact);
        assert!(history(&mut d).contains(&exact));
        assert_eq!(
            output(&d.source, ids[0]).as_deref(),
            Some("durable signed evidence")
        );
        for id in &ids {
            unknown(&d.source, *id);
        }
        assert!(matches!(
            SignedEvent::parse(&result(&d, assignments[1]).unwrap())
                .unwrap()
                .event
                .action,
            Action::Result {
                outcome: Outcome::Unknown,
                ..
            }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let before = history(&mut d);
        for a in assignments {
            assert!(
                d.admit(a, |_| async { panic!("reexecuted") })
                    .await
                    .is_err()
            );
        }
        d.shutdown().await.unwrap();
        let mut d = f.reopen().await;
        f.authorize(&mut d);
        d.reconcile().await.unwrap();
        assert_eq!(history(&mut d), before);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runner_proposals_lost_start_signing_response_reserves_head_and_recovers_unknown() {
    bounded(async {
        let (f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        let prefix = d.publisher.domain_history(&d.origin).unwrap();
        d.runner
            .lose_signing_response(&prefix, Action::Start { assignment: a })
            .await
            .unwrap();
        let exact = d.runner.saved().await.unwrap().pop().unwrap();
        assert_eq!(history(&mut d).len(), 1);
        assert_eq!(
            d.target.get_task_run(1).unwrap().unwrap().status,
            "waiting_remote"
        );
        d.shutdown().await.unwrap();
        let mut d = f.reopen().await;
        f.authorize(&mut d);
        d.reconcile().await.unwrap();
        assert!(history(&mut d).contains(&exact));
        assert_eq!(history(&mut d).len(), 3);
        unknown(&d.source, id);
        unknown(&d.target, 1);
        assert!(
            d.admit(a, |_| async { panic!("must not invoke") })
                .await
                .is_err()
        );
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runner_proposals_report_before_coordinator_crash_is_late_evidence_not_execution_retry() {
    bounded(async {
        let (f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let (sent, received) = oneshot::channel();
        d.admit(a, move |w| async move {
            c.fetch_add(1, Ordering::SeqCst);
            w.result
                .finish(Outcome::Completed, "observed before crash")
                .await
                .unwrap();
            sent.send(()).unwrap();
        })
        .await
        .unwrap();
        received.await.unwrap(); // Actual executor finished, coordinator hasn't accepted.
        assert!(result(&d, a).is_none());
        let proposal = d.runner.stored_proposal(a).await.unwrap().unwrap();
        d.shutdown().await.unwrap();
        let mut d = f.reopen().await;
        f.authorize(&mut d);
        d.reconcile().await.unwrap();
        assert_eq!(
            d.runner.stored_proposal(a).await.unwrap().unwrap(),
            proposal
        );
        unknown(&d.source, id);
        unknown(&d.target, 1);
        assert_eq!(
            output(&d.source, id).as_deref(),
            Some("observed before crash")
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
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runner_proposals_maximum_text_and_escaped_envelope_bounds_use_scoped_route() {
    bounded(async {
        let (_f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        let (started, ready) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        d.admit(a, move |w| async move {
            started.send(()).unwrap();
            gate.await.unwrap();
            w.result
                .finish(Outcome::Completed, &"x".repeat(2048))
                .await
                .unwrap();
        })
        .await
        .unwrap();
        ready.await.unwrap();
        assert!(
            d.runner
                .report(a, Outcome::Completed, "\n".repeat(2048))
                .await
                .is_err()
        );
        assert!(d.runner.stored_proposal(a).await.unwrap().is_none());
        release.send(()).unwrap();
        applied(&mut d, a).await;
        assert_eq!(output(&d.source, id).unwrap().len(), 2048);
        assert_eq!(
            d.source.get_task_run(id).unwrap().unwrap().status,
            "completed"
        );
        assert!(
            d.publisher
                .received()
                .unwrap()
                .iter()
                .any(|r| r.text == super::super::super::proposals::PROPOSAL_RECORD)
        );
        let bytes = result(&d, a).unwrap();
        assert!(bytes.len() <= coconutclaw_sync::synthetic::MAX_ARTIFACT);
        d.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn runner_proposals_changed_signed_challenge_refuses_before_store_start() {
    struct Wrong(SecretKey);
    impl MockSigner for Wrong {
        fn public(&self) -> EndpointId {
            self.0.public()
        }
        async fn sign(
            &self,
            origin: &BoundOrigin,
            history: &History,
            _action: Action,
        ) -> Result<Vec<u8>> {
            SignedEvent::sign(
                history.next(origin, self.public(), Action::Start { assignment: 999 })?,
                &self.0,
            )?
            .bytes()
        }
    }
    bounded(async {
        let (f, mut d) = Fixture::new().await;
        d.reconcile().await.unwrap();
        let id = enqueue(&d.source, &d.session, 0);
        let a = d.queue(id).await.unwrap();
        let wrong = Wrong(f.runner.clone());
        let target = TaskAdapter::open(&d.target, &d.origin, &d.session).unwrap();
        assert!(
            target
                .start_mock(&mut d.publisher, a, &wrong)
                .await
                .is_err()
        );
        assert!(
            d.target
                .get_task_run(1)
                .unwrap()
                .unwrap()
                .started_at
                .is_none()
        );
        assert_eq!(history(&mut d).len(), 1);
        d.shutdown().await.unwrap();
    })
    .await;
}
