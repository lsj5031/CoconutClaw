use super::*;
use iroh_docs::NamespaceSecret;

#[test]
fn owner_fence_preserves_origin_and_history_but_never_activates_replacement() {
    let (owner, core, runner, origin) = fixture();
    let replacement = SecretKey::from([206; 32]);
    let fence = Action::Fence {
        previous_epoch: 1,
        epoch: 2,
        core: replacement.public(),
        runners: BTreeSet::from([runner.public()]),
    };
    let h = append(
        &History::default(),
        &origin,
        &core,
        Action::Assign {
            job: 1,
            attempt: 1,
            runner: runner.public(),
            request: "inert".into(),
        },
    )
    .unwrap();
    assert!(append(&h, &origin, &core, fence.clone()).is_err());
    assert!(append(&h, &origin, &runner, fence.clone()).is_err());
    for (previous_epoch, epoch) in [(0, 2), (1, 1), (2, 3), (1, 3), (1, 0)] {
        assert!(
            append(
                &h,
                &origin,
                &owner,
                Action::Fence {
                    previous_epoch,
                    epoch,
                    core: replacement.public(),
                    runners: BTreeSet::new(),
                }
            )
            .is_err()
        );
    }
    let fenced = append(&h, &origin, &owner, fence.clone()).unwrap();
    assert_eq!(fenced.pending_fence().unwrap().epoch, 2);
    assert_eq!(fenced.jobs[&1].core, origin.origin.core);
    assert!(!fenced.jobs[&1].started);
    assert!(
        fenced
            .validate_replacement_activation()
            .unwrap_err()
            .to_string()
            .contains("unresolved")
    );
    let running = append(&h, &origin, &runner, Action::Start { assignment: 1 }).unwrap();
    let unknown = append(
        &running,
        &origin,
        &runner,
        Action::Result {
            assignment: 1,
            outcome: Outcome::Unknown,
            output: "effect uncertain".into(),
        },
    )
    .unwrap();
    for history in [&running, &unknown] {
        let suspended = append(history, &origin, &owner, fence.clone()).unwrap();
        assert!(
            suspended
                .validate_replacement_activation()
                .unwrap_err()
                .to_string()
                .contains("unresolved")
        );
        assert_eq!(suspended.jobs[&1], history.jobs[&1]);
    }
    for (key, action) in [
        (&core, Action::Promote { record: 1 }),
        (&core, Action::Undo { history: 1 }),
        (
            &core,
            Action::Source {
                reference: "stale".into(),
            },
        ),
        (
            &replacement,
            Action::Source {
                reference: "new but unproven".into(),
            },
        ),
        (&runner, Action::Start { assignment: 1 }),
        (
            &runner,
            Action::Result {
                assignment: 1,
                outcome: Outcome::Completed,
                output: "late".into(),
            },
        ),
        (
            &core,
            Action::Assign {
                job: 3,
                attempt: 2,
                runner: runner.public(),
                request: "retry".into(),
            },
        ),
        (&owner, fence.clone()),
    ] {
        assert!(append(&fenced, &origin, key, action).is_err());
    }
    let bytes = fenced.export();
    let hash = Hash::new(&bytes[1]).to_string();
    fenced.validate_fence_checkpoint(2, &hash).unwrap();
    assert!(h.validate_fence_checkpoint(2, &hash).is_err());
    assert!(fenced.validate_fence_checkpoint(1, &hash).is_err());
    assert!(
        fenced
            .validate_fence_checkpoint(2, &"00".repeat(32))
            .is_err()
    );
    assert!(replay(&origin, vec![bytes[1].clone()]).is_err());
    let mut duplicate = bytes.clone();
    duplicate.extend(bytes.clone());
    assert_eq!(replay(&origin, duplicate).unwrap().export(), bytes);
    let conflicting = SignedEvent::sign(
        h.next(
            &origin,
            owner.public(),
            Action::Fence {
                previous_epoch: 1,
                epoch: 2,
                core: SecretKey::from([207; 32]).public(),
                runners: BTreeSet::new(),
            },
        )
        .unwrap(),
        &owner,
    )
    .unwrap()
    .bytes()
    .unwrap();
    let mut conflict = bytes;
    conflict.push(conflicting);
    assert!(replay(&origin, conflict).is_err());
    // Even no outstanding attempts and an owner signature cannot prove that
    // the old core no longer holds external credentials.
    let clean = append(&History::default(), &origin, &owner, fence).unwrap();
    assert!(
        clean
            .validate_replacement_activation()
            .unwrap_err()
            .to_string()
            .contains("credential exclusion")
    );
    let mut forged: serde_json::Value = serde_json::from_slice(&clean.export()[0]).unwrap();
    forged["event"]["action"]["external_credentials_excluded"] = true.into();
    assert!(SignedEvent::parse(&serde_json::to_vec(&forged).unwrap()).is_err());
}

fn fixture() -> (SecretKey, SecretKey, SecretKey, BoundOrigin) {
    let owner = SecretKey::from([201; 32]);
    let core = SecretKey::from([202; 32]);
    let runner = SecretKey::from([203; 32]);
    let origin = BoundOrigin::sign(
        Origin {
            owner: owner.public(),
            store: "ab".repeat(32),
            namespace: NamespaceSecret::from_bytes(&[204; 32]).id(),
            scope: SCOPE.into(),
            core: core.public(),
            runners: BTreeSet::from([runner.public(), SecretKey::from([205; 32]).public()]),
        },
        &owner,
    )
    .unwrap();
    (owner, core, runner, origin)
}

fn replay(origin: &BoundOrigin, bytes: Vec<Vec<u8>>) -> Result<History> {
    History::replay(origin, origin.origin.owner, origin.origin.namespace, bytes)
}

fn append(h: &History, origin: &BoundOrigin, key: &SecretKey, action: Action) -> Result<History> {
    let mut bytes = h.export();
    bytes.push(SignedEvent::sign(h.next(origin, key.public(), action)?, key)?.bytes()?);
    replay(origin, bytes)
}

#[test]
fn runtime_memory_correction_latest_undo_and_no_peer_promotion_or_replay_resurrection() {
    let (_, core, runner, origin) = fixture();
    let h = append(
        &History::default(),
        &origin,
        &core,
        Action::Source {
            reference: "fixture turn 1".into(),
        },
    )
    .unwrap();
    let h = append(
        &h,
        &origin,
        &core,
        Action::Candidate {
            source: 1,
            content: "orchard".into(),
        },
    )
    .unwrap();
    assert!(!h.memory[&2].active);
    assert!(append(&h, &origin, &runner, Action::Promote { record: 2 }).is_err());
    assert!(
        append(
            &h,
            &origin,
            &core,
            Action::Correct {
                source: 1,
                record: 2,
                content: "inactive".into()
            }
        )
        .is_err()
    );
    let h = append(&h, &origin, &core, Action::Promote { record: 2 }).unwrap();
    let promoted = h.export()[2].clone();
    let h = append(
        &h,
        &origin,
        &core,
        Action::Correct {
            source: 1,
            record: 2,
            content: "corrected orchard".into(),
        },
    )
    .unwrap();
    assert!(!h.memory[&2].active && h.memory[&4].active);
    assert!(append(&h, &origin, &core, Action::Undo { history: 3 }).is_err());
    let h = append(&h, &origin, &core, Action::Undo { history: 4 }).unwrap();
    assert!(h.memory[&2].active && !h.memory[&4].active);
    let h = append(&h, &origin, &core, Action::Undo { history: 3 }).unwrap();
    assert!(h.memory.values().all(|m| !m.active));
    let mut duplicates = h.export();
    duplicates.push(promoted);
    let h = replay(&origin, duplicates).unwrap();
    assert_eq!(h.export().len(), 6);
    assert!(h.memory.values().all(|m| !m.active));
    assert!(append(&h, &origin, &core, Action::Promote { record: 2 }).is_err());
    assert!(append(&h, &origin, &core, Action::Promote { record: 4 }).is_err());
    assert!(append(&h, &origin, &core, Action::Undo { history: 3 }).is_err());
}

#[test]
fn exact_assignment_runner_start_terminal_and_uncertain_attempt_refusal() {
    let (_, core, runner, origin) = fixture();
    let empty = History::default();
    let assign = Action::Assign {
        job: 1,
        attempt: 1,
        runner: runner.public(),
        request: "synthetic job".into(),
    };
    assert!(append(&empty, &origin, &runner, assign.clone()).is_err());
    assert!(append(&empty, &origin, &core, Action::Start { assignment: 1 }).is_err());
    let h = append(&empty, &origin, &core, assign).unwrap();
    let result = Action::Result {
        assignment: 1,
        outcome: Outcome::Unknown,
        output: "inspect locally; no automatic retry".into(),
    };
    assert!(append(&h, &origin, &runner, result.clone()).is_err());
    let wrong = SecretKey::from([205; 32]);
    assert!(append(&h, &origin, &wrong, Action::Start { assignment: 1 }).is_err());
    let h = append(&h, &origin, &runner, Action::Start { assignment: 1 }).unwrap();
    assert!(append(&h, &origin, &runner, Action::Start { assignment: 1 }).is_err());
    assert!(append(&h, &origin, &wrong, result.clone()).is_err());
    assert!(append(&h, &origin, &core, result.clone()).is_err());
    let h = append(&h, &origin, &runner, result.clone()).unwrap();
    assert_eq!(h.jobs[&1].core, core.public());
    assert_eq!(h.jobs[&1].runner, runner.public());
    assert_eq!(h.jobs[&1].result.as_ref().unwrap().0, Outcome::Unknown);
    assert!(append(&h, &origin, &runner, result).is_err());
    assert!(
        append(
            &h,
            &origin,
            &core,
            Action::Assign {
                job: 4,
                attempt: 2,
                runner: runner.public(),
                request: "retry forbidden".into(),
            }
        )
        .is_err()
    );
    let bytes = h.export();
    let mut duplicate = bytes.clone();
    duplicate.extend(bytes);
    assert_eq!(replay(&origin, duplicate).unwrap().export().len(), 3);
}

#[test]
fn canonical_owner_scope_predecessor_fork_signature_and_bounds() {
    let (owner, core, _, origin) = fixture();
    let h = append(
        &History::default(),
        &origin,
        &core,
        Action::Source {
            reference: "fixture".into(),
        },
    )
    .unwrap();
    let h = append(
        &h,
        &origin,
        &core,
        Action::Candidate {
            source: 1,
            content: "x".repeat(2048),
        },
    )
    .unwrap();
    assert_eq!(h.memory[&2].content.len(), 2048);
    let bytes = h.export();
    assert!(replay(&origin, vec![bytes[1].clone()]).is_err());
    let mut fork = SignedEvent::parse(&bytes[0]).unwrap().event;
    fork.action = Action::Source {
        reference: "fork".into(),
    };
    let mut conflict = bytes.clone();
    conflict.push(SignedEvent::sign(fork, &core).unwrap().bytes().unwrap());
    assert!(replay(&origin, conflict).is_err());
    let mut broken = SignedEvent::parse(&bytes[1]).unwrap().event;
    broken.previous = None;
    assert!(
        replay(
            &origin,
            vec![
                bytes[0].clone(),
                SignedEvent::sign(broken, &core).unwrap().bytes().unwrap()
            ]
        )
        .is_err()
    );
    let mut noncanonical = bytes[0].clone();
    noncanonical.push(b'\n');
    assert!(SignedEvent::parse(&noncanonical).is_err());
    let mut tamper = SignedEvent::parse(&bytes[0]).unwrap();
    tamper.event.version = 2;
    assert!(SignedEvent::parse(&tamper.bytes().unwrap()).is_err());
    let mut unknown: serde_json::Value = serde_json::from_slice(&bytes[0]).unwrap();
    unknown["event"]["action"]["extra"] = true.into();
    assert!(SignedEvent::parse(&serde_json::to_vec(&unknown).unwrap()).is_err());
    assert!(SignedEvent::parse(&vec![b'x'; MAX_ARTIFACT + 1]).is_err());
    assert!(
        origin
            .verify(core.public(), origin.origin.namespace)
            .is_err()
    );
    assert!(
        origin
            .verify(owner.public(), NamespaceSecret::from_bytes(&[206; 32]).id())
            .is_err()
    );
    for field in ["scope", "store", "core"] {
        let mut other = origin.origin.clone();
        match field {
            "scope" => other.scope = "synthetic/chat-b".into(),
            "store" => other.store = "cd".repeat(32),
            _ => other.core = owner.public(),
        }
        if let Ok(other) = BoundOrigin::sign(other, &owner) {
            assert!(replay(&other, bytes.clone()).is_err());
        }
    }
    assert!(
        append(
            &h,
            &origin,
            &core,
            Action::Candidate {
                source: 99,
                content: "no source".into()
            }
        )
        .is_err()
    );
    assert!(
        append(
            &h,
            &origin,
            &core,
            Action::Candidate {
                source: 1,
                content: "x".repeat(2049)
            }
        )
        .is_err()
    );
    assert!(
        append(
            &h,
            &origin,
            &core,
            Action::Candidate {
                source: 1,
                content: "\0".into()
            }
        )
        .is_err()
    );
    let h = append(
        &h,
        &origin,
        &core,
        Action::Remember {
            source: 1,
            content: "r".repeat(512),
        },
    )
    .unwrap();
    assert!(h.memory[&3].curated && h.memory[&3].active);
    assert!(
        append(
            &h,
            &origin,
            &core,
            Action::Correct {
                source: 1,
                record: 3,
                content: "c".repeat(513)
            }
        )
        .is_err()
    );
    let h = append(
        &h,
        &origin,
        &core,
        Action::Correct {
            source: 1,
            record: 3,
            content: "c".repeat(512),
        },
    )
    .unwrap();
    assert!(h.memory[&4].curated);
}

#[test]
fn exact_canonical_envelope_limit_never_truncates_valid_runtime_content() {
    let (_, core, _, origin) = fixture();
    let h = append(
        &History::default(),
        &origin,
        &core,
        Action::Source {
            reference: "fixture".into(),
        },
    )
    .unwrap();
    // Escaping expands otherwise valid 2048-byte runtime text. Find the exact
    // existing artifact boundary, not an average-sized fixture.
    let envelope = |quotes: usize| {
        let text = "\"".repeat(quotes) + &"x".repeat(2048 - quotes);
        SignedEvent::sign(
            h.next(
                &origin,
                core.public(),
                Action::Candidate {
                    source: 1,
                    content: text,
                },
            )
            .unwrap(),
            &core,
        )
        .and_then(|s| s.bytes())
    };
    let (mut low, mut high) = (0, 2048);
    while low + 1 < high {
        let mid = (low + high) / 2;
        if envelope(mid).is_ok() {
            low = mid;
        } else {
            high = mid;
        }
    }
    let bytes = envelope(low).unwrap();
    assert_eq!(bytes.len(), MAX_ARTIFACT);
    assert!(envelope(high).is_err());
    let mut events = h.export();
    events.push(bytes);
    assert_eq!(
        replay(&origin, events).unwrap().memory[&2].content.len(),
        2048
    );
}

#[tokio::test]
async fn scoped_domain_tombstone_refuses_disclosure_and_new_publication() {
    let (owner, core, _, mut origin) = fixture();
    let root = tempfile::tempdir().unwrap();
    let ka = SecretKey::from([207; 32]);
    let kb = SecretKey::from([208; 32]);
    let namespace = NamespaceSecret::from_bytes(&[209; 32]);
    origin.origin.namespace = namespace.id();
    origin = BoundOrigin::sign(origin.origin, &owner).unwrap();
    let mut a = ScopedPeer::create(
        &root.path().join("a"),
        ka.clone(),
        owner.public(),
        Capability::Write(namespace.clone()),
    )
    .await
    .unwrap();
    let mut b = ScopedPeer::create(
        &root.path().join("b"),
        kb.clone(),
        owner.public(),
        Capability::Read(namespace.id()),
    )
    .await
    .unwrap();
    let author = a.author().await.unwrap();
    let policy = SignedPolicy::sign(
        Policy {
            generation: 1,
            expires_at: now().unwrap() + 3600,
            namespace: namespace.id(),
            scope: SCOPE.into(),
            members: vec![
                Member {
                    endpoint: ka.public(),
                    authors: BTreeSet::from([author]),
                },
                Member {
                    endpoint: kb.public(),
                    authors: BTreeSet::new(),
                },
            ],
        },
        &owner,
    )
    .unwrap();
    a.install_policy(&policy).unwrap();
    b.install_policy(&policy).unwrap();
    let e = SignedEvent::sign(
        History::default()
            .next(
                &origin,
                core.public(),
                Action::Source {
                    reference: "fixture".into(),
                },
            )
            .unwrap(),
        &core,
    )
    .unwrap();
    let record = a
        .publish_domain(&origin, &e.bytes().unwrap())
        .await
        .unwrap();
    let source_prefix = format!("v1/synthetic/{author}/");
    a.doc.del(author, source_prefix.clone()).await.unwrap();
    a.peer.docs_sync.flush_store().await.unwrap();
    assert!(
        b.receive_domain(&origin, a.addr(), author, &record.key())
            .await
            .is_err()
    );
    assert!(b.received().unwrap().is_empty());
    assert!(
        a.publish_domain(&origin, &e.bytes().unwrap())
            .await
            .is_err()
    );
    assert_eq!(
        a.peer
            .sql
            .query_row("SELECT count(*) FROM publication", [], |r| r
                .get::<_, usize>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        a.doc
            .get_exact(author, source_prefix, true)
            .await
            .unwrap()
            .unwrap()
            .content_len(),
        0
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
