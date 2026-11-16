//! Public-boundary, synthetic-only two-peer transfer in five fresh processes.
use coconutclaw_sync::synthetic::{
    Record,
    membership::{
        Member, Policy, ScopedPeer, SignedPolicy,
        domain::{Action, BoundOrigin, History, Origin, Outcome, SignedEvent},
    },
};
use iroh::SecretKey;
use iroh_docs::{Capability, NamespaceSecret};
use rusqlite::Connection;
use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const STAGE: &str = "COCONUTCLAW_DOMAIN_STAGE";
const ROOT: &str = "COCONUTCLAW_DOMAIN_ROOT";

fn load<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> T {
    serde_json::from_slice(&fs::read(root.join(name)).unwrap()).unwrap()
}
fn save<T: serde::Serialize>(root: &Path, name: &str, value: &T) {
    fs::write(root.join(name), serde_json::to_vec(value).unwrap()).unwrap();
}
fn check(h: &History, core: &SecretKey, runner: &SecretKey) {
    assert_eq!(h.export().len(), 9);
    assert_eq!(h.memory.len(), 2);
    assert!(h.memory.values().all(|m| !m.active));
    let job = &h.jobs[&7];
    assert_eq!(job.core, core.public());
    assert_eq!(job.runner, runner.public());
    assert!(job.started);
    assert_eq!(job.attempt, 1);
    assert_eq!(
        job.result,
        Some((Outcome::Completed, "inert fixture result".into()))
    );
}

async fn stage(root: &Path, stage: &str) {
    let owner = SecretKey::from([211; 32]);
    let ka = SecretKey::from([212; 32]);
    let kb = SecretKey::from([213; 32]);
    let kc = SecretKey::from([214; 32]);
    let core = SecretKey::from([215; 32]);
    let runner = SecretKey::from([216; 32]);
    let ns = NamespaceSecret::from_bytes(&[217; 32]);
    let namespace = ns.id();
    if stage == "transfer" {
        let origin = BoundOrigin::sign(
            Origin {
                owner: owner.public(),
                store: "ef".repeat(32),
                namespace,
                scope: "synthetic/chat-a".into(),
                core: core.public(),
                runners: BTreeSet::from([runner.public()]),
            },
            &owner,
        )
        .unwrap();
        let mut a = ScopedPeer::create(
            &root.join("a"),
            ka.clone(),
            owner.public(),
            Capability::Write(ns.clone()),
        )
        .await
        .unwrap();
        let mut b = ScopedPeer::create(
            &root.join("b"),
            kb.clone(),
            owner.public(),
            Capability::Read(namespace),
        )
        .await
        .unwrap();
        let author = a.author().await.unwrap();
        let policy = SignedPolicy::sign(
            Policy {
                generation: 1,
                expires_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
                namespace,
                scope: "synthetic/chat-a".into(),
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
        save(root, "origin.json", &origin);
        save(root, "policy.json", &policy);
        a.install_policy(&policy).unwrap();
        b.install_policy(&policy).unwrap();
        let actions = vec![
            Action::Source {
                reference: "synthetic turn, not imported".into(),
            },
            Action::Candidate {
                source: 1,
                content: "x".repeat(2048),
            },
            Action::Promote { record: 2 },
            Action::Correct {
                source: 1,
                record: 2,
                content: "corrected fixture".into(),
            },
            Action::Undo { history: 4 },
            Action::Undo { history: 3 },
            Action::Assign {
                job: 7,
                attempt: 1,
                runner: runner.public(),
                request: "inert fixture request".into(),
            },
            Action::Start { assignment: 7 },
            Action::Result {
                assignment: 7,
                outcome: Outcome::Completed,
                output: "inert fixture result".into(),
            },
        ];
        let mut records = Vec::new();
        for action in actions {
            let key = if matches!(action, Action::Start { .. } | Action::Result { .. }) {
                &runner
            } else {
                &core
            };
            let h = a.domain_history(&origin).unwrap();
            let signed =
                SignedEvent::sign(h.next(&origin, key.public(), action).unwrap(), key).unwrap();
            let record = a
                .publish_domain(&origin, &signed.bytes().unwrap())
                .await
                .unwrap();
            if records.is_empty() {
                records.push(record);
                continue;
            }
            if records.len() == 1 {
                assert!(
                    b.receive_domain(&origin, a.addr(), author, &record.key())
                        .await
                        .is_err()
                );
                assert!(
                    b.domain_history(&origin).is_err(),
                    "no partial domain view with a missing predecessor"
                );
                assert!(
                    b.receive_domain(&origin, a.addr(), author, &records[0].key())
                        .await
                        .unwrap()
                );
                assert!(!b.domain_history(&origin).unwrap().memory[&2].active);
            } else {
                assert!(
                    b.receive_domain(&origin, a.addr(), author, &record.key())
                        .await
                        .unwrap()
                );
            }
            assert!(
                !b.receive_domain(&origin, a.addr(), author, &record.key())
                    .await
                    .unwrap()
            );
            records.push(record);
        }
        let h = b.domain_history(&origin).unwrap();
        check(&h, &core, &runner);
        assert_eq!(h.export(), a.domain_history(&origin).unwrap().export());
        save(root, "events.json", &h.export());
        save(root, "records.json", &records);
        save(root, "stable-id.json", &origin.event_id(7).unwrap());
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
        // Deliberately lose ONLY derived projection, not policy or receipts.
        Connection::open(root.join("b/projection.sqlite"))
            .unwrap()
            .execute("DROP TABLE received", [])
            .unwrap();
        return;
    }
    let origin: BoundOrigin = load(root, "origin.json");
    let mut policy: SignedPolicy = load(root, "policy.json");
    let events: Vec<Vec<u8>> = load(root, "events.json");
    let records: Vec<Record> = load(root, "records.json");
    let mut b = ScopedPeer::reopen(
        &root.join("b"),
        kb,
        owner.public(),
        Capability::Read(namespace),
    )
    .await
    .unwrap();
    assert!(
        b.domain_history(&origin).is_err(),
        "restart must quarantine domain repair"
    );
    b.install_policy(&policy).unwrap();
    if stage != "refuse" {
        let h = b.domain_history(&origin).unwrap();
        check(&h, &core, &runner);
        assert_eq!(h.export(), events);
    }
    assert_eq!(
        origin.event_id(7).unwrap(),
        load::<String>(root, "stable-id.json")
    );
    if stage == "restart" {
        let mut a = ScopedPeer::reopen(&root.join("a"), ka, owner.public(), Capability::Write(ns))
            .await
            .unwrap();
        a.install_policy(&policy).unwrap();
        a.flush().await.unwrap();
        for record in records {
            assert!(
                !b.receive_domain(
                    &origin,
                    a.addr(),
                    record.origin.parse().unwrap(),
                    &record.key()
                )
                .await
                .unwrap()
            );
        }
        check(&b.domain_history(&origin).unwrap(), &core, &runner);
        a.shutdown().await.unwrap();
    } else if stage == "rotate" {
        // New endpoint AND docs author; exact signed application bytes and IDs
        // remain unchanged. Both transport origins remain authorized for replay.
        let mut c = ScopedPeer::create(
            &root.join("c"),
            kc.clone(),
            owner.public(),
            Capability::Write(ns),
        )
        .await
        .unwrap();
        let author = c.author().await.unwrap();
        assert_ne!(author.to_string(), records[0].origin);
        policy.policy.generation = 2;
        policy.policy.members.push(Member {
            endpoint: kc.public(),
            authors: BTreeSet::from([author]),
        });
        policy = SignedPolicy::sign(policy.policy, &owner).unwrap();
        b.install_policy(&policy).unwrap();
        c.install_policy(&policy).unwrap();
        let mut rotated = Vec::new();
        for bytes in &events {
            let record = c.publish_domain(&origin, bytes).await.unwrap();
            assert!(
                b.receive_domain(&origin, c.addr(), author, &record.key())
                    .await
                    .unwrap()
            );
            rotated.push(record);
        }
        assert_eq!(b.received().unwrap().len(), 18);
        assert_eq!(b.domain_history(&origin).unwrap().export(), events);
        check(&c.domain_history(&origin).unwrap(), &core, &runner);
        save(root, "policy.json", &policy);
        save(root, "rotated.json", &rotated);
        c.shutdown().await.unwrap();
    } else if stage == "reject" {
        assert_eq!(stage, "reject");
        let mut c = ScopedPeer::reopen(&root.join("c"), kc, owner.public(), Capability::Write(ns))
            .await
            .unwrap();
        c.install_policy(&policy).unwrap();
        let author = c.author().await.unwrap();
        let mut wrong_owner = origin.clone();
        wrong_owner.origin.owner = core.public();
        let rotated: Vec<Record> = load(root, "rotated.json");
        let before = b.received().unwrap().len();
        assert!(
            b.receive_domain(&wrong_owner, c.addr(), author, &rotated[0].key())
                .await
                .is_err()
        );
        assert_eq!(b.received().unwrap().len(), before);
        let h = c.domain_history(&origin).unwrap();
        let peer_promote = SignedEvent::sign(
            h.next(&origin, runner.public(), Action::Promote { record: 2 })
                .unwrap(),
            &runner,
        )
        .unwrap();
        assert!(
            c.publish_domain(&origin, &peer_promote.bytes().unwrap())
                .await
                .is_err()
        );
        assert_eq!(c.domain_history(&origin).unwrap().export(), events);
        // Hostile peer uses generic inert record API, bypassing friendly export
        // validation. Domain validation must fail even though transport accepts.
        let mut fork = SignedEvent::parse(&events[2]).unwrap().event;
        fork.action = Action::Promote { record: 999 };
        let fork = SignedEvent::sign(fork, &core).unwrap().bytes().unwrap();
        let record = c
            .publish("CoconutClaw synthetic domain event v1", &fork)
            .await
            .unwrap();
        assert!(
            b.receive_domain(&origin, c.addr(), author, &record.key())
                .await
                .is_err()
        );
        assert!(
            b.domain_history(&origin).is_err(),
            "conflict cannot silently select a winner"
        );
        assert_eq!(
            b.received().unwrap().len(),
            before + 1,
            "rejected domain evidence stays inert"
        );
        // Current revocation fences repair even of old, valid receipts.
        policy.policy.generation = 3;
        policy.policy.members.retain(|m| m.endpoint != ka.public());
        policy = SignedPolicy::sign(policy.policy, &owner).unwrap();
        b.install_policy(&policy).unwrap();
        save(root, "policy.json", &policy);
        assert!(b.repair_projection().is_err());
        assert!(b.domain_history(&origin).is_err());
        c.shutdown().await.unwrap();
    } else {
        assert_eq!(stage, "refuse");
        // Both unavailable states survive a fresh process: revoked receipts
        // on the receiver and conflicting local publication on the new sender.
        assert!(b.domain_history(&origin).is_err());
        let mut c = ScopedPeer::reopen(&root.join("c"), kc, owner.public(), Capability::Write(ns))
            .await
            .unwrap();
        c.install_policy(&policy).unwrap();
        assert!(c.domain_history(&origin).is_err());
        c.shutdown().await.unwrap();
    }
    b.shutdown().await.unwrap();
}

#[test]
fn scoped_domain_transfer_restart_rotation_and_rejection() {
    if let Ok(value) = std::env::var(STAGE) {
        let root = std::env::var_os(ROOT).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            tokio::time::timeout(Duration::from_secs(60), stage(Path::new(&root), &value))
                .await
                .unwrap();
        });
        return;
    }
    let root = tempfile::tempdir().unwrap();
    for stage in ["transfer", "restart", "rotate", "reject", "refuse"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "scoped_domain_transfer_restart_rotation_and_rejection",
                "--nocapture",
            ])
            .env(STAGE, stage)
            .env(ROOT, root.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{stage}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
