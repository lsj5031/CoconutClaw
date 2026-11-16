//! Public scoped boundary, synthetic identities only, three fresh processes.
use coconutclaw_sync::synthetic::membership::{
    Member, Policy, ScopedPeer, SignedPolicy,
    domain::{Action, BoundOrigin, History, Origin, Outcome, SignedEvent},
};
use iroh::SecretKey;
use iroh_docs::{Capability, NamespaceSecret};
use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn load<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> T {
    serde_json::from_slice(&fs::read(root.join(name)).unwrap()).unwrap()
}
fn save<T: serde::Serialize>(root: &Path, name: &str, value: &T) {
    fs::write(root.join(name), serde_json::to_vec(value).unwrap()).unwrap();
}
async fn stage(root: &Path, stage: &str) {
    let owner = SecretKey::from([181; 32]);
    let core = SecretKey::from([182; 32]);
    let runner = SecretKey::from([183; 32]);
    let replacement = SecretKey::from([184; 32]);
    let akey = SecretKey::from([185; 32]);
    let bkey = SecretKey::from([186; 32]);
    let ns = NamespaceSecret::from_bytes(&[187; 32]);
    let origin = BoundOrigin::sign(
        Origin {
            owner: owner.public(),
            store: "ac".repeat(32),
            namespace: ns.id(),
            scope: "synthetic/chat-a".into(),
            core: core.public(),
            runners: BTreeSet::from([runner.public()]),
        },
        &owner,
    )
    .unwrap();
    let fresh = stage == "transfer";
    let mut a = if fresh {
        ScopedPeer::create(
            &root.join("a"),
            akey,
            owner.public(),
            Capability::Write(ns.clone()),
        )
        .await
    } else {
        ScopedPeer::reopen(
            &root.join("a"),
            akey,
            owner.public(),
            Capability::Write(ns.clone()),
        )
        .await
    }
    .unwrap();
    let mut b = if fresh {
        ScopedPeer::create(
            &root.join("b"),
            bkey,
            owner.public(),
            Capability::Read(ns.id()),
        )
        .await
    } else {
        ScopedPeer::reopen(
            &root.join("b"),
            bkey,
            owner.public(),
            Capability::Read(ns.id()),
        )
        .await
    }
    .unwrap();
    let author = a.author().await.unwrap();
    if !fresh {
        assert!(a.domain_history(&origin).is_err());
        assert!(b.domain_history(&origin).is_err());
    }
    let policy = if fresh {
        let p = SignedPolicy::sign(
            Policy {
                generation: 1,
                expires_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
                namespace: ns.id(),
                scope: "synthetic/chat-a".into(),
                members: vec![
                    Member {
                        endpoint: SecretKey::from([185; 32]).public(),
                        authors: BTreeSet::from([author]),
                    },
                    Member {
                        endpoint: SecretKey::from([186; 32]).public(),
                        authors: BTreeSet::new(),
                    },
                ],
            },
            &owner,
        )
        .unwrap();
        save(root, "policy.json", &p);
        p
    } else {
        load(root, "policy.json")
    };
    a.install_policy(&policy).unwrap();
    b.install_policy(&policy).unwrap();
    if fresh {
        let mut records = Vec::new();
        for (key, action) in [
            (
                &core,
                Action::Assign {
                    job: 1,
                    attempt: 1,
                    runner: runner.public(),
                    request: "never execute".into(),
                },
            ),
            (&runner, Action::Start { assignment: 1 }),
            (
                &runner,
                Action::Result {
                    assignment: 1,
                    outcome: Outcome::Completed,
                    output: "historical fixture".into(),
                },
            ),
            (
                &owner,
                Action::Fence {
                    previous_epoch: 1,
                    epoch: 2,
                    core: replacement.public(),
                    runners: BTreeSet::from([runner.public()]),
                },
            ),
        ] {
            let h = a.domain_history(&origin).unwrap();
            let bytes = SignedEvent::sign(h.next(&origin, key.public(), action).unwrap(), key)
                .unwrap()
                .bytes()
                .unwrap();
            records.push(a.publish_domain(&origin, &bytes).await.unwrap());
        }
        let events = a.domain_history(&origin).unwrap().export();
        let checkpoint = (4u64, iroh_blobs::Hash::new(&events[3]).to_string());
        save(root, "checkpoint.json", &checkpoint);
        // A fence arriving without its predecessor must not yield partial state.
        assert!(
            b.receive_domain(&origin, a.addr(), author, &records[3].key())
                .await
                .is_err()
        );
        assert!(b.domain_history(&origin).is_err());
        for record in &records[..3] {
            let result = b
                .receive_domain(&origin, a.addr(), author, &record.key())
                .await;
            if record.id == records[2].id {
                assert!(result.unwrap());
            } else {
                assert!(result.is_err());
            }
        }
        save(root, "events.json", &events);
        save(root, "records.json", &records);
    }
    let checkpoint: (u64, String) = load(root, "checkpoint.json");
    if stage == "refuse" {
        assert!(a.domain_history(&origin).is_err());
        assert!(b.domain_history(&origin).is_err());
    } else {
        let h = b.domain_history(&origin).unwrap();
        h.validate_fence_checkpoint(checkpoint.0, &checkpoint.1)
            .unwrap();
        assert_eq!(h.jobs[&1].core, core.public());
        assert_eq!(h.pending_fence().unwrap().core, replacement.public());
        assert!(
            h.validate_replacement_activation()
                .unwrap_err()
                .to_string()
                .contains("credential exclusion")
        );
        let events: Vec<Vec<u8>> = load(root, "events.json");
        assert_eq!(h.export(), events);
        let prefix =
            History::replay(&origin, owner.public(), ns.id(), events[..3].to_vec()).unwrap();
        assert!(
            prefix
                .validate_fence_checkpoint(checkpoint.0, &checkpoint.1)
                .is_err()
        );
        if stage == "restart" {
            let records: Vec<coconutclaw_sync::synthetic::Record> = load(root, "records.json");
            for record in records {
                assert!(
                    !b.receive_domain(&origin, a.addr(), author, &record.key())
                        .await
                        .unwrap()
                );
            }
            for (key, action) in [
                (
                    &core,
                    Action::Assign {
                        job: 5,
                        attempt: 1,
                        runner: runner.public(),
                        request: "stale".into(),
                    },
                ),
                (
                    &runner,
                    Action::Result {
                        assignment: 1,
                        outcome: Outcome::Completed,
                        output: "late".into(),
                    },
                ),
                (
                    &replacement,
                    Action::Source {
                        reference: "no activation proof".into(),
                    },
                ),
                (
                    &owner,
                    Action::Fence {
                        previous_epoch: 2,
                        epoch: 3,
                        core: core.public(),
                        runners: BTreeSet::new(),
                    },
                ),
            ] {
                let bytes = SignedEvent::sign(h.next(&origin, key.public(), action).unwrap(), key)
                    .unwrap()
                    .bytes()
                    .unwrap();
                assert!(a.publish_domain(&origin, &bytes).await.is_err());
            }
            // Bypass friendly validation: a hostile old core's scoped record
            // remains inert evidence and poisons the whole derived domain view.
            let bytes = SignedEvent::sign(
                h.next(
                    &origin,
                    core.public(),
                    Action::Source {
                        reference: "stale actor".into(),
                    },
                )
                .unwrap(),
                &core,
            )
            .unwrap()
            .bytes()
            .unwrap();
            let record = a
                .publish("CoconutClaw synthetic domain event v1", &bytes)
                .await
                .unwrap();
            assert!(
                b.receive_domain(&origin, a.addr(), author, &record.key())
                    .await
                    .is_err()
            );
            assert!(b.domain_history(&origin).is_err());
        }
    }
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[test]
fn epoch_fence_scoped_transfer_and_external_restart() {
    const STAGE: &str = "COCONUTCLAW_EPOCH_STAGE";
    const ROOT: &str = "COCONUTCLAW_EPOCH_ROOT";
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
    for stage in ["transfer", "restart", "refuse"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "epoch_fence_scoped_transfer_and_external_restart",
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
