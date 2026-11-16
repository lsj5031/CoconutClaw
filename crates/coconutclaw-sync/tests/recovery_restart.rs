use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use coconutclaw_sync::synthetic::{
    Record,
    membership::{Member, Policy, ScopedPeer, SignedPolicy, recovery::RecoveryManifest},
};
use iroh::SecretKey;
use iroh_docs::{Capability, NamespaceSecret};

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn save<T: serde::Serialize>(root: &Path, name: &str, value: &T) {
    fs::write(root.join(name), serde_json::to_vec(value).unwrap()).unwrap();
}

fn load<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> T {
    serde_json::from_slice(&fs::read(root.join(name)).unwrap()).unwrap()
}

async fn stage(root: &Path, stage: &str) {
    // Public deterministic synthetic fixtures only, no real owner credentials.
    let owner = SecretKey::from([101; 32]);
    let ka = SecretKey::from([102; 32]);
    let kb = SecretKey::from([103; 32]);
    let kc = SecretKey::from([104; 32]);
    let ns = NamespaceSecret::from_bytes(&[105; 32]);
    let namespace = ns.id();
    if stage == "create" {
        let mut a = ScopedPeer::create(
            &root.join("a"),
            ka.clone(),
            owner.public(),
            Capability::Write(ns.clone()),
        )
        .await
        .unwrap();
        let b = ScopedPeer::create(
            &root.join("b"),
            kb.clone(),
            owner.public(),
            Capability::Read(namespace),
        )
        .await
        .unwrap();
        let c = ScopedPeer::create(
            &root.join("c"),
            kc.clone(),
            owner.public(),
            Capability::Read(namespace),
        )
        .await
        .unwrap();
        let author = a.author().await.unwrap();
        let mut policy = Policy {
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
                Member {
                    endpoint: kc.public(),
                    authors: BTreeSet::new(),
                },
            ],
        };
        let old = SignedPolicy::sign(policy.clone(), &owner).unwrap();
        a.install_policy(&old).unwrap();
        let record = a
            .publish("fresh-process recovery", b"artifact")
            .await
            .unwrap();
        save(root, "record.json", &record);
        save(root, "old-policy.json", &old);
        a.shutdown().await.unwrap();
        let old_manifest = RecoveryManifest::capture(&root.join("a")).unwrap();
        save(root, "old-manifest.json", &old_manifest);
        copy_tree(&root.join("a"), &root.join("old-backup"));
        let mut a = ScopedPeer::reopen(&root.join("a"), ka, owner.public(), Capability::Write(ns))
            .await
            .unwrap();
        policy.generation = 2;
        policy.members.retain(|m| m.endpoint != kb.public());
        let current = SignedPolicy::sign(policy, &owner).unwrap();
        a.install_policy(&current).unwrap();
        save(root, "current-policy.json", &current);
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
        c.shutdown().await.unwrap();
        let manifest = RecoveryManifest::capture(&root.join("a")).unwrap();
        save(root, "current-manifest.json", &manifest);
        // Emulates separately retained trusted input, NOT a file inside backup a.
        save(root, "independent-latest.json", &manifest.hash().unwrap());
        return;
    }
    let old: SignedPolicy = load(root, "old-policy.json");
    let current: SignedPolicy = load(root, "current-policy.json");
    let manifest: RecoveryManifest = load(root, "current-manifest.json");
    let latest = load(root, "independent-latest.json");
    let record: Record = load(root, "record.json");
    let author = record.origin.parse().unwrap();
    if stage == "restore" {
        let stale: RecoveryManifest = load(root, "old-manifest.json");
        assert!(
            ScopedPeer::restore(
                &root.join("old-backup"),
                ka.clone(),
                owner.public(),
                Capability::Read(namespace),
                Some((&stale, latest)),
            )
            .await
            .is_err()
        );
        assert!(
            ScopedPeer::restore(
                &root.join("old-backup"),
                ka.clone(),
                owner.public(),
                Capability::Read(namespace),
                Some((&manifest, latest)),
            )
            .await
            .is_err()
        );
    }
    // An EndpointAddr identifies a peer, not a particular copy of its store.
    // Probe the rollback holder and then stop BOTH transport endpoints before
    // opening the current holder/client: remembered paths are keyed by identity.
    let mut rolled = ScopedPeer::reopen(
        &root.join("old-backup"),
        ka.clone(),
        owner.public(),
        Capability::Read(namespace),
    )
    .await
    .unwrap();
    assert!(rolled.install_policy(&old).is_err());
    assert!(
        rolled
            .install_recovery_policy(&old, &manifest, latest)
            .is_err()
    );
    let mut rollback_client = ScopedPeer::reopen(
        &root.join("c"),
        kc.clone(),
        owner.public(),
        Capability::Read(namespace),
    )
    .await
    .unwrap();
    rollback_client.install_policy(&current).unwrap();
    let disclosed = rollback_client.received().unwrap();
    assert!(
        rollback_client
            .receive(rolled.addr(), author, &record.key())
            .await
            .is_err()
    );
    assert_eq!(rollback_client.received().unwrap(), disclosed);
    rollback_client.shutdown().await.unwrap();
    rolled.shutdown().await.unwrap();

    let mut a = if stage == "restore" {
        ScopedPeer::restore(
            &root.join("a"),
            ka.clone(),
            owner.public(),
            Capability::Read(namespace),
            Some((&manifest, latest)),
        )
        .await
        .unwrap()
    } else {
        ScopedPeer::reopen(
            &root.join("a"),
            ka.clone(),
            owner.public(),
            Capability::Read(namespace),
        )
        .await
        .unwrap()
    };
    let mut b = ScopedPeer::reopen(
        &root.join("b"),
        kb,
        owner.public(),
        Capability::Read(namespace),
    )
    .await
    .unwrap();
    b.install_policy(&old).unwrap(); // Revoked endpoint deliberately keeps g1.
    let mut c = ScopedPeer::reopen(
        &root.join("c"),
        kc,
        owner.public(),
        Capability::Read(namespace),
    )
    .await
    .unwrap();
    c.install_policy(&current).unwrap();
    assert!(a.install_policy(&current).is_err());
    assert!(a.install_recovery_policy(&old, &manifest, latest).is_err());
    assert!(a.publish("quarantined", b"x").await.is_err());
    assert!(c.receive(a.addr(), author, &record.key()).await.is_err());
    if stage == "restore" {
        let sql = rusqlite::Connection::open(root.join("a/projection.sqlite")).unwrap();
        sql.execute_batch(
            "CREATE TRIGGER reject_recovery BEFORE UPDATE ON membership
             BEGIN SELECT RAISE(ABORT,'injected recovery commit failure'); END;",
        )
        .unwrap();
        assert!(
            a.install_recovery_policy(&current, &manifest, latest)
                .is_err()
        );
        assert!(c.receive(a.addr(), author, &record.key()).await.is_err());
        assert!(c.received().unwrap().is_empty());
        drop(sql);
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
        c.shutdown().await.unwrap();
        return;
    }
    if stage == "activate" {
        let sql = rusqlite::Connection::open(root.join("a/projection.sqlite")).unwrap();
        sql.execute_batch("DROP TRIGGER reject_recovery").unwrap();
    }
    a.install_recovery_policy(&current, &manifest, latest)
        .unwrap();
    assert!(b.receive(a.addr(), author, &record.key()).await.is_err());
    assert!(b.received().unwrap().is_empty());
    let inserted = c.receive(a.addr(), author, &record.key()).await.unwrap();
    assert_eq!(inserted, stage == "activate");
    assert_eq!(c.received().unwrap(), vec![record]);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

#[test]
fn whole_backup_recovery_survives_fresh_process_restart() {
    const STAGE: &str = "COCONUTCLAW_RECOVERY_STAGE";
    const ROOT: &str = "COCONUTCLAW_RECOVERY_ROOT";
    if let Ok(value) = std::env::var(STAGE) {
        assert!(matches!(
            value.as_str(),
            "create" | "restore" | "activate" | "reopen"
        ));
        let root = std::env::var_os(ROOT).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            tokio::time::timeout(Duration::from_secs(45), stage(Path::new(&root), &value))
                .await
                .unwrap();
        });
        return;
    }
    let root = tempfile::tempdir().unwrap();
    for stage in ["create", "restore", "activate", "reopen"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "whole_backup_recovery_survives_fresh_process_restart",
                "--nocapture",
            ])
            .env(STAGE, stage)
            .env(ROOT, root.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{stage} failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
