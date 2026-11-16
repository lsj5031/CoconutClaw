use std::{
    collections::BTreeSet,
    fs,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use coconutclaw_sync::synthetic::{
    MAX_ARTIFACT, Record,
    membership::{Member, Policy, ScopedPeer, SignedPolicy},
};
use iroh::SecretKey;
use iroh_docs::{Capability, NamespaceSecret};
use rusqlite::{Connection, params};

const STAGE: &str = "COCONUTCLAW_PROJECTION_RECOVERY_STAGE";
const ROOT: &str = "COCONUTCLAW_PROJECTION_RECOVERY_ROOT";

async fn stage(root: &Path, stage: &str) {
    let owner = SecretKey::from([111; 32]);
    let ka = SecretKey::from([112; 32]);
    let kb = SecretKey::from([113; 32]);
    let ns = NamespaceSecret::from_bytes(&[114; 32]);
    let namespace = ns.id();
    if stage == "crash" {
        let mut a = ScopedPeer::create(
            &root.join("a"),
            ka.clone(),
            owner.public(),
            Capability::Write(ns),
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
                generation: 2,
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
        fs::write(
            root.join("policy.json"),
            serde_json::to_vec(&policy).unwrap(),
        )
        .unwrap();
        a.install_policy(&policy).unwrap();
        b.install_policy(&policy).unwrap();
        let one = a
            .publish(&"x".repeat(2048), &[171; MAX_ARTIFACT])
            .await
            .unwrap();
        let two = a
            .publish("pending projection", &[172; MAX_ARTIFACT])
            .await
            .unwrap();
        fs::write(
            root.join("records.json"),
            serde_json::to_vec(&vec![one.clone(), two.clone()]).unwrap(),
        )
        .unwrap();
        assert!(b.receive(a.addr(), author, &one.key()).await.unwrap());
        Connection::open(root.join("b/projection.sqlite"))
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fail_projection BEFORE INSERT ON received
             BEGIN SELECT RAISE(ABORT, 'synthetic projection failure'); END;",
            )
            .unwrap();
        assert!(b.receive(a.addr(), author, &two.key()).await.is_err());
        assert_eq!(b.received().unwrap(), vec![one]);
        let journal = Connection::open(root.join("b/record-journal.sqlite")).unwrap();
        assert_eq!(
            journal
                .query_row("SELECT count(*) FROM receipt", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            2
        );
        println!("RECEIPT_COMMITTED");
        std::io::stdout().flush().unwrap();
        // No destructor, shutdown, settling delay or simulated crash helps
        // either receipt persist. The parent kills this process immediately.
        loop {
            std::thread::park();
        }
    }
    let policy: SignedPolicy =
        serde_json::from_slice(&fs::read(root.join("policy.json")).unwrap()).unwrap();
    let records: Vec<Record> =
        serde_json::from_slice(&fs::read(root.join("records.json")).unwrap()).unwrap();
    if stage == "unavailable" {
        for (store, name, key, capability) in [
            (
                "a",
                "docs/docs.redb",
                ka.clone(),
                Capability::Write(ns.clone()),
            ),
            (
                "b",
                "projection.sqlite",
                kb.clone(),
                Capability::Read(namespace),
            ),
        ] {
            fs::remove_file(root.join(store).join(name)).unwrap();
            assert!(
                ScopedPeer::reopen(&root.join(store), key, owner.public(), capability)
                    .await
                    .is_err()
            );
            assert!(!root.join(store).join(name).exists());
        }
        return;
    }
    let mut b = ScopedPeer::reopen(
        &root.join("b"),
        kb,
        owner.public(),
        Capability::Read(namespace),
    )
    .await
    .unwrap();
    assert!(b.repair_projection().is_err(), "restart is quarantined");
    let mut stale = policy.policy.clone();
    stale.generation = 1;
    assert!(
        b.install_policy(&SignedPolicy::sign(stale, &owner).unwrap())
            .is_err()
    );
    b.install_policy(&policy).unwrap();
    let sql = Connection::open(root.join("b/projection.sqlite")).unwrap();
    if stage == "repair" {
        assert!(
            b.repair_projection().is_err(),
            "injected failure must roll back all replay"
        );
        assert_eq!(b.received().unwrap(), vec![records[0].clone()]);
        sql.execute_batch("DROP TRIGGER fail_projection").unwrap();
        assert_eq!(b.repair_projection().unwrap(), 1);
        assert_eq!(b.repair_projection().unwrap(), 0);
        assert_eq!(b.received().unwrap(), records);
        // SQL behind with policy/identity intact, not whole authority loss.
        sql.execute("DROP TABLE received", []).unwrap();
    } else {
        assert_eq!(stage, "behind");
        assert!(b.received().unwrap().is_empty());
        assert_eq!(b.repair_projection().unwrap(), 2);
        assert_eq!(b.repair_projection().unwrap(), 0);
        assert_eq!(b.received().unwrap(), records);
        let artifacts: Vec<Vec<u8>> = sql
            .prepare("SELECT artifact FROM received ORDER BY key")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            artifacts,
            vec![vec![171; MAX_ARTIFACT], vec![172; MAX_ARTIFACT]]
        );
        // SQL-ahead cannot be adopted from local projection bytes alone.
        sql.execute(
            "INSERT INTO received SELECT namespace,author,X'00',hash,body,artifact
            FROM received LIMIT 1",
            [],
        )
        .unwrap();
        assert!(b.repair_projection().is_err());
        sql.execute("DELETE FROM received WHERE key=X'00'", [])
            .unwrap();
        // Matching hash but divergent bytes must fail on the real receive path.
        sql.execute(
            "UPDATE received SET artifact=X'00' WHERE key=?1",
            [records[0].key().as_bytes()],
        )
        .unwrap();
        assert!(b.repair_projection().is_err());
        let mut a = ScopedPeer::reopen(&root.join("a"), ka, owner.public(), Capability::Write(ns))
            .await
            .unwrap();
        a.install_policy(&policy).unwrap();
        assert!(
            b.receive(
                a.addr(),
                records[0].origin.parse().unwrap(),
                &records[0].key()
            )
            .await
            .is_err()
        );
        sql.execute(
            "UPDATE received SET artifact=?1 WHERE key=?2",
            params![vec![171u8; MAX_ARTIFACT], records[0].key().as_bytes()],
        )
        .unwrap();
        assert_eq!(b.repair_projection().unwrap(), 0);
        let mut revoked = policy.policy;
        revoked.generation = 3;
        revoked.members[0].authors.clear();
        b.install_policy(&SignedPolicy::sign(revoked, &owner).unwrap())
            .unwrap();
        sql.execute("DELETE FROM received", []).unwrap();
        assert!(
            b.repair_projection().is_err(),
            "revocation fences local replay"
        );
        assert!(b.received().unwrap().is_empty());
        a.shutdown().await.unwrap();
    }
    b.shutdown().await.unwrap();
}

#[test]
fn receipt_crash_sql_loss_divergence_and_revocation_across_processes() {
    if let Ok(value) = std::env::var(STAGE) {
        let root = std::env::var_os(ROOT).unwrap();
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(stage(Path::new(&root), &value));
        return;
    }
    let root = tempfile::tempdir().unwrap();
    for value in ["crash", "repair", "behind", "unavailable"] {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "receipt_crash_sql_loss_divergence_and_revocation_across_processes",
                "--nocapture",
            ])
            .env(STAGE, value)
            .env(ROOT, root.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        if value == "crash" {
            let output = child.stdout.take().unwrap();
            let (send, recv) = std::sync::mpsc::channel();
            let reader = std::thread::spawn(move || {
                for line in BufReader::new(output).lines() {
                    if line.unwrap().contains("RECEIPT_COMMITTED") {
                        send.send(()).unwrap();
                        break;
                    }
                }
            });
            let ready = recv.recv_timeout(Duration::from_secs(45));
            child.kill().unwrap();
            let status = child.wait().unwrap();
            reader.join().unwrap();
            ready.expect("receipt never committed");
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(status.signal(), Some(9));
            }
        } else {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{value}: {}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
    }
}
