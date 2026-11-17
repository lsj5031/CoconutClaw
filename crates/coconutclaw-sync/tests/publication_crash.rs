use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use coconutclaw_sync::synthetic::{MAX_ARTIFACT, MAX_RECORD, Peer, ReadPin, Record};
use iroh::SecretKey;
use tokio::time::{sleep, timeout};

const STAGE: &str = "COCONUTCLAW_PUBLICATION_CRASH_STAGE";
const ROOT: &str = "COCONUTCLAW_PUBLICATION_CRASH_ROOT";

async fn stage(root: &Path, stage: &str) {
    let ka = SecretKey::from([91; 32]);
    let kb = SecretKey::from([92; 32]);
    let peers = BTreeSet::from([kb.public()]);
    if stage == "init" {
        let a = Peer::create(&root.join("a"), ka, peers).await.unwrap();
        let doc = a.create_document().await.unwrap();
        std::fs::write(root.join("namespace"), doc.id().to_string()).unwrap();
        a.shutdown().await.unwrap();
        return;
    }
    let mut a = Peer::reopen(&root.join("a"), ka.clone(), peers)
        .await
        .unwrap();
    let namespace = std::fs::read_to_string(root.join("namespace"))
        .unwrap()
        .parse()
        .unwrap();
    let doc = a.open_document(namespace).await.unwrap();
    let pin = ReadPin {
        namespace,
        author: a.author().await.unwrap(),
    };
    if stage == "publish" {
        // Independent R1 recipe: initialize in another process, then publish on
        // reopen. No settling delay or persistence sleep is needed or permitted.
        let record = a
            .prepare(&doc, &"x".repeat(2048), &[173; MAX_ARTIFACT])
            .await
            .unwrap();
        std::fs::write(
            root.join("record.json"),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        a.flush(&doc).await.unwrap();
        println!("FLUSH_OK");
        std::io::stdout().flush().unwrap();
        // The parent kills this process at the public success boundary. Neither
        // destructors nor protocol shutdown may help it persist.
        loop {
            std::thread::park();
        }
    }
    assert_eq!(stage, "verify");
    let record: Record =
        serde_json::from_slice(&std::fs::read(root.join("record.json")).unwrap()).unwrap();
    assert_eq!(
        rusqlite::Connection::open(root.join("a/projection.sqlite"))
            .unwrap()
            .query_row("SELECT published FROM publication WHERE id=1", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    // Crucially check BEFORE replay, on the intact stores.
    assert!(
        doc.get_exact(pin.author, record.key(), false)
            .await
            .unwrap()
            .is_some(),
        "acknowledged protocol metadata lost across immediate SIGKILL"
    );
    let mut b = Peer::create(&root.join("b"), kb, BTreeSet::from([ka.public()]))
        .await
        .unwrap();
    let bd = b.import_readonly(&pin).await.unwrap();
    b.connect(&bd, a.addr()).await.unwrap();
    let entry = timeout(Duration::from_secs(20), async {
        loop {
            if let Some(entry) = bd.get_exact(pin.author, record.key(), false).await.unwrap() {
                break entry;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Fetch both blobs before flush can reconstruct them from the SQL journal.
    b.fetch(
        entry.content_hash(),
        entry.content_len(),
        MAX_RECORD,
        a.addr(),
    )
    .await
    .unwrap();
    assert!(b.project(&bd, &pin, record.key().as_bytes()).await.is_err());
    b.fetch(
        record.artifact.hash.parse().unwrap(),
        record.artifact.size,
        MAX_ARTIFACT,
        a.addr(),
    )
    .await
    .unwrap();
    assert!(b.project(&bd, &pin, record.key().as_bytes()).await.unwrap());
    a.flush(&doc).await.unwrap();
    a.flush(&doc).await.unwrap();
    assert!(!b.project(&bd, &pin, record.key().as_bytes()).await.unwrap());
    assert_eq!(b.received().unwrap(), vec![record]);
    let sql = rusqlite::Connection::open(root.join("b/projection.sqlite")).unwrap();
    let artifact: Vec<u8> = sql
        .query_row("SELECT artifact FROM received", [], |r| r.get(0))
        .unwrap();
    assert_eq!(artifact, vec![173; MAX_ARTIFACT]);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[test]
fn successful_flush_survives_external_immediate_sigkill() {
    if let Ok(value) = std::env::var(STAGE) {
        let root = std::env::var_os(ROOT).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            timeout(Duration::from_secs(45), stage(Path::new(&root), &value))
                .await
                .expect("crash child timed out");
        });
        return;
    }
    for _ in 0..3 {
        let root = tempfile::tempdir().unwrap();
        for value in ["init", "publish", "verify"] {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "successful_flush_survives_external_immediate_sigkill",
                    "--nocapture",
                ])
                .env(STAGE, value)
                .env(ROOT, root.path())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            if value == "publish" {
                let stdout = child.stdout.take().unwrap();
                let (tx, rx) = std::sync::mpsc::channel();
                let reader = std::thread::spawn(move || {
                    for line in BufReader::new(stdout).lines() {
                        if line.unwrap() == "FLUSH_OK" {
                            tx.send(()).unwrap();
                            break;
                        }
                    }
                });
                let ready = rx.recv_timeout(Duration::from_secs(45));
                // Child::kill is an external SIGKILL on the Linux review host.
                child.kill().unwrap();
                let status = child.wait().unwrap();
                reader.join().unwrap();
                ready.expect("publisher did not acknowledge flush");
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    assert_eq!(status.signal(), Some(9));
                }
                #[cfg(not(unix))]
                assert!(!status.success(), "publisher survived external kill");
            } else {
                let output = child.wait_with_output().unwrap();
                assert!(
                    output.status.success(),
                    "{value} failed: {}",
                    String::from_utf8_lossy(&output.stdout)
                );
            }
        }
    }
}

#[tokio::test]
async fn acknowledged_missing_metadata_repairs_and_conflicts_remain_immutable() {
    timeout(Duration::from_secs(45), async {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a");
        let key = SecretKey::from([93; 32]);
        let receiver_key = SecretKey::from([94; 32]);
        let peers = BTreeSet::from([receiver_key.public()]);
        let mut a = Peer::create(&path, key.clone(), peers.clone())
            .await
            .unwrap();
        let doc = a.create_document().await.unwrap();
        let namespace = doc.id();
        let author = a.author().await.unwrap();
        let mut records = Vec::new();
        for _ in 0..3 {
            records.push(
                a.prepare(&doc, "old acknowledgment", b"repair artifact")
                    .await
                    .unwrap(),
            );
        }
        let record = records.last().unwrap();
        a.shutdown().await.unwrap();
        // Exact journal shape left by the old ordering defect. The namespace,
        // author and SQL bytes survive, but no protocol event was committed.
        let sql = rusqlite::Connection::open(path.join("projection.sqlite")).unwrap();
        sql.execute("UPDATE publication SET published=1", [])
            .unwrap();
        drop(sql);
        let a = Peer::reopen(&path, key.clone(), peers.clone())
            .await
            .unwrap();
        let doc = a.open_document(namespace).await.unwrap();
        // Other namespace/non-covering deletions and nonempty ancestors are not
        // evidence that this author's missing event was intentionally deleted.
        let other = a.create_document().await.unwrap();
        other.del(author, Vec::new()).await.unwrap();
        doc.del(author, b"unrelated/".to_vec()).await.unwrap();
        doc.set_bytes(author, b"v1/syn".to_vec(), b"not a tombstone".to_vec())
            .await
            .unwrap();
        assert!(
            doc.get_exact(author, record.key(), true)
                .await
                .unwrap()
                .is_none()
        );
        a.flush(&doc).await.unwrap();
        let original = doc
            .get_exact(author, record.key(), true)
            .await
            .unwrap()
            .unwrap();
        a.flush(&doc).await.unwrap();
        assert_eq!(
            doc.get_exact(author, record.key(), true)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        a.shutdown().await.unwrap();
        let a = Peer::reopen(&path, key.clone(), peers).await.unwrap();
        let doc = a.open_document(namespace).await.unwrap();
        assert_eq!(
            doc.get_exact(author, record.key(), true)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        let mut b = Peer::create(
            &root.path().join("b"),
            receiver_key,
            BTreeSet::from([key.public()]),
        )
        .await
        .unwrap();
        let pin = ReadPin { namespace, author };
        let bd = b.import_readonly(&pin).await.unwrap();
        b.connect(&bd, a.addr()).await.unwrap();
        for repaired in &records {
            let entry = timeout(Duration::from_secs(20), async {
                loop {
                    if let Some(entry) = bd.get_exact(author, repaired.key(), false).await.unwrap()
                    {
                        break entry;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            b.fetch(
                entry.content_hash(),
                entry.content_len(),
                MAX_RECORD,
                a.addr(),
            )
            .await
            .unwrap();
            b.fetch(
                repaired.artifact.hash.parse().unwrap(),
                repaired.artifact.size,
                MAX_ARTIFACT,
                a.addr(),
            )
            .await
            .unwrap();
            assert!(
                b.project(&bd, &pin, repaired.key().as_bytes())
                    .await
                    .unwrap()
            );
            assert!(
                !b.project(&bd, &pin, repaired.key().as_bytes())
                    .await
                    .unwrap()
            );
        }
        assert_eq!(b.received().unwrap(), records);
        b.shutdown().await.unwrap();
        // An already acknowledged conflicting body must not be silently skipped
        // or overwritten during reconciliation.
        doc.set_bytes(
            author,
            record.key(),
            b"conflicting immutable event".to_vec(),
        )
        .await
        .unwrap();
        let conflict = doc
            .get_exact(author, record.key(), true)
            .await
            .unwrap()
            .unwrap();
        assert!(
            a.flush(&doc)
                .await
                .unwrap_err()
                .to_string()
                .contains("immutable publication conflict")
        );
        assert_eq!(
            doc.get_exact(author, record.key(), true)
                .await
                .unwrap()
                .unwrap(),
            conflict
        );
        // A deletion marker is a conflict too, not missing metadata to resurrect.
        doc.del(author, record.key()).await.unwrap();
        let tombstone = doc
            .get_exact(author, record.key(), true)
            .await
            .unwrap()
            .unwrap();
        assert!(a.flush(&doc).await.is_err());
        assert_eq!(
            doc.get_exact(author, record.key(), true)
                .await
                .unwrap()
                .unwrap(),
            tombstone
        );
        a.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
