use std::{collections::BTreeSet, path::Path, process::Command, time::Duration};

use coconutclaw_sync::synthetic::{MAX_ARTIFACT, MAX_RECORD, Peer, ReadPin, Record};
use iroh::SecretKey;
use tokio::time::{sleep, timeout};

const STAGE: &str = "COCONUTCLAW_PREFIX_STAGE";
const ROOT: &str = "COCONUTCLAW_PREFIX_ROOT";
const CASE: &str = "COCONUTCLAW_PREFIX_CASE";

async fn stage(root: &Path, stage: &str, case: &str) {
    let ka = SecretKey::from([111; 32]);
    let kb = SecretKey::from([112; 32]);
    let peers = BTreeSet::from([kb.public()]);
    if stage == "delete" {
        let mut a = Peer::create(&root.join("a"), ka, peers).await.unwrap();
        let doc = a.create_document().await.unwrap();
        let author = a.author().await.unwrap();
        let mut records = Vec::new();
        for n in 0..3 {
            records.push(
                a.prepare(&doc, &format!("deleted event {n}"), b"prefix artifact")
                    .await
                    .unwrap(),
            );
        }
        a.flush(&doc).await.unwrap();
        let prefix = match case {
            "empty" => String::new(),
            "exact" => records[0].key(),
            // Prefix semantics are byte-based, not path-component-based.
            "partial" => "v1/syn".to_owned(),
            "author" => format!("v1/synthetic/{author}/"),
            _ => unreachable!(),
        };
        let removed = doc.del(author, prefix.clone()).await.unwrap();
        assert_eq!(removed, if case == "exact" { 1 } else { 3 });
        std::fs::write(root.join("prefix"), prefix).unwrap();
        std::fs::write(root.join("namespace"), doc.id().to_string()).unwrap();
        std::fs::write(root.join("records"), serde_json::to_vec(&records).unwrap()).unwrap();
        a.shutdown().await.unwrap();
        return;
    }
    let a = Peer::reopen(&root.join("a"), ka.clone(), peers)
        .await
        .unwrap();
    let namespace = std::fs::read_to_string(root.join("namespace"))
        .unwrap()
        .parse()
        .unwrap();
    let doc = a.open_document(namespace).await.unwrap();
    let author = a.author().await.unwrap();
    let prefix = std::fs::read(root.join("prefix")).unwrap();
    let records: Vec<Record> =
        serde_json::from_slice(&std::fs::read(root.join("records")).unwrap()).unwrap();
    let tombstone = doc.get_exact(author, &prefix, true).await.unwrap().unwrap();
    assert_eq!(tombstone.content_len(), 0);
    for record in &records {
        if record.key().as_bytes().starts_with(&prefix) {
            assert!(
                doc.get_exact(author, record.key(), false)
                    .await
                    .unwrap()
                    .is_none(),
                "deleted event resurrected across reopen"
            );
            if case != "exact" {
                assert!(
                    doc.get_exact(author, record.key(), true)
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        }
    }
    if stage == "replay" {
        for _ in 0..2 {
            assert!(
                a.flush(&doc)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("immutable publication conflict")
            );
        }
        assert_eq!(
            doc.get_exact(author, &prefix, true).await.unwrap().unwrap(),
            tombstone
        );
    } else {
        assert_eq!(stage, "receiver");
        let mut b = Peer::create(&root.join("b"), kb, BTreeSet::from([ka.public()]))
            .await
            .unwrap();
        let pin = ReadPin { namespace, author };
        let bd = b.import_readonly(&pin).await.unwrap();
        b.connect(&bd, a.addr()).await.unwrap();
        // Wait for positive synchronization evidence, not a delay-based absence
        // assertion. No sleep here is used as a persistence barrier.
        timeout(Duration::from_secs(20), async {
            loop {
                if bd.get_exact(author, &prefix, true).await.unwrap().is_some() {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        for record in &records {
            if record.key().as_bytes().starts_with(&prefix) {
                assert!(
                    bd.get_exact(author, record.key(), false)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(b.project(&bd, &pin, record.key().as_bytes()).await.is_err());
            } else {
                let entry = timeout(Duration::from_secs(20), async {
                    loop {
                        if let Some(entry) =
                            bd.get_exact(author, record.key(), false).await.unwrap()
                        {
                            break entry;
                        }
                        sleep(Duration::from_millis(10)).await;
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
                    record.artifact.hash.parse().unwrap(),
                    record.artifact.size,
                    MAX_ARTIFACT,
                    a.addr(),
                )
                .await
                .unwrap();
                assert!(b.project(&bd, &pin, record.key().as_bytes()).await.unwrap());
                assert!(!b.project(&bd, &pin, record.key().as_bytes()).await.unwrap());
            }
        }
        assert_eq!(
            b.received().unwrap().len(),
            if case == "exact" { 2 } else { 0 }
        );
        b.shutdown().await.unwrap();
    }
    a.shutdown().await.unwrap();
}

#[test]
fn persisted_prefix_deletions_never_resurrect() {
    if let Ok(stage_name) = std::env::var(STAGE) {
        let root = std::env::var_os(ROOT).unwrap();
        let case = std::env::var(CASE).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            timeout(
                Duration::from_secs(45),
                stage(Path::new(&root), &stage_name, &case),
            )
            .await
            .unwrap();
        });
        return;
    }
    for case in ["author", "empty", "partial", "exact"] {
        let root = tempfile::tempdir().unwrap();
        for stage in ["delete", "replay", "receiver"] {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "persisted_prefix_deletions_never_resurrect",
                    "--nocapture",
                ])
                .env(STAGE, stage)
                .env(ROOT, root.path())
                .env(CASE, case)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{case}/{stage}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
