use std::{collections::BTreeSet, path::Path, process::Command, time::Duration};

use coconutclaw_sync::synthetic::{MAX_ARTIFACT, MAX_RECORD, Peer, ReadPin, Record};
use iroh::SecretKey;
use iroh_docs::api::Doc;
use tokio::time::{sleep, timeout};

async fn exchange(a: &Peer, b: &mut Peer, doc: &Doc, pin: &ReadPin, record: &Record) {
    b.connect(doc, a.addr()).await.unwrap();
    let entry = timeout(Duration::from_secs(20), async {
        loop {
            if let Some(entry) = doc
                .get_exact(pin.author, record.key(), false)
                .await
                .unwrap()
            {
                break entry;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(b.project(doc, pin, record.key().as_bytes()).await.is_err());
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
    assert!(b.project(doc, pin, record.key().as_bytes()).await.unwrap());
}

async fn stage(root: &Path, first: bool) {
    // Public fixture keys only. They never represent an owner or production peer.
    let ka = SecretKey::from([51; 32]);
    let kb = SecretKey::from([52; 32]);
    let ap = BTreeSet::from([kb.public()]);
    let bp = BTreeSet::from([ka.public()]);
    let (mut a, mut b) = if first {
        (
            Peer::create(&root.join("a"), ka, ap).await.unwrap(),
            Peer::create(&root.join("b"), kb, bp).await.unwrap(),
        )
    } else {
        (
            Peer::reopen(&root.join("a"), ka, ap).await.unwrap(),
            Peer::reopen(&root.join("b"), kb, bp).await.unwrap(),
        )
    };
    let ad = if first {
        a.create_document().await.unwrap()
    } else {
        let id = std::fs::read_to_string(root.join("namespace")).unwrap();
        a.open_document(id.parse().unwrap()).await.unwrap()
    };
    let pin = ReadPin {
        namespace: ad.id(),
        author: a.author().await.unwrap(),
    };
    let bd = if first {
        std::fs::write(root.join("namespace"), ad.id().to_string()).unwrap();
        b.import_readonly(&pin).await.unwrap()
    } else {
        let previous = b.received().unwrap();
        assert_eq!(previous.len(), 1);
        assert_eq!(previous[0].origin, pin.author.to_string());
        let bd = b.open_document(pin.namespace).await.unwrap();
        assert!(
            !b.project(&bd, &pin, previous[0].key().as_bytes())
                .await
                .unwrap()
        );
        bd
    };
    let expected = if first { 1 } else { 2 };
    let record = a
        .prepare(&ad, "fresh-process synthetic event", &[expected as u8; 64])
        .await
        .unwrap();
    assert_eq!(record.id, expected);
    a.flush(&ad).await.unwrap();
    exchange(&a, &mut b, &bd, &pin, &record).await;
    assert_eq!(b.received().unwrap().len() as u64, expected);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[test]
fn persistent_exchange_survives_fresh_process_restart() {
    const STAGE: &str = "COCONUTCLAW_SYNTHETIC_RESTART_STAGE";
    const ROOT: &str = "COCONUTCLAW_SYNTHETIC_RESTART_ROOT";
    if let Ok(value) = std::env::var(STAGE) {
        assert!(value == "create" || value == "reopen");
        let root = std::env::var_os(ROOT).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            timeout(
                Duration::from_secs(45),
                stage(Path::new(&root), value == "create"),
            )
            .await
            .expect("child exchange timed out");
        });
        return;
    }
    let root = tempfile::tempdir().unwrap();
    for value in ["create", "reopen"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "persistent_exchange_survives_fresh_process_restart",
                "--nocapture",
            ])
            .env(STAGE, value)
            .env(ROOT, root.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{value} failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
