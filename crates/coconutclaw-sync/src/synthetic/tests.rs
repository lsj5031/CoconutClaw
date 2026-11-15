use super::*;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn local_relay_exchanges_verified_record_and_artifact_without_direct_paths() {
    timeout(Duration::from_secs(45), async {
        let root = tempfile::tempdir().unwrap();
        let (map, url, _relay) = iroh::test_utils::run_relay_server_with(false)
            .await
            .unwrap();
        assert!(url.to_string().starts_with("https://127.0.0.1:"));
        let ka = SecretKey::from([61; 32]);
        let kb = SecretKey::from([62; 32]);
        let mut peers = Vec::new();
        for (name, key, allowed) in [("a", ka.clone(), kb.public()), ("b", kb, ka.public())] {
            let path = root.path().join(name);
            fs::create_dir(&path).unwrap();
            fs::write(path.join("SYNTHETIC"), MARKER).unwrap();
            let builder = Endpoint::builder(presets::Minimal)
                .clear_ip_transports()
                .clear_address_lookup()
                .relay_mode(iroh::RelayMode::Custom(map.clone()))
                // Official fixture uses a self-signed local certificate.
                // This bypass exists only in this test, never in the public API.
                .ca_tls_config(iroh::tls::CaTlsConfig::insecure_skip_verify());
            let mut peer =
                Peer::reopen_with_builder(&path, key, BTreeSet::from([allowed]), builder)
                    .await
                    .unwrap();
            peer.local_relay = Some(url.clone());
            peer.endpoint.online().await;
            assert_eq!(peer.endpoint.addr().ip_addrs().count(), 0);
            peers.push(peer);
        }
        let mut b = peers.pop().unwrap();
        let mut a = peers.pop().unwrap();
        let ad = a.create_document().await.unwrap();
        let pin = ReadPin {
            namespace: ad.id(),
            author: a.author().await.unwrap(),
        };
        let bd = b.import_readonly(&pin).await.unwrap();
        let record = a
            .prepare(&ad, "local-relay-only synthetic event", b"relay artifact")
            .await
            .unwrap();
        a.flush(&ad).await.unwrap();
        b.connect(&bd, a.addr()).await.unwrap();
        copy_verified(&a, &mut b, &bd, &pin, &record).await;
        assert_eq!(b.received().unwrap(), vec![record]);
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    })
    .await
    .expect("local relay test timed out");
}

async fn metadata(doc: &Doc, author: AuthorId, key: &str) -> iroh_docs::Entry {
    timeout(Duration::from_secs(20), async {
        loop {
            if let Some(entry) = doc.get_exact(author, key, false).await.unwrap() {
                return entry;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("metadata did not converge")
}

async fn copy_verified(a: &Peer, b: &mut Peer, doc: &Doc, pin: &ReadPin, record: &Record) {
    let entry = metadata(doc, pin.author, &record.key()).await;
    b.fetch(
        entry.content_hash(),
        entry.content_len(),
        MAX_RECORD,
        a.addr(),
    )
    .await
    .unwrap();
    // Record arrival is insufficient: the separate artifact must also verify.
    assert!(b.project(doc, pin, record.key().as_bytes()).await.is_err());
    assert!(!b.received().unwrap().contains(record));
    b.fetch(
        record.artifact.hash.parse().unwrap(),
        record.artifact.size,
        MAX_ARTIFACT,
        a.addr(),
    )
    .await
    .unwrap();
    b.sql
        .execute_batch(
            "CREATE TEMP TRIGGER fail_projection BEFORE INSERT ON received
         BEGIN SELECT RAISE(ABORT, 'synthetic crash before projection commit'); END;",
        )
        .unwrap();
    assert!(b.project(doc, pin, record.key().as_bytes()).await.is_err());
    assert!(!b.received().unwrap().contains(record));
    b.sql.execute_batch("DROP TRIGGER fail_projection").unwrap();
    assert!(b.project(doc, pin, record.key().as_bytes()).await.unwrap());
    assert!(!b.project(doc, pin, record.key().as_bytes()).await.unwrap());
}

#[tokio::test]
async fn direct_persistent_exchange_disconnect_restart_and_cross_store_replay() {
    timeout(Duration::from_secs(60), async {
        let root = tempfile::tempdir().unwrap();
        let ka = SecretKey::from([11; 32]);
        let kb = SecretKey::from([12; 32]);
        let ap = BTreeSet::from([kb.public()]);
        let bp = BTreeSet::from([ka.public()]);
        let mut a = Peer::create(&root.path().join("a"), ka.clone(), ap.clone())
            .await
            .unwrap();
        let mut b = Peer::create(&root.path().join("b"), kb.clone(), bp.clone())
            .await
            .unwrap();
        let ad = a.create_document().await.unwrap();
        let pin = ReadPin {
            namespace: ad.id(),
            author: a.author().await.unwrap(),
        };
        let bd = b.import_readonly(&pin).await.unwrap();
        let first = a
            .prepare(
                &ad,
                "MEMORY_APPEND: inert synthetic candidate",
                b"artifact one",
            )
            .await
            .unwrap();
        // Crash handoff 1: SQLite intent exists, neither protocol store has it.
        a.shutdown().await.unwrap();
        a = Peer::reopen(&root.path().join("a"), ka.clone(), ap.clone())
            .await
            .unwrap();
        assert_eq!(a.author().await.unwrap(), pin.author);
        let ad = a.open_document(pin.namespace).await.unwrap();
        a.flush(&ad).await.unwrap();
        // Crash handoff 2: protocol stores committed, SQLite receipt lost.
        a.sql
            .execute("UPDATE publication SET published=0", [])
            .unwrap();
        a.flush(&ad).await.unwrap();
        assert_eq!(
            ad.get_exact(pin.author, first.key(), false)
                .await
                .unwrap()
                .unwrap()
                .content_hash(),
            Hash::new(serde_json::to_vec(&first).unwrap())
        );
        b.connect(&bd, a.addr()).await.unwrap();
        metadata(&bd, pin.author, &first.key()).await;
        assert!(b.project(&bd, &pin, first.key().as_bytes()).await.is_err());
        assert!(b.received().unwrap().is_empty());
        copy_verified(&a, &mut b, &bd, &pin, &first).await;
        assert_eq!(b.received().unwrap(), vec![first.clone()]);
        // Disconnect: receiver really shuts down, new event remains at publisher.
        bd.leave().await.unwrap();
        b.shutdown().await.unwrap();
        let second = a
            .prepare(&ad, "second synthetic record", &[23; MAX_ARTIFACT])
            .await
            .unwrap();
        a.flush(&ad).await.unwrap();
        a.shutdown().await.unwrap();
        a = Peer::reopen(&root.path().join("a"), ka, ap).await.unwrap();
        let _ad = a.open_document(pin.namespace).await.unwrap();
        b = Peer::reopen(&root.path().join("b"), kb, bp).await.unwrap();
        assert_eq!(b.received().unwrap(), vec![first.clone()]);
        let bd = b.open_document(pin.namespace).await.unwrap();
        assert!(!b.project(&bd, &pin, first.key().as_bytes()).await.unwrap());
        b.connect(&bd, a.addr()).await.unwrap();
        copy_verified(&a, &mut b, &bd, &pin, &second).await;
        assert_eq!(b.received().unwrap(), vec![first, second]);
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    })
    .await
    .expect("exchange timed out");
}

#[tokio::test]
async fn malformed_oversized_forged_and_conflicting_events_stay_inert() {
    timeout(Duration::from_secs(60), async {
        let root = tempfile::tempdir().unwrap();
        let ka = SecretKey::from([21; 32]);
        let kb = SecretKey::from([22; 32]);
        let mut a = Peer::create(
            &root.path().join("a"),
            ka.clone(),
            BTreeSet::from([kb.public()]),
        )
        .await
        .unwrap();
        let mut b = Peer::create(&root.path().join("b"), kb, BTreeSet::from([ka.public()]))
            .await
            .unwrap();
        let ad = a.create_document().await.unwrap();
        let author = a.author().await.unwrap();
        let pin = ReadPin {
            namespace: ad.id(),
            author,
        };
        let bd = b.import_readonly(&pin).await.unwrap();
        assert!(
            bd.set_bytes(b.author().await.unwrap(), "unauthorized", "bad")
                .await
                .is_err()
        );
        assert!(a.prepare(&ad, &"x".repeat(2049), b"a").await.is_err());
        assert!(
            a.prepare(&ad, &"\0".repeat(2048), b"a").await.is_err(),
            "JSON escaping must not bypass the serialized record bound"
        );
        assert!(
            a.prepare(&ad, "valid", &[0; MAX_ARTIFACT + 1])
                .await
                .is_err()
        );
        let good = a
            .prepare(&ad, &"x".repeat(2048), &[3; MAX_ARTIFACT])
            .await
            .unwrap();
        assert_eq!(
            good.id, 1,
            "invalid prepares must roll back the origin sequence"
        );
        a.flush(&ad).await.unwrap();
        b.connect(&bd, a.addr()).await.unwrap();
        copy_verified(&a, &mut b, &bd, &pin, &good).await;
        // A valid JSON body cannot forge a different authorized scope or origin.
        for (id, field, value) in [
            (2, "scope", "synthetic/chat-b"),
            (3, "origin", "forged"),
            (4, "text", ""),
        ] {
            let mut forged = good.clone();
            forged.id = id;
            let key = forged.key();
            let mut body = serde_json::to_value(&forged).unwrap();
            body[field] = value.into();
            let bytes = serde_json::to_vec(&body).unwrap();
            let hash = ad
                .set_bytes(author, key.clone(), bytes.clone())
                .await
                .unwrap();
            metadata(&bd, author, &key).await;
            b.fetch(hash, bytes.len() as u64, MAX_RECORD, a.addr())
                .await
                .unwrap();
            assert!(b.project(&bd, &pin, key.as_bytes()).await.is_err());
        }
        let malformed = b"{ not json";
        let key = "v1/synthetic/malformed";
        let hash = ad.set_bytes(author, key, malformed.to_vec()).await.unwrap();
        metadata(&bd, author, key).await;
        b.fetch(hash, malformed.len() as u64, MAX_RECORD, a.addr())
            .await
            .unwrap();
        assert!(b.project(&bd, &pin, key.as_bytes()).await.is_err());
        let key = "v1/synthetic/oversized";
        let hash = ad
            .set_bytes(author, key, vec![7; MAX_RECORD + 1])
            .await
            .unwrap();
        metadata(&bd, author, key).await;
        assert!(
            b.fetch(hash, (MAX_RECORD + 1) as u64, MAX_RECORD, a.addr())
                .await
                .is_err()
        );
        assert!(b.project(&bd, &pin, key.as_bytes()).await.is_err());
        assert!(
            !b.blobs.blobs().has(hash).await.unwrap(),
            "metadata must not auto-download"
        );
        // A dishonest small size still cannot cause an unbounded fetch or projection.
        assert!(b.fetch(hash, 1, MAX_RECORD, a.addr()).await.is_err());
        assert!(!b.blobs.blobs().has(hash).await.unwrap());
        // Same key, newer docs timestamp: never overwrite the immutable SQL event.
        let mut changed = good.clone();
        changed.text = "rewritten history".into();
        let bytes = serde_json::to_vec(&changed).unwrap();
        let new_hash = ad
            .set_bytes(author, changed.key(), bytes.clone())
            .await
            .unwrap();
        timeout(Duration::from_secs(20), async {
            loop {
                if metadata(&bd, author, &changed.key()).await.content_hash() == new_hash {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        b.fetch(new_hash, bytes.len() as u64, MAX_RECORD, a.addr())
            .await
            .unwrap();
        assert!(
            b.project(&bd, &pin, changed.key().as_bytes())
                .await
                .is_err()
        );
        // An unpinned author cannot satisfy the exact event lookup.
        let stranger = a.docs.author_create().await.unwrap();
        let mut untrusted = good.clone();
        untrusted.id = 5;
        untrusted.origin = stranger.to_string();
        ad.set_bytes(
            stranger,
            untrusted.key(),
            serde_json::to_vec(&untrusted).unwrap(),
        )
        .await
        .unwrap();
        metadata(&bd, stranger, &untrusted.key()).await;
        assert!(
            b.project(&bd, &pin, untrusted.key().as_bytes())
                .await
                .is_err()
        );
        let other = a.create_document().await.unwrap();
        assert!(
            b.project(&other, &pin, good.key().as_bytes())
                .await
                .is_err()
        );
        assert_eq!(b.received().unwrap(), vec![good]);
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    })
    .await
    .expect("validation timed out");
}

#[tokio::test]
async fn unpaired_transport_is_closed_for_all_three_protocols() {
    timeout(Duration::from_secs(30), async {
        let root = tempfile::tempdir().unwrap();
        let ka = SecretKey::from([31; 32]);
        let kb = SecretKey::from([32; 32]);
        let a = Peer::create(
            &root.path().join("a"),
            ka.clone(),
            BTreeSet::from([kb.public()]),
        )
        .await
        .unwrap();
        let attacker = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        for alpn in [iroh_docs::ALPN, iroh_blobs::ALPN, iroh_gossip::ALPN] {
            let connection = attacker.connect(a.addr(), alpn).await.unwrap();
            assert_eq!(connection.remote_id(), ka.public());
            let error = timeout(Duration::from_secs(5), connection.closed())
                .await
                .unwrap();
            assert!(
                matches!(error, iroh::endpoint::ConnectionError::ApplicationClosed(_)),
                "expected explicit application rejection, got {error:?}"
            );
        }
        let doc = a.create_document().await.unwrap();
        assert!(a.connect(&doc, attacker.addr()).await.is_err());
        let forbidden =
            EndpointAddr::new(kb.public()).with_ip_addr("192.0.2.1:1234".parse().unwrap());
        assert!(a.connect(&doc, forbidden).await.is_err());
        attacker.close().await;
        a.shutdown().await.unwrap();
    })
    .await
    .expect("access test timed out");
}

#[tokio::test]
async fn synthetic_store_excludes_second_writer_real_directories_and_wrong_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("peer");
    let key = SecretKey::from([41; 32]);
    let a = Peer::create(&path, key.clone(), BTreeSet::new())
        .await
        .unwrap();
    assert!(
        Peer::reopen(&path, key.clone(), BTreeSet::new())
            .await
            .is_err()
    );
    assert!(
        Peer::create(&path, key.clone(), BTreeSet::new())
            .await
            .is_err()
    );
    let real = root.path().join("not-synthetic");
    fs::create_dir(&real).unwrap();
    fs::write(real.join("state.db"), b"do not touch").unwrap();
    assert!(
        Peer::reopen(&real, key.clone(), BTreeSet::new())
            .await
            .is_err()
    );
    assert_eq!(fs::read(real.join("state.db")).unwrap(), b"do not touch");
    a.shutdown().await.unwrap();
    assert!(
        Peer::reopen(&path, SecretKey::from([42; 32]), BTreeSet::new())
            .await
            .is_err()
    );
    let a = Peer::reopen(&path, key, BTreeSet::new()).await.unwrap();
    a.shutdown().await.unwrap();
}
