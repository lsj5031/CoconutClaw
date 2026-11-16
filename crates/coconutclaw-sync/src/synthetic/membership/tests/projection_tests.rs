use super::*;

#[tokio::test]
async fn receipt_before_projection_commit_replays_atomically_and_checks_all_bytes() {
    let mut f = Fixture::new().await;
    f.activate();
    let record =
        f.a.publish("journal survives SQL failure", b"artifact")
            .await
            .unwrap();
    let author = f.a.author().await.unwrap();
    f.b.peer
        .sql
        .execute_batch(
            "CREATE TEMP TRIGGER fail_projection BEFORE INSERT ON received
         BEGIN SELECT RAISE(ABORT, 'projection failure'); END;",
        )
        .unwrap();
    assert!(
        f.b.receive(f.a.addr(), author, &record.key())
            .await
            .is_err()
    );
    assert!(f.b.received().unwrap().is_empty());
    assert_eq!(
        f.b.journal
            .query_row("SELECT count(*) FROM receipt", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        1
    );
    f.b.peer
        .sql
        .execute_batch("DROP TRIGGER fail_projection")
        .unwrap();
    assert_eq!(f.b.repair_projection().unwrap(), 1);
    assert_eq!(f.b.repair_projection().unwrap(), 0);
    assert_eq!(f.b.received().unwrap(), vec![record.clone()]);
    // A matching hash column does NOT legitimize different persisted bytes.
    f.b.peer
        .sql
        .execute("UPDATE received SET artifact=X'00'", [])
        .unwrap();
    assert!(
        f.b.receive(f.a.addr(), author, &record.key())
            .await
            .is_err()
    );
    assert!(f.b.repair_projection().is_err());
    f.b.peer
        .sql
        .execute("UPDATE received SET artifact=?1", [b"artifact".as_slice()])
        .unwrap();
    f.b.peer.sql.execute("DELETE FROM received", []).unwrap();
    f.policy.generation = 2;
    f.policy.members[0].authors.clear();
    f.activate();
    assert!(
        f.b.repair_projection().is_err(),
        "revoked author must not replay"
    );
    assert!(f.b.received().unwrap().is_empty());
    f.a.shutdown().await.unwrap();
    f.b.shutdown().await.unwrap();
}

#[tokio::test]
async fn sql_ahead_journal_conflicts_and_corruption_never_become_authority() {
    let mut f = Fixture::new().await;
    f.activate();
    let one = f.a.publish("one", b"one").await.unwrap();
    let two = f.a.publish("two", b"two").await.unwrap();
    let author = f.a.author().await.unwrap();
    for record in [&one, &two] {
        f.b.receive(f.a.addr(), author, &record.key())
            .await
            .unwrap();
    }
    f.b.peer
        .sql
        .execute("DELETE FROM received WHERE key=?1", [one.key().as_bytes()])
        .unwrap();
    f.b.journal
        .execute("DELETE FROM receipt WHERE key=?1", [two.key().as_bytes()])
        .unwrap();
    assert!(
        f.b.repair_projection().is_err(),
        "SQL ahead is not trusted history"
    );
    assert_eq!(
        f.b.received().unwrap(),
        vec![two.clone()],
        "failure is atomic"
    );
    // Re-fetch from an authorized serving boundary can establish the missing
    // receipt. Locally ahead SQL alone cannot.
    assert!(!f.b.receive(f.a.addr(), author, &two.key()).await.unwrap());
    f.b.journal
        .execute(
            "UPDATE receipt SET artifact=X'00' WHERE key=?1",
            [one.key().as_bytes()],
        )
        .unwrap();
    assert!(f.b.repair_projection().is_err());
    assert!(f.b.receive(f.a.addr(), author, &one.key()).await.is_err());
    assert_eq!(f.b.received().unwrap(), vec![two]);
    f.a.shutdown().await.unwrap();
    f.b.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_protocol_or_authority_stores_refuse_before_recreation() {
    for missing in [
        "projection.sqlite",
        "docs/docs.redb",
        "docs/default-author",
        "blobs/blobs.db",
        "record-journal.sqlite",
    ] {
        let f = Fixture::new().await;
        let path = f._root.path().join("a");
        f.a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
        fs::remove_file(path.join(missing)).unwrap();
        let result = ScopedPeer::reopen(
            &path,
            f.ka,
            f.owner.public(),
            Capability::Write(NamespaceSecret::from_bytes(&[94; 32])),
        )
        .await;
        assert!(result.is_err(), "missing {missing} must be unavailable");
        assert!(!path.join(missing).exists(), "must not recreate {missing}");
    }
}

#[tokio::test]
async fn lost_authority_tables_and_wrong_journal_binding_do_not_reset_pins() {
    for table in ["identity", "membership", "publication"] {
        let mut f = Fixture::new().await;
        f.policy.generation = 2;
        f.activate();
        let path = f._root.path().join("a");
        f.a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
        let sql = Sqlite::open(path.join("projection.sqlite")).unwrap();
        sql.execute_batch(&format!("DROP TABLE {table}")).unwrap();
        assert!(
            ScopedPeer::reopen(
                &path,
                f.ka,
                f.owner.public(),
                Capability::Write(NamespaceSecret::from_bytes(&[94; 32]))
            )
            .await
            .is_err()
        );
        assert_eq!(
            sql.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
            0
        );
    }
    let mut f = Fixture::new().await;
    f.activate();
    let path = f._root.path().join("b");
    f.a.shutdown().await.unwrap();
    f.b.shutdown().await.unwrap();
    Sqlite::open(path.join("record-journal.sqlite"))
        .unwrap()
        .execute("UPDATE binding SET owner=?1", [f.ka.public().to_string()])
        .unwrap();
    assert!(
        ScopedPeer::reopen(&path, f.kb, f.owner.public(), Capability::Read(f.namespace))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn missing_publication_blobs_repair_only_from_valid_journal_and_no_tombstone() {
    let mut f = Fixture::new().await;
    f.activate();
    let record =
        f.a.publish("acknowledged", b"retained artifact")
            .await
            .unwrap();
    let author = f.a.author().await.unwrap();
    f.a.shutdown().await.unwrap();
    f.b.shutdown().await.unwrap();
    // An intact but empty protocol blob database models content loss without
    // making up an API for deleting blobs or discarding the trusted SQL intent.
    fs::copy(
        f._root.path().join("b/blobs/blobs.db"),
        f._root.path().join("a/blobs/blobs.db"),
    )
    .unwrap();
    let mut a = ScopedPeer::reopen(
        &f._root.path().join("a"),
        f.ka,
        f.owner.public(),
        Capability::Write(NamespaceSecret::from_bytes(&[94; 32])),
    )
    .await
    .unwrap();
    let mut b = ScopedPeer::reopen(
        &f._root.path().join("b"),
        f.kb,
        f.owner.public(),
        Capability::Read(f.namespace),
    )
    .await
    .unwrap();
    let signed = SignedPolicy::sign(f.policy, &f.owner).unwrap();
    a.install_policy(&signed).unwrap();
    b.install_policy(&signed).unwrap();
    assert!(b.receive(a.addr(), author, &record.key()).await.is_err());
    assert!(b.received().unwrap().is_empty());
    a.flush().await.unwrap();
    assert!(b.receive(a.addr(), author, &record.key()).await.unwrap());
    a.doc
        .del(author, record.key().as_bytes()[..7].to_vec())
        .await
        .unwrap();
    a.peer.docs_sync.flush_store().await.unwrap();
    assert!(a.flush().await.is_err());
    assert!(b.receive(a.addr(), author, &record.key()).await.is_err());
    // Already disclosed inert data remains inspectable. Repair does not claim
    // remote erasure or turn a historical receipt into live docs metadata.
    assert_eq!(b.repair_projection().unwrap(), 0);
    assert_eq!(b.received().unwrap(), vec![record]);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
