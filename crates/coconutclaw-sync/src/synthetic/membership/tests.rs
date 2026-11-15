use super::*;
use iroh_docs::NamespaceSecret;
use tokio::time::timeout;

struct Fixture {
    _root: tempfile::TempDir,
    owner: SecretKey,
    ka: SecretKey,
    kb: SecretKey,
    namespace: NamespaceId,
    a: ScopedPeer,
    b: ScopedPeer,
    policy: Policy,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let owner = SecretKey::from([91; 32]);
        let ka = SecretKey::from([92; 32]);
        let kb = SecretKey::from([93; 32]);
        let ns = NamespaceSecret::from_bytes(&[94; 32]);
        let namespace = ns.id();
        let a = ScopedPeer::create(
            &root.path().join("a"),
            ka.clone(),
            owner.public(),
            Capability::Write(ns),
        )
        .await
        .unwrap();
        let b = ScopedPeer::create(
            &root.path().join("b"),
            kb.clone(),
            owner.public(),
            Capability::Read(namespace),
        )
        .await
        .unwrap();
        let policy = Policy {
            generation: 1,
            expires_at: now().unwrap() + 3600,
            namespace,
            scope: SCOPE.into(),
            members: vec![
                Member {
                    endpoint: ka.public(),
                    authors: BTreeSet::from([a.author().await.unwrap()]),
                },
                Member {
                    endpoint: kb.public(),
                    authors: BTreeSet::new(),
                },
            ],
        };
        Self {
            _root: root,
            owner,
            ka,
            kb,
            namespace,
            a,
            b,
            policy,
        }
    }

    fn activate(&mut self) {
        let signed = SignedPolicy::sign(self.policy.clone(), &self.owner).unwrap();
        self.a.install_policy(&signed).unwrap();
        self.b.install_policy(&signed).unwrap();
    }
}

async fn rejected(conn: &Connection, request: &Request) {
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(&serde_json::to_vec(request).unwrap())
        .await
        .unwrap();
    send.finish().unwrap();
    assert!(
        !matches!(recv.read_chunk(MAX_WIRE).await, Ok(Some(_))),
        "rejected request disclosed response bytes"
    );
}

#[tokio::test]
async fn owner_policy_exchange_restart_rollback_fork_and_stale_author() {
    timeout(Duration::from_secs(45), async {
        let mut f = Fixture::new().await;
        assert!(f.a.publish("unknown policy", b"artifact").await.is_err());
        let forged = SignedPolicy::sign(f.policy.clone(), &f.ka).unwrap();
        assert!(f.a.install_policy(&forged).is_err());
        let mut altered = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        altered.policy.members.clear();
        assert!(f.a.install_policy(&altered).is_err());
        let mut wrong_scope = f.policy.clone();
        wrong_scope.scope = "synthetic/chat-b".into();
        assert!(
            f.a.install_policy(&SignedPolicy::sign(wrong_scope, &f.owner).unwrap())
                .is_err()
        );
        f.activate();
        let old = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        let record =
            f.a.publish("DELEGATE_TASK: inert only", b"separate artifact")
                .await
                .unwrap();
        let author = f.a.author().await.unwrap();
        assert!(
            f.b.receive(f.a.addr(), author, &record.key())
                .await
                .unwrap()
        );
        assert!(
            !f.b.receive(f.a.addr(), author, &record.key())
                .await
                .unwrap()
        );
        assert_eq!(f.b.received().unwrap(), vec![record.clone()]);
        f.policy.generation = 2;
        f.activate();
        f.a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
        let mut a = ScopedPeer::reopen(
            &f._root.path().join("a"),
            f.ka,
            f.owner.public(),
            Capability::Read(f.namespace),
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
        assert!(
            b.receive(a.addr(), author, &record.key()).await.is_err(),
            "reopen must quarantine"
        );
        assert!(a.install_policy(&old).is_err());
        assert!(b.install_policy(&old).is_err());
        let mut fork = f.policy.clone();
        fork.expires_at += 1;
        assert!(
            a.install_policy(&SignedPolicy::sign(fork, &f.owner).unwrap())
                .is_err()
        );
        let current = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        a.install_policy(&current).unwrap();
        b.install_policy(&current).unwrap();
        assert!(!b.receive(a.addr(), author, &record.key()).await.unwrap());
        f.policy.generation += 1;
        f.policy.members[0].authors.clear();
        let revoked = SignedPolicy::sign(f.policy, &f.owner).unwrap();
        a.install_policy(&revoked).unwrap();
        b.install_policy(&revoked).unwrap();
        assert!(b.receive(a.addr(), author, &record.key()).await.is_err());
        // Bypass the receiver's predicate: exercise the actual serving boundary.
        let conn = b.peer.endpoint.connect(a.addr(), ALPN).await.unwrap();
        rejected(
            &conn,
            &Request {
                generation: 3,
                namespace: f.namespace,
                scope: SCOPE.into(),
                author,
                key: record.key(),
            },
        )
        .await;
        assert_eq!(
            b.received().unwrap(),
            vec![record],
            "already disclosed data is not erased"
        );
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn revocation_closes_active_connection_and_blocks_reconnect_and_retained_capabilities() {
    timeout(Duration::from_secs(45), async {
        let mut f = Fixture::new().await;
        f.activate();
        let record = f.a.publish("active revocation", b"artifact").await.unwrap();
        let author = f.a.author().await.unwrap();
        // Keep a genuine authenticated connection open before sending a request.
        let conn = f.b.peer.endpoint.connect(f.a.addr(), ALPN).await.unwrap();
        // Revocation interrupts an in-progress request, not just an idle socket.
        let (mut pending_send, mut pending_recv) = conn.open_bi().await.unwrap();
        pending_send.write_all(b"{\"generation\":1,").await.unwrap();
        // Confirm server-side registration, not merely client handshake completion.
        timeout(Duration::from_secs(5), async {
            loop {
                if !f.a.boundary.lock().unwrap().connections.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        f.policy.generation = 2;
        f.policy.members.retain(|m| m.endpoint != f.kb.public());
        f.a.install_policy(&SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap())
            .unwrap();
        timeout(Duration::from_secs(5), conn.closed())
            .await
            .unwrap();
        assert!(conn.open_bi().await.is_err());
        assert!(
            !matches!(pending_recv.read_chunk(MAX_WIRE).await, Ok(Some(_))),
            "revoked request disclosed response bytes"
        );
        // B deliberately retains old owner policy and old capabilities.
        if let Ok(reconnected) = f.b.peer.endpoint.connect(f.a.addr(), ALPN).await {
            timeout(Duration::from_secs(5), reconnected.closed())
                .await
                .unwrap();
        }
        assert!(
            f.b.receive(f.a.addr(), author, &record.key())
                .await
                .is_err()
        );
        assert!(f.b.received().unwrap().is_empty());
        f.a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn forbidden_scope_native_protocols_and_stale_generation_disclose_nothing() {
    timeout(Duration::from_secs(45), async {
        let mut f = Fixture::new().await;
        f.activate();
        let record =
            f.a.publish("scope-a only", b"scope-a secret artifact")
                .await
                .unwrap();
        let author = f.a.author().await.unwrap();
        let request = Request {
            generation: 1,
            namespace: f.namespace,
            scope: "synthetic/chat-b".into(),
            author,
            key: record.key(),
        };
        for variant in 0..3 {
            let mut req = request.clone();
            req.key = record.key();
            if variant == 1 {
                req.scope = SCOPE.into();
                req.namespace = NamespaceSecret::from_bytes(&[95; 32]).id();
            }
            if variant == 2 {
                req.scope = SCOPE.into();
                req.generation = 0;
            }
            let conn = f.b.peer.endpoint.connect(f.a.addr(), ALPN).await.unwrap();
            rejected(&conn, &req).await;
        }
        // A raw endpoint bypasses all client hooks and retains the namespace
        // ID, author, record key and artifact hash. Native capability/hash-only
        // requests must still be unavailable at the server.
        let raw = Endpoint::builder(presets::Minimal)
            .secret_key(f.kb.clone())
            .clear_ip_transports()
            .bind_addr(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .clear_address_lookup()
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        for alpn in [iroh_docs::ALPN, iroh_gossip::ALPN, iroh_blobs::ALPN] {
            if let Ok(conn) = raw.connect(f.a.addr(), alpn).await {
                timeout(Duration::from_secs(5), conn.closed())
                    .await
                    .unwrap();
                assert!(conn.open_bi().await.is_err());
            }
        }
        // Positive raw-client control: native rejection is not a routing outage.
        let conn = raw.connect(f.a.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        let allowed = Request {
            scope: SCOPE.into(),
            ..request
        };
        send.write_all(&serde_json::to_vec(&allowed).unwrap())
            .await
            .unwrap();
        send.finish().unwrap();
        let bundle: Bundle =
            serde_json::from_slice(&recv.read_to_end(MAX_WIRE).await.unwrap()).unwrap();
        assert_eq!(
            serde_json::from_slice::<Record>(&bundle.body).unwrap(),
            record
        );
        assert_eq!(bundle.artifact, b"scope-a secret artifact");
        conn.close(0u32.into(), b"positive control complete");
        raw.close().await;
        assert!(f.b.received().unwrap().is_empty());
        // Rejections did not disable the permitted content path.
        assert!(
            f.b.receive(f.a.addr(), author, &record.key())
                .await
                .unwrap()
        );
        f.a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn policy_expiry_closes_existing_connection_and_rejects_new_work() {
    timeout(Duration::from_secs(20), async {
        let mut f = Fixture::new().await;
        f.policy.expires_at = now().unwrap() + 2;
        f.activate();
        let conn = f.b.peer.endpoint.connect(f.a.addr(), ALPN).await.unwrap();
        timeout(Duration::from_secs(5), conn.closed())
            .await
            .unwrap();
        assert!(f.a.publish("expired", b"artifact").await.is_err());
        let expired = SignedPolicy::sign(f.policy, &f.owner).unwrap();
        assert!(f.a.install_policy(&expired).is_err());
        f.a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unknown_and_invalid_policy_have_no_network_access_and_failed_commit_cannot_activate() {
    timeout(Duration::from_secs(30), async {
        let mut f = Fixture::new().await;
        let raw = Endpoint::builder(presets::Minimal)
            .secret_key(f.kb.clone())
            .clear_ip_transports()
            .bind_addr(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .clear_address_lookup()
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        for invalid in [
            None,
            Some(SignedPolicy::sign(f.policy.clone(), &f.ka).unwrap()),
        ] {
            if let Some(invalid) = invalid {
                assert!(f.a.install_policy(&invalid).is_err());
            }
            if let Ok(conn) = raw.connect(f.a.addr(), ALPN).await {
                timeout(Duration::from_secs(5), conn.closed())
                    .await
                    .unwrap();
                assert!(conn.open_bi().await.is_err());
            }
        }
        raw.close().await;
        f.a.peer
            .sql
            .execute_batch(
                "CREATE TEMP TRIGGER fail_policy BEFORE UPDATE ON membership
             BEGIN SELECT RAISE(ABORT, 'synthetic policy persistence failure'); END;",
            )
            .unwrap();
        let signed = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        assert!(f.a.install_policy(&signed).is_err());
        assert!(
            f.a.publish("not authorized after failed commit", b"artifact")
                .await
                .is_err()
        );
        assert_eq!(
            f.a.peer
                .sql
                .query_row("SELECT generation FROM membership", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        f.a.peer
            .sql
            .execute_batch("DROP TRIGGER fail_policy")
            .unwrap();
        f.activate();
        let record = f.a.publish("committed policy", b"artifact").await.unwrap();
        assert!(
            f.b.receive(f.a.addr(), f.a.author().await.unwrap(), &record.key())
                .await
                .unwrap()
        );
        f.a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn scoped_reopen_repairs_pending_intent_but_never_covering_deletion() {
    timeout(Duration::from_secs(45), async {
        let mut f = Fixture::new().await;
        f.activate();
        // Inject the existing durable handoff seam, before protocol publication.
        let record =
            f.a.peer
                .prepare(&f.a.doc, "pending intent", &[255; MAX_ARTIFACT])
                .await
                .unwrap();
        let author = f.a.author().await.unwrap();
        assert!(
            f.b.receive(f.a.addr(), author, &record.key())
                .await
                .is_err()
        );
        f.a.shutdown().await.unwrap();
        let mut a = ScopedPeer::reopen(
            &f._root.path().join("a"),
            f.ka.clone(),
            f.owner.public(),
            Capability::Write(NamespaceSecret::from_bytes(&[94; 32])),
        )
        .await
        .unwrap();
        assert!(
            a.flush().await.is_err(),
            "quarantine must also block repair"
        );
        let signed = SignedPolicy::sign(f.policy, &f.owner).unwrap();
        a.install_policy(&signed).unwrap();
        a.flush().await.unwrap();
        assert!(f.b.receive(a.addr(), author, &record.key()).await.unwrap());
        assert_eq!(f.b.received().unwrap(), vec![record.clone()]);
        a.doc
            .del(author, format!("v1/synthetic/{author}/"))
            .await
            .unwrap();
        a.peer.docs_sync.flush_store().await.unwrap();
        a.shutdown().await.unwrap();
        let mut a = ScopedPeer::reopen(
            &f._root.path().join("a"),
            f.ka,
            f.owner.public(),
            Capability::Write(NamespaceSecret::from_bytes(&[94; 32])),
        )
        .await
        .unwrap();
        a.install_policy(&signed).unwrap();
        assert!(a.flush().await.is_err());
        assert!(
            a.doc
                .get_exact(author, record.key(), true)
                .await
                .unwrap()
                .is_none()
        );
        assert!(f.b.receive(a.addr(), author, &record.key()).await.is_err());
        a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
