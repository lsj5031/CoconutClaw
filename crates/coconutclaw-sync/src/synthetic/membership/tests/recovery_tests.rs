use super::*;
use crate::synthetic::membership::recovery::RecoveryManifest;

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

#[tokio::test]
async fn whole_backup_rollback_cannot_reinstall_still_valid_revoked_authority() {
    timeout(Duration::from_secs(45), async {
        let mut f = Fixture::new().await;
        f.activate();
        let old = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        let record = f.a.publish("old author", b"artifact").await.unwrap();
        let author = f.a.author().await.unwrap();
        f.a.shutdown().await.unwrap();
        let root = f._root.path().join("a");
        let old_manifest = RecoveryManifest::capture(&root).unwrap();
        copy_tree(&root, &f._root.path().join("old-backup"));
        let mut a = ScopedPeer::reopen(
            &root,
            f.ka.clone(),
            f.owner.public(),
            Capability::Write(NamespaceSecret::from_bytes(&[94; 32])),
        )
        .await
        .unwrap();
        f.policy.generation = 2;
        f.policy.members[0].authors.clear();
        let current = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        a.install_policy(&current).unwrap();
        a.shutdown().await.unwrap();
        let current_manifest = RecoveryManifest::capture(&root).unwrap();
        let latest = current_manifest.hash().unwrap();

        let rolled = f._root.path().join("old-backup");
        // An internally valid, signed old backup is not the independent latest.
        assert!(
            ScopedPeer::restore(
                &rolled,
                f.ka.clone(),
                f.owner.public(),
                Capability::Read(f.namespace),
                Some((&old_manifest, latest)),
            )
            .await
            .is_err()
        );
        // Nor may current authority bless an inconsistent older checkpoint.
        assert!(
            ScopedPeer::restore(
                &rolled,
                f.ka.clone(),
                f.owner.public(),
                Capability::Read(f.namespace),
                Some((&current_manifest, latest)),
            )
            .await
            .is_err()
        );
        let mut restored = ScopedPeer::reopen(
            &rolled,
            f.ka.clone(),
            f.owner.public(),
            Capability::Read(f.namespace),
        )
        .await
        .unwrap();
        assert!(restored.install_policy(&old).is_err());
        assert!(
            restored
                .install_recovery_policy(&old, &old_manifest, latest)
                .is_err()
        );
        assert!(restored.publish("no authority", b"x").await.is_err());
        assert!(restored.flush().await.is_err());
        assert!(
            f.b.receive(restored.addr(), author, &record.key())
                .await
                .is_err()
        );
        assert!(f.b.received().unwrap().is_empty());
        if let Ok(conn) = f.b.peer.endpoint.connect(restored.addr(), ALPN).await {
            timeout(Duration::from_secs(3), conn.closed())
                .await
                .unwrap();
            assert!(conn.open_bi().await.is_err());
        }
        restored.shutdown().await.unwrap();
        // The exact current backup admits only its anchored current policy.
        let mut restored = ScopedPeer::restore(
            &root,
            f.ka,
            f.owner.public(),
            Capability::Read(f.namespace),
            Some((&current_manifest, latest)),
        )
        .await
        .unwrap();
        assert!(restored.install_policy(&old).is_err());
        assert!(
            restored
                .install_recovery_policy(&old, &current_manifest, latest)
                .is_err()
        );
        restored
            .install_recovery_policy(&current, &current_manifest, latest)
            .unwrap();
        f.b.install_policy(&current).unwrap();
        assert!(restored.publish("revoked author", b"x").await.is_err());
        assert!(
            f.b.receive(restored.addr(), author, &record.key())
                .await
                .is_err()
        );
        let conn =
            f.b.peer
                .endpoint
                .connect(restored.addr(), ALPN)
                .await
                .unwrap();
        rejected(
            &conn,
            &Request {
                generation: 2,
                namespace: f.namespace,
                scope: SCOPE.into(),
                author,
                key: record.key(),
            },
        )
        .await;
        // Revoking the endpoint also fences an already established connection.
        f.policy.generation = 3;
        f.policy.members.remove(1);
        restored
            .install_policy(&SignedPolicy::sign(f.policy, &f.owner).unwrap())
            .unwrap();
        timeout(Duration::from_secs(3), conn.closed())
            .await
            .unwrap();
        if let Ok(conn) = f.b.peer.endpoint.connect(restored.addr(), ALPN).await {
            timeout(Duration::from_secs(3), conn.closed())
                .await
                .unwrap();
            assert!(conn.open_bi().await.is_err());
        }
        assert!(f.b.received().unwrap().is_empty());
        restored.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn restore_rejects_missing_wrong_identity_fork_and_missing_data() {
    timeout(Duration::from_secs(45), async {
        let mut f = Fixture::new().await;
        f.activate();
        f.a.publish("checkpoint", b"artifact").await.unwrap();
        f.a.shutdown().await.unwrap();
        let root = f._root.path().join("a");
        let manifest = RecoveryManifest::capture(&root).unwrap();
        let latest = manifest.hash().unwrap();
        #[cfg(unix)]
        for name in [
            "RESTORE_REQUIRED.tmp",
            "RESTORE_REQUIRED",
            "writer.lock",
            "unexpected-link",
        ] {
            let linked = f._root.path().join(format!("linked-{name}"));
            copy_tree(&root, &linked);
            let external = f._root.path().join(format!("external-{name}"));
            fs::write(&external, b"preserve external data").unwrap();
            let path = linked.join(name);
            if path.exists() {
                fs::remove_file(&path).unwrap();
            }
            std::os::unix::fs::symlink(&external, path).unwrap();
            assert!(
                ScopedPeer::restore(
                    &linked,
                    f.ka.clone(),
                    f.owner.public(),
                    Capability::Read(f.namespace),
                    Some((&manifest, latest)),
                )
                .await
                .is_err()
            );
            assert_eq!(fs::read(external).unwrap(), b"preserve external data");
        }
        // Interrupted latch writes must not fall back to ordinary policy install.
        let interrupted = f._root.path().join("interrupted");
        copy_tree(&root, &interrupted);
        fs::write(interrupted.join("RESTORE_REQUIRED.tmp"), b"{").unwrap();
        let mut interrupted = ScopedPeer::reopen(
            &interrupted,
            f.ka.clone(),
            f.owner.public(),
            Capability::Read(f.namespace),
        )
        .await
        .unwrap();
        let signed = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        assert!(interrupted.install_policy(&signed).is_err());
        assert!(
            interrupted
                .install_recovery_policy(&signed, &manifest, latest)
                .is_err()
        );
        interrupted.shutdown().await.unwrap();
        assert!(
            ScopedPeer::restore(
                &root,
                f.ka.clone(),
                f.owner.public(),
                Capability::Read(f.namespace),
                None,
            )
            .await
            .is_err()
        );
        let mut missing = ScopedPeer::reopen(
            &root,
            f.ka.clone(),
            f.owner.public(),
            Capability::Read(f.namespace),
        )
        .await
        .unwrap();
        assert!(missing.install_policy(&signed).is_err());
        assert!(
            missing
                .install_recovery_policy(&signed, &manifest, latest)
                .is_err()
        );
        missing.shutdown().await.unwrap();
        // Reopening changes storage-engine files. Adopt a fresh exact checkpoint
        // rather than pretending the old bytes still describe the current data.
        let manifest = RecoveryManifest::capture(&root).unwrap();
        let latest = manifest.hash().unwrap();
        for field in 0..4 {
            let mut wrong = manifest.clone();
            match field {
                0 => wrong.owner = f.kb.public(),
                1 => wrong.store = f.kb.public(),
                2 => wrong.namespace = NamespaceSecret::from_bytes(&[21; 32]).id(),
                _ => wrong.scope = "synthetic/chat-b".into(),
            }
            assert!(
                ScopedPeer::restore(
                    &root,
                    f.ka.clone(),
                    f.owner.public(),
                    Capability::Read(f.namespace),
                    Some((&wrong, wrong.hash().unwrap())),
                )
                .await
                .is_err()
            );
        }
        for missing in ["projection.sqlite", "docs/docs.redb", "blobs/blobs.db"] {
            let broken = f
                ._root
                .path()
                .join(format!("broken-{}", missing.replace('/', "-")));
            copy_tree(&root, &broken);
            fs::remove_file(broken.join(missing)).unwrap();
            assert!(
                ScopedPeer::restore(
                    &broken,
                    f.ka.clone(),
                    f.owner.public(),
                    Capability::Read(f.namespace),
                    Some((&manifest, latest)),
                )
                .await
                .is_err()
            );
            assert!(
                !broken.join(missing).exists(),
                "restore recreated missing authoritative data"
            );
        }
        let mut a = ScopedPeer::restore(
            &root,
            f.ka,
            f.owner.public(),
            Capability::Read(f.namespace),
            Some((&manifest, latest)),
        )
        .await
        .unwrap();
        let mut fork = f.policy.clone();
        fork.expires_at += 1;
        let fork = SignedPolicy::sign(fork, &f.owner).unwrap();
        assert!(a.install_recovery_policy(&fork, &manifest, latest).is_err());
        // Even an independently pinned fork cannot bypass the intact SQL fence.
        let mut fork_manifest = manifest.clone();
        fork_manifest.policy_hash = Hash::new(policy_bytes(&fork.policy).unwrap());
        assert!(
            a.install_recovery_policy(&fork, &fork_manifest, fork_manifest.hash().unwrap())
                .is_err()
        );
        assert!(a.boundary.lock().unwrap().active.is_none());
        a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn restore_commit_failure_restart_and_monotonic_activation() {
    timeout(Duration::from_secs(45), async {
        let mut f = Fixture::new().await;
        f.activate();
        let record = f.a.publish("restored", b"artifact").await.unwrap();
        let author = f.a.author().await.unwrap();
        f.a.shutdown().await.unwrap();
        let root = f._root.path().join("a");
        let manifest = RecoveryManifest::capture(&root).unwrap();
        let latest = manifest.hash().unwrap();
        let signed = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        let mut a = ScopedPeer::restore(
            &root,
            f.ka.clone(),
            f.owner.public(),
            Capability::Read(f.namespace),
            Some((&manifest, latest)),
        )
        .await
        .unwrap();
        a.peer
            .sql
            .execute_batch(
                "CREATE TRIGGER reject_recovery BEFORE UPDATE ON membership
             BEGIN SELECT RAISE(ABORT,'injected recovery commit failure'); END;",
            )
            .unwrap();
        assert!(
            a.install_recovery_policy(&signed, &manifest, latest)
                .is_err()
        );
        assert!(f.b.receive(a.addr(), author, &record.key()).await.is_err());
        assert!(a.boundary.lock().unwrap().active.is_none());
        a.shutdown().await.unwrap();
        let mut a = ScopedPeer::reopen(
            &root,
            f.ka.clone(),
            f.owner.public(),
            Capability::Read(f.namespace),
        )
        .await
        .unwrap();
        assert!(a.install_policy(&signed).is_err());
        assert!(f.b.receive(a.addr(), author, &record.key()).await.is_err());
        a.peer
            .sql
            .execute_batch("DROP TRIGGER reject_recovery")
            .unwrap();
        a.install_recovery_policy(&signed, &manifest, latest)
            .unwrap();
        assert!(f.b.receive(a.addr(), author, &record.key()).await.unwrap());
        f.policy.generation = 2;
        let current = SignedPolicy::sign(f.policy.clone(), &f.owner).unwrap();
        a.install_policy(&current).unwrap();
        a.shutdown().await.unwrap();
        let mut a =
            ScopedPeer::reopen(&root, f.ka, f.owner.public(), Capability::Read(f.namespace))
                .await
                .unwrap();
        assert!(
            a.install_recovery_policy(&signed, &manifest, latest)
                .is_err()
        );
        let mut updated = manifest.clone();
        updated.generation = 2;
        updated.policy_hash = Hash::new(policy_bytes(&current.policy).unwrap());
        assert!(
            a.install_recovery_policy(&signed, &updated, updated.hash().unwrap())
                .is_err()
        );
        a.install_recovery_policy(&current, &updated, updated.hash().unwrap())
            .unwrap();
        f.b.install_policy(&current).unwrap();
        assert!(!f.b.receive(a.addr(), author, &record.key()).await.unwrap());
        assert_eq!(f.b.received().unwrap(), vec![record]);
        a.shutdown().await.unwrap();
        f.b.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
