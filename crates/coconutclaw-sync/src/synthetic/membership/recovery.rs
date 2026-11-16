//! Exact stopped-store adoption, separate from policy authority.
//!
//! The hash pin MUST come from an independently trusted, current owner channel,
//! outside the rollbackable backup. This module cannot establish its freshness.
//! Neither a manifest nor a signature found in the backup is such a channel.

use super::*;
use std::io::{Read, Write};

const RECEIPT: &str = "RESTORE_REQUIRED";
const RECEIPT_TMP: &str = "RESTORE_REQUIRED.tmp";

pub(super) enum RecoveryState {
    Ordinary,
    Quarantined,
    Verified(RecoveryManifest),
}

/// An exact synthetic backup checkpoint and the owner policy required to use it.
/// Store identity is the existing persistent endpoint, not a runtime core ID.
/// This bounded fixture does not support device rotation or application restore.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryManifest {
    pub owner: EndpointId,
    pub store: EndpointId,
    pub namespace: NamespaceId,
    pub scope: String,
    pub generation: u64,
    pub policy_hash: Hash,
    pub checkpoint: Hash,
}

impl RecoveryManifest {
    /// Capture only a stopped, exclusively locked store. Export this manifest
    /// and its pin OUTSIDE the backup, and independently establish that the
    /// policy is current. Capture itself makes no latestness assertion.
    pub fn capture(root: &Path) -> Result<Self> {
        let _lock = lock(root)?;
        let sql = Sqlite::open_with_flags(
            root.join("projection.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let store: String = sql.query_row("SELECT endpoint FROM identity", [], |r| r.get(0))?;
        let (owner, namespace, generation, policy): (String, String, u64, Vec<u8>) = sql
            .query_row(
                "SELECT owner,namespace,generation,policy FROM membership WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
        ensure!(generation > 0, "no accepted policy to checkpoint");
        drop(sql);
        Ok(Self {
            owner: owner.parse()?,
            store: store.parse()?,
            namespace: namespace.parse()?,
            scope: SCOPE.into(),
            generation,
            policy_hash: Hash::new(policy),
            checkpoint: checkpoint(root)?,
        })
    }

    /// A content pin, NOT proof that this is the latest manifest.
    pub fn hash(&self) -> Result<Hash> {
        let mut bytes = b"CoconutClaw synthetic recovery manifest v1\0".to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        Ok(Hash::new(bytes))
    }
}

pub(super) fn lock(root: &Path) -> Result<File> {
    ensure!(
        fs::read_to_string(root.join("SYNTHETIC"))? == MARKER,
        "not a synthetic store"
    );
    regular_if_present(&root.join("writer.lock"))?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("writer.lock"))?;
    lock.try_lock_exclusive()
        .context("synthetic store already in use")?;
    Ok(lock)
}

pub(super) fn read_receipt(root: &Path) -> Result<RecoveryState> {
    // A process may die while durably latching quarantine, before rename.
    // Never reinterpret a partly written latch as an ordinary restart.
    regular_if_present(&root.join(RECEIPT_TMP))?;
    regular_if_present(&root.join(RECEIPT))?;
    if root.join(RECEIPT_TMP).try_exists()? {
        return Ok(RecoveryState::Quarantined);
    }
    match fs::read(root.join(RECEIPT)) {
        Ok(bytes) => Ok(match serde_json::from_slice(&bytes)? {
            Some(manifest) => RecoveryState::Verified(manifest),
            None => RecoveryState::Quarantined,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(RecoveryState::Ordinary),
        Err(e) => Err(e.into()),
    }
}

fn write_receipt(root: &Path, manifest: Option<&RecoveryManifest>) -> Result<()> {
    regular_if_present(&root.join(RECEIPT_TMP))?;
    regular_if_present(&root.join(RECEIPT))?;
    let mut file = File::create(root.join(RECEIPT_TMP))?;
    file.write_all(&serde_json::to_vec(&manifest)?)?;
    file.sync_all()?;
    fs::rename(root.join(RECEIPT_TMP), root.join(RECEIPT))?;
    File::open(root)?.sync_all()?;
    Ok(())
}

fn regular_if_present(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => ensure!(metadata.file_type().is_file(), "non-regular recovery file"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

// Hash the exact inventory, not just SQL high-water marks. No protocol open,
// reconstruction, missing-file repair or history deletion occurs here.
fn checkpoint(root: &Path) -> Result<Hash> {
    for required in [
        "SYNTHETIC",
        "projection.sqlite",
        "docs/docs.redb",
        "docs/default-author",
        "blobs/blobs.db",
        "record-journal.sqlite",
    ] {
        ensure!(
            root.join(required).is_file(),
            "incomplete backup: {required}"
        );
    }
    fn visit(root: &Path, dir: &Path, files: &mut Vec<(String, Option<Hash>)>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let relative = path
                .strip_prefix(root)?
                .to_str()
                .context("non-UTF8 backup path")?;
            if matches!(relative, "writer.lock" | RECEIPT | RECEIPT_TMP) {
                continue;
            }
            let kind = entry.file_type()?;
            if kind.is_dir() {
                files.push((relative.to_owned(), None));
                visit(root, &path, files)?;
            } else {
                ensure!(kind.is_file(), "non-regular backup entry");
                let mut file = File::open(&path)?;
                let mut hasher = blake3::Hasher::new();
                let mut buffer = [0; 65536];
                loop {
                    let count = file.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    hasher.update(&buffer[..count]);
                }
                files.push((
                    relative.to_owned(),
                    Some(Hash::from(*hasher.finalize().as_bytes())),
                ));
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, root, &mut files)?;
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(Hash::new(serde_json::to_vec(&files)?))
}

impl ScopedPeer {
    /// Adopt a whole backup, never ordinary `reopen`. Before opening any mutable
    /// store, durably latch quarantine and verify the exact checkpoint under the
    /// same exclusive lock retained by the peer. Missing/untrusted input fails.
    ///
    /// The supplied hash must be independently established as CURRENT, not read
    /// from this backup, obtained from a first peer, or inferred from its age.
    pub async fn restore(
        root: &Path,
        key: SecretKey,
        owner: EndpointId,
        capability: Capability,
        trusted: Option<(&RecoveryManifest, Hash)>,
    ) -> Result<Self> {
        let lock = lock(root)?;
        write_receipt(root, None)?;
        let (manifest, independent_latest) =
            trusted.context("independent freshness unavailable")?;
        ensure!(
            manifest.hash()? == independent_latest,
            "untrusted or stale recovery manifest"
        );
        ensure!(
            manifest.owner == owner
                && manifest.store == key.public()
                && manifest.namespace == capability.id()
                && manifest.scope == SCOPE,
            "recovery identity mismatch"
        );
        ensure!(
            manifest.checkpoint == checkpoint(root)?,
            "recovery checkpoint mismatch"
        );
        write_receipt(root, Some(manifest))?;
        Self::open_locked(root, key, owner, capability, lock).await
    }

    /// A receipt proves only prior data verification. Authority activation needs
    /// the independently current pin AGAIN after every restart. A newer manifest
    /// may bind the same verified checkpoint to a newer owner policy.
    pub fn install_recovery_policy(
        &mut self,
        signed: &SignedPolicy,
        manifest: &RecoveryManifest,
        independent_latest: Hash,
    ) -> Result<()> {
        let RecoveryState::Verified(receipt) = &self.recovery else {
            bail!("no verified recovery checkpoint");
        };
        ensure!(
            manifest.hash()? == independent_latest,
            "untrusted or stale recovery manifest"
        );
        ensure!(
            manifest.owner == self.boundary.owner
                && manifest.store == self.boundary.local
                && manifest.namespace == self.boundary.namespace
                && manifest.scope == SCOPE
                && manifest.checkpoint == receipt.checkpoint
                && manifest.generation >= receipt.generation,
            "recovery binding mismatch"
        );
        ensure!(
            signed.policy.generation == manifest.generation
                && Hash::new(policy_bytes(&signed.policy)?) == manifest.policy_hash,
            "policy does not match independent recovery anchor"
        );
        self.persist_policy(signed)
    }
}
