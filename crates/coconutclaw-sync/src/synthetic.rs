//! Loopback-only, synthetic stores and inert projections. No runtime adapters.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    net::{Ipv4Addr, SocketAddrV4},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, SecretKey,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_blobs::{
    BlobsProtocol, Hash,
    api::{Store, blobs::BlobStatus},
    protocol::{ChunkRanges, ChunkRangesExt, GetRequest},
    store::fs::{FsStore, options::Options as BlobOptions},
};
use iroh_docs::{
    AuthorId, Capability, NamespaceId,
    actor::SyncHandle,
    api::Doc,
    engine::{DefaultAuthorStorage, Engine},
    protocol::Docs,
    store::DownloadPolicy,
};
use iroh_gossip::net::Gossip;
use rusqlite::{Connection as Sqlite, OptionalExtension, params};
use serde::{Deserialize, Serialize};

pub const MAX_RECORD: usize = 8192;
pub const MAX_ARTIFACT: usize = 4096;
/// Retained wire scope label. Signed origins/namespaces provide task isolation;
/// callers must still independently authorize each origin and actor.
pub const SCOPE: &str = "synthetic/chat-a";
const MARKER: &str = "CoconutClaw synthetic iroh increment 1\n";

/// This pin is local test policy, not replicated membership or owner authority.
#[derive(Debug, Clone)]
pub struct ReadPin {
    pub namespace: NamespaceId,
    pub author: AuthorId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub hash: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub version: u8,
    pub origin: String,
    pub id: u64,
    pub scope: String,
    pub text: String,
    pub artifact: Artifact,
}

impl Record {
    pub fn key(&self) -> String {
        format!("v1/synthetic/{}/{}", self.origin, self.id)
    }

    fn validate(&self, author: AuthorId, key: &[u8]) -> Result<Hash> {
        ensure!(self.version == 1, "unsupported record version");
        ensure!(
            self.origin == author.to_string(),
            "origin does not match author"
        );
        ensure!(
            self.id > 0 && self.id <= i64::MAX as u64,
            "invalid origin ID"
        );
        ensure!(self.scope == SCOPE, "unauthorized scope");
        ensure!(key == self.key().as_bytes(), "record key mismatch");
        ensure!(
            !self.text.is_empty() && self.text.len() <= 2048,
            "invalid text size"
        );
        ensure!(
            self.artifact.size > 0 && self.artifact.size <= MAX_ARTIFACT as u64,
            "invalid artifact size"
        );
        self.artifact.hash.parse().context("invalid artifact hash")
    }
}

#[derive(Debug)]
struct PinnedProtocol<P> {
    inner: P,
    peers: Arc<BTreeSet<EndpointId>>,
}

impl<P: ProtocolHandler> ProtocolHandler for PinnedProtocol<P> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if !self.peers.contains(&connection.remote_id()) {
            connection.close(1u32.into(), b"unpaired synthetic peer");
            return Ok(());
        }
        self.inner.accept(connection).await
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

/// Owns only an explicitly created synthetic directory. No real DB can be opened.
pub struct Peer {
    endpoint: Endpoint,
    router: Router,
    docs: Docs,
    docs_sync: SyncHandle,
    blobs: Store,
    sql: Sqlite,
    peers: Arc<BTreeSet<EndpointId>>,
    _lock: File,
    #[cfg(test)]
    local_relay: Option<iroh::RelayUrl>,
}

impl Peer {
    fn check_address(&self, peer: &EndpointAddr) -> Result<()> {
        ensure!(self.peers.contains(&peer.id), "unpaired synthetic peer");
        #[cfg(test)]
        if let Some(url) = &self.local_relay {
            ensure!(
                peer.addrs == BTreeSet::from([iroh::TransportAddr::Relay(url.clone())]),
                "test fixture permits only its exact local relay"
            );
            return Ok(());
        }
        ensure!(
            !peer.addrs.is_empty()
                && peer.addrs.iter().all(|addr| {
                    matches!(addr, iroh::TransportAddr::Ip(ip) if ip.ip().is_loopback())
                }),
            "synthetic transport requires explicit loopback addresses only"
        );
        Ok(())
    }

    /// Create a new directory, never adopt existing data or protocol stores.
    pub async fn create(root: &Path, key: SecretKey, peers: BTreeSet<EndpointId>) -> Result<Self> {
        fs::create_dir(root).context("synthetic directory must not already exist")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        fs::write(root.join("SYNTHETIC"), MARKER)?;
        Self::reopen(root, key, peers).await
    }

    /// The caller supplies the same synthetic endpoint identity after restart.
    /// Device/owner key persistence and recovery are deliberately not implemented.
    pub async fn reopen(root: &Path, key: SecretKey, peers: BTreeSet<EndpointId>) -> Result<Self> {
        let builder = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?
            .clear_address_lookup()
            .relay_mode(iroh::RelayMode::Disabled);
        Self::reopen_with_builder(root, key, peers, builder).await
    }

    async fn reopen_with_builder(
        root: &Path,
        key: SecretKey,
        peers: BTreeSet<EndpointId>,
        builder: iroh::endpoint::Builder,
    ) -> Result<Self> {
        Self::reopen_with_boundary(root, key, peers, builder, None).await
    }

    async fn reopen_with_boundary(
        root: &Path,
        key: SecretKey,
        peers: BTreeSet<EndpointId>,
        builder: iroh::endpoint::Builder,
        boundary: Option<membership::Boundary>,
    ) -> Result<Self> {
        ensure!(
            fs::read_to_string(root.join("SYNTHETIC"))? == MARKER,
            "not a synthetic store"
        );
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("writer.lock"))?;
        lock.try_lock_exclusive()
            .context("synthetic store already in use")?;
        Self::reopen_locked(root, key, peers, builder, boundary, lock).await
    }

    async fn reopen_locked(
        root: &Path,
        key: SecretKey,
        peers: BTreeSet<EndpointId>,
        builder: iroh::endpoint::Builder,
        boundary: Option<membership::Boundary>,
        lock: File,
    ) -> Result<Self> {
        let sql = Sqlite::open(root.join("projection.sqlite"))?;
        sql.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS identity(endpoint TEXT PRIMARY KEY);
             CREATE TABLE IF NOT EXISTS publication(
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               namespace TEXT NOT NULL, author TEXT NOT NULL,
               body BLOB NOT NULL, artifact BLOB NOT NULL, published INTEGER NOT NULL DEFAULT 0);
             CREATE TABLE IF NOT EXISTS received(
               namespace TEXT NOT NULL, author TEXT NOT NULL, key BLOB NOT NULL,
               hash TEXT NOT NULL, body BLOB NOT NULL, artifact BLOB NOT NULL,
               PRIMARY KEY(namespace,author,key));
             CREATE INDEX IF NOT EXISTS publication_namespace_id ON publication(namespace,id);",
        )?;
        let identity = key.public().to_string();
        let saved: Option<String> = sql
            .query_row("SELECT endpoint FROM identity", [], |r| r.get(0))
            .optional()?;
        if let Some(saved) = saved {
            ensure!(
                saved == identity,
                "wrong endpoint identity for synthetic store"
            );
        } else {
            sql.execute("INSERT INTO identity VALUES (?1)", [&identity])?;
        }
        fs::create_dir_all(root.join("docs"))?;
        let blob_path = root.join("blobs");
        let mut blob_options = BlobOptions::new(&blob_path);
        // Every supported publication fits wholly in redb, including its Bao
        // outboard. sync_db is a commit barrier for inline bytes, not a general
        // fsync barrier for arbitrary external blob files.
        blob_options.inline.max_data_inlined = MAX_RECORD as u64;
        blob_options.inline.max_outboard_inlined = MAX_RECORD as u64;
        let blobs = FsStore::load_with_opts(blob_path.join("blobs.db"), blob_options).await?;
        let endpoint = builder.secret_key(key).bind().await?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        // Docs::persistent hides the Engine's persistence handle in this pin.
        // Construct the same engine through its public API and retain a clone.
        let engine = Engine::spawn(
            endpoint.clone(),
            gossip.clone(),
            iroh_docs::store::Store::persistent(root.join("docs/docs.redb"))?,
            (*blobs).clone(),
            blobs.downloader(&endpoint),
            DefaultAuthorStorage::Persistent(root.join("docs/default-author")),
            None,
        )
        .await?;
        let docs_sync = engine.sync.clone();
        let docs = Docs::new(engine);
        let peers = Arc::new(peers);
        let router = Router::builder(endpoint.clone());
        let router = if let Some(boundary) = boundary {
            router.accept(
                membership::ALPN,
                membership::Protocol::new(boundary, docs.clone(), (*blobs).clone()),
            )
        } else {
            router
                .accept(
                    iroh_docs::ALPN,
                    PinnedProtocol {
                        inner: docs.clone(),
                        peers: peers.clone(),
                    },
                )
                .accept(
                    iroh_blobs::ALPN,
                    PinnedProtocol {
                        inner: BlobsProtocol::new(&blobs, None),
                        peers: peers.clone(),
                    },
                )
                .accept(
                    iroh_gossip::ALPN,
                    PinnedProtocol {
                        inner: gossip,
                        peers: peers.clone(),
                    },
                )
        };
        let router = router.spawn();
        Ok(Self {
            endpoint,
            router,
            docs,
            docs_sync,
            blobs: (*blobs).clone(),
            sql,
            peers,
            _lock: lock,
            #[cfg(test)]
            local_relay: None,
        })
    }

    pub fn addr(&self) -> EndpointAddr {
        #[cfg(test)]
        if let Some(url) = &self.local_relay {
            return EndpointAddr::new(self.endpoint.id()).with_relay_url(url.clone());
        }
        self.endpoint.addr()
    }

    pub async fn author(&self) -> Result<AuthorId> {
        self.docs.author_default().await
    }

    pub async fn create_document(&self) -> Result<Doc> {
        let doc = self.docs.create().await?;
        // Metadata must never trigger automatic unbounded content downloads.
        doc.set_download_policy(DownloadPolicy::NothingExcept(vec![]))
            .await?;
        doc.start_sync(vec![]).await?;
        Ok(doc)
    }

    pub async fn open_document(&self, id: NamespaceId) -> Result<Doc> {
        let doc = self
            .docs
            .open(id)
            .await?
            .context("synthetic document missing")?;
        doc.start_sync(vec![]).await?;
        Ok(doc)
    }

    pub async fn import_readonly(&self, pin: &ReadPin) -> Result<Doc> {
        let doc = self
            .docs
            .import_namespace(Capability::Read(pin.namespace))
            .await?;
        doc.set_download_policy(DownloadPolicy::NothingExcept(vec![]))
            .await?;
        Ok(doc)
    }

    pub async fn connect(&self, doc: &Doc, peer: EndpointAddr) -> Result<()> {
        self.check_address(&peer)?;
        // Authenticate before allowing docs to start reconciliation.
        let connection = self.endpoint.connect(peer.clone(), iroh_docs::ALPN).await?;
        ensure!(
            connection.remote_id() == peer.id,
            "wrong transport identity"
        );
        connection.close(0u32.into(), b"connectivity checked");
        doc.start_sync(vec![peer]).await?;
        Ok(())
    }

    /// Commit the application publication intent before touching protocol stores.
    pub async fn prepare(&mut self, doc: &Doc, text: &str, artifact: &[u8]) -> Result<Record> {
        ensure!(
            !artifact.is_empty() && artifact.len() <= MAX_ARTIFACT,
            "invalid artifact size"
        );
        let author = self.author().await?;
        let tx = self.sql.transaction()?;
        tx.execute(
            "INSERT INTO publication(namespace,author,body,artifact) VALUES (?1,?2,X'',?3)",
            params![doc.id().to_string(), author.to_string(), artifact],
        )?;
        let record = Record {
            version: 1,
            origin: author.to_string(),
            id: tx.last_insert_rowid() as u64,
            scope: SCOPE.into(),
            text: text.into(),
            artifact: Artifact {
                hash: Hash::new(artifact).to_string(),
                size: artifact.len() as u64,
            },
        };
        record.validate(author, record.key().as_bytes())?;
        let body = serde_json::to_vec(&record)?;
        ensure!(body.len() <= MAX_RECORD, "record too large");
        tx.execute(
            "UPDATE publication SET body=?1 WHERE id=?2",
            params![body, record.id],
        )?;
        tx.commit()?;
        Ok(record)
    }

    /// Reconcile all intents, including receipts from before persistence barriers.
    /// Each iteration owns one bounded record; no total history cap skips repair.
    /// Duplicate protocol writes cannot alter an event.
    pub async fn flush(&self, doc: &Doc) -> Result<()> {
        let mut cursor = 0i64;
        loop {
            let row = self
                .sql
                .query_row(
                    "SELECT id,author,body,artifact FROM publication
                 WHERE namespace=?1 AND id>?2 ORDER BY id LIMIT 1",
                    params![doc.id().to_string(), cursor],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Vec<u8>>(2)?,
                            r.get::<_, Vec<u8>>(3)?,
                        ))
                    },
                )
                .optional()?;
            let Some((id, author, body, artifact)) = row else {
                break;
            };
            ensure!(body.len() <= MAX_RECORD, "publication record too large");
            let author: AuthorId = author.parse()?;
            let record: Record = serde_json::from_slice(&body)?;
            let hash = record.validate(author, record.key().as_bytes())?;
            ensure!(record.id == id as u64, "publication ID mismatch");
            ensure!(
                artifact.len() as u64 == record.artifact.size,
                "publication artifact size mismatch"
            );
            ensure!(Hash::new(&artifact) == hash, "publication artifact corrupt");
            let body_hash = Hash::new(&body);
            let existing = doc.get_exact(author, record.key(), true).await?;
            if let Some(existing) = &existing {
                ensure!(
                    existing.content_hash() == body_hash
                        && existing.content_len() == body.len() as u64,
                    "immutable publication conflict"
                );
            } else {
                // get_exact does not return a covering deletion. Check every
                // proper byte prefix, including empty, for this author in this
                // namespace before interpreting absence as lost metadata.
                // The validated record key bounds the number of indexed reads;
                // no namespace-wide metadata scan or arbitrary repair cap.
                let key = record.key();
                for end in 0..key.len() {
                    if let Some(prefix) =
                        doc.get_exact(author, &key.as_bytes()[..end], true).await?
                    {
                        ensure!(prefix.content_len() != 0, "immutable publication conflict");
                    }
                }
            }
            // Reimport both even when metadata exists: a historical receipt is
            // not proof that the separately buffered blob transaction survived.
            let artifact_tag = self.blobs.blobs().add_slice(&artifact).await?;
            let body_tag = self.blobs.blobs().add_slice(&body).await?;
            self.blobs.sync_db().await?;
            if existing.is_none() {
                doc.set_hash(author, record.key(), body_hash, body.len() as u64)
                    .await?;
            }
            self.docs_sync.flush_store().await?;
            self.sql
                .execute("UPDATE publication SET published=1 WHERE id=?1", [id])?;
            drop((artifact_tag, body_tag));
            cursor = id;
        }
        Ok(())
    }

    /// Project only exact pinned events with complete, verified bounded content.
    /// This table has no connection to runtime memory, jobs, prompts or delivery.
    pub async fn project(&mut self, doc: &Doc, pin: &ReadPin, key: &[u8]) -> Result<bool> {
        ensure!(doc.id() == pin.namespace, "unauthorized namespace");
        ensure!(key.len() <= 200, "key too long");
        let entry = doc
            .get_exact(pin.author, key, false)
            .await?
            .context("metadata absent")?;
        ensure!(
            entry.content_len() > 0 && entry.content_len() <= MAX_RECORD as u64,
            "invalid record size"
        );
        let body = self
            .verified(entry.content_hash(), entry.content_len(), MAX_RECORD)
            .await?;
        let record: Record = serde_json::from_slice(&body)?;
        let hash = record.validate(pin.author, key)?;
        let artifact = self
            .verified(hash, record.artifact.size, MAX_ARTIFACT)
            .await?;
        let tx = self.sql.transaction()?;
        let old: Option<String> = tx
            .query_row(
                "SELECT hash FROM received WHERE namespace=?1 AND author=?2 AND key=?3",
                params![doc.id().to_string(), pin.author.to_string(), key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            ensure!(
                old == entry.content_hash().to_string(),
                "immutable projection conflict"
            );
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO received VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                doc.id().to_string(),
                pin.author.to_string(),
                key,
                entry.content_hash().to_string(),
                body,
                artifact
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    async fn verified(&self, hash: Hash, size: u64, limit: usize) -> Result<Vec<u8>> {
        ensure!(size > 0 && size <= limit as u64, "content exceeds limit");
        match self.blobs.blobs().status(hash).await? {
            BlobStatus::Complete { size: actual } if actual == size => {}
            _ => bail!("verified content absent or size mismatch"),
        }
        let bytes = self.blobs.blobs().get_bytes(hash).await?;
        ensure!(
            bytes.len() as u64 == size && Hash::new(&bytes) == hash,
            "content integrity mismatch"
        );
        Ok(bytes.to_vec())
    }

    /// Explicit bounded fetch, never triggered by metadata arrival.
    pub async fn fetch(
        &self,
        hash: Hash,
        size: u64,
        limit: usize,
        peer: EndpointAddr,
    ) -> Result<()> {
        ensure!(
            limit <= MAX_RECORD && size > 0 && size <= limit as u64,
            "invalid download size"
        );
        self.check_address(&peer)?;
        // Connect using explicit loopback addresses to seed the endpoint's path cache.
        let connection = self
            .endpoint
            .connect(peer.clone(), iroh_blobs::ALPN)
            .await?;
        let request = GetRequest::builder()
            .root(ChunkRanges::bytes(..size))
            .build(hash);
        self.blobs
            .downloader(&self.endpoint)
            .download(request, vec![peer.id])
            .await?;
        drop(connection);
        self.verified(hash, size, limit).await?;
        Ok(())
    }

    pub fn received(&self) -> Result<Vec<Record>> {
        let mut stmt = self
            .sql
            .prepare("SELECT body FROM received ORDER BY author,key")?;
        let bodies = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        bodies
            .map(|body| Ok(serde_json::from_slice(&body?)?))
            .collect()
    }

    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

pub mod membership;
