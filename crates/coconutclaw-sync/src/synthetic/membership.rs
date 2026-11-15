//! Owner-authorized, one-scope synthetic record transfer.
//! Native namespace capabilities and hash-only blob serving are never exposed.

use super::*;
use std::{
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use iroh::{
    Signature,
    endpoint::{AfterHandshakeOutcome, BeforeConnectOutcome, EndpointHooks, WeakConnectionHandle},
};

pub const ALPN: &[u8] = b"/coconutclaw/synthetic-record/1";
const MAX_WIRE: usize = 65536;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub endpoint: EndpointId,
    /// Only these docs authors may originate records on this endpoint.
    pub authors: BTreeSet<AuthorId>,
}

/// A complete snapshot for exactly the existing synthetic chat scope.
/// Absence revokes a member/author. This is not a general policy language.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub generation: u64,
    pub expires_at: u64,
    pub namespace: NamespaceId,
    pub scope: String,
    pub members: Vec<Member>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedPolicy {
    pub policy: Policy,
    pub signature: Signature,
}

impl SignedPolicy {
    pub fn sign(policy: Policy, owner: &SecretKey) -> Result<Self> {
        Ok(Self {
            signature: owner.sign(&policy_bytes(&policy)?),
            policy,
        })
    }
}

fn policy_bytes(policy: &Policy) -> Result<Vec<u8>> {
    let mut bytes = b"CoconutClaw synthetic membership v1\0".to_vec();
    bytes.extend(serde_json::to_vec(policy)?);
    ensure!(bytes.len() <= MAX_WIRE, "policy too large");
    Ok(bytes)
}

fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

#[derive(Debug)]
struct Active {
    policy: SignedPolicy,
    deadline: Instant,
}

#[derive(Debug, Default)]
struct State {
    active: Option<Active>,
    connections: Vec<WeakConnectionHandle>,
}

/// No Endpoint is retained here. Hooks track weak connection handles, including
/// internal outgoing connections, not just connections made by public methods.
#[derive(Debug, Clone)]
pub(super) struct Boundary {
    owner: EndpointId,
    local: EndpointId,
    namespace: NamespaceId,
    state: Arc<Mutex<State>>,
}

impl Boundary {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("policy lock poisoned"))
    }

    fn policy<'a>(&self, state: &'a State) -> Result<&'a Policy> {
        let active = state.active.as_ref().context("policy unknown")?;
        ensure!(
            Instant::now() < active.deadline && now()? < active.policy.policy.expires_at,
            "policy expired"
        );
        ensure!(
            active
                .policy
                .policy
                .members
                .iter()
                .any(|m| m.endpoint == self.local),
            "local endpoint revoked"
        );
        Ok(&active.policy.policy)
    }

    fn allowed(&self, state: &State, remote: EndpointId) -> Result<()> {
        ensure!(
            self.policy(state)?
                .members
                .iter()
                .any(|m| m.endpoint == remote),
            "remote endpoint revoked"
        );
        Ok(())
    }

    fn author(&self, state: &State, endpoint: EndpointId, author: AuthorId) -> Result<()> {
        ensure!(
            self.policy(state)?
                .members
                .iter()
                .any(|m| m.endpoint == endpoint && m.authors.contains(&author)),
            "stale or unauthorized author"
        );
        Ok(())
    }

    fn verify(&self, signed: &SignedPolicy) -> Result<()> {
        let p = &signed.policy;
        self.owner.verify(&policy_bytes(p)?, &signed.signature)?;
        ensure!(
            p.generation > 0 && p.generation <= i64::MAX as u64,
            "invalid generation"
        );
        ensure!(
            p.namespace == self.namespace && p.scope == SCOPE,
            "wrong policy scope"
        );
        let time = now()?;
        ensure!(
            p.expires_at > time && p.expires_at - time <= 86400,
            "policy expired or lease exceeds one day"
        );
        ensure!(p.members.len() <= 32, "too many synthetic members");
        let mut endpoints = BTreeSet::new();
        let mut authors = BTreeSet::new();
        for m in &p.members {
            ensure!(endpoints.insert(m.endpoint), "duplicate member");
            ensure!(m.authors.len() <= 4, "too many authors");
            for a in &m.authors {
                ensure!(authors.insert(*a), "author bound to multiple endpoints");
            }
        }
        Ok(())
    }
}

impl EndpointHooks for Boundary {
    async fn before_connect(&self, remote: &EndpointAddr, alpn: &[u8]) -> BeforeConnectOutcome {
        // Prevent docs/gossip background paths, native blob downloaders and
        // retained namespace capabilities from bypassing the record boundary.
        if alpn == ALPN
            && !remote.addrs.is_empty()
            && remote
                .addrs
                .iter()
                .all(|a| matches!(a, iroh::TransportAddr::Ip(ip) if ip.ip().is_loopback()))
            && self
                .lock()
                .and_then(|s| self.allowed(&s, remote.id))
                .is_ok()
        {
            BeforeConnectOutcome::Accept
        } else {
            BeforeConnectOutcome::Reject
        }
    }

    async fn after_handshake(&self, conn: &Connection) -> AfterHandshakeOutcome {
        let accepted = (|| -> Result<Instant> {
            ensure!(conn.alpn() == ALPN, "native protocols disabled");
            let mut state = self.lock()?;
            self.allowed(&state, conn.remote_id())?;
            state.connections.retain(|c| c.upgrade().is_some());
            state.connections.push(conn.weak_handle());
            Ok(state.active.as_ref().context("policy unknown")?.deadline)
        })();
        let Ok(deadline) = accepted else {
            return AfterHandshakeOutcome::Reject {
                error_code: 1u32.into(),
                reason: b"membership required".to_vec(),
            };
        };
        let weak = conn.weak_handle();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline.into()) => {
                    if let Some(conn) = weak.upgrade() {
                        conn.close(1u32.into(), b"policy expired");
                    }
                }
                _ = weak.closed() => {}
            }
        });
        AfterHandshakeOutcome::Accept
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    generation: u64,
    namespace: NamespaceId,
    scope: String,
    author: AuthorId,
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bundle {
    body: Vec<u8>,
    artifact: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(super) struct Protocol {
    boundary: Boundary,
    docs: Docs,
    blobs: Store,
}

impl Protocol {
    pub(super) fn new(boundary: Boundary, docs: Docs, blobs: Store) -> Self {
        Self {
            boundary,
            docs,
            blobs,
        }
    }

    async fn serve(&self, conn: &Connection) -> Result<()> {
        let (mut send, mut recv) = conn.accept_bi().await?;
        let request: Request = serde_json::from_slice(&recv.read_to_end(1024).await?)?;
        {
            let state = self.boundary.lock()?;
            self.boundary.allowed(&state, conn.remote_id())?;
            let p = self.boundary.policy(&state)?;
            ensure!(
                request.generation == p.generation
                    && request.namespace == p.namespace
                    && request.scope == p.scope,
                "stale capability or forbidden scope"
            );
            self.boundary
                .author(&state, self.boundary.local, request.author)?;
        }
        ensure!(request.key.len() <= 200, "key too long");
        let doc = self
            .docs
            .open(request.namespace)
            .await?
            .context("document absent")?;
        let entry = doc
            .get_exact(request.author, &request.key, false)
            .await?
            .context("metadata absent")?;
        let body = verified(
            &self.blobs,
            entry.content_hash(),
            entry.content_len(),
            MAX_RECORD,
        )
        .await?;
        let record: Record = serde_json::from_slice(&body)?;
        let artifact_hash = record.validate(request.author, request.key.as_bytes())?;
        let artifact = verified(
            &self.blobs,
            artifact_hash,
            record.artifact.size,
            MAX_ARTIFACT,
        )
        .await?;
        let response = serde_json::to_vec(&Bundle { body, artifact })?;
        ensure!(response.len() <= MAX_WIRE, "response too large");
        // A policy update closes this tracked connection before returning.
        // Bytes already handed to the transport cannot be retroactively erased.
        {
            let state = self.boundary.lock()?;
            self.boundary.allowed(&state, conn.remote_id())?;
            ensure!(
                self.boundary.policy(&state)?.generation == request.generation,
                "policy changed"
            );
            self.boundary
                .author(&state, self.boundary.local, request.author)?;
        }
        send.write_all(&response).await?;
        send.finish()?;
        send.stopped().await?;
        Ok(())
    }
}

impl ProtocolHandler for Protocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        // One bounded request per connection; a peer cannot hold policy updates
        // hostage with a partially sent request or an unread response.
        if !matches!(
            tokio::time::timeout(Duration::from_secs(10), self.serve(&conn)).await,
            Ok(Ok(()))
        ) {
            conn.close(1u32.into(), b"record request rejected");
        }
        Ok(())
    }
}

async fn verified(blobs: &Store, hash: Hash, size: u64, limit: usize) -> Result<Vec<u8>> {
    ensure!(size > 0 && size <= limit as u64, "invalid content size");
    ensure!(
        matches!(blobs.blobs().status(hash).await?, BlobStatus::Complete {size: actual} if actual == size),
        "content absent or size mismatch"
    );
    let bytes = blobs.blobs().get_bytes(hash).await?;
    ensure!(
        bytes.len() as u64 == size && Hash::new(&bytes) == hash,
        "content corrupt"
    );
    Ok(bytes.to_vec())
}

/// This wrapper never returns a Doc, Store, Endpoint or a namespace capability.
/// Its private Peer retains the reviewed local publication handoff unchanged.
pub struct ScopedPeer {
    peer: Peer,
    doc: Doc,
    boundary: Boundary,
}

impl ScopedPeer {
    pub async fn create(
        root: &Path,
        key: SecretKey,
        owner: EndpointId,
        capability: Capability,
    ) -> Result<Self> {
        fs::create_dir(root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        fs::write(root.join("SYNTHETIC"), MARKER)?;
        Self::reopen(root, key, owner, capability).await
    }

    /// Reopen quarantined. Persisted high-water marks are not a freshness oracle:
    /// the caller must supply independently trusted current owner policy through
    /// install_policy before any exchange. Backup recovery is not implemented.
    pub async fn reopen(
        root: &Path,
        key: SecretKey,
        owner: EndpointId,
        capability: Capability,
    ) -> Result<Self> {
        let namespace = capability.id();
        let boundary = Boundary {
            owner,
            local: key.public(),
            namespace,
            state: Arc::new(Mutex::new(State::default())),
        };
        let builder = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?
            .clear_address_lookup()
            .relay_mode(iroh::RelayMode::Disabled)
            .hooks(boundary.clone());
        let peer =
            Peer::reopen_with_boundary(root, key, BTreeSet::new(), builder, Some(boundary.clone()))
                .await?;
        peer.sql.execute_batch(
            "CREATE TABLE IF NOT EXISTS membership(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                owner TEXT NOT NULL, namespace TEXT NOT NULL,
                generation INTEGER NOT NULL, policy BLOB NOT NULL, observed_at INTEGER NOT NULL);",
        )?;
        let binding: Option<(String, String)> = peer
            .sql
            .query_row("SELECT owner,namespace FROM membership", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        if let Some((saved_owner, saved_namespace)) = binding {
            ensure!(
                saved_owner == owner.to_string() && saved_namespace == namespace.to_string(),
                "store authority mismatch"
            );
        } else {
            peer.sql.execute(
                "INSERT INTO membership VALUES (1,?1,?2,0,X'',?3)",
                params![owner.to_string(), namespace.to_string(), now()?],
            )?;
        }
        let doc = peer.docs.import_namespace(capability).await?;
        doc.set_download_policy(DownloadPolicy::NothingExcept(vec![]))
            .await?;
        Ok(Self {
            peer,
            doc,
            boundary,
        })
    }

    pub fn addr(&self) -> EndpointAddr {
        self.peer.addr()
    }

    pub async fn author(&self) -> Result<AuthorId> {
        self.peer.author().await
    }

    /// Persist before activating. All connections close on every new snapshot,
    /// even when a peer remains a member, fencing old generation requests.
    pub fn install_policy(&mut self, signed: &SignedPolicy) -> Result<()> {
        self.boundary.verify(signed)?;
        let body = policy_bytes(&signed.policy)?;
        let remaining = Duration::from_secs(signed.policy.expires_at)
            .checked_sub(SystemTime::now().duration_since(UNIX_EPOCH)?)
            .context("policy expired before persistence")?;
        let mut deadline = Instant::now() + remaining;
        let mut state = self.boundary.lock()?;
        let tx = self.peer.sql.transaction()?;
        let (generation, saved, observed): (u64, Vec<u8>, u64) = tx.query_row(
            "SELECT generation,policy,observed_at FROM membership WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let time = now()?;
        ensure!(time >= observed, "clock rollback");
        ensure!(signed.policy.generation >= generation, "policy rollback");
        if signed.policy.generation == generation {
            ensure!(body == saved, "equal-generation policy fork");
        }
        tx.execute(
            "UPDATE membership SET generation=?1,policy=?2,observed_at=?3 WHERE singleton=1",
            params![signed.policy.generation, body, time],
        )?;
        tx.commit()?;
        for connection in state.connections.drain(..).filter_map(|c| c.upgrade()) {
            connection.close(1u32.into(), b"policy updated");
        }
        if let Some(active) = &state.active
            && active.policy.policy.generation == signed.policy.generation
        {
            // Reinstalling the identical signed lease cannot extend it after a
            // wall-clock adjustment.
            deadline = deadline.min(active.deadline);
        }
        state.active = Some(Active {
            deadline,
            policy: signed.clone(),
        });
        Ok(())
    }

    pub async fn publish(&mut self, text: &str, artifact: &[u8]) -> Result<Record> {
        let author = self.author().await?;
        {
            let state = self.boundary.lock()?;
            self.boundary.author(&state, self.boundary.local, author)?;
        }
        let record = self.peer.prepare(&self.doc, text, artifact).await?;
        self.flush().await?;
        Ok(record)
    }

    /// Replay the reviewed publication journal after reopening with the original
    /// write capability. Authorization is required before repairing old intents.
    pub async fn flush(&self) -> Result<()> {
        let author = self.author().await?;
        {
            let state = self.boundary.lock()?;
            self.boundary.author(&state, self.boundary.local, author)?;
        }
        self.peer.flush(&self.doc).await
    }

    pub async fn receive(
        &mut self,
        remote: EndpointAddr,
        author: AuthorId,
        key: &str,
    ) -> Result<bool> {
        let request = {
            let state = self.boundary.lock()?;
            self.boundary.allowed(&state, remote.id)?;
            self.boundary.author(&state, remote.id, author)?;
            let p = self.boundary.policy(&state)?;
            Request {
                generation: p.generation,
                namespace: p.namespace,
                scope: p.scope.clone(),
                author,
                key: key.to_owned(),
            }
        };
        let bundle = tokio::time::timeout(Duration::from_secs(10), async {
            let conn = self.peer.endpoint.connect(remote.clone(), ALPN).await?;
            let (mut send, mut recv) = conn.open_bi().await?;
            send.write_all(&serde_json::to_vec(&request)?).await?;
            send.finish()?;
            let bytes = recv.read_to_end(MAX_WIRE).await?;
            conn.close(0u32.into(), b"record received");
            Ok::<Bundle, anyhow::Error>(serde_json::from_slice(&bytes)?)
        })
        .await??;
        ensure!(bundle.body.len() <= MAX_RECORD, "record too large");
        let record: Record = serde_json::from_slice(&bundle.body)?;
        let hash = record.validate(author, key.as_bytes())?;
        ensure!(
            bundle.artifact.len() as u64 == record.artifact.size
                && Hash::new(&bundle.artifact) == hash,
            "artifact corrupt"
        );
        // Content is verified, but policy may have changed while it was in flight.
        let state = self.boundary.lock()?;
        self.boundary.allowed(&state, remote.id)?;
        self.boundary.author(&state, remote.id, author)?;
        ensure!(
            self.boundary.policy(&state)?.generation == request.generation,
            "stale response"
        );
        let tx = self.peer.sql.transaction()?;
        let body_hash = Hash::new(&bundle.body).to_string();
        let old: Option<String> = tx
            .query_row(
                "SELECT hash FROM received WHERE namespace=?1 AND author=?2 AND key=?3",
                params![
                    request.namespace.to_string(),
                    author.to_string(),
                    key.as_bytes()
                ],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            ensure!(old == body_hash, "immutable projection conflict");
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO received VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                request.namespace.to_string(),
                author.to_string(),
                key.as_bytes(),
                body_hash,
                bundle.body,
                bundle.artifact
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Inspection of an already disclosed inert projection is not erasure.
    pub fn received(&self) -> Result<Vec<Record>> {
        self.peer.received()
    }

    pub async fn shutdown(self) -> Result<()> {
        self.peer.router.shutdown().await?;
        self.peer.docs.shutdown().await;
        self.peer.blobs.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
