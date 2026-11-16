//! Synthetic immutable application history. All views are inert, never runtime
//! memory, runnable tasks, approval, delivery or external-effect authority.
pub mod reconciliation;

use super::*;
use std::collections::BTreeMap;

const LABEL: &str = "CoconutClaw synthetic domain event v1";

/// Independently provisioned, fixed owner binding. Endpoint/docs keys are NOT
/// application identities. A fence never rewrites these historical role bindings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Origin {
    pub owner: EndpointId,
    pub store: String,
    pub namespace: NamespaceId,
    pub scope: String,
    pub core: EndpointId,
    pub runners: BTreeSet<EndpointId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundOrigin {
    pub origin: Origin,
    pub signature: Signature,
}

fn signed_bytes<T: Serialize>(label: &[u8], value: &T) -> Result<Vec<u8>> {
    let mut bytes = label.to_vec();
    bytes.extend(serde_json::to_vec(value)?);
    ensure!(bytes.len() <= MAX_ARTIFACT, "domain bytes exceed bound");
    Ok(bytes)
}

impl BoundOrigin {
    pub fn event_id(&self, sequence: u64) -> Result<String> {
        self.verify(self.origin.owner, self.origin.namespace)?;
        ensure!(
            sequence > 0 && sequence <= i64::MAX as u64,
            "invalid event sequence"
        );
        Ok(format!(
            "{}/{}/{}",
            self.origin.owner, self.origin.store, sequence
        ))
    }

    pub fn sign(origin: Origin, owner: &SecretKey) -> Result<Self> {
        let binding = Self {
            signature: owner.sign(&signed_bytes(b"CoconutClaw domain origin v1\0", &origin)?),
            origin,
        };
        binding.verify(owner.public(), binding.origin.namespace)?;
        Ok(binding)
    }

    pub fn verify(&self, owner: EndpointId, namespace: NamespaceId) -> Result<()> {
        let o = &self.origin;
        ensure!(
            o.owner == owner && o.namespace == namespace && o.scope == SCOPE,
            "domain owner/namespace/scope mismatch"
        );
        ensure!(
            o.store.len() == 64
                && o.store
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "invalid stable store identity"
        );
        ensure!(
            o.runners.len() <= 32 && !o.runners.contains(&o.core),
            "invalid domain roles"
        );
        owner.verify(
            &signed_bytes(b"CoconutClaw domain origin v1\0", o)?,
            &self.signature,
        )?;
        Ok(())
    }

    pub fn hash(&self) -> Result<String> {
        Ok(Hash::new(&signed_bytes(
            b"CoconutClaw domain origin v1\0",
            &self.origin,
        )?)
        .to_string())
    }
}

/// Source is a core attestation to a fixture reference, not a live turn import.
/// IDs reference event sequence numbers within this owner-bound store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Action {
    /// Owner-only manual suspension of epoch 1. This is NOT activation.
    Fence {
        previous_epoch: u64,
        epoch: u64,
        core: EndpointId,
        runners: BTreeSet<EndpointId>,
    },
    Source {
        reference: String,
    },
    Candidate {
        source: u64,
        content: String,
    },
    Remember {
        source: u64,
        content: String,
    },
    Promote {
        record: u64,
    },
    Correct {
        source: u64,
        record: u64,
        content: String,
    },
    Undo {
        history: u64,
    },
    Assign {
        job: u64,
        attempt: u64,
        runner: EndpointId,
        request: String,
    },
    Start {
        assignment: u64,
    },
    Result {
        assignment: u64,
        outcome: Outcome,
        output: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Outcome {
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub version: u8,
    pub origin: String,
    pub sequence: u64,
    pub previous: Option<String>,
    pub actor: EndpointId,
    pub action: Action,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedEvent {
    pub event: Event,
    pub signature: Signature,
}

impl SignedEvent {
    pub fn sign(event: Event, actor: &SecretKey) -> Result<Self> {
        ensure!(event.actor == actor.public(), "event signer mismatch");
        let signed = Self {
            signature: actor.sign(&signed_bytes(b"CoconutClaw domain event v1\0", &event)?),
            event,
        };
        signed.bytes()?;
        Ok(signed)
    }

    pub fn bytes(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)?;
        ensure!(bytes.len() <= MAX_ARTIFACT, "domain envelope exceeds bound");
        Ok(bytes)
    }

    pub fn parse(bytes: &[u8]) -> Result<Self> {
        ensure!(
            !bytes.is_empty() && bytes.len() <= MAX_ARTIFACT,
            "invalid domain size"
        );
        let signed: Self = serde_json::from_slice(bytes)?;
        ensure!(signed.bytes()? == bytes, "noncanonical domain bytes");
        signed.event.actor.verify(
            &signed_bytes(b"CoconutClaw domain event v1\0", &signed.event)?,
            &signed.signature,
        )?;
        Ok(signed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Memory {
    pub content: String,
    pub curated: bool,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub core: EndpointId,
    pub runner: EndpointId,
    pub attempt: u64,
    pub started: bool,
    pub result: Option<(Outcome, String)>,
}

#[derive(Debug, Clone)]
struct Change {
    old: Option<u64>,
    new: Option<u64>,
    undone: bool,
}

/// Rebuilt from complete canonical events. No partially validated view escapes.
#[derive(Debug, Default)]
pub struct History {
    events: BTreeMap<u64, Vec<u8>>,
    pub memory: BTreeMap<u64, Memory>,
    pub jobs: BTreeMap<u64, Job>,
    sources: BTreeSet<u64>,
    changes: BTreeMap<u64, Change>,
    spent: BTreeSet<u64>,
    fence: Option<PendingFence>,
}

/// Proposed replacement roles, not authorized writers or executable work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingFence {
    pub sequence: u64,
    pub epoch: u64,
    pub core: EndpointId,
    pub runners: BTreeSet<EndpointId>,
}

fn content(text: &str, max: usize) -> Result<()> {
    ensure!(
        !text.is_empty()
            && text.len() <= max
            && !text
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t'),
        "invalid domain text"
    );
    Ok(())
}

impl History {
    pub fn pending_fence(&self) -> Option<&PendingFence> {
        self.fence.as_ref()
    }

    /// Check an independently retained current owner-fence checkpoint. Never
    /// obtain this input from the same rollbackable history or an arbitrary peer.
    /// Hash agreement establishes identity, not global latestness.
    pub fn validate_fence_checkpoint(&self, sequence: u64, hash: &str) -> Result<()> {
        let fence = self
            .fence
            .as_ref()
            .context("owner fence checkpoint absent")?;
        ensure!(
            fence.sequence == sequence,
            "owner fence checkpoint mismatch"
        );
        let bytes = self.events.get(&sequence).context("fence bytes absent")?;
        ensure!(
            Hash::new(bytes).to_string() == hash,
            "owner fence hash mismatch"
        );
        Ok(())
    }

    /// No credential-exclusion verifier exists in this inert domain. Neither an
    /// owner signature nor synthetic attestations can fence Telegram/Codex use
    /// by a partitioned old core. Do not offer a caller-supplied boolean bypass.
    pub fn validate_replacement_activation(&self) -> Result<()> {
        ensure!(
            self.fence.is_some(),
            "replacement unavailable: owner fence absent"
        );
        ensure!(
            self.jobs.values().all(|job| matches!(
                job.result,
                Some((Outcome::Completed | Outcome::Failed | Outcome::Cancelled, _))
            )),
            "replacement unavailable: unresolved attempts/effects"
        );
        bail!(
            "replacement unavailable: external Telegram/Codex credential exclusion is unproven; no operational proof verifier"
        )
    }

    pub fn replay(
        origin: &BoundOrigin,
        owner: EndpointId,
        namespace: NamespaceId,
        bytes: impl IntoIterator<Item = Vec<u8>>,
    ) -> Result<Self> {
        origin.verify(owner, namespace)?;
        let mut events = BTreeMap::new();
        for bytes in bytes {
            let signed = SignedEvent::parse(&bytes)?;
            ensure!(
                signed.event.origin == origin.hash()?,
                "event origin mismatch"
            );
            let seq = signed.event.sequence;
            ensure!(seq > 0 && seq <= i64::MAX as u64, "invalid event sequence");
            if let Some(old) = events.insert(seq, bytes.clone()) {
                ensure!(old == bytes, "stable event identity conflict");
            }
        }
        let mut history = Self::default();
        let mut previous = None;
        for (seq, bytes) in events {
            ensure!(
                seq == history.events.len() as u64 + 1,
                "domain unavailable: missing predecessor"
            );
            let signed = SignedEvent::parse(&bytes)?;
            ensure!(
                signed.event.previous == previous,
                "domain unavailable: conflicting history"
            );
            history.apply(origin, &signed.event)?;
            previous = Some(Hash::new(&bytes).to_string());
            history.events.insert(seq, bytes);
        }
        Ok(history)
    }

    /// Exact immutable bytes, stable across transport author/device rotation.
    pub fn export(&self) -> Vec<Vec<u8>> {
        self.events.values().cloned().collect()
    }

    pub fn next(&self, origin: &BoundOrigin, actor: EndpointId, action: Action) -> Result<Event> {
        Ok(Event {
            version: 1,
            origin: origin.hash()?,
            sequence: self.events.len() as u64 + 1,
            previous: self
                .events
                .last_key_value()
                .map(|(_, b)| Hash::new(b).to_string()),
            actor,
            action,
        })
    }

    fn source(&self, id: u64) -> Result<()> {
        ensure!(
            self.sources.contains(&id),
            "domain unavailable: source attestation absent"
        );
        Ok(())
    }

    fn apply(&mut self, origin: &BoundOrigin, e: &Event) -> Result<()> {
        ensure!(e.version == 1, "unsupported domain event");
        let o = &origin.origin;
        ensure!(
            self.fence.is_none(),
            "domain fenced: replacement activation unavailable"
        );
        if let Action::Fence {
            previous_epoch,
            epoch,
            core,
            runners,
        } = &e.action
        {
            ensure!(e.actor == o.owner, "fence requires origin owner authority");
            ensure!(
                *previous_epoch == 1 && *epoch == 2,
                "unproven epoch predecessor/rollback"
            );
            ensure!(
                *core != o.core && runners.len() <= 32 && !runners.contains(core),
                "invalid replacement roles"
            );
            self.fence = Some(PendingFence {
                sequence: e.sequence,
                epoch: *epoch,
                core: *core,
                runners: runners.clone(),
            });
            return Ok(());
        }
        let runner_action = matches!(e.action, Action::Start { .. } | Action::Result { .. });
        ensure!(
            if runner_action {
                o.runners.contains(&e.actor)
            } else {
                e.actor == o.core
            },
            "domain writer unauthorized"
        );
        let id = e.sequence;
        let change = match &e.action {
            Action::Fence { .. } => unreachable!("fence handled before role validation"),
            Action::Source { reference } => {
                content(reference, 256)?;
                self.sources.insert(id);
                return Ok(());
            }
            Action::Candidate {
                source,
                content: text,
            }
            | Action::Remember {
                source,
                content: text,
            } => {
                self.source(*source)?;
                let curated = matches!(e.action, Action::Remember { .. });
                content(text, if curated { 512 } else { 2048 })?;
                self.memory.insert(
                    id,
                    Memory {
                        content: text.clone(),
                        curated,
                        active: curated,
                    },
                );
                if !curated {
                    return Ok(());
                }
                Change {
                    old: None,
                    new: Some(id),
                    undone: false,
                }
            }
            Action::Promote { record } => {
                let memory = self
                    .memory
                    .get_mut(record)
                    .context("domain unavailable: memory absent")?;
                // Runtime permits explicit re-promotion of inactive records.
                // This narrower transfer contract refuses spent IDs rather than
                // guessing whether stale promotion constitutes fresh intent.
                ensure!(
                    !memory.active && !self.spent.contains(record),
                    "memory already used"
                );
                memory.active = true;
                Change {
                    old: None,
                    new: Some(*record),
                    undone: false,
                }
            }
            Action::Correct {
                source,
                record,
                content: text,
            } => {
                self.source(*source)?;
                let old = self
                    .memory
                    .get_mut(record)
                    .context("domain unavailable: memory absent")?;
                ensure!(old.active, "only active memory can be corrected");
                content(text, if old.curated { 512 } else { 2048 })?;
                old.active = false;
                let curated = old.curated;
                self.memory.insert(
                    id,
                    Memory {
                        content: text.clone(),
                        curated,
                        active: true,
                    },
                );
                Change {
                    old: Some(*record),
                    new: Some(id),
                    undone: false,
                }
            }
            Action::Undo { history } => {
                let (&latest, change) = self
                    .changes
                    .iter_mut()
                    .rev()
                    .find(|(_, c)| !c.undone)
                    .context("domain unavailable: undo history absent")?;
                ensure!(
                    *history == latest,
                    "undo must target latest eligible change"
                );
                if let Some(new) = change.new {
                    let memory = self.memory.get_mut(&new).context("undo memory absent")?;
                    ensure!(memory.active, "undo effective state inconsistent");
                    memory.active = false;
                }
                if let Some(old) = change.old {
                    let memory = self
                        .memory
                        .get_mut(&old)
                        .context("undo predecessor absent")?;
                    ensure!(!memory.active, "undo predecessor already active");
                    memory.active = true;
                }
                change.undone = true;
                return Ok(());
            }
            Action::Assign {
                job,
                attempt,
                runner,
                request,
            } => {
                content(request, 2048)?;
                // Runtime approval/retry creates a new task_run, not an
                // automatic second attempt. Unknown attempts cannot be retried.
                ensure!(
                    *job == id && *attempt == 1 && o.runners.contains(runner),
                    "unproven assignment/attempt"
                );
                ensure!(!self.jobs.contains_key(job), "job already assigned");
                self.jobs.insert(
                    *job,
                    Job {
                        core: e.actor,
                        runner: *runner,
                        attempt: *attempt,
                        started: false,
                        result: None,
                    },
                );
                return Ok(());
            }
            Action::Start { assignment } => {
                let job = self
                    .jobs
                    .get_mut(assignment)
                    .context("domain unavailable: assignment absent")?;
                ensure!(
                    e.actor == job.runner && !job.started && job.result.is_none(),
                    "invalid assigned runner/start"
                );
                job.started = true;
                return Ok(());
            }
            Action::Result {
                assignment,
                outcome,
                output,
            } => {
                content(output, 2048)?;
                let job = self
                    .jobs
                    .get_mut(assignment)
                    .context("domain unavailable: assignment absent")?;
                ensure!(
                    e.actor == job.runner && job.started && job.result.is_none(),
                    "invalid assigned runner/result"
                );
                job.result = Some((*outcome, output.clone()));
                return Ok(());
            }
        };
        self.spent.extend(change.old);
        self.spent.extend(change.new);
        self.changes.insert(id, change);
        Ok(())
    }
}

impl ScopedPeer {
    /// Explicit historical inspection/export. Receipt repair retains current
    /// policy, quarantine and byte-conflict checks. A domain conflict makes the
    /// entire view unavailable, not a best-effort subset or an execution queue.
    pub fn domain_history(&mut self, origin: &BoundOrigin) -> Result<History> {
        origin.verify(self.boundary.owner, self.boundary.namespace)?;
        self.repair_projection()?;
        let state = self.boundary.lock()?;
        self.boundary.policy(&state)?;
        let mut events = Vec::new();
        let mut publications = self.peer.sql.prepare(
            "SELECT author,body,artifact FROM publication WHERE namespace=?1 ORDER BY id",
        )?;
        let mut rows = publications.query([self.boundary.namespace.to_string()])?;
        while let Some(row) = rows.next()? {
            let author: String = row.get(0)?;
            let body: Vec<u8> = row.get(1)?;
            let record: Record = serde_json::from_slice(&body)?;
            if record.text == LABEL {
                self.boundary
                    .author(&state, self.boundary.local, author.parse()?)?;
                let bytes: Vec<u8> = row.get(2)?;
                projection::validate(
                    author.parse()?,
                    record.key().as_bytes(),
                    &Hash::new(&body).to_string(),
                    &body,
                    &bytes,
                )?;
                events.push(bytes);
            }
        }
        let mut received = self
            .peer
            .sql
            .prepare("SELECT body,artifact FROM received")?;
        let mut rows = received.query([])?;
        while let Some(row) = rows.next()? {
            let body: Vec<u8> = row.get(0)?;
            let record: Record = serde_json::from_slice(&body)?;
            if record.text == LABEL {
                events.push(row.get(1)?);
            }
        }
        let history =
            History::replay(origin, self.boundary.owner, self.boundary.namespace, events)?;
        self.boundary.policy(&state)?;
        Ok(history)
    }

    /// Only synthetic fixture events, never a runtime SQL importer.
    pub async fn publish_domain(&mut self, origin: &BoundOrigin, bytes: &[u8]) -> Result<Record> {
        // Refuse lost/conflicting/tombstoned local publication history before
        // adding another intent. This does not prove remote deletion continuity.
        self.flush().await?;
        let mut events = self.domain_history(origin)?.export();
        events.push(bytes.to_vec());
        History::replay(origin, self.boundary.owner, self.boundary.namespace, events)?;
        self.publish(LABEL, bytes).await
    }

    /// Transport acceptance is inert. Invalid domain history is retained as
    /// disclosed evidence and makes the view unavailable, never executable.
    pub async fn receive_domain(
        &mut self,
        origin: &BoundOrigin,
        remote: EndpointAddr,
        author: AuthorId,
        key: &str,
    ) -> Result<bool> {
        origin.verify(self.boundary.owner, self.boundary.namespace)?;
        let inserted = self.receive(remote, author, key).await?;
        let body: Vec<u8> = self.peer.sql.query_row(
            "SELECT body FROM received WHERE namespace=?1 AND author=?2 AND key=?3",
            params![
                self.boundary.namespace.to_string(),
                author.to_string(),
                key.as_bytes()
            ],
            |r| r.get(0),
        )?;
        let record: Record = serde_json::from_slice(&body)?;
        ensure!(record.text == LABEL, "not a domain event");
        self.domain_history(origin)?;
        Ok(inserted)
    }
}

#[cfg(test)]
mod tests;
