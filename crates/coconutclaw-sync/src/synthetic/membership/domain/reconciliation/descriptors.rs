//! Exact redacted fixture descriptors. No SQLite reader/exporter or effect permit.
//! Numeric content references are artificial aliases, never hashes of prompts,
//! sessions, credentials or other private runtime values.
use super::*;

/// Pins the exact schema, migrations and callers whose local states are mapped.
/// This identifies source bytes, not a deployed database or authentic producer.
pub fn source_identity() -> Hash {
    macro_rules! source {
        ($path:literal) => {
            (
                $path,
                Hash::new(include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../",
                    $path
                ))),
            )
        };
    }
    let sources = [
        source!("sql/schema.sql"),
        source!("crates/coconutclaw-cli/src/store/migrations.rs"),
        source!("crates/coconutclaw-cli/src/store/mod.rs"),
        source!("crates/coconutclaw-cli/src/store/task_runs.rs"),
        source!("crates/coconutclaw-cli/src/store/approvals.rs"),
        source!("crates/coconutclaw-cli/src/store/kv.rs"),
        source!("crates/coconutclaw-cli/src/types.rs"),
        source!("crates/coconutclaw-cli/src/scheduler.rs"),
        source!("crates/coconutclaw-cli/src/approval.rs"),
        source!("crates/coconutclaw-cli/src/delivery/mod.rs"),
        source!("crates/coconutclaw-cli/src/delivery/telegram.rs"),
        source!("crates/coconutclaw-cli/src/loops/webhook.rs"),
        source!("crates/coconutclaw-cli/src/loops/poll.rs"),
        source!("crates/coconutclaw-cli/src/recovery/pending.rs"),
        source!("crates/coconutclaw-sync/src/synthetic/membership/domain/reconciliation.rs"),
        source!(
            "crates/coconutclaw-sync/src/synthetic/membership/domain/reconciliation/descriptors.rs"
        ),
    ];
    let mut bytes = b"CoconutClaw redacted fixture source v1\0".to_vec();
    for (path, hash) in sources {
        bytes.extend_from_slice(path.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(hash.as_bytes());
    }
    Hash::new(bytes)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Journal {
    TaskRuns,
    Payloads,
    Approvals,
    Turns,
    Inbound,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum TaskLink {
    Delegation { parent: i64, index: u32 },
    Approval { approval: i64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Plan {
    Legacy,
    Rich,
    Document,
    Chunks,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Phase {
    Ready,
    Sending,
    Unknown,
}

/// Missing rows/journals are explicit nulls, never fabricated empty successes.
/// Acknowledgment is separate from phase: durable_op leaves phase `sending`
/// when it adds a completed operation ID.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Row {
    Task {
        run: i64,
        status: Option<String>,
        started: bool,
        unknown_outcome: bool,
        update: Option<u64>,
        link: Option<TaskLink>,
    },
    Payload {
        run: i64,
        request: Option<u32>,
        output: Option<u32>,
        delivered: bool,
        journal: bool,
        plan: Option<Plan>,
    },
    Delivery {
        run: i64,
        operation: String,
        phase: Option<Phase>,
        attempts: Option<u64>,
        acknowledged: bool,
    },
    Approval {
        id: i64,
        run: Option<i64>,
        status: Option<String>,
        resume: Option<u32>,
    },
    Turn {
        id: i64,
        run: Option<i64>,
        status: Option<String>,
        side_effects_applied: Option<bool>,
        update: Option<u64>,
    },
    Inbound {
        last_update: Option<u64>,
        inflight: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub source: Hash,
    pub row: Row,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub version: u8,
    pub source: Hash,
    pub missing: BTreeSet<Journal>,
    pub observations: BTreeMap<String, Hash>,
}

/// Borrowed exact application fixture bytes, never a store or collection handle.
pub struct Application<'a> {
    pub checkpoint: &'a [u8],
    pub descriptors: &'a [Vec<u8>],
}

/// Exact serde JSON bytes are the v1 canonical format. Both a descriptor and
/// the checkpoint use the existing 4096-byte artifact bound.
pub(super) fn parse<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= MAX_ARTIFACT,
        "descriptor size"
    );
    let value: T = serde_json::from_slice(bytes)?;
    ensure!(
        serde_json::to_vec(&value)? == bytes,
        "noncanonical descriptor"
    );
    Ok(value)
}

impl Descriptor {
    pub fn id(&self) -> String {
        match &self.row {
            Row::Task { run, .. } => format!("task/{run}"),
            Row::Payload { run, .. } => format!("payload/{run}"),
            Row::Delivery { run, operation, .. } => format!("delivery/{run}/{operation}"),
            Row::Approval { id, .. } => format!("approval/{id}"),
            Row::Turn { id, .. } => format!("turn/{id}"),
            Row::Inbound { .. } => "inbound".into(),
        }
    }

    pub fn local_state(&self) -> String {
        match &self.row {
            Row::Task {
                status,
                unknown_outcome,
                ..
            } => format!(
                "task:{}",
                if *unknown_outcome {
                    "unknown"
                } else {
                    status.as_deref().unwrap_or("unknown")
                }
            ),
            Row::Payload {
                request, output, ..
            } => if request.is_none() {
                "payload:unknown"
            } else if output.is_some() {
                "payload:output-recorded"
            } else {
                "payload:output-unknown"
            }
            .into(),
            Row::Delivery {
                phase,
                acknowledged,
                ..
            } => if *acknowledged {
                "delivery:local-ack"
            } else {
                match phase {
                    Some(Phase::Ready) => "delivery:ready",
                    Some(Phase::Sending) => "delivery:sending",
                    _ => "delivery:unknown",
                }
            }
            .into(),
            Row::Approval { status, .. } => {
                format!("approval:{}", status.as_deref().unwrap_or("unknown"))
            }
            Row::Turn { status, .. } => format!("turn:{}", status.as_deref().unwrap_or("unknown")),
            Row::Inbound { .. } => "inbound:unreconciled".into(),
        }
    }
}

/// Verify exact descriptor preimages and closed, typed local links. All returned
/// data remains inert; even terminal/acknowledged records prove no settled effect.
/// No evidence-digest authentication or service reconciliation is performed.
pub fn inspect(
    history: &History,
    current: &Binding,
    independently_pinned_bundle: Option<Hash>,
    bundle: &[u8],
    checkpoint: &[u8],
    descriptors: &[Vec<u8>],
) -> Result<BTreeMap<String, Descriptor>> {
    let observations = super::inspect(history, current, independently_pinned_bundle, bundle)?;
    inspect_observations(&observations, current, checkpoint, descriptors)
}

// Only already-bound observations may reach this shared typed-link validator.
pub(super) fn inspect_observations(
    observations: &BTreeMap<String, Observation>,
    current: &Binding,
    checkpoint: &[u8],
    descriptors: &[Vec<u8>],
) -> Result<BTreeMap<String, Descriptor>> {
    ensure!(
        Hash::new(checkpoint) == current.application_checkpoint,
        "application checkpoint mismatch"
    );
    let checkpoint: Checkpoint = parse(checkpoint)?;
    ensure!(
        checkpoint.version == 1 && checkpoint.source == source_identity(),
        "descriptor source/version mismatch"
    );
    ensure!(descriptors.len() <= 64, "too many descriptors");
    let mut rows = BTreeMap::new();
    for bytes in descriptors {
        let descriptor: Descriptor = parse(bytes)?;
        ensure!(
            descriptor.source == checkpoint.source,
            "descriptor source mismatch"
        );
        let id = descriptor.id();
        ensure!(label(&id), "invalid descriptor identity");
        ensure!(
            checkpoint.observations.get(&id) == Some(&Hash::new(bytes)),
            "descriptor checkpoint link mismatch"
        );
        let observation = observations
            .get(&id)
            .context("descriptor observation absent")?;
        ensure!(
            observation.snapshot == Hash::new(bytes)
                && observation.local_state == descriptor.local_state(),
            "descriptor observation mismatch"
        );
        if let Some(old) = rows.insert(id, descriptor.clone()) {
            ensure!(old == descriptor, "conflicting descriptor");
        }
    }
    ensure!(
        rows.len() == checkpoint.observations.len() && rows.len() == observations.len(),
        "missing descriptor or observation"
    );
    let task = |run: i64| -> Result<&Descriptor> {
        ensure!(run > 0, "invalid task link");
        rows.get(&format!("task/{run}"))
            .context("dangling task link")
    };
    let mut links = BTreeSet::new();
    let mut updates = BTreeSet::new();
    let mut turn_updates = BTreeSet::new();
    for descriptor in rows.values() {
        match &descriptor.row {
            Row::Task {
                run,
                status,
                started,
                unknown_outcome,
                update,
                link,
            } => {
                task(*run)?;
                ensure!(
                    status.as_deref().is_none_or(|s| matches!(
                        s,
                        "queued"
                            | "running"
                            | "awaiting_approval"
                            | "cancel_requested"
                            | "completed"
                            | "failed"
                            | "cancelled"
                    )),
                    "invalid task status"
                );
                ensure!(
                    status.is_some()
                        || (!started && !unknown_outcome && update.is_none() && link.is_none()),
                    "missing task has claims"
                );
                ensure!(
                    !checkpoint.missing.contains(&Journal::TaskRuns) || status.is_none(),
                    "missing task journal conflict"
                );
                if let Some(update) = update {
                    ensure!(
                        *update <= i64::MAX as u64 && updates.insert(*update),
                        "conflicting inbound task"
                    );
                }
                if let Some(link) = link {
                    ensure!(update.is_none(), "conflicting task origins");
                    match link {
                        TaskLink::Delegation { parent, index } => {
                            task(*parent)?;
                            ensure!(
                                parent < run
                                    && links.insert(format!("delegation/{parent}/{index}")),
                                "conflicting delegation link"
                            );
                        }
                        TaskLink::Approval { approval } => {
                            let row = rows
                                .get(&format!("approval/{approval}"))
                                .context("dangling approval link")?;
                            let Row::Approval {
                                run: original,
                                status,
                                ..
                            } = &row.row
                            else {
                                unreachable!()
                            };
                            ensure!(
                                *approval > 0 && original.is_none_or(|original| original < *run),
                                "invalid approval resume"
                            );
                            ensure!(
                                status.as_deref().is_none_or(|s| s == "approved"),
                                "resume of unapproved request"
                            );
                            ensure!(
                                links.insert(format!("approval/{approval}")),
                                "conflicting approval resume"
                            );
                        }
                    }
                }
            }
            Row::Payload {
                run,
                request,
                output,
                delivered,
                journal,
                plan,
            } => {
                task(*run)?;
                ensure!(
                    request.is_none_or(|r| r > 0) && output.is_none_or(|r| r > 0),
                    "invalid redacted reference"
                );
                ensure!(
                    request.is_some()
                        || (output.is_none() && !delivered && !journal && plan.is_none()),
                    "missing payload has claims"
                );
                ensure!(
                    !delivered || output.is_some(),
                    "delivery ack without output"
                );
                ensure!(*journal || plan.is_none(), "plan without journal");
                ensure!(
                    !checkpoint.missing.contains(&Journal::Payloads) || request.is_none(),
                    "missing payload journal conflict"
                );
            }
            Row::Delivery {
                run,
                operation,
                phase,
                attempts,
                acknowledged,
            } => {
                task(*run)?;
                let indexed = [
                    "telegram:text:chunk:",
                    "telegram:photo:",
                    "telegram:document:",
                    "telegram:video:",
                ]
                .iter()
                .any(|prefix| {
                    operation.strip_prefix(prefix).is_some_and(|index| {
                        index.parse::<u32>().is_ok_and(|n| n.to_string() == index)
                    })
                });
                ensure!(
                    operation == "telegram:text"
                        || operation == "telegram:text:document"
                        || indexed,
                    "invalid operation link"
                );
                ensure!(
                    phase.is_some() == attempts.is_some()
                        && attempts.is_none_or(|n| n <= i64::MAX as u64),
                    "invalid delivery attempts"
                );
                let payload = rows
                    .get(&format!("payload/{run}"))
                    .context("delivery payload absent")?;
                ensure!(
                    matches!(
                        &payload.row,
                        Row::Payload {
                            request: Some(_),
                            journal: true,
                            ..
                        }
                    ),
                    "delivery journal unavailable"
                );
                ensure!(
                    phase.is_some() || *acknowledged,
                    "missing operation must be an acknowledged legacy ID"
                );
            }
            Row::Approval {
                id,
                run,
                status,
                resume,
            } => {
                ensure!(
                    *id > 0 && resume.is_none_or(|n| n > 0),
                    "invalid approval reference"
                );
                ensure!(
                    status.as_deref().is_none_or(|s| matches!(
                        s,
                        "pending" | "approved" | "rejected" | "expired"
                    )),
                    "invalid approval status"
                );
                ensure!(
                    run.is_some() == status.is_some() && run.is_some() == resume.is_some(),
                    "partial approval link"
                );
                if let Some(run) = run {
                    task(*run)?;
                }
                ensure!(
                    !checkpoint.missing.contains(&Journal::Approvals) || run.is_none(),
                    "missing approval journal conflict"
                );
            }
            Row::Turn {
                id,
                run,
                status,
                side_effects_applied,
                update,
            } => {
                ensure!(
                    *id > 0
                        && status.as_deref().is_none_or(|s| matches!(
                            s,
                            "ok" | "cancelled"
                                | "agent_error"
                                | "parse_recovered"
                                | "parse_fallback"
                                | "agent_error_recovered"
                        )),
                    "invalid turn"
                );
                ensure!(
                    status.is_some() == side_effects_applied.is_some(),
                    "partial turn state"
                );
                ensure!(
                    status.is_some() || (run.is_none() && update.is_none()),
                    "missing turn has links"
                );
                if let Some(run) = run {
                    let Row::Task {
                        update: task_update,
                        ..
                    } = &task(*run)?.row
                    else {
                        unreachable!()
                    };
                    ensure!(
                        task_update.is_none() || update.is_none() || task_update == update,
                        "turn/update substitution"
                    );
                }
                if let Some(update) = update {
                    ensure!(
                        *update <= i64::MAX as u64 && turn_updates.insert(*update),
                        "conflicting turn update"
                    );
                }
                ensure!(
                    !checkpoint.missing.contains(&Journal::Turns) || status.is_none(),
                    "missing turn journal conflict"
                );
            }
            Row::Inbound {
                last_update,
                inflight,
            } => {
                ensure!(
                    [last_update, inflight]
                        .into_iter()
                        .all(|n| n.is_none_or(|n| n <= i64::MAX as u64)),
                    "invalid inbound checkpoint"
                );
                ensure!(
                    !checkpoint.missing.contains(&Journal::Inbound)
                        || (last_update.is_none() && inflight.is_none()),
                    "missing inbound journal conflict"
                );
            }
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests;
