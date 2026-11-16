//! Redacted synthetic authority declarations, never issuer/exclusion evidence.
//! Inspection closes declared links only. All real effects remain unproven.
use super::*;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Authority {
    TelegramApi,
    TelegramFile,
    CodexProvider,
    Tool,
    Workspace,
}

/// Artificial holder alias. None retains unknown attribution. A known actor is
/// an original application core/runner, not proof of credential possession.
/// Different holder aliases may share an actor (e.g. a copied holder).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Holder {
    pub id: u32,
    pub actor: Option<EndpointId>,
}

/// Resource/version values are positive artificial aliases, never actual
/// account IDs, credentials, private-content hashes or secret material.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub id: u32,
    pub kind: Authority,
    pub resource: u32,
    pub credential_version: Option<u32>,
    pub holders: Vec<u32>,
    pub unknown_holders: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Recording {
    Recorded,
    Unjournaled,
}

/// One declared possible effect per observation, not proof of an accepted or
/// completed operation. Aggregate work needs separate observations; no automatic
/// runtime task-to-effect mapping is implemented.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Effect {
    pub observation: String,
    pub snapshot: Hash,
    pub entry: u32,
    pub kind: Authority,
    pub resource: u32,
    pub holder: Option<u32>,
    pub recording: Recording,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub version: u8,
    pub origin: String,
    pub scope: String,
    pub application_checkpoint: Hash,
    pub holders: Vec<Holder>,
    pub entries: Vec<Entry>,
    pub effects: Vec<Effect>,
    /// Observation IDs whose authority coverage is unknown, not "no effect".
    pub unmapped: Vec<String>,
    /// Additional authority outside the declared slice may be unknown. False
    /// is a declaration only, never operational completeness.
    pub unknown_authorities: bool,
}

/// A composed inert view. Only `application` has validated typed row preimages.
/// Unjournaled snapshots/evidence remain opaque, independently pinned ledger
/// observations, NOT application rows, authenticated receipts or settled effects.
#[derive(Debug)]
pub struct ApplicationInspection {
    pub application: BTreeMap<String, descriptors::Descriptor>,
    pub unjournaled: BTreeMap<String, Observation>,
    pub inventory: Inventory,
}

/// Inspect one exact independently bound bundle, its application preimages and
/// authority declarations together. The application checkpoint covers exactly
/// the typed rows; the bundle pin covers those AND explicit unjournaled records.
/// No unrecognized non-application observation is silently discarded.
pub fn inspect_application(
    origin: &BoundOrigin,
    history: &History,
    current: &Binding,
    independently_pinned_bundle: Option<Hash>,
    bundle: &[u8],
    bytes: &[u8],
    application: descriptors::Application<'_>,
) -> Result<ApplicationInspection> {
    let observations = super::inspect(history, current, independently_pinned_bundle, bundle)?;
    let inventory = inspect_observations(origin, current, &observations, bytes)?;
    let (unjournaled, recorded): (BTreeMap<_, _>, BTreeMap<_, _>) = observations
        .into_iter()
        .partition(|(id, _)| id.starts_with("unjournaled/"));
    ensure!(
        unjournaled.keys().all(|id| id.len() > "unjournaled/".len()),
        "unjournaled subject absent"
    );
    let application = descriptors::inspect_observations(
        &recorded,
        current,
        application.checkpoint,
        application.descriptors,
    )?;
    Ok(ApplicationInspection {
        application,
        unjournaled,
        inventory,
    })
}

/// Reuses the independently pinned ledger/fence boundary and canonical bounded
/// descriptor parser. Returns only declarations, with no proof/ready/retry result.
/// Does not retrieve evidence, parse application snapshot preimages or collect
/// real holder/resource information.
pub fn inspect(
    origin: &BoundOrigin,
    history: &History,
    current: &Binding,
    independently_pinned_bundle: Option<Hash>,
    bundle: &[u8],
    bytes: &[u8],
) -> Result<Inventory> {
    let observations = super::inspect(history, current, independently_pinned_bundle, bundle)?;
    inspect_observations(origin, current, &observations, bytes)
}

fn inspect_observations(
    origin: &BoundOrigin,
    current: &Binding,
    observations: &BTreeMap<String, Observation>,
    bytes: &[u8],
) -> Result<Inventory> {
    for observation in observations
        .values()
        .filter(|o| o.id.starts_with("unjournaled/"))
    {
        ensure!(
            observation.local_state == "effect:unknown",
            "unjournaled effect must remain unknown"
        );
    }
    ensure!(
        Hash::new(bytes) == current.inventory,
        "inventory checkpoint mismatch"
    );
    let inventory: Inventory = descriptors::parse(bytes)?;
    origin.verify(origin.origin.owner, origin.origin.namespace)?;
    ensure!(
        inventory.version == 1
            && inventory.origin == current.origin
            && inventory.origin == origin.hash()?
            && inventory.scope == origin.origin.scope
            && inventory.scope == SCOPE
            && inventory.application_checkpoint == current.application_checkpoint,
        "inventory origin/scope/application binding mismatch"
    );
    let mut holders = BTreeMap::new();
    for holder in &inventory.holders {
        ensure!(
            holder.id > 0 && holders.insert(holder.id, holder).is_none(),
            "duplicate/invalid holder"
        );
        if let Some(actor) = holder.actor {
            ensure!(
                actor == origin.origin.core || origin.origin.runners.contains(&actor),
                "holder actor outside original roles"
            );
        }
    }
    let mut entries = BTreeMap::new();
    let mut resources = BTreeSet::new();
    for entry in &inventory.entries {
        ensure!(
            entry.id > 0 && entry.resource > 0 && entry.credential_version.is_none_or(|v| v > 0),
            "invalid authority alias"
        );
        ensure!(
            entries.insert(entry.id, entry).is_none(),
            "duplicate authority entry"
        );
        ensure!(
            resources.insert((entry.kind, entry.resource, entry.credential_version)),
            "duplicate/conflicting authority resource"
        );
        ensure!(
            !entry.holders.is_empty() || entry.unknown_holders,
            "holder coverage absent"
        );
        let mut seen = BTreeSet::new();
        for holder in &entry.holders {
            ensure!(
                holders.contains_key(holder) && seen.insert(holder),
                "dangling/duplicate holder link"
            );
        }
    }
    let mut covered = BTreeSet::new();
    for effect in &inventory.effects {
        let observation = observations
            .get(&effect.observation)
            .context("dangling effect observation")?;
        ensure!(
            covered.insert(&effect.observation),
            "duplicate/conflicting effect link"
        );
        ensure!(
            effect.snapshot == observation.snapshot,
            "effect snapshot substitution"
        );
        let entry = entries
            .get(&effect.entry)
            .context("dangling authority link")?;
        ensure!(
            effect.kind == entry.kind && effect.resource == entry.resource,
            "effect authority/resource mismatch"
        );
        if let Some(holder) = effect.holder {
            ensure!(
                entry.holders.contains(&holder),
                "effect holder outside authority"
            );
        }
        let unjournaled = effect.observation.starts_with("unjournaled/");
        ensure!(
            unjournaled == (effect.recording == Recording::Unjournaled),
            "unjournaled effect relabelled"
        );
        // These known local observation families have a narrow declared mapping,
        // not provenance or service verification.
        if effect.observation.starts_with("delivery/") || effect.observation == "inbound" {
            ensure!(
                effect.kind == Authority::TelegramApi,
                "Telegram observation authority mismatch"
            );
        }
        if effect.observation.starts_with("task/") {
            ensure!(
                effect.kind == Authority::CodexProvider,
                "task observation authority mismatch"
            );
        }
    }
    for id in &inventory.unmapped {
        ensure!(
            observations.contains_key(id) && covered.insert(id),
            "dangling/duplicate/conflicting unknown coverage"
        );
    }
    ensure!(
        covered.len() == observations.len(),
        "observation coverage omitted"
    );
    Ok(inventory)
}

#[cfg(test)]
mod tests;
