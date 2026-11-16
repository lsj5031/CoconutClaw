//! Inert, synthetic reconciliation observations, not operational evidence.
//! Inspection checks shape and exact input identity, never trust, freshness,
//! completeness, non-execution, exclusion, retry eligibility or activation.
use super::*;
use std::collections::BTreeMap;

pub mod descriptors;
pub mod inventory;

/// Separately supplied current input. Digests refer to public/redacted fixture
/// records, never credentials. Origin and canonical fence bind roles and scope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub origin: String,
    pub fence_sequence: u64,
    pub fence_hash: Hash,
    pub application_checkpoint: Hash,
    pub inventory: Hash,
    pub policy_generation: u64,
    pub policy_hash: Hash,
    pub recovery_manifest: Option<Hash>,
}

/// An immutable observation at one checkpoint, not a mutable task or effect.
/// State is a bounded local label (e.g. `delivery:sending`), not an admission
/// decision. Even a terminal result/ack leaves external consequences unknown.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub id: String,
    pub local_state: String,
    pub snapshot: Hash,
    pub evidence: BTreeSet<Hash>,
}

/// A separate bundle, never a domain event or publication/receipt authority.
/// Missing observations/evidence are allowed for inert inspection only. There
/// is deliberately no "resolved", "complete", "approved" or execution field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bundle {
    pub binding: Binding,
    pub observations: Vec<Observation>,
}

fn label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

/// The independent hash is required even on restart; loading it from the bundle
/// or its backup defeats rollback detection. Matching stale independent inputs
/// cannot be detected here. No persisted high-water service is implemented.
///
/// Return only inspectable observations, never an activation or effect permit.
pub fn inspect(
    history: &History,
    current: &Binding,
    independently_pinned_bundle: Option<Hash>,
    bytes: &[u8],
) -> Result<BTreeMap<String, Observation>> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= MAX_WIRE,
        "reconciliation bundle size"
    );
    ensure!(
        independently_pinned_bundle.context("independent reconciliation checkpoint absent")?
            == Hash::new(bytes),
        "reconciliation checkpoint mismatch"
    );
    let bundle: Bundle = serde_json::from_slice(bytes)?;
    ensure!(
        serde_json::to_vec(&bundle)? == bytes,
        "noncanonical reconciliation bytes"
    );
    ensure!(
        &bundle.binding == current && current.policy_generation > 0,
        "reconciliation binding mismatch"
    );
    history.validate_fence_checkpoint(current.fence_sequence, &current.fence_hash.to_string())?;
    let fence = history
        .events
        .get(&current.fence_sequence)
        .context("reconciliation fence absent")?;
    ensure!(
        SignedEvent::parse(fence)?.event.origin == current.origin,
        "reconciliation origin mismatch"
    );
    let mut observations = BTreeMap::new();
    for observation in bundle.observations {
        ensure!(
            label(&observation.id) && label(&observation.local_state),
            "invalid reconciliation label"
        );
        if let Some(previous) = observations.insert(observation.id.clone(), observation.clone()) {
            ensure!(
                previous == observation,
                "reconciliation observation conflict"
            );
        }
    }
    Ok(observations)
}

#[cfg(test)]
mod tests;
