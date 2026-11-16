//! Public composition boundary, reusing the actual-schema descriptor fixture.
use super::*;
use crate::synthetic::membership::domain::reconciliation::inventory::{
    self, ApplicationInspection, Authority, Effect, Entry, Holder, Inventory, Recording,
};

struct Fixture {
    origin: BoundOrigin,
    history: History,
    bundle: Bundle,
    inventory: Inventory,
    checkpoint: Vec<u8>,
    rows: Vec<Vec<u8>>,
}

impl Fixture {
    fn new() -> Self {
        let (origin, history, _) = super::super::super::tests::fixture();
        let (rows, missing) = descriptors(&database());
        let (_, bundle, checkpoint, rows) = encode(&rows, missing);
        let mut bundle: Bundle = serde_json::from_slice(&bundle).unwrap();
        for subject in ["progress", "tool"] {
            bundle.observations.push(Observation {
                id: format!("unjournaled/{subject}"),
                local_state: "effect:unknown".into(),
                // Opaque alias, intentionally NOT an application descriptor.
                snapshot: Hash::new(subject.as_bytes()),
                evidence: BTreeSet::from([Hash::new(b"artificial unauthenticated evidence")]),
            });
        }
        let inventory = Inventory {
            version: 1,
            origin: origin.hash().unwrap(),
            scope: SCOPE.into(),
            application_checkpoint: Hash::new(&checkpoint),
            holders: vec![
                Holder {
                    id: 1,
                    actor: Some(origin.origin.core),
                },
                Holder { id: 2, actor: None },
                Holder {
                    id: 3,
                    actor: Some(origin.origin.core),
                },
            ],
            entries: vec![Entry {
                id: 1,
                kind: Authority::TelegramApi,
                resource: 1,
                credential_version: None,
                holders: vec![1, 2, 3],
                unknown_holders: true,
            }],
            effects: bundle
                .observations
                .iter()
                .filter(|o| o.id.starts_with("delivery/") || o.id == "unjournaled/progress")
                .map(|o| Effect {
                    observation: o.id.clone(),
                    snapshot: o.snapshot,
                    entry: 1,
                    kind: Authority::TelegramApi,
                    resource: 1,
                    holder: None,
                    recording: if o.id.starts_with("unjournaled/") {
                        Recording::Unjournaled
                    } else {
                        Recording::Recorded
                    },
                })
                .collect(),
            unmapped: bundle
                .observations
                .iter()
                .filter(|o| !o.id.starts_with("delivery/") && o.id != "unjournaled/progress")
                .map(|o| o.id.clone())
                .collect(),
            unknown_authorities: true,
        };
        Self {
            origin,
            history,
            bundle,
            inventory,
            checkpoint,
            rows,
        }
    }

    fn encoded(&self) -> (Binding, Vec<u8>, Vec<u8>) {
        let inventory = serde_json::to_vec(&self.inventory).unwrap();
        let mut bundle = self.bundle.clone();
        bundle.binding.inventory = Hash::new(&inventory);
        (
            bundle.binding.clone(),
            serde_json::to_vec(&bundle).unwrap(),
            inventory,
        )
    }

    fn check(&self) -> Result<ApplicationInspection> {
        let (binding, bundle, inventory) = self.encoded();
        inventory::inspect_application(
            &self.origin,
            &self.history,
            &binding,
            Some(Hash::new(&bundle)),
            &bundle,
            &inventory,
            Application {
                checkpoint: &self.checkpoint,
                descriptors: &self.rows,
            },
        )
    }

    fn repin_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.checkpoint = serde_json::to_vec(&checkpoint).unwrap();
        self.bundle.binding.application_checkpoint = Hash::new(&self.checkpoint);
        self.inventory.application_checkpoint = Hash::new(&self.checkpoint);
    }
}

#[test]
fn composition_exact_rows_and_opaque_unjournaled_observations_stay_distinct() {
    let f = Fixture::new();
    let before = f.history.export();
    let view = f.check().unwrap();
    assert_eq!(view.application.len(), 11);
    assert_eq!(view.unjournaled.len(), 2);
    assert_eq!(view.inventory, f.inventory);
    assert_eq!(
        view.application["delivery/1/telegram:text:chunk:0"].local_state(),
        "delivery:sending"
    );
    for observation in f
        .bundle
        .observations
        .iter()
        .filter(|o| o.id.starts_with("unjournaled/"))
    {
        assert_eq!(&view.unjournaled[&observation.id], observation);
        assert!(!view.application.contains_key(&observation.id));
    }
    assert!(view.inventory.unmapped.contains(&"unjournaled/tool".into()));
    assert!(view.inventory.unknown_authorities && view.inventory.entries[0].unknown_holders);
    assert_eq!(view.inventory.holders[1].actor, None);
    assert_eq!(
        view.inventory.holders[0].actor,
        view.inventory.holders[2].actor
    );
    assert_eq!(
        f.history.jobs[&1].result.as_ref().unwrap().0,
        Outcome::Unknown
    );
    assert_eq!(f.history.export(), before);
    // The old standalone application inspector still requires ONLY typed rows.
    let (binding, bundle, inventory) = f.encoded();
    assert!(
        inspect(
            &f.history,
            &binding,
            Some(Hash::new(&bundle)),
            &bundle,
            &f.checkpoint,
            &f.rows
        )
        .is_err()
    );
    println!(
        "COMPOSITION_FIXTURE {}",
        json!({
            "bundle": std::str::from_utf8(&bundle).unwrap(),
            "inventory": std::str::from_utf8(&inventory).unwrap(),
            "checkpoint": std::str::from_utf8(&f.checkpoint).unwrap(),
            "descriptors": f.rows.iter().map(|r| std::str::from_utf8(r).unwrap()).collect::<Vec<_>>(),
            "application_rows": view.application.len(), "opaque_unjournaled": view.unjournaled,
            "disposition": "inert; Unknown and activation refusal unchanged"
        })
    );
}

#[test]
fn composition_independent_pins_and_each_exact_input_refuse_substitution() {
    let f = Fixture::new();
    let (binding, bundle, inventory) = f.encoded();
    let call = |history: &History,
                binding: &Binding,
                pin,
                bundle: &[u8],
                inventory: &[u8],
                checkpoint: &[u8],
                rows: &[Vec<u8>]| {
        inventory::inspect_application(
            &f.origin,
            history,
            binding,
            pin,
            bundle,
            inventory,
            Application {
                checkpoint,
                descriptors: rows,
            },
        )
    };
    assert!(
        call(
            &f.history,
            &binding,
            None,
            &bundle,
            &inventory,
            &f.checkpoint,
            &f.rows
        )
        .is_err()
    );
    assert!(
        call(
            &f.history,
            &binding,
            Some(Hash::new(b"old")),
            &bundle,
            &inventory,
            &f.checkpoint,
            &f.rows
        )
        .is_err()
    );
    assert!(
        call(
            &f.history,
            &binding,
            Some(Hash::new(&bundle)),
            &bundle,
            b"{}",
            &f.checkpoint,
            &f.rows
        )
        .is_err()
    );
    assert!(
        call(
            &f.history,
            &binding,
            Some(Hash::new(&bundle)),
            &bundle,
            &inventory,
            b"{}",
            &f.rows
        )
        .is_err()
    );
    assert!(
        call(
            &f.history,
            &binding,
            Some(Hash::new(&bundle)),
            &bundle,
            &inventory,
            &f.checkpoint,
            &f.rows[1..]
        )
        .is_err()
    );
    for field in 0..8 {
        let mut altered = binding.clone();
        match field {
            0 => altered.origin = "wrong".into(),
            1 => altered.fence_sequence -= 1,
            2 => altered.fence_hash = Hash::new(b"wrong"),
            3 => altered.application_checkpoint = Hash::new(b"wrong"),
            4 => altered.inventory = Hash::new(b"wrong"),
            5 => altered.policy_generation += 1,
            6 => altered.policy_hash = Hash::new(b"wrong"),
            _ => altered.recovery_manifest = None,
        }
        assert!(
            call(
                &f.history,
                &altered,
                Some(Hash::new(&bundle)),
                &bundle,
                &inventory,
                &f.checkpoint,
                &f.rows
            )
            .is_err()
        );
    }
    let prefix = History::replay(
        &f.origin,
        f.origin.origin.owner,
        f.origin.origin.namespace,
        f.history.export().into_iter().take(3),
    )
    .unwrap();
    assert!(
        call(
            &prefix,
            &binding,
            Some(Hash::new(&bundle)),
            &bundle,
            &inventory,
            &f.checkpoint,
            &f.rows
        )
        .is_err()
    );
    let mut forged = Fixture::new();
    forged.bundle.binding.fence_hash = Hash::new(b"self-selected fence");
    assert!(forged.check().is_err()); // Rehashing the bundle cannot forge history.
}

#[test]
fn composition_rehashed_contradictions_and_malformed_coverage_refuse_whole_view() {
    for case in 0..12 {
        let mut f = Fixture::new();
        match case {
            0 => {
                f.inventory.unmapped.pop();
            }
            1 => {
                f.inventory
                    .unmapped
                    .push(f.inventory.effects[0].observation.clone());
            }
            2 => {
                f.inventory.effects.push(f.inventory.effects[0].clone());
            }
            3 => {
                f.inventory.effects[0].snapshot = Hash::new(b"substitute");
            }
            4 => {
                f.inventory.effects.last_mut().unwrap().recording = Recording::Recorded;
            }
            5 => {
                f.bundle.observations.last_mut().unwrap().local_state = "effect:resolved".into();
            }
            6 => {
                f.bundle.observations[0].local_state = "task:unknown".into();
            }
            7 => {
                f.inventory.application_checkpoint = Hash::new(b"other application");
            }
            8 => {
                let mut checkpoint: Checkpoint = serde_json::from_slice(&f.checkpoint).unwrap();
                checkpoint.missing.insert(Journal::TaskRuns);
                f.repin_checkpoint(checkpoint);
            }
            9 => {
                let mut checkpoint: Checkpoint = serde_json::from_slice(&f.checkpoint).unwrap();
                let o = f.bundle.observations.last().unwrap();
                checkpoint.observations.insert(o.id.clone(), o.snapshot);
                f.repin_checkpoint(checkpoint); // Opaque snapshots cannot become rows.
            }
            10 => {
                let o = f.bundle.observations.last_mut().unwrap();
                let old = o.id.clone();
                o.id = "unjournaled/".into();
                *f.inventory
                    .unmapped
                    .iter_mut()
                    .find(|id| **id == old)
                    .unwrap() = o.id.clone();
            }
            _ => {
                let mut o = f.bundle.observations.last().unwrap().clone();
                o.id = "opaque/other".into();
                f.inventory.unmapped.push(o.id.clone());
                f.bundle.observations.push(o); // Non-application families cannot disappear.
            }
        }
        assert!(f.check().is_err(), "contradiction {case}");
    }
}

#[test]
fn composition_duplicates_reload_and_missing_journals_preserve_exact_unknowns() {
    let mut f = Fixture::new();
    f.bundle
        .observations
        .push(f.bundle.observations.last().unwrap().clone());
    f.rows.push(f.rows[0].clone());
    assert_eq!(f.check().unwrap().application.len(), 11);
    let mut conflict = f.bundle.observations.last().unwrap().clone();
    conflict
        .evidence
        .insert(Hash::new(b"contradictory immutable record"));
    f.bundle.observations.push(conflict);
    assert!(f.check().is_err());

    let mut f = Fixture::new();
    let empty = database();
    empty.execute_batch("DROP TABLE task_payloads; DROP TABLE approvals; DROP TABLE turns; DROP TABLE kv; DROP TABLE task_runs;").unwrap();
    let (rows, missing) = descriptors(&empty);
    let (_, bundle, checkpoint, rows) = encode(&rows, missing);
    f.bundle = serde_json::from_slice(&bundle).unwrap();
    f.checkpoint = checkpoint;
    f.rows = rows;
    f.inventory.application_checkpoint = Hash::new(&f.checkpoint);
    f.inventory.effects.clear();
    f.inventory.unmapped = f.bundle.observations.iter().map(|o| o.id.clone()).collect();
    let view = f.check().unwrap();
    assert!(
        view.application
            .values()
            .all(|d| d.local_state().contains("unknown") || d.id() == "inbound")
    );
    let (binding, bundle, inventory) = f.encoded();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    std::fs::write(&path, &bundle).unwrap();
    let restored = History::replay(
        &f.origin,
        f.origin.origin.owner,
        f.origin.origin.namespace,
        f.history.export(),
    )
    .unwrap();
    assert!(
        inventory::inspect_application(
            &f.origin,
            &restored,
            &binding,
            Some(Hash::new(&bundle)),
            &std::fs::read(path).unwrap(),
            &inventory,
            Application {
                checkpoint: &f.checkpoint,
                descriptors: &f.rows
            }
        )
        .is_ok()
    );
}

#[test]
fn composition_rebinding_digests_never_validates_forged_descriptor_content() {
    for case in 0..4 {
        let mut f = Fixture::new();
        let mut descriptor: Descriptor = serde_json::from_slice(&f.rows[0]).unwrap();
        match case {
            0 => descriptor.source = Hash::new(b"other source"),
            1 => {
                let Row::Task { status, .. } = &mut descriptor.row else {
                    unreachable!()
                };
                *status = Some("resolved".into());
            }
            2 => {}
            _ => {
                let Row::Task { run, .. } = &mut descriptor.row else {
                    unreachable!()
                };
                *run = 0;
            }
        }
        f.rows[0] = serde_json::to_vec(&descriptor).unwrap();
        if case == 2 {
            f.rows[0].pop();
            f.rows[0].extend_from_slice(b",\"credential_exclusion\":true}");
        }
        // Repin every digest and the declared state. Content still must pass
        // the existing canonical/source/typed-link checks, not just hashes.
        let mut checkpoint: Checkpoint = serde_json::from_slice(&f.checkpoint).unwrap();
        let snapshot = Hash::new(&f.rows[0]);
        checkpoint.observations.insert("task/1".into(), snapshot);
        f.bundle.observations[0].snapshot = snapshot;
        if case == 1 {
            f.bundle.observations[0].local_state = "task:resolved".into();
        }
        f.repin_checkpoint(checkpoint);
        assert!(f.check().is_err(), "forged descriptor {case}");
    }
}

#[test]
fn composition_complete_looking_declarations_never_authorize_retry_or_activation() {
    let mut f = Fixture::new();
    f.inventory.unknown_authorities = false;
    f.inventory.entries[0].unknown_holders = false;
    f.inventory.entries[0].credential_version = Some(1);
    f.inventory.holders[1].actor = Some(f.origin.origin.core);
    // Cover every row, without claiming a service receipt or reconciling effects.
    for id in std::mem::take(&mut f.inventory.unmapped) {
        let o = f.bundle.observations.iter().find(|o| o.id == id).unwrap();
        let kind = if id.starts_with("task/") {
            Authority::CodexProvider
        } else if id == "inbound" {
            Authority::TelegramApi
        } else {
            Authority::Tool
        };
        let entry = if kind == Authority::CodexProvider {
            2
        } else if kind == Authority::TelegramApi {
            1
        } else {
            3
        };
        if !f.inventory.entries.iter().any(|e| e.id == entry) {
            f.inventory.entries.push(Entry {
                id: entry,
                kind,
                resource: entry,
                credential_version: Some(1),
                holders: vec![1],
                unknown_holders: false,
            });
        }
        f.inventory.effects.push(Effect {
            observation: id.clone(),
            snapshot: o.snapshot,
            entry,
            kind,
            resource: entry,
            holder: Some(1),
            recording: if id.starts_with("unjournaled/") {
                Recording::Unjournaled
            } else {
                Recording::Recorded
            },
        });
    }
    let before = f.history.export();
    let view = f.check().unwrap();
    assert!(view.inventory.unmapped.is_empty());
    assert!(
        view.unjournaled
            .values()
            .all(|o| o.local_state == "effect:unknown")
    );
    assert!(f.history.validate_replacement_activation().is_err());
    let key = SecretKey::from([212; 32]);
    let retry = SignedEvent::sign(
        f.history
            .next(
                &f.origin,
                key.public(),
                Action::Assign {
                    job: 1,
                    attempt: 2,
                    runner: *f.origin.origin.runners.first().unwrap(),
                    request: "prohibited mock retry".into(),
                },
            )
            .unwrap(),
        &key,
    )
    .unwrap();
    let mut events = before.clone();
    events.push(retry.bytes().unwrap());
    assert!(
        History::replay(
            &f.origin,
            f.origin.origin.owner,
            f.origin.origin.namespace,
            events
        )
        .is_err()
    );
    assert_eq!(f.history.export(), before);
}
