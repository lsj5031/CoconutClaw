use super::*;
use iroh_docs::NamespaceSecret;

pub(super) fn fixture() -> (BoundOrigin, History, Bundle) {
    let owner = SecretKey::from([211; 32]);
    let core = SecretKey::from([212; 32]);
    let runner = SecretKey::from([213; 32]);
    let origin = BoundOrigin::sign(
        Origin {
            owner: owner.public(),
            store: "c".repeat(64),
            namespace: NamespaceSecret::from_bytes(&[214; 32]).id(),
            scope: SCOPE.into(),
            core: core.public(),
            runners: BTreeSet::from([runner.public()]),
        },
        &owner,
    )
    .unwrap();
    let mut history = History::default();
    for (actor, action) in [
        (
            &core,
            Action::Assign {
                job: 1,
                attempt: 1,
                runner: runner.public(),
                request: "mock only".into(),
            },
        ),
        (&runner, Action::Start { assignment: 1 }),
        (
            &runner,
            Action::Result {
                assignment: 1,
                outcome: Outcome::Unknown,
                output: "unknown consequences".into(),
            },
        ),
        (
            &owner,
            Action::Fence {
                previous_epoch: 1,
                epoch: 2,
                core: SecretKey::from([215; 32]).public(),
                runners: BTreeSet::new(),
            },
        ),
    ] {
        let signed = SignedEvent::sign(
            history.next(&origin, actor.public(), action).unwrap(),
            actor,
        )
        .unwrap();
        let mut bytes = history.export();
        bytes.push(signed.bytes().unwrap());
        history = History::replay(&origin, owner.public(), origin.origin.namespace, bytes).unwrap();
    }
    let binding = Binding {
        origin: origin.hash().unwrap(),
        fence_sequence: 4,
        fence_hash: Hash::new(&history.export()[3]),
        application_checkpoint: Hash::new(b"mock application checkpoint"),
        inventory: Hash::new(b"mock complete inventory revision"),
        policy_generation: 7,
        policy_hash: Hash::new(b"mock policy"),
        recovery_manifest: Some(Hash::new(b"mock recovery manifest")),
    };
    let observations = [
        ("task/1", "attempt:unknown"),
        ("payload/1", "output:recorded"),
        ("delivery/1/telegram:text:0", "delivery:sending"),
        ("delivery/1/telegram:text:1", "delivery:acknowledged"),
        ("approval/2", "approval:pending"),
        ("unjournaled/progress", "effect:unknown"),
    ]
    .into_iter()
    .map(|(id, state)| Observation {
        id: id.into(),
        local_state: state.into(),
        snapshot: Hash::new(state.as_bytes()),
        evidence: BTreeSet::new(),
    })
    .collect();
    (
        origin,
        history,
        Bundle {
            binding,
            observations,
        },
    )
}

fn inspect_fixture(
    history: &History,
    current: &Binding,
    bundle: &Bundle,
) -> Result<BTreeMap<String, Observation>> {
    let bytes = serde_json::to_vec(bundle)?;
    inspect(history, current, Some(Hash::new(&bytes)), &bytes)
}

#[test]
fn partial_unknown_records_are_inspectable_but_never_authorize_effects_or_activation() {
    let (origin, history, bundle) = fixture();
    let before = history.export();
    let records = inspect_fixture(&history, &bundle.binding, &bundle).unwrap();
    assert_eq!(records.len(), 6);
    assert_eq!(records["task/1"].local_state, "attempt:unknown");
    assert_eq!(
        records["delivery/1/telegram:text:0"].local_state,
        "delivery:sending"
    );
    assert!(records.values().all(|r| r.evidence.is_empty()));
    assert_eq!(
        history.jobs[&1].result.as_ref().unwrap().0,
        Outcome::Unknown
    );
    assert!(history.validate_replacement_activation().is_err());
    // Inspection cannot insert a post-fence result, resend intent or higher
    // attempt. Exercise the existing real domain refusal, not a mock boolean.
    let runner = SecretKey::from([213; 32]);
    let late = SignedEvent::sign(
        history
            .next(
                &origin,
                runner.public(),
                Action::Result {
                    assignment: 1,
                    outcome: Outcome::Completed,
                    output: "ledger does not settle this".into(),
                },
            )
            .unwrap(),
        &runner,
    )
    .unwrap();
    let mut attempted = before.clone();
    attempted.push(late.bytes().unwrap());
    assert!(
        History::replay(
            &origin,
            origin.origin.owner,
            origin.origin.namespace,
            attempted
        )
        .is_err()
    );
    assert_eq!(history.export(), before);
}

#[test]
fn every_checkpoint_component_and_inventory_substitution_refuses() {
    let (_, history, bundle) = fixture();
    let other = Hash::new(b"other fixture");
    let mut variants = Vec::new();
    macro_rules! changed {
        ($field:ident, $value:expr) => {{
            let mut b = bundle.binding.clone();
            b.$field = $value;
            variants.push(b);
        }};
    }
    changed!(origin, "d".repeat(64));
    changed!(fence_sequence, 3);
    changed!(fence_hash, other);
    changed!(application_checkpoint, other);
    changed!(inventory, other);
    changed!(policy_generation, 6);
    changed!(policy_hash, other);
    changed!(recovery_manifest, None);
    for current in variants {
        assert!(inspect_fixture(&history, &current, &bundle).is_err());
    }
    // Equal caller/bundle bindings still cannot launder another fence/origin.
    for field in [
        "origin",
        "fence_sequence",
        "fence_hash",
        "policy_generation",
    ] {
        let mut forged = serde_json::to_value(&bundle).unwrap();
        forged["binding"][field] = match field {
            "origin" => serde_json::json!("d".repeat(64)),
            "fence_sequence" => serde_json::json!(3),
            "policy_generation" => serde_json::json!(0),
            _ => serde_json::to_value(other).unwrap(),
        };
        let forged: Bundle = serde_json::from_value(forged).unwrap();
        assert!(inspect_fixture(&history, &forged.binding, &forged).is_err());
    }
}

#[test]
fn exact_duplicates_collapse_conflicting_or_relabelled_unknowns_refuse_entire_view() {
    let (_, history, mut bundle) = fixture();
    bundle.observations.push(bundle.observations[0].clone());
    assert_eq!(
        inspect_fixture(&history, &bundle.binding, &bundle)
            .unwrap()
            .len(),
        6
    );
    for change in ["state", "snapshot", "evidence"] {
        let mut conflict = bundle.clone();
        let row = conflict.observations.last_mut().unwrap();
        match change {
            "state" => row.local_state = "attempt:completed".into(),
            "snapshot" => row.snapshot = Hash::new(b"conflicting result"),
            _ => {
                row.evidence.insert(Hash::new(b"untrusted receipt"));
            }
        }
        assert!(inspect_fixture(&history, &bundle.binding, &conflict).is_err());
    }
}

#[test]
fn restart_requires_independent_checkpoint_and_rejects_ledger_or_history_rollback() {
    let (origin, history, old) = fixture();
    let mut newer = old.clone();
    newer.observations.push(Observation {
        id: "unjournaled/upload".into(),
        local_state: "effect:unknown".into(),
        snapshot: Hash::new(b"additional mock observation"),
        evidence: BTreeSet::new(),
    });
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("inert-ledger.json");
    let current = serde_json::to_vec(&newer).unwrap();
    std::fs::write(&file, &current).unwrap();
    let pinned_outside_backup = Hash::new(&current);
    let restarted = History::replay(
        &origin,
        origin.origin.owner,
        origin.origin.namespace,
        history.export(),
    )
    .unwrap();
    let saved = std::fs::read(&file).unwrap();
    assert!(inspect(&restarted, &old.binding, None, &saved).is_err());
    assert_eq!(
        inspect(
            &restarted,
            &old.binding,
            Some(pinned_outside_backup),
            &saved
        )
        .unwrap()
        .len(),
        7
    );
    std::fs::write(&file, serde_json::to_vec(&old).unwrap()).unwrap();
    assert!(
        inspect(
            &restarted,
            &old.binding,
            Some(pinned_outside_backup),
            &std::fs::read(&file).unwrap(),
        )
        .is_err()
    );
    let prefix = History::replay(
        &origin,
        origin.origin.owner,
        origin.origin.namespace,
        history.export().into_iter().take(3),
    )
    .unwrap();
    assert!(inspect(&prefix, &newer.binding, Some(pinned_outside_backup), &saved).is_err());
    assert!(restarted.validate_replacement_activation().is_err());
}

#[test]
fn malformed_oversized_and_proof_fields_are_not_accepted() {
    let (_, history, bundle) = fixture();
    for (id, state) in [("", "delivery:ready"), ("task/1", "unknown\nresolved")] {
        let mut malformed = bundle.clone();
        malformed.observations[0].id = id.into();
        malformed.observations[0].local_state = state.into();
        assert!(inspect_fixture(&history, &bundle.binding, &malformed).is_err());
    }
    let mut forged = serde_json::to_value(&bundle).unwrap();
    forged["approved"] = true.into();
    let bytes = serde_json::to_vec(&forged).unwrap();
    assert!(inspect(&history, &bundle.binding, Some(Hash::new(&bytes)), &bytes).is_err());
    let bytes = vec![b' '; MAX_WIRE + 1];
    assert!(inspect(&history, &bundle.binding, Some(Hash::new(&bytes)), &bytes).is_err());
}
