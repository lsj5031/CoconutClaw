use super::*;
use serde_json::json;

const LIMIT: usize = 4096;

fn fixture(terminal: bool) -> (BoundOrigin, History, Bundle, Inventory) {
    let (origin, mut history, mut bundle) = super::super::tests::fixture();
    if terminal {
        let fence = SignedEvent::parse(&history.export()[3])
            .unwrap()
            .event
            .action;
        history = History::replay(
            &origin,
            origin.origin.owner,
            origin.origin.namespace,
            history.export().into_iter().take(2),
        )
        .unwrap();
        for (key, action) in [
            (
                SecretKey::from([213; 32]),
                Action::Result {
                    assignment: 1,
                    outcome: Outcome::Completed,
                    output: "artificial result".into(),
                },
            ),
            (SecretKey::from([211; 32]), fence),
        ] {
            let signed =
                SignedEvent::sign(history.next(&origin, key.public(), action).unwrap(), &key)
                    .unwrap();
            let mut events = history.export();
            events.push(signed.bytes().unwrap());
            history = History::replay(
                &origin,
                origin.origin.owner,
                origin.origin.namespace,
                events,
            )
            .unwrap();
        }
        bundle.binding.fence_hash = Hash::new(&history.export()[3]);
        bundle.observations[0].local_state = "attempt:completed".into();
    }
    bundle.observations.push(Observation {
        id: "unjournaled/tool".into(),
        local_state: "effect:unknown".into(),
        snapshot: Hash::new(b"artificial tool-effect observation"),
        evidence: BTreeSet::new(),
    });
    let holders = vec![
        Holder {
            id: 1,
            actor: Some(origin.origin.core),
        },
        Holder {
            id: 2,
            actor: origin.origin.runners.first().copied(),
        },
        Holder { id: 3, actor: None },
    ];
    let entries = vec![
        Entry {
            id: 1,
            kind: Authority::CodexProvider,
            resource: 11,
            credential_version: Some(1),
            holders: vec![1],
            unknown_holders: false,
        },
        Entry {
            id: 2,
            kind: Authority::TelegramApi,
            resource: 12,
            credential_version: Some(1),
            holders: vec![1, 3],
            unknown_holders: true,
        },
        Entry {
            id: 3,
            kind: Authority::Tool,
            resource: 13,
            credential_version: None,
            holders: vec![2, 3],
            unknown_holders: true,
        },
    ];
    let mut effects = Vec::new();
    let mut unmapped = Vec::new();
    for observation in &bundle.observations {
        let (entry, holder) = if observation.id.starts_with("task/") {
            (1, Some(1))
        } else if observation.id.starts_with("delivery/") {
            (2, Some(1))
        } else if observation.id == "unjournaled/progress" {
            (2, None)
        } else if observation.id == "unjournaled/tool" {
            (3, None)
        } else {
            unmapped.push(observation.id.clone());
            continue;
        };
        let authority = &entries[entry - 1];
        effects.push(Effect {
            observation: observation.id.clone(),
            snapshot: observation.snapshot,
            entry: authority.id,
            kind: authority.kind,
            resource: authority.resource,
            holder,
            recording: if observation.id.starts_with("unjournaled/") {
                Recording::Unjournaled
            } else {
                Recording::Recorded
            },
        });
    }
    let inventory = Inventory {
        version: 1,
        origin: origin.hash().unwrap(),
        scope: SCOPE.into(),
        application_checkpoint: bundle.binding.application_checkpoint,
        holders,
        entries,
        effects,
        unmapped,
        unknown_authorities: true,
    };
    (origin, history, bundle, inventory)
}

fn encode(bundle: &Bundle, inventory: &Inventory) -> (Bundle, Vec<u8>, Vec<u8>) {
    let bytes = serde_json::to_vec(inventory).unwrap();
    let mut bundle = bundle.clone();
    bundle.binding.inventory = Hash::new(&bytes);
    let saved = serde_json::to_vec(&bundle).unwrap();
    (bundle, saved, bytes)
}

fn check(
    origin: &BoundOrigin,
    history: &History,
    bundle: &Bundle,
    inventory: &Inventory,
) -> Result<Inventory> {
    let (bundle, saved, bytes) = encode(bundle, inventory);
    inspect(
        origin,
        history,
        &bundle.binding,
        Some(Hash::new(&saved)),
        &saved,
        &bytes,
    )
}

#[test]
fn partial_inventory_preserves_known_and_unjournaled_links_and_unknown_holders() {
    let (origin, history, bundle, inventory) = fixture(false);
    let before = history.export();
    let inspected = check(&origin, &history, &bundle, &inventory).unwrap();
    assert_eq!(inspected, inventory);
    assert!(inspected.unknown_authorities && inspected.entries[2].unknown_holders);
    assert!(inspected.holders[2].actor.is_none());
    assert_eq!(inspected.unmapped, ["payload/1", "approval/2"]);
    assert_eq!(
        inspected
            .effects
            .iter()
            .filter(|e| e.recording == Recording::Unjournaled)
            .count(),
        2
    );
    assert_eq!(
        history.jobs[&1].result.as_ref().unwrap().0,
        Outcome::Unknown
    );
    assert_eq!(history.export(), before);
    assert!(history.validate_replacement_activation().is_err());
    let (_, saved, bytes) = encode(&bundle, &inventory);
    println!(
        "INVENTORY_FIXTURE {}",
        json!({
            "inventory":std::str::from_utf8(&bytes).unwrap(),
            "bundle":std::str::from_utf8(&saved).unwrap(),
            "disposition":"inert; all real effects remain unproven",
        })
    );
}

#[test]
fn complete_looking_inventory_and_evidence_never_settle_effects_or_enable_retry() {
    let (origin, history, mut bundle, mut inventory) = fixture(true);
    bundle
        .observations
        .retain(|o| !inventory.unmapped.contains(&o.id));
    inventory.unmapped.clear();
    inventory.unknown_authorities = false;
    inventory.holders[2].actor = Some(origin.origin.core); // Another copied holder, not deduplicated.
    for entry in &mut inventory.entries {
        entry.unknown_holders = false;
        entry.credential_version = Some(1);
    }
    for effect in &mut inventory.effects {
        effect.holder = Some(
            inventory
                .entries
                .iter()
                .find(|e| e.id == effect.entry)
                .unwrap()
                .holders[0],
        );
    }
    for observation in &mut bundle.observations {
        observation.evidence.insert(Hash::new(
            b"opaque artificial evidence reference, not authenticated",
        ));
    }
    let before = history.export();
    let inspected = check(&origin, &history, &bundle, &inventory).unwrap();
    assert!(!inspected.unknown_authorities && inspected.unmapped.is_empty());
    assert_eq!(inspected.holders.len(), 3); // Same key never erases the second holder.
    assert!(
        bundle
            .observations
            .iter()
            .filter(|o| o.id.starts_with("unjournaled/"))
            .all(|o| o.local_state == "effect:unknown")
    );
    assert!(
        history
            .validate_replacement_activation()
            .unwrap_err()
            .to_string()
            .contains("unproven")
    );
    let core = SecretKey::from([212; 32]);
    let retry = SignedEvent::sign(
        history
            .next(
                &origin,
                core.public(),
                Action::Assign {
                    job: 1,
                    attempt: 2,
                    runner: *origin.origin.runners.first().unwrap(),
                    request: "prohibited artificial retry".into(),
                },
            )
            .unwrap(),
        &core,
    )
    .unwrap();
    let mut events = before.clone();
    events.push(retry.bytes().unwrap());
    assert!(
        History::replay(
            &origin,
            origin.origin.owner,
            origin.origin.namespace,
            events
        )
        .is_err()
    );
    assert_eq!(history.export(), before);
}

#[test]
fn inventory_checkpoint_origin_scope_actor_and_target_substitution_refuse() {
    let (origin, history, bundle, inventory) = fixture(false);
    let mut variants = Vec::new();
    let mut wrong = inventory.clone();
    wrong.scope = "synthetic/chat-b".into();
    variants.push(wrong);
    let mut wrong = inventory.clone();
    wrong.origin = "d".repeat(64);
    variants.push(wrong);
    let mut wrong = inventory.clone();
    wrong.application_checkpoint = Hash::new(b"other checkpoint");
    variants.push(wrong);
    let mut wrong = inventory.clone();
    wrong.holders[0].actor = Some(SecretKey::from([250; 32]).public());
    variants.push(wrong);
    let mut wrong = inventory.clone();
    wrong.effects[0].resource = 99;
    variants.push(wrong);
    let mut wrong = inventory.clone();
    wrong.effects[0].kind = Authority::Workspace;
    variants.push(wrong);
    for wrong in variants {
        assert!(check(&origin, &history, &bundle, &wrong).is_err());
    }
    let (pinned, saved, _) = encode(&bundle, &inventory);
    let mut changed = inventory.clone();
    changed.unknown_authorities = false;
    assert!(
        inspect(
            &origin,
            &history,
            &pinned.binding,
            Some(Hash::new(&saved)),
            &saved,
            &serde_json::to_vec(&changed).unwrap()
        )
        .is_err()
    );
    let mut substituted = origin.origin.clone();
    substituted.store = "d".repeat(64);
    let substituted = BoundOrigin::sign(substituted, &SecretKey::from([211; 32])).unwrap();
    assert!(check(&substituted, &history, &bundle, &inventory).is_err());
    assert!(
        inspect(
            &origin,
            &history,
            &pinned.binding,
            None,
            &saved,
            &serde_json::to_vec(&inventory).unwrap()
        )
        .is_err()
    );
}

#[test]
fn duplicate_conflicting_dangling_and_malformed_links_refuse_whole_inventory() {
    let (origin, history, bundle, inventory) = fixture(false);
    let mut variants = Vec::new();
    macro_rules! changed {
        ($edit:expr) => {{
            let mut i = inventory.clone();
            $edit(&mut i);
            variants.push(i);
        }};
    }
    changed!(|i: &mut Inventory| i.holders.push(i.holders[0].clone()));
    changed!(|i: &mut Inventory| {
        let mut h = i.holders[0].clone();
        h.actor = None;
        i.holders.push(h);
    });
    changed!(|i: &mut Inventory| i.entries.push(i.entries[0].clone()));
    changed!(|i: &mut Inventory| {
        let mut e = i.entries[0].clone();
        e.id = 99;
        i.entries.push(e);
    });
    changed!(|i: &mut Inventory| i.entries[0].holders.push(1));
    changed!(|i: &mut Inventory| i.entries[0].holders.push(99));
    changed!(|i: &mut Inventory| i.effects.push(i.effects[0].clone()));
    changed!(|i: &mut Inventory| {
        let mut e = i.effects[0].clone();
        e.entry = 2;
        i.effects.push(e);
    });
    changed!(|i: &mut Inventory| i.effects[0].entry = 99);
    changed!(|i: &mut Inventory| i.effects[0].observation = "missing-observation".into());
    changed!(|i: &mut Inventory| i.effects[0].snapshot = Hash::new(b"other observation"));
    changed!(|i: &mut Inventory| i.effects[0].holder = Some(2));
    changed!(|i: &mut Inventory| i.effects[0].recording = Recording::Unjournaled);
    changed!(|i: &mut Inventory| i.entries[0].id = 0);
    changed!(|i: &mut Inventory| i.entries[0].resource = 0);
    changed!(|i: &mut Inventory| i.entries[0].credential_version = Some(0));
    changed!(|i: &mut Inventory| i.holders[0].id = 0);
    changed!(|i: &mut Inventory| {
        i.entries[0].holders.clear();
        i.entries[0].unknown_holders = false;
    });
    for invalid in variants {
        assert!(check(&origin, &history, &bundle, &invalid).is_err());
    }
    // A self-consistent wrong authority for a known local observation still refuses.
    let mut wrong = inventory.clone();
    wrong.entries[0].kind = Authority::Workspace;
    wrong.effects[0].kind = Authority::Workspace;
    assert!(check(&origin, &history, &bundle, &wrong).is_err());
    let mut relabelled = bundle.clone();
    relabelled
        .observations
        .iter_mut()
        .find(|o| o.id == "unjournaled/tool")
        .unwrap()
        .local_state = "effect:resolved".into();
    assert!(check(&origin, &history, &relabelled, &inventory).is_err());
}

#[test]
fn coverage_gaps_are_explicit_and_never_become_absent_effects() {
    let (origin, history, mut bundle, mut inventory) = fixture(false);
    let removed = inventory.effects.remove(0);
    assert!(check(&origin, &history, &bundle, &inventory).is_err());
    inventory.unmapped.push(removed.observation.clone());
    assert!(
        check(&origin, &history, &bundle, &inventory)
            .unwrap()
            .unmapped
            .contains(&removed.observation)
    );
    bundle.observations.push(Observation {
        id: "unjournaled/new-holder".into(),
        local_state: "effect:unknown".into(),
        snapshot: Hash::new(b"additional artificial coverage gap"),
        evidence: BTreeSet::new(),
    });
    assert!(check(&origin, &history, &bundle, &inventory).is_err());
    inventory.unmapped.push("unjournaled/new-holder".into());
    assert_eq!(
        check(&origin, &history, &bundle, &inventory)
            .unwrap()
            .unmapped
            .len(),
        4
    );
    for id in ["missing-observation", "payload/1", "unjournaled/progress"] {
        let mut duplicate = inventory.clone();
        duplicate.unmapped.push(id.into());
        assert!(check(&origin, &history, &bundle, &duplicate).is_err());
    }
    bundle.observations.last_mut().unwrap().local_state = "effect:resolved".into();
    assert!(check(&origin, &history, &bundle, &inventory).is_err());
    assert!(history.validate_replacement_activation().is_err());
}

#[test]
fn canonical_artifact_bound_and_forged_proof_fields_refuse() {
    let (origin, history, bundle, inventory) = fixture(false);
    for (field, value) in [
        ("excluded", json!(true)),
        ("issuer", json!("imaginary")),
        ("token", json!("forbidden artificial content")),
    ] {
        let mut value_json = serde_json::to_value(&inventory).unwrap();
        value_json[field] = value;
        let bytes = serde_json::to_vec(&value_json).unwrap();
        let mut pinned = bundle.clone();
        pinned.binding.inventory = Hash::new(&bytes);
        let saved = serde_json::to_vec(&pinned).unwrap();
        assert!(
            inspect(
                &origin,
                &history,
                &pinned.binding,
                Some(Hash::new(&saved)),
                &saved,
                &bytes
            )
            .is_err()
        );
    }
    for path in ["/holders/0", "/entries/0", "/effects/0"] {
        let mut forged = serde_json::to_value(&inventory).unwrap();
        forged.pointer_mut(path).unwrap()["excluded"] = json!(true);
        let bytes = serde_json::to_vec(&forged).unwrap();
        let mut pinned = bundle.clone();
        pinned.binding.inventory = Hash::new(&bytes);
        let saved = serde_json::to_vec(&pinned).unwrap();
        assert!(
            inspect(
                &origin,
                &history,
                &pinned.binding,
                Some(Hash::new(&saved)),
                &saved,
                &bytes
            )
            .is_err()
        );
    }
    let (pinned, _, mut bytes) = encode(&bundle, &inventory);
    bytes.push(b'\n');
    let mut changed = pinned;
    changed.binding.inventory = Hash::new(&bytes);
    let changed_bytes = serde_json::to_vec(&changed).unwrap();
    assert!(
        inspect(
            &origin,
            &history,
            &changed.binding,
            Some(Hash::new(&changed_bytes)),
            &changed_bytes,
            &bytes
        )
        .is_err()
    );
    // Exercise the real boundary with a valid exact-4096-byte unknown slice.
    let mut bounded = inventory.clone();
    bounded.holders.clear();
    bounded.entries.clear();
    bounded.effects.clear();
    bounded.unmapped.clear();
    for count in 1..=32 {
        bounded.unmapped = (0..count).map(|n| format!("g{n:02}")).collect();
        let base = serde_json::to_vec(&bounded).unwrap().len();
        if base <= LIMIT && LIMIT - base <= count * 253 {
            let mut padding = LIMIT - base;
            for id in &mut bounded.unmapped {
                let n = padding.min(253);
                id.push_str(&"x".repeat(n));
                padding -= n;
            }
            break;
        }
    }
    assert_eq!(serde_json::to_vec(&bounded).unwrap().len(), LIMIT);
    let mut slice = bundle;
    slice.observations = bounded
        .unmapped
        .iter()
        .map(|id| Observation {
            id: id.clone(),
            local_state: "effect:unknown".into(),
            snapshot: Hash::new(id.as_bytes()),
            evidence: BTreeSet::new(),
        })
        .collect();
    assert_eq!(check(&origin, &history, &slice, &bounded).unwrap(), bounded);
    bounded.scope.push('x');
    assert_eq!(serde_json::to_vec(&bounded).unwrap().len(), LIMIT + 1);
    assert!(
        check(&origin, &history, &slice, &bounded)
            .unwrap_err()
            .to_string()
            .contains("descriptor size")
    );
}
