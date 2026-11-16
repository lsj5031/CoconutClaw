mod composition;

use super::*;
use rusqlite::Connection;
use serde_json::{Value, json};

const SCHEMA: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../sql/schema.sql"));

fn database() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(
        "INSERT INTO task_runs(id,session_id,channel,prompt,status,created_at,started_at,update_id)
         VALUES(1,'local:fixture','local','','completed','fixture','fixture','41'),
               (2,'local:fixture','local','','queued','fixture',NULL,'delegation:1:0'),
               (3,'local:fixture','local','','queued','fixture',NULL,'approval:7');
         INSERT INTO task_payloads(task_run_id,request,output,delivery_state)
         VALUES(1,'{\"fixture\":101}','{\"fixture\":201}',
         '{\"completed_ops\":[\"telegram:text:chunk:1\"],\"text_plan\":{\"kind\":\"chunks\"},
           \"operations\":{\"telegram:text:chunk:0\":{\"phase\":\"sending\",\"attempts\":1},
                         \"telegram:text:chunk:1\":{\"phase\":\"sending\",\"attempts\":2}}}'),
               (2,'{\"fixture\":102}',NULL,'[]'),(3,'{\"fixture\":103}',NULL,'[]');
         INSERT INTO approvals(id,task_run_id,session_id,channel,prompt_text,status,resume_payload,created_at)
         VALUES(7,1,'local:fixture','local','','approved','{\"fixture\":301}','fixture');
         INSERT INTO turns(id,ts,chat_id,input_type,provider_raw,status,update_id,task_run_id,side_effects_applied)
         VALUES(11,'fixture','local:fixture','text','','ok','41',1,1);
         UPDATE kv SET value='40' WHERE key='last_update_id';
         INSERT INTO kv(key,value) VALUES('inflight_update_id','41');",
    ).unwrap();
    db
}

fn has(db: &Connection, table: &str) -> bool {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |r| r.get(0),
    )
    .unwrap()
}

// Deliberately fixture-specific, not a runtime exporter: it knows only these
// artificial IDs and numeric JSON aliases and cannot accept a filesystem path.
fn descriptors(db: &Connection) -> (Vec<Descriptor>, BTreeSet<Journal>) {
    let mut rows = Vec::new();
    let mut missing = BTreeSet::new();
    for (table, journal) in [
        ("task_runs", Journal::TaskRuns),
        ("task_payloads", Journal::Payloads),
        ("approvals", Journal::Approvals),
        ("turns", Journal::Turns),
        ("kv", Journal::Inbound),
    ] {
        if !has(db, table) {
            missing.insert(journal);
        }
    }
    for run in 1..=3 {
        let mut task = Row::Task {
            run,
            status: None,
            started: false,
            unknown_outcome: false,
            update: None,
            link: None,
        };
        if has(db, "task_runs") {
            task = db.query_row(
                "SELECT status,started_at IS NOT NULL,error_summary,update_id FROM task_runs WHERE id=?1",
                [run], |r| {
                    let update: String = r.get(3)?;
                    let (update, link) = match update.as_str() {
                        "41" => (Some(41), None),
                        "delegation:1:0" => (None, Some(TaskLink::Delegation { parent: 1, index: 0 })),
                        "approval:7" => (None, Some(TaskLink::Approval { approval: 7 })),
                        _ => panic!("not an artificial fixture update"),
                    };
                    Ok(Row::Task { run, status: Some(r.get(0)?), started: r.get(1)?,
                        unknown_outcome: r.get::<_, Option<String>>(2)?.is_some_and(|s| s == "fixture unknown outcome"),
                        update, link })
                },
            ).unwrap();
        }
        rows.push(task);
        if has(db, "task_payloads") {
            let (request, output, delivered, delivery): (String, Option<String>, bool, String) = db.query_row(
                "SELECT request,output,delivered,delivery_state FROM task_payloads WHERE task_run_id=?1", [run],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
            ).unwrap();
            let alias = |s: &str| {
                serde_json::from_str::<Value>(s).unwrap()["fixture"]
                    .as_u64()
                    .unwrap() as u32
            };
            let delivery: Value = serde_json::from_str(&delivery).unwrap();
            let plan = if delivery.is_array() {
                Some(Plan::Legacy)
            } else {
                match delivery["text_plan"]["kind"].as_str() {
                    Some("rich") => Some(Plan::Rich),
                    Some("document") => Some(Plan::Document),
                    Some("chunks") => Some(Plan::Chunks),
                    None => None,
                    _ => panic!("fixture plan"),
                }
            };
            rows.push(Row::Payload {
                run,
                request: Some(alias(&request)),
                output: output.as_deref().map(alias),
                delivered,
                journal: true,
                plan,
            });
            if let Some(operations) = delivery["operations"].as_object() {
                for (operation, state) in operations {
                    rows.push(Row::Delivery {
                        run,
                        operation: operation.clone(),
                        phase: Some(match state["phase"].as_str().unwrap() {
                            "ready" => Phase::Ready,
                            "sending" => Phase::Sending,
                            "unknown" => Phase::Unknown,
                            _ => panic!("fixture phase"),
                        }),
                        attempts: state["attempts"].as_u64(),
                        acknowledged: delivery["completed_ops"]
                            .as_array()
                            .unwrap()
                            .contains(&json!(operation)),
                    });
                }
            }
            if delivery["text_plan"]["kind"] == "rich" {
                let state = &delivery["text_plan"];
                rows.push(Row::Delivery {
                    run,
                    operation: "telegram:text".into(),
                    phase: Some(match state["phase"].as_str().unwrap() {
                        "ready" => Phase::Ready,
                        "sending" => Phase::Sending,
                        "unknown" => Phase::Unknown,
                        _ => panic!("fixture rich phase"),
                    }),
                    attempts: state["attempts"].as_u64(),
                    acknowledged: delivery["completed_ops"]
                        .as_array()
                        .unwrap()
                        .contains(&json!("telegram:text")),
                });
            }
            let completed = if delivery.is_array() {
                delivery.as_array()
            } else {
                delivery["completed_ops"].as_array()
            };
            for operation in completed.into_iter().flatten() {
                let operation = operation.as_str().unwrap();
                if !rows.iter().any(|row| matches!(row, Row::Delivery { run: r, operation: op, .. } if *r == run && op == operation)) {
                    rows.push(Row::Delivery { run, operation: operation.into(), phase: None, attempts: None, acknowledged: true });
                }
            }
        } else {
            rows.push(Row::Payload {
                run,
                request: None,
                output: None,
                delivered: false,
                journal: false,
                plan: None,
            });
        }
    }
    rows.push(if has(db, "approvals") {
        db.query_row(
            "SELECT task_run_id,status,resume_payload FROM approvals WHERE id=7",
            [],
            |r| {
                let resume: String = r.get(2)?;
                Ok(Row::Approval {
                    id: 7,
                    run: Some(r.get(0)?),
                    status: Some(r.get(1)?),
                    resume: Some(
                        serde_json::from_str::<Value>(&resume).unwrap()["fixture"]
                            .as_u64()
                            .unwrap() as u32,
                    ),
                })
            },
        )
        .unwrap()
    } else {
        Row::Approval {
            id: 7,
            run: None,
            status: None,
            resume: None,
        }
    });
    rows.push(if has(db, "turns") {
        db.query_row(
            "SELECT task_run_id,status,side_effects_applied,update_id FROM turns WHERE id=11",
            [],
            |r| {
                Ok(Row::Turn {
                    id: 11,
                    run: r.get(0)?,
                    status: Some(r.get(1)?),
                    side_effects_applied: Some(r.get(2)?),
                    update: Some(r.get::<_, String>(3)?.parse().unwrap()),
                })
            },
        )
        .unwrap()
    } else {
        Row::Turn {
            id: 11,
            run: None,
            status: None,
            side_effects_applied: None,
            update: None,
        }
    });
    let get = |key: &str| -> Option<u64> {
        if !has(db, "kv") {
            return None;
        }
        db.query_row("SELECT value FROM kv WHERE key=?1", [key], |r| {
            r.get::<_, String>(0)
        })
        .ok()
        .map(|s| s.parse().unwrap())
    };
    rows.push(Row::Inbound {
        last_update: get("last_update_id"),
        inflight: get("inflight_update_id"),
    });
    (
        rows.into_iter()
            .map(|row| Descriptor {
                source: source_identity(),
                row,
            })
            .collect(),
        missing,
    )
}

fn encode(
    rows: &[Descriptor],
    missing: BTreeSet<Journal>,
) -> (Binding, Vec<u8>, Vec<u8>, Vec<Vec<u8>>) {
    let (_, _, mut bundle) = super::super::tests::fixture();
    let bytes: Vec<_> = rows
        .iter()
        .map(|d| serde_json::to_vec(d).unwrap())
        .collect();
    let checkpoint = Checkpoint {
        version: 1,
        source: source_identity(),
        missing,
        observations: rows
            .iter()
            .zip(&bytes)
            .map(|(d, b)| (d.id(), Hash::new(b)))
            .collect(),
    };
    let checkpoint = serde_json::to_vec(&checkpoint).unwrap();
    bundle.binding.application_checkpoint = Hash::new(&checkpoint);
    bundle.observations = rows
        .iter()
        .zip(&bytes)
        .map(|(d, b)| Observation {
            id: d.id(),
            local_state: d.local_state(),
            snapshot: Hash::new(b),
            evidence: BTreeSet::new(),
        })
        .collect();
    (
        bundle.binding.clone(),
        serde_json::to_vec(&bundle).unwrap(),
        checkpoint,
        bytes,
    )
}

fn check(rows: &[Descriptor], missing: BTreeSet<Journal>) -> Result<BTreeMap<String, Descriptor>> {
    let (_, history, _) = super::super::tests::fixture();
    let (binding, bundle, checkpoint, descriptors) = encode(rows, missing);
    inspect(
        &history,
        &binding,
        Some(Hash::new(&bundle)),
        &bundle,
        &checkpoint,
        &descriptors,
    )
}

#[test]
fn actual_schema_fixture_links_exact_bytes_and_never_permits_effects() {
    let db = database();
    let (rows, missing) = descriptors(&db);
    let result = check(&rows, missing.clone()).unwrap();
    assert_eq!(result.len(), 11);
    assert_eq!(
        result["delivery/1/telegram:text:chunk:0"].local_state(),
        "delivery:sending"
    );
    assert_eq!(
        result["delivery/1/telegram:text:chunk:1"].local_state(),
        "delivery:local-ack"
    );
    assert!(matches!(
        &result["task/3"].row,
        Row::Task {
            link: Some(TaskLink::Approval { approval: 7 }),
            ..
        }
    ));
    assert!(matches!(
        &result["task/2"].row,
        Row::Task {
            link: Some(TaskLink::Delegation {
                parent: 1,
                index: 0
            }),
            ..
        }
    ));
    let encoded = encode(&rows, missing.clone());
    println!(
        "DESCRIPTOR_FIXTURE {}",
        json!({
            "source":source_identity().to_string(),
            "bundle":std::str::from_utf8(&encoded.1).unwrap(),
            "checkpoint":std::str::from_utf8(&encoded.2).unwrap(),
            "descriptors":encoded.3.iter().map(|b| std::str::from_utf8(b).unwrap()).collect::<Vec<_>>(),
        })
    );
    let mut reordered = rows.clone();
    reordered.reverse();
    assert_eq!(encoded.2, encode(&reordered, missing).2); // sorted checkpoint map
    let (_, history, _) = super::super::tests::fixture();
    assert!(history.validate_replacement_activation().is_err());
    assert_eq!(
        db.query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        3
    );
    assert_eq!(
        db.query_row(
            "SELECT delivered FROM task_payloads WHERE task_run_id=1",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    // Reload exact descriptor/checkpoint/bundle bytes, not object equality alone.
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint.json");
    std::fs::write(&path, &encoded.2).unwrap();
    assert_eq!(
        inspect(
            &history,
            &encoded.0,
            Some(Hash::new(&encoded.1)),
            &encoded.1,
            &std::fs::read(path).unwrap(),
            &encoded.3
        )
        .unwrap(),
        result
    );
}

#[test]
fn interrupted_result_delivery_and_missing_journals_remain_unknown() {
    let db = database();
    db.execute_batch(
        "UPDATE task_runs SET status='failed',error_summary='fixture unknown outcome' WHERE id=1;
        UPDATE task_payloads SET output=NULL,delivery_state='[]' WHERE task_run_id=1;",
    )
    .unwrap();
    let (rows, missing) = descriptors(&db);
    let result = check(&rows, missing).unwrap();
    assert_eq!(result["task/1"].local_state(), "task:unknown");
    assert_eq!(result["payload/1"].local_state(), "payload:output-unknown");
    assert!(matches!(
        result["turn/11"].row,
        Row::Turn {
            side_effects_applied: Some(true),
            ..
        }
    ));
    for table in ["task_payloads", "approvals", "turns", "kv", "task_runs"] {
        db.execute_batch(&format!("DROP TABLE {table}")).unwrap();
        let (rows, missing) = descriptors(&db);
        let result = check(&rows, missing).unwrap();
        assert!(
            result
                .values()
                .all(|d| !d.local_state().contains("resolved"))
        );
        assert!(matches!(
            result["payload/1"].row,
            Row::Payload {
                request: None,
                output: None,
                ..
            }
        ));
    }
    let (_, history, _) = super::super::tests::fixture();
    assert!(history.validate_replacement_activation().is_err());
}

#[test]
fn rich_unknown_and_legacy_ack_preserve_distinct_local_evidence() {
    let db = database();
    for (state, expected) in [
        (
            r#"{"completed_ops":[],"text_plan":{"kind":"rich","phase":"unknown","attempts":1},"operations":{}}"#,
            "delivery:unknown",
        ),
        (r#"["telegram:text"]"#, "delivery:local-ack"),
    ] {
        db.execute(
            "UPDATE task_payloads SET delivery_state=?1 WHERE task_run_id=1",
            [state],
        )
        .unwrap();
        let (rows, missing) = descriptors(&db);
        let result = check(&rows, missing).unwrap();
        assert_eq!(result["delivery/1/telegram:text"].local_state(), expected);
        let (_, history, _) = super::super::tests::fixture();
        assert!(history.validate_replacement_activation().is_err());
        assert_eq!(
            db.query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }
}

#[test]
fn schema_checkpoint_descriptor_and_observation_substitution_refuse() {
    let (rows, missing) = descriptors(&database());
    let (binding, bundle, checkpoint, bytes) = encode(&rows, missing);
    let (_, history, _) = super::super::tests::fixture();
    let run = |b: &[u8], c: &[u8], d: &[Vec<u8>]| {
        inspect(&history, &binding, Some(Hash::new(b)), b, c, d)
    };
    let mut changed = bytes.clone();
    changed[0] = serde_json::to_vec(&Descriptor {
        source: Hash::new(b"wrong source"),
        row: rows[0].row.clone(),
    })
    .unwrap();
    assert!(run(&bundle, &checkpoint, &changed).is_err());
    assert!(run(&bundle, b"{}", &bytes).is_err());
    assert!(run(&bundle, &checkpoint, &bytes[1..]).is_err());
    let mut forged: Bundle = serde_json::from_slice(&bundle).unwrap();
    forged.observations[0].snapshot = Hash::new(b"other descriptor");
    assert!(run(&serde_json::to_vec(&forged).unwrap(), &checkpoint, &bytes).is_err());
    forged = serde_json::from_slice(&bundle).unwrap();
    forged.observations[0].local_state = "task:completed".into();
    // First task already completed, so force a genuinely different label.
    forged.observations[0]
        .local_state
        .push_str(":safe-to-retry");
    assert!(run(&serde_json::to_vec(&forged).unwrap(), &checkpoint, &bytes).is_err());
    let mut wrong_source = rows.clone();
    for d in &mut wrong_source {
        d.source = Hash::new(b"substituted source");
    }
    assert!(check(&wrong_source, BTreeSet::new()).is_err());
    // Re-pinning all attacker-controlled bytes still cannot substitute the
    // compiled schema/mapping source identity.
    let (mut binding, forged, checkpoint, bytes) = encode(&wrong_source, BTreeSet::new());
    let mut checkpoint: Checkpoint = serde_json::from_slice(&checkpoint).unwrap();
    checkpoint.source = wrong_source[0].source;
    let checkpoint = serde_json::to_vec(&checkpoint).unwrap();
    binding.application_checkpoint = Hash::new(&checkpoint);
    let mut forged: Bundle = serde_json::from_slice(&forged).unwrap();
    forged.binding = binding.clone();
    let forged = serde_json::to_vec(&forged).unwrap();
    assert!(
        inspect(
            &history,
            &binding,
            Some(Hash::new(&forged)),
            &forged,
            &checkpoint,
            &bytes
        )
        .is_err()
    );
}

#[test]
fn terminal_results_and_local_acks_never_grant_retry_or_activation() {
    use crate::synthetic::membership::domain::{Action, Outcome};
    let (origin, original, _) = super::super::tests::fixture();
    let mut history = History::replay(
        &origin,
        origin.origin.owner,
        origin.origin.namespace,
        original.export().into_iter().take(2),
    )
    .unwrap();
    let fence = SignedEvent::parse(&original.export()[3])
        .unwrap()
        .event
        .action;
    for (key, action) in [
        (
            SecretKey::from([213; 32]),
            Action::Result {
                assignment: 1,
                outcome: Outcome::Completed,
                output: "mock terminal result".into(),
            },
        ),
        (SecretKey::from([211; 32]), fence),
    ] {
        let signed =
            SignedEvent::sign(history.next(&origin, key.public(), action).unwrap(), &key).unwrap();
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
    let before = history.export();
    for status in ["completed", "failed", "cancelled"] {
        let db = database();
        db.execute("UPDATE task_runs SET status=?1 WHERE id=1", [status])
            .unwrap();
        db.execute_batch("UPDATE task_payloads SET delivered=1 WHERE task_run_id=1")
            .unwrap();
        let (rows, missing) = descriptors(&db);
        let (mut binding, bytes, checkpoint, descriptors) = encode(&rows, missing);
        binding.fence_hash = Hash::new(&before[3]);
        let mut bundle: Bundle = serde_json::from_slice(&bytes).unwrap();
        bundle.binding = binding.clone();
        let bytes = serde_json::to_vec(&bundle).unwrap();
        let inspected = inspect(
            &history,
            &binding,
            Some(Hash::new(&bytes)),
            &bytes,
            &checkpoint,
            &descriptors,
        )
        .unwrap();
        assert!(matches!(
            inspected["payload/1"].row,
            Row::Payload {
                delivered: true,
                ..
            }
        ));
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
                        job: 5,
                        attempt: 2,
                        runner: SecretKey::from([213; 32]).public(),
                        request: "prohibited mock retry".into(),
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
        assert_eq!(
            db.query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
    }
}

#[test]
fn dangling_conflicting_and_malformed_local_links_refuse() {
    let (rows, _) = descriptors(&database());
    let mut variants = Vec::new();
    for replacement in [
        Row::Task {
            run: 2,
            status: Some("queued".into()),
            started: false,
            unknown_outcome: false,
            update: None,
            link: Some(TaskLink::Delegation {
                parent: 99,
                index: 0,
            }),
        },
        Row::Task {
            run: 2,
            status: Some("queued".into()),
            started: false,
            unknown_outcome: false,
            update: None,
            link: Some(TaskLink::Delegation {
                parent: 2,
                index: 0,
            }),
        },
        Row::Task {
            run: 2,
            status: Some("queued".into()),
            started: false,
            unknown_outcome: false,
            update: None,
            link: Some(TaskLink::Approval { approval: 999 }),
        },
        Row::Task {
            run: 2,
            status: Some("queued".into()),
            started: false,
            unknown_outcome: false,
            update: None,
            link: Some(TaskLink::Approval { approval: 7 }),
        }, // duplicate resume
        Row::Task {
            run: 2,
            status: Some("queued".into()),
            started: false,
            unknown_outcome: false,
            update: Some(41),
            link: None,
        },
        Row::Approval {
            id: 7,
            run: Some(1),
            status: Some("pending".into()),
            resume: Some(301),
        },
        Row::Turn {
            id: 11,
            run: Some(1),
            status: Some("ok".into()),
            side_effects_applied: Some(true),
            update: Some(99),
        },
        Row::Payload {
            run: 1,
            request: None,
            output: None,
            delivered: true,
            journal: false,
            plan: None,
        },
        Row::Delivery {
            run: 1,
            operation: "telegram:token-like-data".into(),
            phase: Some(Phase::Sending),
            attempts: Some(1),
            acknowledged: false,
        },
    ] {
        let descriptor = Descriptor {
            source: source_identity(),
            row: replacement,
        };
        let mut variant = rows.clone();
        if let Some(slot) = variant.iter_mut().find(|d| d.id() == descriptor.id()) {
            *slot = descriptor;
        } else {
            variant.push(descriptor);
        }
        variants.push(variant);
    }
    for variant in variants {
        assert!(check(&variant, BTreeSet::new()).is_err());
    }
    assert!(check(&rows, BTreeSet::from([Journal::Payloads])).is_err());
    let mut duplicate = rows.clone();
    duplicate.push(rows[0].clone());
    assert_eq!(
        check(&duplicate, BTreeSet::new()).unwrap().len(),
        rows.len()
    );
    let mut conflict = rows.clone();
    let mut other = rows[0].clone();
    if let Row::Task { status, .. } = &mut other.row {
        *status = Some("cancelled".into());
    }
    conflict.push(other);
    assert!(check(&conflict, BTreeSet::new()).is_err());
}

#[test]
fn canonical_size_and_unknown_field_refusals() {
    let (rows, missing) = descriptors(&database());
    let (binding, bundle, checkpoint, mut bytes) = encode(&rows, missing);
    let (_, history, _) = super::super::tests::fixture();
    let run = |d: &[Vec<u8>]| {
        inspect(
            &history,
            &binding,
            Some(Hash::new(&bundle)),
            &bundle,
            &checkpoint,
            d,
        )
    };
    let mut unknown: Value = serde_json::from_slice(&bytes[0]).unwrap();
    unknown["row"]["retry_allowed"] = true.into();
    bytes[0] = serde_json::to_vec(&unknown).unwrap();
    assert!(run(&bytes).is_err());
    assert!(parse::<Descriptor>(&vec![b' '; MAX_ARTIFACT + 1]).is_err());
    assert!(parse::<Checkpoint>(&vec![b' '; MAX_ARTIFACT + 1]).is_err());
    assert!(parse::<Descriptor>(b"{}").is_err());
    let canonical = serde_json::to_vec(&rows[0]).unwrap();
    let mut whitespace = canonical.clone();
    whitespace.push(b'\n');
    assert!(parse::<Descriptor>(&whitespace).is_err());
    assert_eq!(parse::<Descriptor>(&canonical).unwrap(), rows[0]);
    // No arbitrary text field can carry an unredacted request or result.
    let mut raw: Value = serde_json::from_slice(&canonical).unwrap();
    raw["row"]["prompt"] = "artificial forbidden content".into();
    assert!(parse::<Descriptor>(&serde_json::to_vec(&raw).unwrap()).is_err());
    let mut maximum = rows.clone();
    maximum.resize(64, rows[0].clone());
    assert_eq!(check(&maximum, BTreeSet::new()).unwrap().len(), rows.len());
    maximum.push(rows[0].clone());
    assert!(check(&maximum, BTreeSet::new()).is_err());
}
