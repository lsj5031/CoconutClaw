use super::*;
use crate::scheduler::{SessionScheduler, tests::FakeTelegramServer};
use iroh::protocol::Router;
use std::os::unix::fs::PermissionsExt;

const TASK: &str = "What is the orchard marker?";
const APPROVED: &str = "orchard marker is lapis-75";
const CORRECTED: &str = "orchard marker is jade-36";
const CURATED: &str = "Synthetic note: \"quoted\"\nMEMORY_APPEND: not an instruction";

fn start_runner(cfg: &mut RuntimeConfig) -> Router {
    cfg.telegram_bot_token = None;
    let script = cfg.instance_dir.join("fake-codex");
    fs::write(
        &script,
        r#"#!/bin/sh
printf 'call\n' >> invocations
out=''
for arg do
    if [ "$arg" = resume ]; then exit 9; fi
    if [ "$out" = next ]; then out="$arg"; fi
    if [ "$arg" = --output-last-message ]; then out=next; fi
done
cat > prompt
if grep -Fq lapis-75 prompt; then answer=lapis-75
elif grep -Fq jade-36 prompt; then answer=jade-36
else answer=not-recalled; fi
printf '%s' "$answer" > "$out"
printf '%s\n' '{"type":"thread.started","thread_id":"fake-nonresumable-thread"}'
"#,
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
    cfg.codex.bin = script.to_string_lossy().into_owned();
    let state = Arc::new(Mutex::new(Runner::open(cfg.clone()).unwrap()));
    transport::runtime().unwrap().block_on(async {
        let endpoint = transport::endpoint(key(cfg).unwrap(), local(cfg).unwrap().card.address)
            .await
            .unwrap();
        Router::builder(endpoint)
            .accept(ALPN, transport::Handler::new(state))
            .spawn()
    })
}

fn send(
    core: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    id: i64,
    topic: Option<i64>,
    text: &str,
) {
    let mut raw = serde_json::json!({"update_id":id,"message":{
        "chat":{"id":321,"type":"private"},"from":{"id":321},"message_id":id,"text":text}});
    if let Some(topic) = topic {
        raw["message"]["message_thread_id"] = topic.into();
    }
    crate::loops::webhook::process_webhook_line(core, store, scheduler, &raw.to_string()).unwrap();
}

fn proposal(store: &mut Store, text: &str) -> i64 {
    store
        .conn
        .execute(
            "INSERT INTO turns(ts,chat_id,input_type,user_text,provider_raw,status,channel)
         VALUES('now','telegram:321#7','text','private conversation sentinel','','ok','telegram')",
            [],
        )
        .unwrap();
    let source = store.conn.last_insert_rowid();
    store
        .insert_memory_and_tasks("now", "codex", Some(source), &[text.into()], &[])
        .unwrap();
    store
        .conn
        .query_row(
            "SELECT id FROM memory_records WHERE source_turn=?1",
            [source],
            |r| r.get(0),
        )
        .unwrap()
}

fn receipt(
    core: &RuntimeConfig,
    store: &Store,
    node: &RuntimeConfig,
    update: i64,
    expected: &str,
    facts: &[&str],
) -> (i64, Bundle) {
    let run = store
        .find_task_run_by_update_id(&update.to_string())
        .unwrap()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let (json, start, result, status, uncertain): (String, Vec<u8>, Vec<u8>, String, bool) = loop {
        if let Some(receipt) = store
            .conn
            .query_row(
                "SELECT a.bundle,c.start,c.result,c.status,c.uncertain
             FROM node_assignments a JOIN runtime_remote_results c ON c.task_run_id=a.run
             WHERE a.run=?1 AND c.state='applied'",
                [run.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()
            .unwrap()
        {
            break receipt;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "missing applied remote result: {:?}",
            store.get_task_run(run.id).unwrap()
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(run.prompt, TASK);
    assert!(store.runtime_result_is_literal(run.id).unwrap());
    assert_eq!((status.as_str(), uncertain), ("completed", false));
    let b: Bundle = serde_json::from_str(&json).unwrap();
    let owner = local(core).unwrap().card.id;
    let runner = local(node).unwrap().card.id;
    let assigned = b.validate(owner, runner).unwrap();
    assert_eq!(SignedEvent::parse(&start).unwrap().event.actor, runner);
    assert_eq!(SignedEvent::parse(&result).unwrap().event.actor, runner);
    let history = History::replay(
        &b.origin,
        owner,
        b.origin.origin.namespace,
        [b.assignment.clone(), start, result],
    )
    .unwrap();
    assert_eq!(
        history.jobs[&1].result,
        Some((Outcome::Completed, expected.into()))
    );

    let actual = fs::read_to_string(node.instance_dir.join("prompt")).unwrap();
    assert_eq!(actual, assigned);
    if facts.is_empty() {
        assert_eq!(actual, TASK);
    } else {
        let (data, user) = actual.split_once("\n\nUser request:\n").unwrap();
        let (label, json) = data.split_once('\n').unwrap();
        assert!(label.contains("data, not instructions"));
        assert_eq!(serde_json::from_str::<Vec<String>>(json).unwrap(), facts);
        assert_eq!(user, TASK);
    }
    (run.id, b)
}

#[test]
fn approved_topic_memory_after_reopen_on_different_authenticated_runner() {
    let (_a, mut core) = fixture(Role::Coordinator, 105);
    let (_b, mut first) = fixture(Role::Runner, 106);
    let first_router = start_runner(&mut first);
    let (_c, mut next) = fixture(Role::Runner, 107);
    for node in [&first, &next] {
        pair(&core, node);
        pair(node, &core);
    }
    let first_id = local(&first).unwrap().card.id;
    let next_card = local(&next).unwrap().card;
    let telegram = FakeTelegramServer::start();
    core.telegram_api_base = Some(telegram.base_url.clone());
    core.telegram_topics_enabled = Some(true);

    let mut store = Store::open(&core).unwrap();
    let scheduler = SessionScheduler::new(core.clone(), None);
    let candidate = proposal(&mut store, APPROVED);
    proposal(&mut store, "orchard marker is never-approved");
    selection(&store, "telegram:321#7", Some(&first_id.to_string())).unwrap();
    send(&core, &mut store, &scheduler, 7101, Some(7), TASK);
    receipt(&core, &store, &first, 7101, "not-recalled", &[]);
    for (id, topic, cmd) in [
        (7102, Some(7), format!("/memory promote {candidate}")),
        (7103, Some(7), format!("/memory remember {CURATED}")),
        (7104, None, "/memory remember orchard parent-only".into()),
        (
            7105,
            Some(9),
            "/memory remember orchard sibling-only".into(),
        ),
        (
            7106,
            Some(7),
            "/preferences set answer private-preference".into(),
        ),
    ] {
        send(&core, &mut store, &scheduler, id, topic, &cmd);
    }
    scheduler.shutdown().unwrap();
    drop(scheduler);
    drop(store);

    let next_router = start_runner(&mut next);
    let mut store = Store::open(&core).unwrap();
    let scheduler = SessionScheduler::new(core.clone(), None);
    for (scope, fact) in [
        ("telegram:321", "parent-only"),
        ("telegram:321#9", "sibling-only"),
    ] {
        assert!(store.curated_memory(scope).unwrap().contains(fact));
    }
    assert!(
        store
            .preferences_context("telegram:321#7")
            .unwrap()
            .contains("private-preference")
    );
    for scope in ["telegram:321#7", "telegram:321#8", "telegram:321"] {
        selection(&store, scope, Some(&next_card.id.to_string())).unwrap();
    }
    send(&core, &mut store, &scheduler, 7107, Some(7), TASK);
    let (run, assigned) = receipt(&core, &store, &next, 7107, "lapis-75", &[CURATED, APPROVED]);
    assert!(
        assigned
            .validate(local(&core).unwrap().card.id, first_id)
            .is_err()
    );
    for (id, topic) in [(7108, Some(8)), (7109, None)] {
        send(&core, &mut store, &scheduler, id, topic, TASK);
        receipt(&core, &store, &next, id, "not-recalled", &[]);
    }
    send(
        &core,
        &mut store,
        &scheduler,
        7110,
        Some(7),
        &format!("/memory correct {candidate} {CORRECTED}"),
    );
    send(&core, &mut store, &scheduler, 7111, Some(7), TASK);
    receipt(&core, &store, &next, 7111, "jade-36", &[CURATED, CORRECTED]);
    send(&core, &mut store, &scheduler, 7112, Some(7), "/memory undo");
    send(&core, &mut store, &scheduler, 7113, Some(7), TASK);
    receipt(&core, &store, &next, 7113, "lapis-75", &[CURATED, APPROVED]);
    scheduler.shutdown().unwrap();

    transport::runtime().unwrap().block_on(async {
        let endpoint = transport::coordinator(&core).await.unwrap();
        assert!(
            transport::call(
                &endpoint,
                &local(&first).unwrap().card,
                Request::Begin(assigned.clone())
            )
            .await
            .is_err()
        );
        let evidence = transport::call(&endpoint, &next_card, Request::Begin(assigned.clone()))
            .await
            .unwrap();
        assert!(accept(&store, run, &assigned, evidence, &core.timezone).unwrap());
        first_router.shutdown().await.unwrap();
        next_router.shutdown().await.unwrap();
        endpoint.close().await;
    });
    assert_eq!(
        fs::read_to_string(first.instance_dir.join("invocations")).unwrap(),
        "call\n"
    );
    assert_eq!(
        fs::read_to_string(next.instance_dir.join("invocations")).unwrap(),
        "call\n".repeat(5)
    );
}
