//! One-shot operator export. The companion receives metadata through a file picker.
use super::{Card, EndpointId, Role, probe, selected_runner};
use crate::{
    session::{SessionKey, SessionPlatform},
    store::{
        Store,
        status::{ScheduleTiming, StatusSchedule, timestamp},
    },
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::Serialize;
use std::path::{Path, PathBuf};

#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    /// Existing coordinator SQLite file, opened read-only.
    #[arg(long)]
    database: PathBuf,
    /// Exact local or Telegram conversation ID, for example telegram:123#4.
    #[arg(long)]
    session: String,
}

#[derive(Serialize)]
struct Snapshot {
    version: u8,
    source: &'static str,
    observed_at: String,
    nodes: Vec<Node>,
    jobs: Vec<Job>,
    routing: Option<Routing>,
    schedules: Vec<Schedule>,
    schedule_count: i64,
}

#[derive(Serialize)]
struct Routing {
    runner: Option<String>,
}

#[derive(Serialize)]
struct Schedule {
    id: String,
    revision: String,
    state: &'static str,
    next_due: Option<String>,
    timing: Option<ScheduleTiming>,
    blocked: bool,
    latest_run_id: Option<String>,
}

impl From<StatusSchedule> for Schedule {
    fn from(schedule: StatusSchedule) -> Self {
        Self {
            id: schedule.id.to_string(),
            revision: schedule.revision.to_string(),
            state: schedule.state,
            next_due: schedule.next_due,
            timing: schedule.timing,
            blocked: schedule.blocked,
            latest_run_id: schedule.latest_run_id.map(|id| id.to_string()),
        }
    }
}

#[derive(Serialize)]
struct Node {
    id: String,
    observation: Observation,
    last_reached: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    check: Option<probe::Check>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Observation {
    Reachable,
    Unreachable,
    Exchange,
    Unconfirmed,
    Unchecked,
}

#[derive(Serialize)]
struct Job {
    run_id: String,
    assigned_node: Option<String>,
    status: &'static str,
    signed_evidence: Evidence,
    delivery: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Evidence {
    None,
    Verified,
}

pub(crate) fn run(args: &Args) -> Result<()> {
    let session = SessionKey::from_id(args.session.clone());
    ensure!(
        !session.root_id.trim().is_empty() && session.id() == args.session,
        "provide an exact local: or telegram: conversation ID"
    );
    let snapshot = read(&args.database, &session, &chrono::Utc::now().to_rfc3339())?;
    println!("{}", serde_json::to_string(&snapshot)?);
    Ok(())
}

fn read(database: &Path, session: &SessionKey, now: &str) -> Result<Snapshot> {
    ensure!(
        database.is_file(),
        "provide an existing coordinator SQLite file"
    );
    let conn = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .context("open coordinator SQLite file read-only")?;
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    let store = Store { conn };
    snapshot(&store, session, now)
}

fn snapshot(store: &Store, session: &SessionKey, now: &str) -> Result<Snapshot> {
    let tx = store.conn.unchecked_transaction()?;
    let status = store.status_snapshot(session, now)?;
    let nodes = if table_exists(&tx, "paired_nodes")? {
        saved_nodes(&tx)?
    } else {
        Vec::new()
    };
    let routes = table_exists(&tx, "node_routes")?;
    let assignments = table_exists(&tx, "node_assignments")?;
    let scope = session.id();
    let routing = if session.platform == SessionPlatform::Telegram {
        let runner = if table_exists(&tx, "node_selection")? {
            selected_runner(&tx, &scope)?
                .map(|id| node_id(&id).map(|key| key.to_string()))
                .transpose()?
        } else {
            None
        };
        Some(Routing { runner })
    } else {
        None
    };
    let mut jobs = Vec::with_capacity(status.tasks.len());
    for task in status.tasks {
        let assigned_node = if routes {
            tx.query_row(
                "SELECT node FROM node_routes WHERE run=?1",
                [task.run_id],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .map(|id| node_id(&id).map(|key| key.to_string()))
            .transpose()?
        } else {
            None
        };
        // Only final evidence accepted by the node adapter is durable here.
        // A returned Ping, local start time, or reserved route proves neither signature.
        let verified = if assignments && assigned_node.is_some() {
            tx.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM node_assignments a
                    JOIN runtime_remote_runs m ON m.task_run_id=a.run
                    JOIN runtime_remote_results r ON r.task_run_id=a.run
                    WHERE a.run=?1 AND m.session_id=?2
                    AND length(r.start)>0 AND length(r.result)>0)",
                params![task.run_id, scope],
                |r| r.get::<_, bool>(0),
            )?
        } else {
            false
        };
        jobs.push(Job {
            run_id: task.run_id.to_string(),
            assigned_node,
            status: if task.outcome_unknown {
                "unknown"
            } else {
                task.status
            },
            signed_evidence: if verified {
                Evidence::Verified
            } else {
                Evidence::None
            },
            delivery: task.delivery,
        });
    }
    tx.commit()?;
    Ok(Snapshot {
        version: 2,
        source: "coordinator",
        observed_at: status.observed_at,
        nodes,
        jobs,
        routing,
        schedules: status.schedules.into_iter().map(Schedule::from).collect(),
        schedule_count: status.schedule_count,
    })
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [name],
        |r| r.get(0),
    )?)
}

fn node_id(raw: &str) -> Result<EndpointId> {
    let id: EndpointId = raw.parse().context("invalid stored node ID")?;
    ensure!(id.to_string() == raw, "non-canonical stored node ID");
    Ok(id)
}

fn saved_nodes(conn: &Connection) -> Result<Vec<Node>> {
    let mut probes = probe::saved(conn)?;
    let mut stmt = conn
        .prepare("SELECT id,card,last_contact,last_error FROM paired_nodes ORDER BY id LIMIT 32")?;
    let mut rows = stmt.query([])?;
    let mut nodes = Vec::new();
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let card: Card = serde_json::from_str(&row.get::<_, String>(1)?)
            .map_err(|_| anyhow::anyhow!("invalid stored public card"))?;
        ensure!(
            card.id == node_id(&id)? && card.version == 1 && card.role == Role::Runner,
            "expected coordinator store with paired runner cards"
        );
        let last_reached = timestamp(row.get(2)?);
        let error: Option<String> = row.get(3)?;
        let observation = match error.as_deref() {
            Some("reachable at observation") if last_reached.is_some() => Observation::Reachable,
            Some("unavailable at observation") => Observation::Unreachable,
            Some(_) => Observation::Unconfirmed,
            None if last_reached.is_some() => Observation::Exchange,
            None => Observation::Unchecked,
        };
        nodes.push(Node {
            check: probes.remove(&id),
            id,
            observation,
            last_reached,
        });
    }
    Ok(nodes)
}

#[cfg(test)]
pub(super) fn test_export(db: &Path, session: &SessionKey) -> serde_json::Value {
    serde_json::to_value(read(db, session, "2026-11-20T01:52:00Z").unwrap()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{nodes, scheduling::Timing, store::InsertTaskRunParams};
    use coconutclaw_config::RuntimeConfig;
    use serde_json::json;

    const NOW: &str = "2026-11-20T01:52:00Z";

    fn insert_job(store: &Store, session: &SessionKey) -> i64 {
        store
            .insert_task_run(InsertTaskRunParams {
                session_id: session.id(),
                channel: "telegram".into(),
                source_chat_id: None,
                source_user_id: None,
                update_id: None,
                prompt: "private job prompt".into(),
                created_at: NOW.into(),
                progress_message_id: None,
                scheduled_task_id: None,
            })
            .unwrap()
    }

    #[test]
    fn read_only_export_cannot_create_a_database_and_scopes_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = RuntimeConfig::test_config_at(temp.path().into());
        let session = SessionKey::telegram("101#4");
        assert!(read(&cfg.sqlite_db_path, &session, NOW).is_err());
        assert!(!cfg.sqlite_db_path.exists());
        let store = Store::open(&cfg).unwrap();
        let run = insert_job(&store, &session);
        insert_job(&store, &SessionKey::telegram("101#3"));
        let value = test_export(&cfg.sqlite_db_path, &session);
        assert_eq!(value["version"], 2);
        assert_eq!(value["nodes"], json!([]));
        assert_eq!(value["routing"], json!({"runner": null}));
        assert_eq!(value["schedules"], json!([]));
        assert_eq!(value["schedule_count"], 0);
        assert_eq!(
            value["jobs"],
            json!([{
                "run_id": run.to_string(), "assigned_node": null, "status": "queued",
                "signed_evidence": "none", "delivery": "unavailable"
            }])
        );
        assert!(!value.to_string().contains("private job prompt"));
        assert!(!table_exists(&store.conn, "paired_nodes").unwrap());
    }

    #[test]
    fn exports_the_exact_topics_selection_and_durable_schedule_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = RuntimeConfig::test_config_at(temp.path().into());
        let store = Store::open(&cfg).unwrap();
        let now = NOW.parse().unwrap();
        let scope = SessionKey::telegram("321#4");
        let sibling = SessionKey::telegram("321#3");
        let chat = SessionKey::telegram("321");
        let local = SessionKey::local("default");
        nodes::initialize(&store).unwrap();
        let pair = |seed| {
            let id = nodes::SecretKey::from_bytes(&[seed; 32]).public();
            let card = Card {
                version: 1,
                role: Role::Runner,
                id,
                address: "192.0.2.1:49731".parse().unwrap(),
                revision: nodes::REVISION.into(),
            };
            store.conn.execute(
                "INSERT INTO paired_nodes(id,card,last_error) VALUES(?1,?2,'private node error')",
                params![id.to_string(), serde_json::to_string(&card).unwrap()],
            ).unwrap();
            id.to_string()
        };
        let frozen_runner = pair(1);
        let selected_runner = pair(2);
        nodes::selection(&store, &scope.id(), Some(&frozen_runner)).unwrap();
        let run = insert_job(&store, &scope);
        nodes::freeze_route(&store, run, &scope.id()).unwrap();
        nodes::selection(&store, &scope.id(), Some(&selected_runner)).unwrap();
        nodes::selection(&store, &sibling.id(), Some(&frozen_runner)).unwrap();
        nodes::selection(&store, &local.id(), Some(&frozen_runner)).unwrap();
        let create = |session: &SessionKey, daily| {
            let request = crate::commands::schedule::request(
                &cfg,
                session.clone(),
                "private schedule prompt",
            )
            .unwrap();
            store
                .create_schedule(
                    "private creation key",
                    &request,
                    &Timing::parse(None, Some(daily), "UTC").unwrap(),
                    now,
                )
                .unwrap()
        };
        let schedule = create(&scope, "12:00");
        let sibling_schedule = create(&sibling, "13:00");
        create(&chat, "14:00");
        let local_schedule = create(&local, "15:00");
        store
            .conn
            .execute(
                "INSERT INTO runtime_schedule_runs VALUES(?1,?2)",
                params![schedule, run],
            )
            .unwrap();

        let export = |session: &SessionKey| test_export(&cfg.sqlite_db_path, session);
        let value = export(&scope);
        assert_eq!(value["routing"], json!({"runner": selected_runner}));
        assert_eq!(value["jobs"][0]["assigned_node"], frozen_runner);
        assert_eq!(value["schedule_count"], 1);
        assert_eq!(
            value["schedules"],
            json!([{
                "id": schedule.to_string(), "revision": "1", "state": "active",
                "next_due": "2026-11-20T12:00:00+00:00", "blocked": true,
                "latest_run_id": run.to_string(),
                "timing": {"cadence": "daily 12:00", "timezone": "UTC",
                    "fold": "first", "gap": "skip", "missed": "coalesce"}
            }])
        );
        let serialized = value.to_string();
        for private in [
            "private job prompt",
            "private schedule prompt",
            "private creation key",
            "private node error",
            "192.0.2.1",
            "telegram:321",
            "local:default",
        ] {
            assert!(!serialized.contains(private));
        }

        let other = export(&sibling);
        assert_eq!(other["routing"], json!({"runner": frozen_runner}));
        assert_eq!(other["schedules"][0]["id"], sibling_schedule.to_string());
        assert_eq!(other["jobs"], json!([]));
        assert_eq!(export(&chat)["routing"], json!({"runner": null}));
        let local = export(&local);
        assert_eq!(local["routing"], json!(null));
        assert_eq!(local["schedules"][0]["id"], local_schedule.to_string());
    }
}
