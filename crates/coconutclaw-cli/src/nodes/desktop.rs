//! One-shot operator export. The companion receives metadata through a file picker.
use super::{Card, EndpointId, Role};
use crate::{session::SessionKey, store::Store, store::status::timestamp};
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
}

#[derive(Serialize)]
struct Node {
    id: String,
    observation: Observation,
    last_reached: Option<String>,
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
    let session = session.id();
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
                params![task.run_id, session],
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
        version: 1,
        source: "coordinator",
        observed_at: status.observed_at,
        nodes,
        jobs,
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
    use crate::store::InsertTaskRunParams;
    use coconutclaw_config::RuntimeConfig;

    #[test]
    fn read_only_export_cannot_create_a_database_and_scopes_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = RuntimeConfig::test_config_at(temp.path().into());
        let now = "2026-11-20T01:52:00Z";
        let session = SessionKey::telegram("101#4");
        assert!(read(&cfg.sqlite_db_path, &session, now).is_err());
        assert!(!cfg.sqlite_db_path.exists());
        let store = Store::open(&cfg).unwrap();
        let insert = |session: SessionKey| {
            store
                .insert_task_run(InsertTaskRunParams {
                    session_id: session.id(),
                    channel: "telegram".into(),
                    source_chat_id: None,
                    source_user_id: None,
                    update_id: None,
                    prompt: "private prompt".into(),
                    created_at: now.into(),
                    progress_message_id: None,
                    scheduled_task_id: None,
                })
                .unwrap()
        };
        let run = insert(session.clone());
        insert(SessionKey::telegram("101#3"));
        let value =
            serde_json::to_value(read(&cfg.sqlite_db_path, &session, now).unwrap()).unwrap();
        assert_eq!(value["nodes"], serde_json::json!([]));
        assert_eq!(
            value["jobs"],
            serde_json::json!([{
                "run_id": run.to_string(), "assigned_node": null, "status": "queued",
                "signed_evidence": "none", "delivery": "unavailable"
            }])
        );
        assert!(!value.to_string().contains("private prompt"));
        assert!(!table_exists(&store.conn, "paired_nodes").unwrap());
    }
}
