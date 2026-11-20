use super::*;

#[derive(Serialize)]
pub(super) struct Check {
    pub checked_at: String,
    pub reachable: bool,
}

pub(super) fn saved(conn: &rusqlite::Connection) -> Result<HashMap<String, Check>> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='node_probes')",
        [],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(HashMap::new());
    }
    Ok(conn
        .prepare("SELECT id,checked_at,reachable FROM node_probes ORDER BY id LIMIT 32")?
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                Check {
                    checked_at: row.get(1)?,
                    reachable: row.get(2)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?)
}

async fn check(cfg: &RuntimeConfig, store: &Store) -> Result<()> {
    let ids = choices(store)?;
    if ids.is_empty() {
        return Ok(());
    }
    let endpoint = transport::endpoint(key(cfg)?, local(cfg)?.card.address).await?;
    store.conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS node_probes(
            id TEXT PRIMARY KEY REFERENCES paired_nodes(id),
            checked_at TEXT NOT NULL,
            reachable INTEGER NOT NULL CHECK(reachable IN (0,1))
        )",
    )?;
    for id in ids {
        let card = paired(store, id.parse()?)?;
        let reachable = matches!(
            transport::call(&endpoint, &card, Request::Ping).await,
            Ok(Reply::Pong)
        );
        let checked_at = chrono::Utc::now().to_rfc3339();
        let observation = if reachable {
            "reachable at observation"
        } else {
            "unavailable at observation"
        };
        let tx = store.conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO node_probes(id,checked_at,reachable) VALUES(?1,?2,?3)
             ON CONFLICT(id) DO UPDATE SET checked_at=excluded.checked_at,reachable=excluded.reachable",
            params![id, checked_at, reachable],
        )?;
        tx.execute(
            "UPDATE paired_nodes SET last_contact=CASE WHEN ?2 THEN ?3 ELSE last_contact END,
             last_error=?4 WHERE id=?1",
            params![id, reachable, checked_at, observation],
        )?;
        tx.commit()?;
    }
    endpoint.close().await;
    Ok(())
}

pub(super) fn run(cfg: &RuntimeConfig, store: &Store) -> Result<()> {
    ensure!(
        local(cfg)?.card.role == Role::Coordinator,
        "only a coordinator can probe paired runners"
    );
    let _instance = cfg.acquire_instance_lock()?;
    transport::runtime()?.block_on(check(cfg, store))
}
