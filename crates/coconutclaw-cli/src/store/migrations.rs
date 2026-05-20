use super::*;
use crate::markers::parse_markers;
use anyhow::Result;
use rusqlite::params;
use serde_json::json;

impl Store {
    /// Run database migrations tracked by the `schema_version` key in `kv`.
    /// Each migration step is guarded by the version number so it runs only once.
    pub(crate) fn run_migrations(&self) -> Result<()> {
        let current: i64 = self
            .kv_get("schema_version")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        if current < 1 {
            let _ = self
                .conn
                .execute("ALTER TABLE turns ADD COLUMN duration_ms INTEGER", []);
            let _ = self.conn.execute(
                "ALTER TABLE turns RENAME COLUMN codex_raw TO provider_raw",
                [],
            );
            self.kv_set("schema_version", "1")?;
        }

        if current < 2 {
            match self.conn.execute(
                "ALTER TABLE turns ADD COLUMN channel TEXT NOT NULL DEFAULT 'telegram'",
                [],
            ) {
                Ok(_) => {}
                Err(err) => {
                    let msg = err.to_string();
                    if !msg.contains("duplicate column") {
                        return Err(err.into());
                    }
                }
            }
            self.kv_set("schema_version", "2")?;
        }

        if current < 3 {
            match self.conn.execute(
                "ALTER TABLE scheduled_tasks ADD COLUMN pending_output TEXT",
                [],
            ) {
                Ok(_) => {}
                Err(err) => {
                    let msg = err.to_string();
                    if !msg.contains("duplicate column") {
                        return Err(err.into());
                    }
                }
            }
            self.kv_set("schema_version", "3")?;
        }

        if current < 4 {
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS task_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    source_chat_id TEXT,
                    source_user_id TEXT,
                    update_id TEXT,
                    prompt TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    cancel_requested_at TEXT,
                    progress_message_id TEXT,
                    last_progress TEXT,
                    error_summary TEXT,
                    result_summary TEXT,
                    scheduled_task_id INTEGER
                 );
                 CREATE INDEX IF NOT EXISTS idx_task_runs_session_status ON task_runs(session_id, status, id DESC);
                 CREATE INDEX IF NOT EXISTS idx_task_runs_status ON task_runs(status, id DESC);
                 CREATE TABLE IF NOT EXISTS approvals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_run_id INTEGER NOT NULL,
                    session_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    source_user_id TEXT,
                    channel_id TEXT,
                    thread_ts TEXT,
                    prompt_text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_message_ts TEXT,
                    resume_payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    resolved_by_user_id TEXT
                 );
                 CREATE INDEX IF NOT EXISTS idx_approvals_task_status ON approvals(task_run_id, status, id DESC);",
            )?;
            self.kv_set("schema_version", "4")?;
        }

        if current < 5 {
            match self.conn.execute(
                "ALTER TABLE task_runs ADD COLUMN scheduled_task_id INTEGER",
                [],
            ) {
                Ok(_) => {}
                Err(err) => {
                    let msg = err.to_string();
                    if !msg.contains("duplicate column") {
                        return Err(err.into());
                    }
                }
            }
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_runs_scheduled_task_status
                 ON task_runs(scheduled_task_id, status, id DESC)",
                [],
            )?;
            self.kv_set("schema_version", "5")?;
        }

        if current < 6 {
            for sql in [
                "ALTER TABLE turns ADD COLUMN task_run_id INTEGER",
                "ALTER TABLE turns ADD COLUMN side_effects_applied INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE scheduled_tasks ADD COLUMN delivery_state TEXT",
            ] {
                match self.conn.execute(sql, []) {
                    Ok(_) => {}
                    Err(err) => {
                        let msg = err.to_string();
                        if !msg.contains("duplicate column") {
                            return Err(err.into());
                        }
                    }
                }
            }
            self.kv_set("schema_version", "6")?;
        }

        if current < 7 {
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS memory_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    source TEXT NOT NULL,
                    content TEXT NOT NULL,
                    turn_id INTEGER NOT NULL,
                    append_index INTEGER NOT NULL
                 );
                 CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_entries_turn_append_unique
                   ON memory_entries(turn_id, append_index);",
            )?;
            for sql in [
                "ALTER TABLE tasks ADD COLUMN turn_id INTEGER",
                "ALTER TABLE tasks ADD COLUMN append_index INTEGER",
                "ALTER TABLE tasks ADD COLUMN managed_file INTEGER NOT NULL DEFAULT 0",
            ] {
                match self.conn.execute(sql, []) {
                    Ok(_) => {}
                    Err(err) => {
                        let msg = err.to_string();
                        if !msg.contains("duplicate column") {
                            return Err(err.into());
                        }
                    }
                }
            }
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_turn_append_unique
                 ON tasks(turn_id, append_index)",
                [],
            )?;
            self.kv_set("schema_version", "7")?;
        }

        if current < 8 {
            self.deduplicate_task_run_update_ids()?;
            self.ensure_task_runs_update_id_unique_index()?;
            self.kv_set("schema_version", "8")?;
        }

        if current < 9 {
            for sql in [
                "ALTER TABLE scheduled_tasks ADD COLUMN origin_session TEXT",
                "ALTER TABLE scheduled_tasks ADD COLUMN delivery_target TEXT",
            ] {
                match self.conn.execute(sql, []) {
                    Ok(_) => {}
                    Err(err) => {
                        let msg = err.to_string();
                        if !msg.contains("duplicate column") {
                            return Err(err.into());
                        }
                    }
                }
            }
            self.kv_set("schema_version", "9")?;
        }

        if current < 10 {
            self.backfill_legacy_scheduled_task_routing()?;
            self.kv_set("schema_version", "10")?;
        }

        Ok(())
    }

    fn deduplicate_task_run_update_ids(&self) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let mut duplicates = tx.prepare(
            "SELECT update_id, MAX(id) AS keep_id
             FROM task_runs
             WHERE update_id IS NOT NULL
             GROUP BY update_id
             HAVING COUNT(*) > 1",
        )?;
        let duplicate_groups = duplicates
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(duplicates);

        for (update_id, keep_id) in duplicate_groups {
            let mut duplicate_rows = tx.prepare(
                "SELECT id
                 FROM task_runs
                 WHERE update_id = ?1
                   AND id != ?2",
            )?;
            let duplicate_ids = duplicate_rows
                .query_map(params![update_id, keep_id], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(duplicate_rows);

            for duplicate_id in duplicate_ids {
                tx.execute(
                    "UPDATE turns
                     SET task_run_id = ?1
                     WHERE task_run_id = ?2",
                    params![keep_id, duplicate_id],
                )?;
                tx.execute(
                    "UPDATE approvals
                     SET task_run_id = ?1
                     WHERE task_run_id = ?2",
                    params![keep_id, duplicate_id],
                )?;
                tx.execute("DELETE FROM task_runs WHERE id = ?1", params![duplicate_id])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    fn ensure_task_runs_update_id_unique_index(&self) -> Result<()> {
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_update_id_unique
             ON task_runs(update_id) WHERE update_id IS NOT NULL",
            [],
        )?;
        Ok(())
    }

    fn backfill_legacy_scheduled_task_routing(&self) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let mut tasks = tx.prepare(
            "SELECT id, ts, prompt, schedule_time, recurring, origin_session, delivery_target
             FROM scheduled_tasks
             WHERE origin_session IS NULL
                OR delivery_target IS NULL
             ORDER BY id ASC",
        )?;
        let scheduled_tasks = tasks
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i32>(4)? != 0,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(tasks);

        for (
            scheduled_task_id,
            ts,
            prompt,
            schedule_time,
            recurring,
            origin_session,
            delivery_target,
        ) in scheduled_tasks
        {
            let Some((matched_origin_session, matched_delivery_target)) =
                find_origin_turn_routing_for_schedule(
                    &tx,
                    &ts,
                    &prompt,
                    &schedule_time,
                    recurring,
                )?
            else {
                continue;
            };

            tx.execute(
                "UPDATE scheduled_tasks
                 SET origin_session = COALESCE(origin_session, ?2),
                     delivery_target = COALESCE(delivery_target, ?3)
                 WHERE id = ?1",
                params![
                    scheduled_task_id,
                    origin_session.or(Some(matched_origin_session)),
                    delivery_target.or(Some(matched_delivery_target))
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }
}

fn find_origin_turn_routing_for_schedule(
    tx: &rusqlite::Transaction<'_>,
    ts: &str,
    prompt: &str,
    schedule_time: &str,
    recurring: bool,
) -> Result<Option<(String, String)>> {
    let mut turns = tx.prepare(
        "SELECT chat_id, channel, provider_raw
         FROM turns
         WHERE ts = ?1
           AND status NOT IN ('boundary', 'cancelled')
         ORDER BY id DESC",
    )?;
    let rows = turns
        .query_map(params![ts], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    for (chat_id, channel, provider_raw) in rows {
        let markers = parse_markers(&provider_raw);
        let matched = markers.schedule_prompt.iter().any(|line| {
            parse_schedule_prompt_for_migration(line)
                .map(|(line_recurring, line_time, line_prompt)| {
                    line_recurring == recurring
                        && line_time == schedule_time
                        && line_prompt == prompt
                })
                .unwrap_or(false)
        });
        if !matched {
            continue;
        }

        let delivery_target = match channel.as_str() {
            "telegram" => {
                let chat_id = chat_id
                    .strip_prefix("telegram:")
                    .unwrap_or(chat_id.as_str())
                    .split_once('#')
                    .map(|(root, _)| root)
                    .unwrap_or_else(|| {
                        chat_id
                            .strip_prefix("telegram:")
                            .unwrap_or(chat_id.as_str())
                    });
                json!({"kind": "telegram", "chat_id": chat_id}).to_string()
            }
            "slack" => {
                let session = chat_id.strip_prefix("slack:").unwrap_or(chat_id.as_str());
                let (channel_id, thread_ts) = session
                    .split_once('#')
                    .map(|(channel_id, thread_ts)| {
                        (channel_id.to_string(), Some(thread_ts.to_string()))
                    })
                    .unwrap_or_else(|| (session.to_string(), None));
                json!({"channel_id": channel_id, "kind": "slack", "thread_ts": thread_ts})
                    .to_string()
            }
            "local" => json!({"kind": "stdout"}).to_string(),
            _ => continue,
        };

        return Ok(Some((chat_id, delivery_target)));
    }

    Ok(None)
}

fn parse_schedule_prompt_for_migration(line: &str) -> Option<(bool, String, String)> {
    let (recurring, rest) = if let Some(stripped) = line.strip_prefix("once ") {
        (false, stripped.trim())
    } else {
        (true, line)
    };

    let (time, prompt) = rest.split_once('|')?;
    let time = time.trim();
    let prompt = prompt.trim();
    let parts: Vec<&str> = time.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let h = parts[0].parse::<u8>().ok()?;
    let m = parts[1].parse::<u8>().ok()?;
    if h > 23 || m > 59 || prompt.is_empty() {
        return None;
    }
    Some((recurring, format!("{:02}:{:02}", h, m), prompt.to_string()))
}
