use super::*;
use anyhow::Result;

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
            // Removed scheduling routing is deliberately not backfilled.
            self.kv_set("schema_version", "10")?;
        }

        Ok(())
    }

    fn ensure_task_runs_update_id_unique_index(&self) -> Result<()> {
        let duplicates: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM (SELECT update_id FROM task_runs WHERE update_id IS NOT NULL GROUP BY update_id HAVING COUNT(*)>1)",
            [], |row| row.get(0))?;
        if duplicates > 0 {
            anyhow::bail!(
                "legacy duplicate task update IDs preserved; back up the database and resolve duplicate identifiers manually before starting, see docs/migration-phase-0.md"
            );
        }
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_update_id_unique
             ON task_runs(update_id) WHERE update_id IS NOT NULL",
            [],
        )?;
        Ok(())
    }
}
