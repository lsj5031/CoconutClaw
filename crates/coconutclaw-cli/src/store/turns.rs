use super::*;
use crate::markers::CompiledOutput;
use anyhow::Result;
use chrono::DateTime;
use rusqlite::{OptionalExtension, params};

impl StoredTurnOutput {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            ts: row.get(1)?,
            provider_raw: row.get(2)?,
            telegram_reply: row.get(3)?,
            status: row.get(4)?,
            admitted_output: row.get(5)?,
        })
    }

    pub(crate) fn compiled(&self, reply: &str) -> Result<CompiledOutput> {
        if self.status == "cancelled" {
            return Ok(CompiledOutput::new(
                if reply.trim().is_empty() {
                    crate::wording::CANCELLATION
                } else {
                    reply
                },
                &[],
            ));
        }
        match &self.admitted_output {
            Some(json) => Ok(serde_json::from_str(json)?),
            None => Ok(CompiledOutput::from_legacy_turn(&self.provider_raw, reply)),
        }
    }
}

impl Store {
    pub(crate) fn recent_turns_snippet(
        &self,
        limit: u32,
        chat_id: &str,
        channel: &str,
    ) -> Result<Vec<String>> {
        let (canonical, alias) = crate::session::history_ids(chat_id, channel);
        let mut stmt = self.conn.prepare(
            "SELECT ts || ' | in=' || COALESCE(REPLACE(user_text, char(10), ' '), '') || ' | out=' || COALESCE(REPLACE(COALESCE(telegram_reply, voice_reply), char(10), ' '), '')
             FROM turns
             WHERE status NOT IN ('boundary', 'memory_command')
               AND chat_id IN (?1, ?4)
               AND channel = ?2
               AND id > COALESCE((SELECT MAX(id) FROM turns WHERE status = 'boundary' AND chat_id IN (?1, ?4) AND channel = ?2), 0)
             ORDER BY id DESC
             LIMIT ?3",
        )?;

        let mut rows = stmt.query(params![canonical, channel, limit, alias])?;
        let mut lines = Vec::new();
        while let Some(row) = rows.next()? {
            lines.push(row.get::<_, String>(0)?);
        }
        Ok(lines)
    }

    pub(crate) fn latest_boundary_unix(&self, chat_id: &str, channel: &str) -> Result<Option<i64>> {
        let (canonical, alias) = crate::session::history_ids(chat_id, channel);
        let mut stmt = self.conn.prepare(
            "SELECT ts
             FROM turns
             WHERE status = 'boundary'
               AND chat_id IN (?1, ?3)
               AND channel = ?2
             ORDER BY id DESC
             LIMIT 1",
        )?;

        let mut rows = stmt.query(params![canonical, channel, alias])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let ts: String = row.get(0)?;
        Ok(DateTime::parse_from_str(&ts, "%Y-%m-%dT%H:%M:%S%z")
            .ok()
            .map(|dt| dt.timestamp()))
    }

    #[cfg(test)]
    pub(crate) fn insert_turn(&self, turn: &TurnRecord) -> Result<Option<i64>> {
        self.insert_turn_with_admission(turn, None)
    }

    pub(crate) fn insert_turn_with_admission(
        &self,
        turn: &TurnRecord,
        admitted: Option<&CompiledOutput>,
    ) -> Result<Option<i64>> {
        let (chat_id, _) = crate::session::history_ids(&turn.chat_id, &turn.channel);
        let session = (turn.channel == "telegram" && !chat_id.starts_with("local:"))
            .then(|| crate::session::SessionKey::telegram(&chat_id));
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms, channel, task_run_id, side_effects_applied, telegram_chat_id, telegram_message_thread_id, admitted_output)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                turn.ts,
                chat_id,
                turn.input_type,
                turn.user_text,
                "",
                turn.provider_raw,
                turn.telegram_reply,
                "",
                turn.status,
                turn.update_id,
                turn.duration_ms,
                turn.channel,
                turn.task_run_id,
                turn.side_effects_applied as i32,
                session.as_ref().map(|session| &session.root_id),
                session.as_ref().and_then(|session| session.thread_id.as_deref()),
                admitted.map(serde_json::to_string).transpose()?,
            ],
        )?;
        if self.conn.changes() > 0 {
            Ok(Some(self.conn.last_insert_rowid()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn insert_memory_and_tasks(
        &mut self,
        ts: &str,
        source: &str,
        turn_id: Option<i64>,
        memory_lines: &[String],
        task_lines: &[String],
    ) -> Result<()> {
        if memory_lines.is_empty() && task_lines.is_empty() {
            return Ok(());
        }

        let tx = self.conn.transaction()?;

        if let Some(turn_id) = turn_id {
            if !memory_lines.is_empty() {
                let mut stmt = tx.prepare_cached(
                    "INSERT OR IGNORE INTO memory_records(scope,source_turn,proposal_index,content)
                     SELECT CASE WHEN chat_id LIKE 'telegram:%' THEN chat_id ELSE 'telegram:'||chat_id END,
                            id,?2,?3 FROM turns WHERE id=?1 AND channel='telegram'",
                )?;
                for (append_index, line) in memory_lines.iter().enumerate() {
                    if super::memory::valid_content(line) {
                        stmt.execute(params![turn_id, append_index as i64, line])?;
                    }
                }
            }

            if !task_lines.is_empty() {
                let mut stmt = tx.prepare_cached(
                    "INSERT OR IGNORE INTO tasks(ts, source, content, done, turn_id, append_index, managed_file)
                     VALUES(?1, ?2, ?3, 0, ?4, ?5, 1)",
                )?;

                for (append_index, line) in task_lines.iter().enumerate() {
                    stmt.execute(params![ts, source, line, turn_id, append_index as i64])?;
                }
            }
        } else if !memory_lines.is_empty() || !task_lines.is_empty() {
            anyhow::bail!("memory/task appends require a persisted turn id");
        }

        tx.commit()?;

        Ok(())
    }

    pub(crate) fn insert_boundary_turn(
        &self,
        ts: &str,
        chat_id: &str,
        update_id: Option<&str>,
        channel: &str,
    ) -> Result<bool> {
        let (chat_id, _) = crate::session::history_ids(chat_id, channel);
        let session = (channel == "telegram" && !chat_id.starts_with("local:"))
            .then(|| crate::session::SessionKey::telegram(&chat_id));
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, channel, telegram_chat_id, telegram_message_thread_id)
             VALUES(?1, ?2, 'system', '---CONTEXT_BOUNDARY---', '', '', '', '', 'boundary', ?3, ?4, ?5, ?6)",
            params![ts, chat_id, update_id, channel,
                session.as_ref().map(|session| &session.root_id),
                session.as_ref().and_then(|session| session.thread_id.as_deref())],
        )?;
        Ok(self.conn.changes() > 0)
    }

    pub(crate) fn latest_boundary_id(&self, chat_id: &str, channel: &str) -> Result<i64> {
        let (canonical, alias) = crate::session::history_ids(chat_id, channel);
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(id),0) FROM turns WHERE chat_id IN (?1,?2) AND channel=?3 AND status='boundary'",
            params![canonical, alias, channel], |r| r.get(0))?)
    }

    pub(crate) fn turn_exists_for_update_id(&self, update_id: &str) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare("SELECT 1 FROM turns WHERE update_id = ?1 LIMIT 1")?;
        let mut rows = stmt.query(params![update_id])?;
        Ok(rows.next()?.is_some())
    }

    pub(crate) fn rendered_output_for_update_id(&self, update_id: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, provider_raw, COALESCE(telegram_reply, ''), status, admitted_output
             FROM turns
             WHERE update_id = ?1
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let Some(turn) = stmt
            .query_row([update_id], StoredTurnOutput::from_row)
            .optional()?
        else {
            return Ok(None);
        };
        let mut output = turn.compiled(&turn.telegram_reply)?;
        if !output.rejected && !turn.telegram_reply.trim().is_empty() {
            output = CompiledOutput::new(&turn.telegram_reply, &output.effects);
        }
        Ok(Some(output.encode().trim_end().to_string()))
    }

    pub(crate) fn turn_output_for_task_run(
        &self,
        task_run_id: i64,
    ) -> Result<Option<StoredTurnOutput>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, provider_raw, COALESCE(telegram_reply, ''), status, admitted_output
             FROM turns
             WHERE task_run_id = ?1
             ORDER BY id DESC
             LIMIT 1",
        )?;
        Ok(stmt
            .query_row([task_run_id], StoredTurnOutput::from_row)
            .optional()?)
    }

    /// Authoritative model context: ownership comes only from the persisted
    /// source turn, never a model-supplied scope or an unscoped manual file.
    pub(crate) fn managed_pending_task_entries_for_session(
        &self,
        session_id: &str,
        channel: &str,
    ) -> Result<Vec<String>> {
        let (canonical, alias) = crate::session::history_ids(session_id, channel);
        let mut stmt = self.conn.prepare(
            "SELECT task.content FROM tasks task JOIN turns source ON source.id=task.turn_id
             WHERE task.managed_file=1 AND task.done=0
               AND source.chat_id IN (?1,?2) AND source.channel=?3
             ORDER BY task.id ASC",
        )?;
        Ok(stmt
            .query_map(params![canonical, alias, channel], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Operator-only aggregate projection (used by sync to bridge write gap).
    pub(crate) fn managed_pending_task_entries_from_db(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT content
             FROM tasks
             WHERE managed_file = 1
               AND done = 0
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            entries.push(row.get(0)?);
        }
        Ok(entries)
    }

    pub(crate) fn update_turn_reply_and_side_effects_by_id(
        &self,
        turn_id: i64,
        telegram_reply: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE turns
             SET telegram_reply = ?2,
                 side_effects_applied = 1
             WHERE id = ?1",
            params![turn_id, telegram_reply],
        )?;
        Ok(())
    }

    pub(crate) fn pending_turn_side_effects(&self) -> Result<Vec<StoredTurnOutput>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, provider_raw, COALESCE(telegram_reply, ''), status, admitted_output
             FROM turns
             WHERE side_effects_applied = 0
               AND status NOT IN ('cancelled', 'boundary')
             ORDER BY id ASC",
        )?;
        Ok(stmt
            .query_map([], StoredTurnOutput::from_row)?
            .collect::<rusqlite::Result<_>>()?)
    }
}
