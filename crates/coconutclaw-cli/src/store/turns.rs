use super::*;
use crate::markers::{parse_markers, render_effects};
use anyhow::Result;
use chrono::DateTime;
use rusqlite::params;

impl Store {
    pub(crate) fn recent_turns_snippet(
        &self,
        limit: u32,
        chat_id: &str,
        channel: &str,
    ) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts || ' | in=' || COALESCE(REPLACE(user_text, char(10), ' '), '') || ' | out=' || COALESCE(REPLACE(COALESCE(telegram_reply, voice_reply), char(10), ' '), '')
             FROM turns
             WHERE status != 'boundary'
               AND chat_id = ?1
               AND channel = ?2
               AND id > COALESCE((SELECT MAX(id) FROM turns WHERE user_text = '---CONTEXT_BOUNDARY---' AND chat_id = ?1 AND channel = ?2), 0)
             ORDER BY id DESC
             LIMIT ?3",
        )?;

        let mut rows = stmt.query(params![chat_id, channel, limit])?;
        let mut lines = Vec::new();
        while let Some(row) = rows.next()? {
            lines.push(row.get::<_, String>(0)?);
        }
        Ok(lines)
    }

    pub(crate) fn latest_boundary_unix(&self, chat_id: &str, channel: &str) -> Result<Option<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts
             FROM turns
             WHERE status = 'boundary'
               AND chat_id = ?1
               AND channel = ?2
             ORDER BY id DESC
             LIMIT 1",
        )?;

        let mut rows = stmt.query(params![chat_id, channel])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let ts: String = row.get(0)?;
        Ok(DateTime::parse_from_str(&ts, "%Y-%m-%dT%H:%M:%S%z")
            .ok()
            .map(|dt| dt.timestamp()))
    }

    pub(crate) fn insert_turn(&self, turn: &TurnRecord) -> Result<Option<i64>> {
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms, channel, task_run_id, side_effects_applied)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                turn.ts,
                turn.chat_id,
                turn.input_type,
                turn.user_text,
                turn.asr_text,
                turn.provider_raw,
                turn.telegram_reply,
                turn.voice_reply,
                turn.status,
                turn.update_id,
                turn.duration_ms,
                turn.channel,
                turn.task_run_id,
                turn.side_effects_applied as i32,
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
                    "INSERT OR IGNORE INTO memory_entries(ts, source, content, turn_id, append_index)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                )?;
                for (append_index, line) in memory_lines.iter().enumerate() {
                    stmt.execute(params![ts, source, line, turn_id, append_index as i64])?;
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
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, channel)
             VALUES(?1, ?2, 'system', '---CONTEXT_BOUNDARY---', '', '', '', '', 'boundary', ?3, ?4)",
            params![ts, chat_id, update_id, channel],
        )?;
        Ok(self.conn.changes() > 0)
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
            "SELECT provider_raw, telegram_reply, voice_reply
             FROM turns
             WHERE update_id = ?1
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![update_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let provider_raw: String = row.get(0)?;
        let telegram_reply: String = row.get(1)?;
        let voice_reply: String = row.get(2)?;
        let markers = parse_markers(&provider_raw);
        let mut effects = markers.to_effects();
        if !telegram_reply.trim().is_empty() {
            effects.retain(|e| !matches!(e, crate::markers::Effect::TelegramReply(_)));
            effects.push(crate::markers::Effect::TelegramReply(
                telegram_reply.clone(),
            ));
        }
        if !voice_reply.trim().is_empty() {
            effects.retain(|e| !matches!(e, crate::markers::Effect::VoiceReply(_)));
            effects.push(crate::markers::Effect::VoiceReply(voice_reply.clone()));
        }

        let rendered = render_effects(&effects);
        Ok(Some(rendered.trim_end().to_string()))
    }

    pub(crate) fn turn_output_for_task_run(
        &self,
        task_run_id: i64,
    ) -> Result<Option<StoredTurnOutput>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, provider_raw, telegram_reply, voice_reply, status, task_run_id, side_effects_applied
             FROM turns
             WHERE task_run_id = ?1
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![task_run_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(StoredTurnOutput {
            id: row.get(0)?,
            ts: row.get(1)?,
            provider_raw: row.get(2)?,
            telegram_reply: row.get(3)?,
            voice_reply: row.get(4)?,
            status: row.get(5)?,
            task_run_id: row.get(6)?,
            side_effects_applied: row.get::<_, i32>(7)? != 0,
        }))
    }

    /// Read managed memory entries from SQLite (used by sync to bridge write gap).
    pub(crate) fn managed_memory_entries_from_db(&self) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, content
             FROM memory_entries
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            entries.push((row.get(0)?, row.get(1)?));
        }
        Ok(entries)
    }

    /// Read managed pending task entries from SQLite (used by sync to bridge write gap).
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
        voice_reply: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE turns
             SET telegram_reply = ?2,
                 voice_reply = ?3,
                 side_effects_applied = 1
             WHERE id = ?1",
            params![turn_id, telegram_reply, voice_reply],
        )?;
        Ok(())
    }

    pub(crate) fn pending_turn_side_effects(&self) -> Result<Vec<StoredTurnOutput>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, provider_raw, telegram_reply, voice_reply, status, task_run_id, side_effects_applied
             FROM turns
             WHERE side_effects_applied = 0
               AND status NOT IN ('cancelled', 'boundary')
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut turns = Vec::new();
        while let Some(row) = rows.next()? {
            turns.push(StoredTurnOutput {
                id: row.get(0)?,
                ts: row.get(1)?,
                provider_raw: row.get(2)?,
                telegram_reply: row.get(3)?,
                voice_reply: row.get(4)?,
                status: row.get(5)?,
                task_run_id: row.get(6)?,
                side_effects_applied: row.get::<_, i32>(7)? != 0,
            });
        }
        Ok(turns)
    }

    pub(crate) fn max_turn_id(&self) -> Result<i64> {
        let id: i64 = self
            .conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM turns", [], |row| {
                row.get(0)
            })?;
        Ok(id)
    }

    pub(crate) fn latest_turn_for_prompt_after_id(
        &self,
        after_id: i64,
        prompt: &str,
    ) -> Result<Option<(String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, COALESCE(NULLIF(telegram_reply, ''), NULLIF(voice_reply, ''), ''), status
             FROM turns
             WHERE id > ?1
               AND user_text = ?2
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![after_id, prompt])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?)))
    }
}
