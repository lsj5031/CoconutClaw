use crate::markers::{parse_markers, render_output};
use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use rusqlite::{Connection, params};
use std::fs;

const SCHEMA_SQL: &str = include_str!("../../../sql/schema.sql");

#[derive(Debug)]
pub(crate) struct TurnRecord {
    pub(crate) ts: String,
    pub(crate) chat_id: String,
    pub(crate) input_type: String,
    pub(crate) user_text: String,
    pub(crate) asr_text: String,
    pub(crate) provider_raw: String,
    pub(crate) telegram_reply: String,
    pub(crate) voice_reply: String,
    pub(crate) status: String,
    pub(crate) update_id: Option<String>,
    pub(crate) duration_ms: Option<i64>,
}

pub(crate) struct Store {
    conn: Connection,
}

impl Store {
    pub(crate) fn open(cfg: &RuntimeConfig) -> Result<Self> {
        if let Some(parent) = cfg.sqlite_db_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let conn = Connection::open(&cfg.sqlite_db_path)
            .with_context(|| format!("failed to open {}", cfg.sqlite_db_path.display()))?;
        conn.execute_batch(SCHEMA_SQL)
            .context("failed to apply sqlite schema")?;
        let _ = conn.execute("ALTER TABLE turns ADD COLUMN duration_ms INTEGER", []);
        let _ = conn.execute(
            "ALTER TABLE turns RENAME COLUMN codex_raw TO provider_raw",
            [],
        );
        Ok(Self { conn })
    }

    pub(crate) fn recent_turns_snippet(&self, limit: u32) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts || ' | in=' || COALESCE(REPLACE(user_text, char(10), ' '), '') || ' | out=' || COALESCE(REPLACE(COALESCE(telegram_reply, voice_reply), char(10), ' '), '')
             FROM turns
             WHERE status != 'boundary'
               AND id > COALESCE((SELECT MAX(id) FROM turns WHERE user_text = '---CONTEXT_BOUNDARY---'), 0)
             ORDER BY id DESC
             LIMIT ?1",
        )?;

        let mut rows = stmt.query(params![limit])?;
        let mut lines = Vec::new();
        while let Some(row) = rows.next()? {
            lines.push(row.get::<_, String>(0)?);
        }
        Ok(lines)
    }

    pub(crate) fn insert_turn(&self, turn: &TurnRecord) -> Result<bool> {
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
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
            ],
        )?;
        Ok(self.conn.changes() > 0)
    }

    pub(crate) fn insert_tasks(&mut self, ts: &str, source: &str, lines: &[String]) -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }

        let tx = self.conn.transaction()?;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO tasks(ts, source, content, done) VALUES(?1, ?2, ?3, 0)",
            )?;

            for line in lines {
                stmt.execute(params![ts, source, line])?;
            }
        }

        tx.commit()?;

        Ok(())
    }

    pub(crate) fn insert_boundary_turn(
        &self,
        ts: &str,
        chat_id: &str,
        update_id: Option<&str>,
    ) -> Result<bool> {
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id)
             VALUES(?1, ?2, 'system', '---CONTEXT_BOUNDARY---', '', '', '', '', 'boundary', ?3)",
            params![ts, chat_id, update_id],
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
        let mut markers = parse_markers(&provider_raw);
        if !telegram_reply.trim().is_empty() {
            markers.telegram_reply = Some(telegram_reply.clone());
        }
        if !voice_reply.trim().is_empty() {
            markers.voice_reply = Some(voice_reply.clone());
        }

        let rendered = render_output(
            markers.telegram_reply.as_deref().unwrap_or_default(),
            markers.voice_reply.as_deref().unwrap_or_default(),
            &markers,
        );
        Ok(Some(rendered.trim_end().to_string()))
    }

    pub(crate) fn kv_get(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM kv WHERE key = ?1 LIMIT 1")?;
        let mut rows = stmt.query(params![key])?;
        if let Some(row) = rows.next()? {
            return Ok(Some(row.get::<_, String>(0)?));
        }
        Ok(None)
    }

    pub(crate) fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO kv(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub(crate) fn clear_inflight(&self) -> Result<()> {
        self.conn.execute(
            "DELETE FROM kv WHERE key IN ('inflight_update_id', 'inflight_update_json', 'inflight_started_at')",
            [],
        )?;
        Ok(())
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
