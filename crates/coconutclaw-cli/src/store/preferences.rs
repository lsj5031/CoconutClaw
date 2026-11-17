use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

use super::Store;
use crate::session::SessionKey;

impl Store {
    pub(crate) fn incoming_control(
        &self,
        session: &SessionKey,
        update: Option<&str>,
    ) -> Result<i64> {
        ensure!(
            !self.conn.is_autocommit(),
            "authenticated SQL control transaction required"
        );
        let (seq, raw): (i64,String) = self.conn.query_row(
            "SELECT seq,payload FROM telegram_ingress WHERE update_id=?1 AND session_id=?2 AND state='applying'",
            params![update,session.id()], |r|Ok((r.get(0)?,r.get(1)?)))?;
        let raw: Value = serde_json::from_str(&raw)?;
        let message = raw
            .get("message")
            .unwrap_or(&raw["callback_query"]["message"]);
        let actor = raw
            .get("callback_query")
            .map(|c| &c["from"])
            .unwrap_or(&message["from"]);
        ensure!(
            SessionKey::from_telegram_message(message)? == *session
                && actor
                    .get("id")
                    .is_some_and(|v| v.to_string().trim_matches('"') == session.root_id),
            "wrong conversation/topic/actor authority"
        );
        Ok(seq)
    }

    pub(crate) fn preference_revision(&self, scope: &str) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT coalesce(max(id),0) FROM runtime_preferences WHERE session_id=?1",
            [scope],
            |r| r.get(0),
        )?)
    }

    pub(crate) fn preferences_context(&self, scope: &str) -> Result<String> {
        let mut stmt=self.conn.prepare(
            "SELECT p.name,p.value,p.actor,p.cause_seq FROM runtime_preferences p
             WHERE p.session_id=?1 AND p.value IS NOT NULL AND NOT EXISTS(
               SELECT 1 FROM runtime_preferences n WHERE n.session_id=p.session_id AND n.name=p.name AND n.id>p.id)
             ORDER BY p.name LIMIT 8")?;
        let rows = stmt
            .query_map([scope], |r| {
                Ok(
                    json!({"name":r.get::<_,String>(0)?,"value":r.get::<_,String>(1)?,
            "actor":r.get::<_,String>(2)?,"source_ingress":r.get::<_,i64>(3)?}),
                )
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(serde_json::to_string(&rows)?)
    }

    pub(crate) fn preferences_command(
        &self,
        session: &SessionKey,
        args: &str,
        update: Option<&str>,
        now: &str,
    ) -> Result<String> {
        let cause = self.incoming_control(session, update)?;
        if args.is_empty() || args == "list" {
            return self.preferences_context(&session.id());
        }
        let mut parts = args.splitn(3, char::is_whitespace);
        let action = parts.next().unwrap_or("");
        let name = parts.next().unwrap_or("");
        let value = parts.next().map(str::trim).unwrap_or("");
        if !matches!(action, "set" | "unset")
            || name.is_empty()
            || name.len() > 32
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
            || (action == "set"
                && (value.is_empty() || value.len() > 512 || value.chars().any(char::is_control)))
            || (action == "unset" && !value.is_empty())
        {
            return Ok("Use /preferences [set <lowercase-key> <1..512 byte text>|unset <key>]. At most 8 active entries.".into());
        }
        let old: Option<String>=self.conn.query_row("SELECT value FROM runtime_preferences WHERE session_id=?1 AND name=?2 ORDER BY id DESC LIMIT 1",
            params![session.id(),name],|r|r.get(0)).optional()?.flatten();
        let new = (action == "set").then_some(value);
        if old.as_deref() == new {
            return Ok("Preference unchanged.".into());
        }
        let active: i64=self.conn.query_row(
            "SELECT count(*) FROM runtime_preferences p WHERE p.session_id=?1 AND p.value IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM runtime_preferences n WHERE n.session_id=p.session_id AND n.name=p.name AND n.id>p.id)",
            [session.id()],|r|r.get(0))?;
        if new.is_some() && old.is_none() && active >= 8 {
            return Ok("At most 8 active preferences; unset one first.".into());
        }
        self.conn.execute("INSERT INTO runtime_preferences(session_id,actor,name,value,cause_seq,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
            params![session.id(),session.root_id,name,new,cause,now])?;
        Ok(if new.is_some() {
            "Preference saved."
        } else {
            "Preference removed."
        }
        .into())
    }

    pub(crate) fn task_status_command(
        &self,
        session: &SessionKey,
        args: &str,
        update: Option<&str>,
    ) -> Result<String> {
        self.incoming_control(session, update)?;
        let Ok(id) = args.parse::<i64>() else {
            return Ok("Use /tasks <positive run ID>.".into());
        };
        let row: Option<Value>=self.conn.query_row(
            "SELECT a.task_id,t.current_run_id,r.status,r.last_progress,a.ordinal,a.parent_run_id,a.cause_event_key,w.kind,w.expected_id,w.state
             FROM task_runs r JOIN runtime_attempts a ON a.run_id=r.id JOIN runtime_tasks t ON t.task_id=a.task_id
             LEFT JOIN runtime_waits w ON w.run_id=r.id
             WHERE r.id=?1 AND r.session_id=?2 AND (r.source_user_id IS NULL OR r.source_user_id=?3)",
            params![id,session.id(),session.root_id],|r|Ok(json!({"task_id":r.get::<_,i64>(0)?,"current_run":r.get::<_,i64>(1)?,
                "requested_run":id,"status":r.get::<_,String>(2)?,"provider_progress":r.get::<_,Option<String>>(3)?,
                "attempt":r.get::<_,i64>(4)?,"previous_run":r.get::<_,Option<i64>>(5)?,"cause_event":r.get::<_,Option<String>>(6)?,
                "wait_kind":r.get::<_,Option<String>>(7)?,"wait_for":r.get::<_,Option<i64>>(8)?,"wait_state":r.get::<_,Option<String>>(9)?}))).optional()?;
        Ok(row
            .map(|r| r.to_string())
            .unwrap_or_else(|| "Task not found in this conversation/topic.".into()))
    }
}
