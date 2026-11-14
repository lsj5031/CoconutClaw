//! Local core transitions. Model strings supply content only, never scope/authority.
use super::Store;
use anyhow::{Result, bail};
use rusqlite::{OptionalExtension, params};

const MAX_CONTENT: usize = 2048;
const MAX_RESULTS: usize = 8;
const MAX_OUTPUT: usize = 2048;
const USAGE: &str = "Use /memory list [before-id], /memory search <text>, /memory get <id>, /memory remember <text>, /memory promote <id>, /memory correct <active-id> <text>, or /memory undo.";

impl Store {
    pub(crate) fn memory_revision(&self, scope: &str) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(id),0) FROM memory_history WHERE scope=?1",
            [scope],
            |r| r.get(0),
        )?)
    }

    pub(crate) fn curated_memory(&self, scope: &str) -> Result<String> {
        let mut stmt = self.conn.prepare(
            "SELECT r.id,r.source_turn,r.content FROM memory_records r
             JOIN memory_active a ON a.record_id=r.id
             WHERE r.scope=?1 AND r.curated=1 ORDER BY r.id DESC LIMIT 4",
        )?;
        let rows = stmt.query_map([scope], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        let mut output = String::new();
        for row in rows {
            let (id, source, content) = row?;
            let line = format!(
                "#{id} [source turn {source}] {}\n",
                serde_json::to_string(&content)?
            );
            if output.len() + line.len() <= 2048 {
                output.push_str(&line);
            }
        }
        Ok(output)
    }

    fn get_memory(&self, scope: &str, id: i64) -> Result<String> {
        let record = self
            .conn
            .query_row(
                "SELECT r.source_turn,r.content,
                    CASE WHEN a.record_id IS NULL THEN 'candidate' ELSE 'active' END
             FROM memory_records r LEFT JOIN memory_active a ON a.record_id=r.id
             WHERE r.scope=?1 AND r.id=?2
               AND (a.record_id IS NOT NULL OR NOT EXISTS(
                   SELECT 1 FROM memory_history h WHERE h.old_record=r.id OR h.new_record=r.id))",
                params![scope, id],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        Ok(match record {
            Some((source, content, state)) => format!(
                "#{id} [{state}, source turn {source}] {}\n",
                serde_json::to_string(&content)?
            ),
            None => "Current memory ID not found in this chat.".into(),
        })
    }

    pub(crate) fn initialize_memory(&self) -> Result<()> {
        // No legacy imports or Markdown projection. FTS5 comes from the
        // existing bundled SQLite; absence fails startup explicitly.
        let tx = self.conn.unchecked_transaction()?;
        tx.execute_batch(include_str!("../../../../sql/memory.sql"))?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn recall_memory(&self, scope: &str, query: &str) -> Result<String> {
        if query.len() > 512 {
            return Ok("Memory query exceeds 512 bytes.".into());
        }
        // Literal tokens only. Quotes, operators, column names and punctuation
        // cannot become FTS syntax. Bound both tokenizer input and term count.
        let terms = query
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .take(16)
            .map(|s| format!("\"{}\"", s.chars().take(64).collect::<String>()))
            .collect::<Vec<_>>();
        if terms.is_empty() {
            return Ok("No matching promoted memories.".into());
        }
        let mut stmt = self.conn.prepare(
            "SELECT r.id,r.source_turn,substr(r.content,1,128) FROM memory_search
             JOIN memory_records r ON r.id=memory_search.rowid
             JOIN memory_active a ON a.record_id=r.id
             WHERE memory_search MATCH ?1 AND r.scope=?2
             ORDER BY rank,r.id LIMIT 8",
        )?;
        let rows = stmt.query_map(params![terms.join(" OR "), scope], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        bounded_rows(rows, "active")
    }

    fn list_memory(&self, scope: &str, before: i64) -> Result<String> {
        let mut stmt = self.conn.prepare(
            "SELECT r.id,r.source_turn,substr(r.content,1,128),
                    CASE WHEN a.record_id IS NULL THEN 'candidate/inactive' ELSE 'active' END
             FROM memory_records r LEFT JOIN memory_active a ON a.record_id=r.id
             WHERE r.scope=?1 AND r.id<?2 ORDER BY r.id DESC LIMIT 8",
        )?;
        let rows = stmt.query_map(params![scope, before], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        let mut output = String::new();
        for row in rows {
            let (id, source, content, state) = row?;
            let line = format!(
                "#{id} [{state}, source turn {source}] {}\n",
                serde_json::to_string(&content)?
            );
            if output.len() + line.len() > MAX_OUTPUT {
                break;
            }
            output.push_str(&line);
        }
        Ok(if output.is_empty() {
            "No memory records for this chat.".into()
        } else {
            output
        })
    }

    /// Called only by incoming command dispatch, never by marker processing.
    /// Receipt, source turn, immutable history and effective/search state share
    /// one immediate transaction; retries return the original receipt.
    pub(crate) fn memory_command(
        &mut self,
        scope: &str,
        text: &str,
        request_id: Option<&str>,
        ts: &str,
    ) -> Result<String> {
        let tx = rusqlite::Transaction::new_unchecked(
            &self.conn,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        if let Some(key) = request_id
            && let Some(reply) = tx
                .query_row(
                    "SELECT reply FROM memory_command_receipts WHERE scope=?1 AND request_id=?2",
                    params![scope, key],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
        {
            return Ok(reply);
        }
        // Store methods below operate on this connection inside the transaction.
        let reply = self.apply_memory_command(scope, text, ts)?;
        if let Some(key) = request_id {
            tx.execute(
                "INSERT INTO memory_command_receipts(scope,request_id,reply) VALUES(?1,?2,?3)",
                params![scope, key, reply],
            )?;
        }
        tx.commit()?;
        Ok(reply)
    }

    fn apply_memory_command(&self, scope: &str, text: &str, ts: &str) -> Result<String> {
        // Dispatch has already authorized the token, including its bot address.
        // Keep the original text for provenance; parse only the command body.
        let arg = text
            .trim()
            .split_once(char::is_whitespace)
            .map_or("", |(_, body)| body)
            .trim();
        let (action, rest) = arg.split_once(char::is_whitespace).unwrap_or((arg, ""));
        let rest = rest.trim();
        if action == "list" {
            if rest.is_empty() {
                return self.list_memory(scope, i64::MAX);
            }
            return match rest.parse::<i64>() {
                Ok(id) if id > 0 => self.list_memory(scope, id),
                _ => Ok(USAGE.into()),
            };
        }
        if matches!(action, "search" | "recall") {
            return self.recall_memory(scope, rest);
        }
        if action == "get" {
            return match rest.parse::<i64>() {
                Ok(id) if id > 0 => self.get_memory(scope, id),
                _ => Ok(USAGE.into()),
            };
        }
        let remember = (action == "remember").then_some(rest);
        if remember.is_some_and(|s| s.is_empty() || s.len() > 512 || !valid_content(s)) {
            return Ok(
                "Remember requires 1..512 bytes of text, without control characters.".into(),
            );
        }
        let (id_text, content) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
        let id = id_text.parse::<i64>().ok().filter(|id| *id > 0);
        let content = remember.unwrap_or(content.trim());
        if action == "undo" && !rest.is_empty() {
            return Ok(USAGE.into());
        }
        if !matches!(action, "undo" | "promote" | "correct" | "remember") {
            return Ok(USAGE.into());
        }
        if matches!(action, "promote" | "correct")
            && (id.is_none() || (action == "promote" && !content.is_empty()))
        {
            return Ok(USAGE.into());
        }
        if action == "correct"
            && (content.is_empty() || content.len() > MAX_CONTENT || !valid_content(content))
        {
            return Ok("Correction must contain 1..2048 bytes.".into());
        }
        let mut curated = false;
        let (old, new, undo_of) = if action == "remember" {
            curated = true;
            (None, None, None)
        } else if action == "undo" {
            let event = self
                .conn
                .query_row(
                    "SELECT h.id,h.old_record,h.new_record FROM memory_history h
                 WHERE h.scope=?1 AND h.kind!='undo'
                   AND NOT EXISTS(SELECT 1 FROM memory_history u WHERE u.undo_of=h.id)
                 ORDER BY h.id DESC LIMIT 1",
                    [scope],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, Option<i64>>(1)?,
                            r.get::<_, Option<i64>>(2)?,
                        ))
                    },
                )
                .optional()?;
            let Some((event, old, new)) = event else {
                return Ok("Nothing to undo for this chat.".into());
            };
            (new, old, Some(event))
        } else {
            let id = id.expect("validated id");
            let active = self
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM memory_active WHERE record_id=r.id),r.curated
                 FROM memory_records r WHERE r.id=?1 AND r.scope=?2",
                    params![id, scope],
                    |r| Ok((r.get::<_, bool>(0)?, r.get::<_, bool>(1)?)),
                )
                .optional()?;
            let Some((active, was_curated)) = active else {
                return Ok("Memory ID not found in this chat.".into());
            };
            if action == "promote" {
                if active {
                    return Ok(format!("Memory #{id} is already active."));
                }
                (None, Some(id), None)
            } else {
                if !active {
                    return Ok("Only an active memory can be corrected.".into());
                }
                curated = was_curated;
                if curated && content.len() > 512 {
                    return Ok("Curated correction exceeds 512 bytes.".into());
                }
                (Some(id), None, None)
            }
        };
        self.conn.execute(
            "INSERT INTO turns(ts,chat_id,input_type,user_text,provider_raw,telegram_reply,status,channel,side_effects_applied)
             VALUES(?1,?2,'system',?3,'','','memory_command','telegram',1)",
            params![ts,scope,text],
        )?;
        let source_turn = self.conn.last_insert_rowid();
        let new = if matches!(action, "correct" | "remember") {
            self.conn.execute(
                "INSERT INTO memory_records(scope,source_turn,content,curated) VALUES(?1,?2,?3,?4)",
                params![scope, source_turn, content, curated],
            )?;
            Some(self.conn.last_insert_rowid())
        } else {
            new
        };
        if let Some(old) = old {
            let changed = self
                .conn
                .execute("DELETE FROM memory_active WHERE record_id=?1", [old])?;
            if changed != 1 {
                bail!("memory effective state inconsistent; transaction rolled back");
            }
        }
        if let Some(new) = new {
            self.conn
                .execute("INSERT INTO memory_active(record_id) VALUES(?1)", [new])?;
        }
        self.conn.execute(
            "INSERT INTO memory_history(scope,source_turn,kind,old_record,new_record,undo_of) VALUES(?1,?2,?3,?4,?5,?6)",
            params![scope,source_turn,action,old,new,undo_of],
        )?;
        Ok(format!(
            "Memory {action} applied (history #{}, active {}).",
            self.conn.last_insert_rowid(),
            new.map(|id| format!("#{id}"))
                .unwrap_or_else(|| "none".into())
        ))
    }
}

pub(crate) fn valid_content(content: &str) -> bool {
    !content.is_empty()
        && content.len() <= MAX_CONTENT
        && !content
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\t')
}

fn bounded_rows(
    rows: impl Iterator<Item = rusqlite::Result<(i64, i64, String)>>,
    state: &str,
) -> Result<String> {
    let mut output = String::new();
    for row in rows.take(MAX_RESULTS) {
        let (id, source, content) = row?;
        let line = format!(
            "#{id} [{state}, source turn {source}] {}\n",
            serde_json::to_string(&content)?
        );
        if output.len() + line.len() > MAX_OUTPUT {
            break;
        }
        output.push_str(&line);
    }
    Ok(if output.is_empty() {
        "No matching promoted memories.".into()
    } else {
        output
    })
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;
