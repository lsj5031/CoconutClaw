//! Local core transitions. Model strings supply content only, never scope/authority.
use super::Store;
use anyhow::{Result, bail};
use rusqlite::{OptionalExtension, Row, params};

const MAX_CONTENT: usize = 2048;
const MAX_OUTPUT: usize = 2048;
const USAGE: &str = "Use /memory list [before-id], /memory search <text>, /memory get <id>, /memory remember <text>, /memory promote <id>, /memory correct <active-id> <text>, or /memory undo.";

struct MemoryMatch {
    id: i64,
    source: String,
    content: String,
}

fn memory_match(row: &Row<'_>) -> rusqlite::Result<MemoryMatch> {
    Ok(MemoryMatch {
        id: row.get(0)?,
        source: row.get(1)?,
        content: row.get(2)?,
    })
}

impl Store {
    pub(crate) fn memory_revision(&self, scope: &str) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(id),0) FROM memory_history WHERE scope=?1",
            [scope],
            |r| r.get(0),
        )?)
    }

    pub(crate) fn curated_memory(&self, scope: &str) -> Result<String> {
        let mut output = String::new();
        for MemoryMatch {
            id,
            source,
            content,
        } in self.curated_matches(scope)?
        {
            let line = format!(
                "#{id} [source {source}] {}\n",
                serde_json::to_string(&content)?
            );
            if output.len() + line.len() <= MAX_OUTPUT {
                output.push_str(&line);
            }
        }
        Ok(output)
    }

    fn curated_matches(&self, scope: &str) -> Result<Vec<MemoryMatch>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.id,COALESCE('turn '||r.source_turn,
               (SELECT 'import '||path||':'||first_line||'-'||last_line FROM memory_import_origins WHERE id=r.import_origin)),r.content FROM memory_records r
             JOIN memory_active a ON a.record_id=r.id
             WHERE r.scope=?1 AND r.curated=1 ORDER BY r.id DESC LIMIT 4",
        )?;
        Ok(stmt
            .query_map([scope], memory_match)?
            .collect::<rusqlite::Result<_>>()?)
    }

    fn get_memory(&self, scope: &str, id: i64) -> Result<String> {
        let record = self
            .conn
            .query_row(
                "SELECT COALESCE('turn '||r.source_turn,
                    (SELECT 'import '||path||':'||first_line||'-'||last_line FROM memory_import_origins WHERE id=r.import_origin)),r.content,
                    CASE WHEN a.record_id IS NULL THEN 'candidate' ELSE 'active' END
             FROM memory_records r LEFT JOIN memory_active a ON a.record_id=r.id
             WHERE r.scope=?1 AND r.id=?2
               AND (a.record_id IS NOT NULL OR NOT EXISTS(
                   SELECT 1 FROM memory_history h WHERE h.old_record=r.id OR h.new_record=r.id))
               AND (a.record_id IS NOT NULL OR r.import_origin IS NULL OR NOT EXISTS(
                   SELECT 1 FROM memory_import_origins n JOIN memory_import_origins o ON o.id=r.import_origin
                   WHERE n.scope=o.scope AND n.root=o.root AND n.path=o.path AND n.chunk=o.chunk AND n.id>o.id))",
                params![scope, id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        Ok(match record {
            Some((source, content, state)) => format!(
                "#{id} [{state}, source {source}] {}\n",
                serde_json::to_string(&content)?
            ),
            None => "Memory not found in this conversation. Open Memory for current IDs.".into(),
        })
    }

    pub(crate) fn initialize_memory(&self) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let migrate = tx
            .prepare("PRAGMA table_info(memory_records)")?
            .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, bool>(3)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|(name, not_null)| name == "source_turn" && *not_null);
        if migrate {
            tx.execute_batch(include_str!("../../../../sql/memory-import-migration.sql"))?;
        }
        tx.execute_batch(include_str!("../../../../sql/memory.sql"))?;
        if migrate {
            tx.execute_batch(
                "INSERT INTO memory_records(id,scope,source_turn,proposal_index,curated,content)
                   SELECT id,scope,source_turn,proposal_index,curated,content FROM saved_records;
                 INSERT INTO memory_active SELECT * FROM saved_active;
                 INSERT INTO memory_history SELECT * FROM saved_history;
                 INSERT INTO memory_command_receipts SELECT * FROM saved_receipts;
                 DROP TABLE saved_records; DROP TABLE saved_active;
                 DROP TABLE saved_history; DROP TABLE saved_receipts;",
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn recall_memory(&self, scope: &str, query: &str) -> Result<String> {
        if query.len() > 512 {
            return Ok("Search text exceeds 512 bytes. Try a shorter phrase.".into());
        }
        let mut output = String::new();
        for MemoryMatch {
            id,
            source,
            content,
        } in self.recall_matches(scope, query)?
        {
            let line = format!(
                "#{id} [active, source {source}] {}\n",
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

    fn recall_matches(&self, scope: &str, query: &str) -> Result<Vec<MemoryMatch>> {
        if query.len() > 512 {
            return Ok(Vec::new());
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
            return Ok(Vec::new());
        }
        let mut stmt = self.conn.prepare(
            "SELECT r.id,COALESCE('turn '||r.source_turn,
               (SELECT 'import '||path||':'||first_line||'-'||last_line FROM memory_import_origins WHERE id=r.import_origin)),substr(r.content,1,128) FROM memory_search
             JOIN memory_records r ON r.id=memory_search.rowid
             JOIN memory_active a ON a.record_id=r.id
             WHERE memory_search MATCH ?1 AND r.scope=?2
             ORDER BY rank,r.id LIMIT 8",
        )?;
        Ok(stmt
            .query_map(params![terms.join(" OR "), scope], memory_match)?
            .collect::<rusqlite::Result<_>>()?)
    }

    /// Reuse coordinator lookup, but only export fact content, never source
    /// identifiers, paths, or command/display text. The caller bounds the
    /// complete signed prompt and reads both queries in the reservation snapshot.
    pub(crate) fn remote_memory(&self, scope: &str, query: &str) -> Result<Vec<String>> {
        let mut matches = self.curated_matches(scope)?;
        for m in self.recall_matches(scope, query)? {
            if !matches.iter().any(|candidate| candidate.id == m.id) {
                matches.push(m);
            }
        }
        Ok(matches.into_iter().map(|m| m.content).collect())
    }

    fn list_memory(&self, scope: &str, before: i64) -> Result<String> {
        let mut stmt = self.conn.prepare(
            "SELECT r.id,COALESCE('turn '||r.source_turn,
               (SELECT 'import '||path||':'||first_line||'-'||last_line FROM memory_import_origins WHERE id=r.import_origin)),substr(r.content,1,128),
                    CASE WHEN a.record_id IS NULL THEN 'candidate/inactive' ELSE 'active' END
             FROM memory_records r LEFT JOIN memory_active a ON a.record_id=r.id
             WHERE r.scope=?1 AND r.id<?2 ORDER BY r.id DESC LIMIT 8",
        )?;
        let rows = stmt.query_map(params![scope, before], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        let mut output = String::new();
        for row in rows {
            let (id, source, content, state) = row?;
            let line = format!(
                "#{id} [{state}, source {source}] {}\n",
                serde_json::to_string(&content)?
            );
            if output.len() + line.len() > MAX_OUTPUT {
                break;
            }
            output.push_str(&line);
        }
        Ok(if output.is_empty() {
            "No memories saved in this conversation. Use Remember to add one.".into()
        } else {
            output
        })
    }

    /// Called only by incoming command dispatch, never by marker processing.
    /// Receipt, source turn, immutable history and effective/search state share
    /// one immediate transaction; retries return the original receipt.
    pub(crate) fn memory_command(
        &self,
        scope: &str,
        text: &str,
        request_id: Option<&str>,
        ts: &str,
    ) -> Result<String> {
        let tx = self
            .conn
            .is_autocommit()
            .then(|| {
                rusqlite::Transaction::new_unchecked(
                    &self.conn,
                    rusqlite::TransactionBehavior::Immediate,
                )
            })
            .transpose()?;
        if let Some(key) = request_id
            && let Some(reply) = self
                .conn
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
            self.conn.execute(
                "INSERT INTO memory_command_receipts(scope,request_id,reply) VALUES(?1,?2,?3)",
                params![scope, key, reply],
            )?;
        }
        if let Some(tx) = tx {
            tx.commit()?;
        }
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
                "Memory text must be 1–512 bytes, without control characters. Try shorter text with /memory remember text.".into(),
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
            return Ok(
                "Replacement text must be 1–2048 bytes. Use /memory correct ID replacement text."
                    .into(),
            );
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
                return Ok(
                    "Memory not found in this conversation. Open Memory for current IDs.".into(),
                );
            };
            if action == "promote" {
                if active {
                    return Ok(format!("Memory #{id} is already active."));
                }
                (None, Some(id), None)
            } else {
                if !active {
                    return Ok("Only an active memory can be corrected. Promote the candidate first, then use Correct.".into());
                }
                curated = was_curated;
                if curated && content.len() > 512 {
                    return Ok("This memory's replacement exceeds 512 bytes. Use shorter text with /memory correct ID text.".into());
                }
                (Some(id), None, None)
            }
        };
        let session = crate::session::SessionKey::telegram(scope);
        self.conn.execute(
            "INSERT INTO turns(ts,chat_id,input_type,user_text,provider_raw,telegram_reply,status,channel,side_effects_applied,telegram_chat_id,telegram_message_thread_id)
             VALUES(?1,?2,'system',?3,'','','memory_command','telegram',1,?4,?5)",
            params![ts,scope,text,session.root_id,session.thread_id],
        )?;
        let source_turn = self.conn.last_insert_rowid();
        let new = if matches!(action, "correct" | "remember") {
            self.conn.execute(
                "INSERT INTO memory_records(scope,source_turn,content,curated,import_origin)
                 VALUES(?1,?2,?3,?4,(SELECT import_origin FROM memory_records WHERE id=?5))",
                params![scope, source_turn, content, curated, old],
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
        let active = new
            .map(|id| format!("#{id}"))
            .unwrap_or_else(|| "none".into());
        let change = self.conn.last_insert_rowid();
        Ok(match action {
            "remember" => format!("Saved memory {active}. Change #{change}."),
            "promote" => format!("Memory {active} is now active. Change #{change}."),
            "correct" => format!("Updated memory {active}. Change #{change}."),
            _ => format!("Last memory change undone. Active memory: {active}. Change #{change}."),
        })
    }
}

pub(crate) fn valid_content(content: &str) -> bool {
    !content.is_empty()
        && content.len() <= MAX_CONTENT
        && !content
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\t')
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;
