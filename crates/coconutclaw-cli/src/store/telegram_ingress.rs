use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

use super::Store;
use crate::types::ProcessOutcome;

impl Store {
    /// The poll cursor acknowledges durable custody, not completion of effects.
    pub(crate) fn admit_telegram(
        &self,
        id: &str,
        payload: &str,
        session: Option<&str>,
        now: &str,
    ) -> Result<i64> {
        let tx = self
            .conn
            .is_autocommit()
            .then(|| Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate))
            .transpose()?;
        self.require_open_legacy_admission()?;
        let old: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT seq,payload FROM telegram_ingress WHERE update_id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let seq = if let Some((seq, old)) = old {
            ensure!(
                old == payload,
                "conflicting Telegram update identity; original preserved"
            );
            seq
        } else {
            let legacy:bool=self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM task_runs WHERE channel='telegram' AND update_id=?1)
                  OR EXISTS(SELECT 1 FROM turns WHERE channel='telegram' AND update_id=?1)
                  OR EXISTS(SELECT 1 FROM memory_command_receipts WHERE scope LIKE 'telegram:%' AND request_id=?1)",
                [id],|r|r.get(0))?;
            self.conn.execute(
                "INSERT INTO telegram_ingress(update_id,payload,session_id,created_at,state,ack_phase)
                 VALUES(?1,?2,?3,?4,?5,?6)",
                params![id,payload,session,now,if legacy{"unknown"}else{"pending"},if legacy{"unknown"}else{"ready"}])?;
            self.conn.last_insert_rowid()
        };
        let cursor = self
            .kv_get("last_update_id")?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        if id.parse::<u64>()? > cursor {
            self.kv_set("last_update_id", id)?;
        }
        if let Some(tx) = tx {
            tx.commit()?;
        }
        Ok(seq)
    }

    /// Caller owns the effect transaction for SQL-only controls. Runtime controls
    /// commit this claim before touching another connection/process or the wire.
    pub(crate) fn claim_telegram(&self, seq: i64) -> Result<bool> {
        Ok(self.conn.execute(
            "UPDATE telegram_ingress AS e SET state='applying' WHERE seq=?1 AND state='pending'
             AND NOT EXISTS(SELECT 1 FROM legacy_admission_freeze)
             AND (EXISTS(SELECT 1 FROM telegram_ui_observations o WHERE o.seq=e.seq)
             OR NOT EXISTS(SELECT 1 FROM telegram_ingress older WHERE older.session_id=e.session_id
                 AND older.seq<e.seq AND (older.state IN ('pending','applying')
                   OR (older.state='unknown' AND NOT EXISTS(
                     SELECT 1 FROM telegram_reconciliation_controls c WHERE c.seq=e.seq)))))",
            [seq],
        )? == 1)
    }

    pub(crate) fn finish_telegram(&self, seq: i64, outcome: &ProcessOutcome) -> Result<()> {
        ensure!(
            !self.conn.is_autocommit(),
            "Telegram outcome requires a transaction"
        );
        ensure!(
            outcome.cleanup_path.is_none(),
            "raw ingress cannot own task attachment cleanup"
        );
        let update_id: String = self.conn.query_row(
            "SELECT update_id FROM telegram_ingress WHERE seq=?1",
            [seq],
            |r| r.get(0),
        )?;
        ensure!(
            outcome.update_id.as_deref() == Some(update_id.as_str()),
            "outcome identity differs from raw admission"
        );
        ensure!(
            self.conn.execute(
                "UPDATE telegram_ingress SET state='done',output=?2,route=?3,progress_message_id=?4
             WHERE seq=?1 AND state='applying'",
                params![
                    seq,
                    outcome.output,
                    outcome.chat_id,
                    outcome.progress_message_id
                ]
            )? == 1,
            "Telegram claim lost; outcome not accepted"
        );
        Ok(())
    }

    pub(crate) fn telegram_outcome(&self, seq: i64) -> Result<ProcessOutcome> {
        Ok(self.conn.query_row(
            "SELECT update_id,route,output,progress_message_id FROM telegram_ingress WHERE seq=?1",
            [seq],
            |r| {
                Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(r.get(0)?),
                    chat_id: r.get(1)?,
                    output: r.get(2)?,
                    progress_message_id: r.get(3)?,
                    cleanup_path: None,
                })
            },
        )?)
    }

    pub(crate) fn pending_telegram(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT e.payload FROM telegram_ingress e WHERE e.state='pending' AND (
               EXISTS(SELECT 1 FROM telegram_ui_observations o WHERE o.seq=e.seq) OR NOT EXISTS(
               SELECT 1 FROM telegram_ingress older WHERE older.session_id=e.session_id
                 AND older.seq<e.seq AND (older.state IN ('pending','applying')
                   OR (older.state='unknown' AND NOT EXISTS(
                     SELECT 1 FROM telegram_reconciliation_controls c WHERE c.seq=e.seq)))))
             ORDER BY e.seq LIMIT 32",
        )?;
        Ok(stmt
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub(crate) fn blocked_telegram_sessions(&self) -> Result<std::collections::HashSet<String>> {
        let mut stmt=self.conn.prepare(
            "SELECT DISTINCT session_id FROM telegram_ingress WHERE session_id IS NOT NULL AND state IN ('pending','applying','unknown')")?;
        Ok(stmt
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub(crate) fn recover_telegram_claims(&self) -> Result<()> {
        // A task receipt proves normal-turn admission, not a control effect or
        // its acknowledgment. Unproven runtime claims remain Unknown.
        self.conn.execute(
            "UPDATE telegram_ingress SET state=CASE WHEN EXISTS(
               SELECT 1 FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id
               WHERE r.update_id=telegram_ingress.update_id AND r.channel='telegram'
                 AND r.session_id=telegram_ingress.session_id)
             THEN 'done' ELSE 'unknown' END WHERE state='applying'",
            [],
        )?;
        self.conn.execute(
            "UPDATE telegram_ingress SET progress_state='unknown' WHERE progress_state='sending'",
            [],
        )?;
        Ok(())
    }
}
