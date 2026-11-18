//! Explicit receipt reconciliation, not an operator-supplied outcome override.
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, params, types::ValueRef};
use serde_json::{Value, json};

use super::Store;
use crate::delivery::{DeliveryKey, DeliveryState, DeliveryTarget, persist_delivery_state};
use crate::session::SessionKey;

#[derive(Clone, Copy)]
enum Target {
    Task(i64),
    Ack(i64),
}

impl Target {
    fn parts(self) -> (&'static str, i64, DeliveryKey) {
        match self {
            Self::Task(id) => ("task", id, DeliveryKey::Task(id)),
            Self::Ack(id) => ("ack", id, DeliveryKey::Telegram(id)),
        }
    }
}

struct Evidence {
    snapshot: String,
    state: DeliveryState,
}

impl Store {
    // Exact rows, including NULLs and signed blobs, not a lossy status hash.
    fn reconciliation_rows(&self, sql: &str, id: i64) -> Result<Value> {
        let mut stmt = self.conn.prepare(sql)?;
        let width = stmt.column_count();
        let rows = stmt
            .query_map([id], |row| {
                (0..width)
                    .map(|i| {
                        Ok(match row.get_ref(i)? {
                            ValueRef::Null => Value::Null,
                            ValueRef::Integer(v) => json!(v),
                            ValueRef::Real(v) => json!(v),
                            ValueRef::Text(v) => json!(String::from_utf8_lossy(v)),
                            ValueRef::Blob(v) => json!(v),
                        })
                    })
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(json!(rows))
    }

    fn reconciliation_evidence(&self, session: &SessionKey, target: Target) -> Result<Evidence> {
        let (_, id, key) = target.parts();
        let (snapshot, output, raw) = match target {
            Target::Ack(_) => {
                let (owner, payload, output, route, state, ack, raw): (String, String, Option<String>, Option<String>, String, String, String) =
                    self.conn.query_row("SELECT session_id,payload,output,route,state,ack_phase,delivery_state FROM telegram_ingress WHERE seq=?1",
                    [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
                ensure!(owner == session.id(), "wrong conversation/topic authority");
                let payload: Value = serde_json::from_str(&payload)?;
                let message = &payload["message"];
                ensure!(
                    SessionKey::from_telegram_message(message)? == *session
                        && message
                            .get("from")
                            .and_then(|v| v.get("id"))
                            .is_none_or(|v| v.to_string().trim_matches('"') == session.root_id)
                        && route.as_deref() == Some(session.telegram_route_id().as_str()),
                    "original actor/route binding differs"
                );
                ensure!(
                    state == "done" && ack != "sent",
                    "Unknown retained: control effect unresolved or acknowledgment already terminal"
                );
                (
                    self.reconciliation_rows("SELECT * FROM telegram_ingress WHERE seq=?1", id)?,
                    output.context("Unknown retained: no committed control outcome")?,
                    raw,
                )
            }
            Target::Task(_) => {
                let run = self.get_task_run(id)?.context("task absent")?;
                ensure!(
                    run.session_id == session.id()
                        && run.channel == "telegram"
                        && run.source_chat_id.as_deref() == Some(session.root_id.as_str())
                        && run
                            .source_user_id
                            .as_deref()
                            .is_none_or(|a| a == session.root_id),
                    "wrong original actor/conversation authority"
                );
                ensure!(
                    matches!(run.status.as_str(), "completed" | "failed" | "cancelled"),
                    "Unknown retained: run is not terminal; approval/wait/cancellation ownership is unchanged"
                );
                let (request, output, raw): (String, Option<String>, String) = self.conn.query_row(
                    "SELECT request,output,delivery_state FROM task_payloads WHERE task_run_id=?1",
                    [id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
                let request = crate::approval::request_from_payload(&request, false)?;
                let thread: Option<String> = self.conn.query_row(
                    "SELECT source_thread_id FROM task_runs WHERE id=?1",
                    [id],
                    |r| r.get(0),
                )?;
                let head: i64 = self.conn.query_row(
                    "SELECT t.current_run_id FROM runtime_tasks t JOIN runtime_attempts a ON a.task_id=t.task_id WHERE a.run_id=?1",
                    [id], |r|r.get(0))?;
                ensure!(head == id, "stale physical run: logical task head changed");
                ensure!(
                    request.session == *session
                        && thread == session.thread_id
                        && request.source.channel_name() == run.channel
                        && request.source_user_id == run.source_user_id
                        && request.update_id == run.update_id
                        && request.input.user_text == run.prompt
                        && request.delivery
                            == (DeliveryTarget::Telegram {
                                chat_id: session.root_id.clone(),
                                thread_id: session.thread_id.clone()
                            }),
                    "original run/request/route binding differs"
                );
                let snapshot = json!({
                    "run":self.reconciliation_rows("SELECT * FROM task_runs WHERE id=?1",id)?,
                    "payload":self.reconciliation_rows("SELECT * FROM task_payloads WHERE task_run_id=?1",id)?,
                    "graph":self.reconciliation_rows("SELECT t.*,a.* FROM runtime_tasks t JOIN runtime_attempts a ON a.task_id=t.task_id WHERE a.run_id=?1",id)?,
                    "wait":self.reconciliation_rows("SELECT * FROM runtime_waits WHERE run_id=?1",id)?,
                    "approvals":self.reconciliation_rows("SELECT * FROM approvals WHERE task_run_id=?1 ORDER BY id",id)?,
                    "inbox":self.reconciliation_rows("SELECT * FROM runtime_inbox WHERE task_run_id=?1 ORDER BY event_key",id)?,
                    "turns":self.reconciliation_rows("SELECT * FROM turns WHERE task_run_id=?1 ORDER BY id",id)?,
                    "remote":self.reconciliation_rows("SELECT * FROM runtime_remote_runs WHERE task_run_id=?1",id)?,
                    "result":self.reconciliation_rows("SELECT * FROM runtime_remote_results WHERE task_run_id=?1",id)?
                });
                (
                    snapshot,
                    output.context("Unknown retained: no committed run outcome")?,
                    raw,
                )
            }
        };
        let mut state = DeliveryState::from_json(&raw)?;
        crate::delivery::telegram::acknowledged_text(self, key, &output, &mut state)?;
        Ok(Evidence {
            snapshot: snapshot.to_string(),
            state,
        })
    }

    /// Called only inside the existing SQL-control transaction. A refusal rolls
    /// back all partial work, then gets its own ordinary command outcome/outbox.
    pub(crate) fn reconciliation_command(
        &self,
        session: &SessionKey,
        args: &str,
        update: Option<&str>,
        now: &str,
    ) -> Result<String> {
        ensure!(
            !self.conn.is_autocommit(),
            "reconciliation requires control transaction"
        );
        self.conn.execute_batch("SAVEPOINT reconcile")?;
        match self.reconciliation_control(session, args, update, now) {
            Ok(reply) => {
                self.conn.execute_batch("RELEASE reconcile")?;
                Ok(reply)
            }
            Err(error) => {
                self.conn
                    .execute_batch("ROLLBACK TO reconcile; RELEASE reconcile")?;
                Ok(format!(
                    "Could not confirm this delivery receipt: {error}.\nCheck the conversation and inspect the current receipt again with /reconcile task RUN_ID or /reconcile ack INGRESS_ID. Unconfirmed outcomes stay Unknown. No retry authorized."
                ))
            }
        }
    }

    fn reconciliation_control(
        &self,
        session: &SessionKey,
        args: &str,
        update: Option<&str>,
        now: &str,
    ) -> Result<String> {
        let parts: Vec<_> = args.split_whitespace().collect();
        ensure!(
            (2..=3).contains(&parts.len()),
            "use /reconcile task|ack <positive ID> [token]"
        );
        let id: i64 = parts[1].parse()?;
        ensure!(id > 0, "target ID must be positive");
        let target = match parts[0] {
            "task" => Target::Task(id),
            "ack" => Target::Ack(id),
            _ => anyhow::bail!(
                "use task or ack; provider/control/progress Unknown has no sufficient trusted nonoccurrence evidence"
            ),
        };
        let (kind, _, key) = target.parts();
        let update = update.context("authenticated raw command identity required")?;
        let cause: i64 = self.conn.query_row(
            "SELECT e.seq FROM telegram_ingress e JOIN telegram_reconciliation_controls c ON c.seq=e.seq
             WHERE e.update_id=?1 AND e.session_id=?2 AND e.state='applying'",
            params![update,session.id()], |r|r.get(0))?;
        if let Some(token) = parts.get(2) {
            let token = token.strip_prefix("token=").unwrap_or(token);
            let (owner, actor, old_kind, old_id, old): (String,String,String,i64,String) =
                self.conn.query_row("SELECT session_id,actor,target_kind,target_id,snapshot FROM runtime_reconciliation_offers WHERE token=?1",
                    [token],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
            ensure!(
                owner == session.id()
                    && actor == session.root_id
                    && old_kind == kind
                    && old_id == id,
                "wrong authority or target identity"
            );
            let applied: Option<i64> = self
                .conn
                .query_row(
                    "SELECT cause_seq FROM runtime_reconciliation_audit WHERE token=?1",
                    [token],
                    |r| r.get(0),
                )
                .optional()?;
            if applied.is_some() {
                return Ok(
                    "Already reconciled. The delivery receipt is recorded; nothing was sent or run again.".into(),
                );
            }
            let evidence = self.reconciliation_evidence(session, target)?;
            ensure!(
                evidence.snapshot == old,
                "stale or conflicting evidence; inspect the exact current state again"
            );
            persist_delivery_state(self, key, &evidence.state)?;
            match target {
                Target::Ack(_) => {
                    self.conn.execute(
                        "UPDATE telegram_ingress SET ack_phase='sent' WHERE seq=?1",
                        [id],
                    )?;
                }
                Target::Task(_) => {
                    self.conn.execute(
                        "UPDATE task_payloads SET delivered=1 WHERE task_run_id=?1",
                        [id],
                    )?;
                }
            }
            self.conn.execute(
                "INSERT INTO runtime_reconciliation_audit VALUES(?1,?2,'durable-text-ack',?3)",
                params![token, cause, now],
            )?;
            Ok("Reconciled: text delivery receipt recorded. Task, command and progress outcomes are unchanged. Nothing was sent or run again.".into())
        } else {
            let evidence = self.reconciliation_evidence(session, target)?;
            let token: String =
                self.conn
                    .query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
            self.conn.execute(
                "INSERT INTO runtime_reconciliation_offers VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    token,
                    session.id(),
                    session.root_id,
                    kind,
                    id,
                    evidence.snapshot,
                    now
                ],
            )?;
            Ok(format!(
                "Durable acknowledgment found: this text already has a delivery receipt.\nRecord it with: /reconcile {kind} {id} token={token}\nThis only records delivery. It does not confirm uncertain task, command or progress outcomes, and does not send or run anything again."
            ))
        }
    }
}
