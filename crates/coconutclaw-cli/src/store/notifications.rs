//! Core-derived blocker decisions. No model strings choose identities or routes.
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::json;

use super::Store;
use crate::delivery::{DeliveryTarget, serialize_delivery_target};
use crate::session::SessionKey;

struct Blocker {
    task: i64,
    run: i64,
    scope: String,
    actor: Option<String>,
    identity: String,
    reason: String,
    target: String,
}

type WaitingOwnerRow = (i64, String, i64, String, String, Option<String>, String);
type DependencyStateRow = (i64, String, Option<String>, bool, String, Option<String>);

impl Store {
    fn current_blocker(&self, run: i64) -> Result<Option<Blocker>> {
        let row: Option<WaitingOwnerRow> = self.conn.query_row(
            "SELECT a.task_id,w.kind,w.expected_id,r.session_id,r.channel,r.source_user_id,p.request
             FROM runtime_waits w JOIN task_runs r ON r.id=w.run_id
             JOIN task_payloads p ON p.task_run_id=r.id JOIN runtime_attempts a ON a.run_id=r.id
             JOIN runtime_tasks t ON t.task_id=a.task_id AND t.current_run_id=r.id
             WHERE r.id=?1 AND w.state='waiting' AND r.cancel_requested_at IS NULL
               AND r.status IN ('awaiting_approval','waiting_event') AND p.output IS NOT NULL
               AND w.session_id=r.session_id AND t.session_id=r.session_id
               AND w.run_prompt=r.prompt AND w.resume_payload=p.request
               AND w.actor IS r.source_user_id AND t.actor IS r.source_user_id AND t.channel=r.channel",
            [run], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
        let Some((task, kind, expected, scope, channel, actor, raw)) = row else {
            return Ok(None);
        };
        let Ok(request) = crate::approval::request_from_payload(&raw, false) else {
            return Ok(None);
        };
        if request.session.id() != scope
            || request.source_user_id != actor
            || request.source.channel_name() != channel
        {
            return Ok(None);
        }
        let native = self.get_task_run(run)?.expect("joined run");
        let thread: Option<String> = self.conn.query_row(
            "SELECT source_thread_id FROM task_runs WHERE id=?1",
            [run],
            |r| r.get(0),
        )?;
        match &request.delivery {
            DeliveryTarget::Telegram { chat_id, thread_id } => {
                if channel != "telegram"
                    || chat_id != &request.session.root_id
                    || thread_id != &request.session.thread_id
                    || native.source_chat_id.as_deref() != Some(chat_id)
                    || &thread != thread_id
                    || actor.as_deref().is_some_and(|a| a != chat_id)
                {
                    return Ok(None);
                }
            }
            DeliveryTarget::Stdout => {
                return Ok(None);
            }
        }
        if native.update_id != request.update_id {
            return Ok(None);
        }
        let (source, reason, detail) = if kind == "approval" {
            let status: Option<String> = self
                .conn
                .query_row(
                    "SELECT status FROM approvals WHERE id=?1 AND task_run_id=?2 AND session_id=?3",
                    params![expected, run, scope],
                    |r| r.get(0),
                )
                .optional()?;
            if status.as_deref() != Some("pending") {
                return Ok(None);
            }
            (
                run,
                format!("approval #{expected} pending"),
                json!({"approval":expected}),
            )
        } else {
            let dep: Option<DependencyStateRow>=self.conn.query_row(
                "SELECT r.id,r.status,r.error_summary,r.cancel_requested_at IS NOT NULL,r.session_id,r.source_user_id
                 FROM runtime_tasks t JOIN task_runs r ON r.id=t.current_run_id
                 WHERE t.task_id=?1 AND r.session_id=t.session_id AND r.source_user_id IS t.actor AND r.channel=t.channel",
                [expected],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
            let Some((source, status, error, cancel, dep_scope, dep_actor)) = dep else {
                return Ok(None);
            };
            let owner = SessionKey::from_id(scope.clone()).root_id;
            if dep_scope != scope
                || dep_actor.as_deref().unwrap_or(&owner) != actor.as_deref().unwrap_or(&owner)
            {
                return Ok(None);
            }
            let remote: bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM runtime_remote_results WHERE task_run_id=?1 AND (uncertain=1 OR state='unknown'))",[source],|r|r.get(0))?;
            let approval: Option<i64>=self.conn.query_row("SELECT id FROM approvals WHERE task_run_id=?1 AND status='pending' ORDER BY id LIMIT 1",[source],|r|r.get(0)).optional()?;
            let delivery: String=self.conn.query_row(
                "SELECT CASE WHEN NOT json_valid(delivery_state) THEN 'unknown'
                 WHEN EXISTS(SELECT 1 FROM json_tree(delivery_state) WHERE key='phase' AND value='unknown') THEN 'unknown'
                 WHEN delivered!=1 AND EXISTS(SELECT 1 FROM json_tree(delivery_state) WHERE key='phase' AND value='sending') THEN 'inflight'
                 ELSE 'settled-or-no-inflight-evidence' END FROM task_payloads WHERE task_run_id=?1",[source],|r|r.get(0))?;
            let state = if remote
                || error
                    .as_deref()
                    .is_some_and(|e| e.contains("outcome unknown"))
            {
                "outcome unknown"
            } else if cancel {
                "cancellation requested"
            } else if approval.is_some() {
                "approval pending"
            } else {
                match status.as_str() {
                    "queued" | "running" => "execution pending",
                    "waiting_remote" => "remote result pending",
                    "waiting_event" => "dependency wait",
                    "failed" => "failed",
                    "cancelled" => "cancelled",
                    "completed" if delivery == "unknown" => "delivery unknown",
                    "completed" if delivery == "inflight" => "delivery inflight",
                    "completed" => "completion evidence/coordination pending",
                    _ => "state unresolved",
                }
            };
            (
                source,
                format!("dependency task #{expected}, run #{source}: {state}"),
                json!({"state":state,"approval":approval}),
            )
        };
        Ok(Some(Blocker{task,run,scope,actor,identity:json!({"run":run,"kind":kind,"expected":expected,"source_run":source,"detail":detail}).to_string(),
            reason,target:serialize_delivery_target(&request.delivery)}))
    }

    fn record_blocker(&self, b: Blocker, initial: bool, now: &str) -> Result<bool> {
        ensure!(
            !self.conn.is_autocommit(),
            "blocker decision requires transaction"
        );
        let old: Option<(String, i64)> = self
            .conn
            .query_row(
                "SELECT identity,generation FROM runtime_task_blockers WHERE task_id=?1",
                [b.task],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if old.as_ref().is_some_and(|old| old.0 == b.identity) {
            return Ok(false);
        }
        let generation = old.as_ref().map_or(1, |old| old.1 + 1);
        let provenance = if initial {
            "initial-outcome"
        } else if old.is_none() {
            "legacy-baseline"
        } else {
            "changed-blocker"
        };
        let output = if provenance == "changed-blocker" {
            crate::markers::render_output(
                &format!(
                    "Task #{}, run #{} remains blocked: {}. No automatic retry.",
                    b.task, b.run, b.reason
                ),
                &[],
            )
        } else {
            self.conn.query_row(
                "SELECT output FROM task_payloads WHERE task_run_id=?1",
                [b.run],
                |r| r.get(0),
            )?
        };
        self.conn.execute("INSERT INTO runtime_task_blockers VALUES(?1,?2,?3)
            ON CONFLICT(task_id) DO UPDATE SET identity=excluded.identity,generation=excluded.generation",
            params![b.task,b.identity,generation])?;
        self.conn.execute("INSERT INTO runtime_notifications(task_id,run_id,generation,session_id,actor,identity,reason,target,output,provenance,created_at,disposition)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![b.task,b.run,generation,b.scope,b.actor,b.identity,b.reason,b.target,output,provenance,now,
                if provenance=="changed-blocker"{"pending"}else{"linked"}])?;
        Ok(true)
    }

    pub(crate) fn record_initial_blocker(&self, run: i64, now: &str) -> Result<()> {
        if let Some(blocker) = self.current_blocker(run)? {
            self.record_blocker(blocker, true, now)?;
        }
        Ok(())
    }

    pub(crate) fn observe_blockers(&self, now: &str) -> Result<usize> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let cursor = self
            .kv_get("blocker_observation_cursor")?
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        let rows=self.conn.prepare(
            "SELECT t.task_id,t.current_run_id FROM runtime_tasks t JOIN runtime_waits w ON w.run_id=t.current_run_id
             WHERE t.task_id>?1 AND w.state='waiting' ORDER BY t.task_id LIMIT 32")?
            .query_map([cursor],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut count = 0;
        for (_, run) in &rows {
            if let Some(blocker) = self.current_blocker(*run)? {
                count += usize::from(self.record_blocker(blocker, false, now)?);
            }
        }
        self.kv_set(
            "blocker_observation_cursor",
            &if rows.len() == 32 {
                rows.last().unwrap().0
            } else {
                0
            }
            .to_string(),
        )?;
        tx.commit()?;
        Ok(count)
    }

    pub(crate) fn notification_is_current(&self, id: i64) -> Result<bool> {
        let (run, identity, target): (i64, String, String) = self.conn.query_row(
            "SELECT run_id,identity,target FROM runtime_notifications WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        Ok(self
            .current_blocker(run)?
            .is_some_and(|b| b.identity == identity && b.target == target))
    }

    /// The sender's sending-phase commit is the external-effect authorization.
    /// Check current identity under the same writer gate, before HTTP. A later
    /// cancellation cannot undo an already authorized request; save its ack.
    pub(crate) fn persist_notification_delivery(
        &self,
        id: i64,
        raw: &str,
        claim: bool,
    ) -> Result<()> {
        let tx = self
            .conn
            .is_autocommit()
            .then(|| Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate))
            .transpose()?;
        if claim {
            ensure!(
                self.notification_is_current(id)?,
                "stale notification; no send authorized"
            );
        }
        ensure!(
            self.conn.execute(
                "UPDATE runtime_notifications SET delivery_state=?2 WHERE id=?1",
                params![id, raw]
            )? == 1,
            "notification owner absent"
        );
        if let Some(tx) = tx {
            tx.commit()?;
        }
        Ok(())
    }

    pub(crate) fn notifications_command(
        &self,
        session: &SessionKey,
        args: &str,
        update: Option<&str>,
    ) -> Result<String> {
        self.incoming_control(session, update)?;
        let run = if args.is_empty() {
            None
        } else {
            match args.parse::<i64>() {
                Ok(n) if n > 0 => Some(n),
                _ => return Ok("Use /notifications [positive run ID].".into()),
            }
        };
        let mut stmt = self.conn.prepare(
            "SELECT n.id,n.task_id,n.run_id,n.reason,n.provenance,n.disposition,
               p.output IS n.output,p.delivered,
               CASE WHEN NOT json_valid(p.delivery_state) THEN 1 ELSE
                 EXISTS(SELECT 1 FROM json_tree(p.delivery_state) WHERE key='phase' AND
                   (value='unknown' OR (value='sending' AND p.delivered!=1))) END
             FROM runtime_notifications n LEFT JOIN task_payloads p ON p.task_run_id=n.run_id
             WHERE n.session_id=?1 AND (n.actor IS NULL OR n.actor=?2)
               AND (?3 IS NULL OR n.task_id=(SELECT task_id FROM runtime_attempts WHERE run_id=?3))
             ORDER BY n.id DESC LIMIT 8",
        )?;
        let rows=stmt.query_map(params![session.id(),session.root_id,run],|r|{
            let disposition:String=r.get(5)?;
            let delivery=if disposition=="linked" {
                if !r.get::<_,bool>(6)? {json!({"state":"original projection changed; acknowledgment not inferred"})}
                else {json!({"state":"original-task-outbox","acknowledged":r.get::<_,bool>(7)?,"uncertain":r.get::<_,bool>(8)?})}
            } else {json!({"state":disposition})};
            Ok(json!({"id":r.get::<_,i64>(0)?,"task_id":r.get::<_,i64>(1)?,"run_id":r.get::<_,i64>(2)?,
                "reason":r.get::<_,String>(3)?,"provenance":r.get::<_,String>(4)?,"delivery":delivery}))
        })?.collect::<rusqlite::Result<Vec<_>>>()?;
        // Do not inject plan/output bodies into history responses or context.
        let mut bounded = Vec::new();
        for row in rows {
            bounded.push(row);
            if serde_json::to_string(&bounded)?.len() > 3072 {
                bounded.pop();
                break;
            }
        }
        Ok(serde_json::to_string(&bounded)?)
    }
}
