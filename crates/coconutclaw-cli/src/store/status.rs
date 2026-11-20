//! Conversation-scoped durable observations, not runner or network health.
use super::Store;
use crate::{scheduling::Timing, session::SessionKey};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Transaction, TransactionBehavior, params};

const LIST_LIMIT: i64 = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusSnapshot {
    pub(crate) observed_at: String,
    pub(crate) tasks: Vec<StatusTask>,
    pub(crate) task_count: i64,
    pub(crate) run_count: i64,
    pub(crate) schedules: Vec<StatusSchedule>,
    pub(crate) schedule_count: i64,
    pub(crate) transport: TransportObservation,
    pub(crate) runtime_health: &'static str,
    pub(crate) node_health: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusTask {
    pub(crate) task_id: i64,
    pub(crate) run_id: i64,
    pub(crate) attempt: i64,
    pub(crate) status: &'static str,
    pub(crate) created_at: Option<String>,
    pub(crate) started_at: Option<String>,
    pub(crate) finished_at: Option<String>,
    pub(crate) progress_recorded: bool,
    pub(crate) cancel_requested: bool,
    pub(crate) approval_id: Option<i64>,
    pub(crate) wait_kind: Option<&'static str>,
    pub(crate) attention: bool,
    pub(crate) outcome_unknown: bool,
    pub(crate) delivery: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusSchedule {
    pub(crate) id: i64,
    pub(crate) revision: i64,
    pub(crate) state: &'static str,
    pub(crate) next_due: Option<String>,
    pub(crate) timing: Option<ScheduleTiming>,
    pub(crate) blocked: bool,
    pub(crate) latest_run_id: Option<i64>,
    pub(crate) latest_status: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScheduleTiming {
    pub(crate) cadence: String,
    pub(crate) timezone: String,
    pub(crate) fold: &'static str,
    pub(crate) gap: &'static str,
    pub(crate) missed: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportObservation {
    pub(crate) pending: i64,
    pub(crate) applying: i64,
    pub(crate) unknown: i64,
    pub(crate) latest_received_at: Option<String>,
}

pub(crate) fn timestamp(raw: Option<String>) -> Option<String> {
    DateTime::parse_from_rfc3339(raw.as_deref()?)
        .ok()
        .map(|time| time.with_timezone(&Utc).to_rfc3339())
}

fn run_status(raw: &str) -> &'static str {
    match raw {
        "queued" => "queued",
        "running" => "running",
        "awaiting_approval" => "awaiting_approval",
        "waiting_remote" => "waiting_remote",
        "waiting_event" => "waiting_event",
        "cancel_requested" => "cancel_requested",
        "completed" => "completed",
        "failed" => "failed",
        "cancelled" => "cancelled",
        _ => "unknown",
    }
}

fn timing(raw: &str) -> Option<ScheduleTiming> {
    // Round-trip through the scheduler's validator, not untrusted JSON fields.
    let value: serde_json::Value =
        serde_json::from_str(&Timing::decode(raw).ok()?.encode()).ok()?;
    let cadence = if let Some(at) = value["at"].as_str() {
        format!("once {at}")
    } else if let Some(daily) = value["daily"].as_str() {
        format!("daily {daily}")
    } else {
        format!(
            "weekly {} {}",
            value["weekly"].as_str()?,
            value["time"].as_str()?
        )
    };
    Some(ScheduleTiming {
        cadence,
        timezone: value["timezone"].as_str()?.into(),
        fold: if value["fold"] == "second" {
            "second"
        } else {
            "first"
        },
        gap: if value["gap"] == "next-valid" {
            "next-valid"
        } else {
            "skip"
        },
        missed: if value["missed"] == "skip" {
            "skip"
        } else {
            "coalesce"
        },
    })
}

// Match logical ownership as well as run scope, even for inconsistent old rows.
const TASK_OWNER: &str = "t.session_id=?1 AND r.session_id=t.session_id
    AND t.actor IS r.source_user_id AND t.channel=r.channel
    AND a.task_id=t.task_id";

impl Store {
    /// A bounded read-only snapshot. Authentication remains the transport's job.
    /// Stored progress is evidence only; its free text may contain private input.
    pub(crate) fn status_snapshot(
        &self,
        session: &SessionKey,
        now: &str,
    ) -> Result<StatusSnapshot> {
        let observed_at = timestamp(Some(now.into())).context("invalid snapshot timestamp")?;
        // Reuse caller custody transactions, otherwise pin all reads ourselves.
        let tx = if self.conn.is_autocommit() {
            Some(Transaction::new_unchecked(
                &self.conn,
                TransactionBehavior::Deferred,
            )?)
        } else {
            None
        };
        let scope = session.id();
        let task_count = self.conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM runtime_tasks t JOIN task_runs r ON r.id=t.current_run_id
                 JOIN runtime_attempts a ON a.run_id=r.id WHERE {TASK_OWNER}"
            ),
            [&scope],
            |r| r.get(0),
        )?;
        let run_count = self.conn.query_row(
            "SELECT COUNT(*) FROM task_runs WHERE session_id=?1",
            [&scope],
            |r| r.get(0),
        )?;
        let tasks = self.status_tasks(&scope)?;
        let schedule_count = self.conn.query_row(
            "SELECT COUNT(*) FROM runtime_schedules WHERE session_id=?1",
            [&scope],
            |r| r.get(0),
        )?;
        let schedules = self.status_schedules(&scope)?;
        let transport = self.conn.query_row(
            "SELECT
                COUNT(*) FILTER(WHERE state='pending'),
                COUNT(*) FILTER(WHERE state='applying'),
                COUNT(*) FILTER(WHERE state='unknown' OR progress_state='unknown'
                    OR ack_phase='unknown' OR NOT json_valid(delivery_state)
                    OR CASE WHEN json_valid(delivery_state) THEN EXISTS(
                        SELECT 1 FROM json_tree(delivery_state)
                        WHERE key='phase' AND value IN ('sending','unknown')) ELSE 0 END),
                (SELECT created_at FROM telegram_ingress WHERE session_id=?1 ORDER BY seq DESC LIMIT 1)
             FROM telegram_ingress WHERE session_id=?1",
            [&scope],
            |r| {
                Ok(TransportObservation {
                    pending: r.get(0)?,
                    applying: r.get(1)?,
                    unknown: r.get(2)?,
                    latest_received_at: timestamp(r.get(3)?),
                })
            },
        )?;
        if let Some(tx) = tx {
            tx.commit()?;
        }
        Ok(StatusSnapshot {
            observed_at,
            tasks,
            task_count,
            run_count,
            schedules,
            schedule_count,
            transport,
            runtime_health: "unavailable",
            node_health: "unavailable",
        })
    }

    fn status_tasks(&self, scope: &str) -> Result<Vec<StatusTask>> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT t.task_id,r.id,a.ordinal,r.status,r.created_at,r.started_at,r.finished_at,
                r.last_progress IS NOT NULL AND length(r.last_progress)>0,
                r.cancel_requested_at IS NOT NULL,
                (SELECT q.id FROM approvals q WHERE q.task_run_id=r.id
                    AND q.session_id=t.session_id AND q.status='pending'
                    AND q.source_user_id IS t.actor ORDER BY q.id DESC LIMIT 1),
                (SELECT w.kind FROM runtime_waits w WHERE w.run_id=r.id
                    AND w.session_id=t.session_id AND w.actor IS t.actor AND w.state='waiting'),
                instr(COALESCE(r.error_summary,''),'outcome unknown')>0 OR EXISTS(
                    SELECT 1 FROM runtime_remote_results rr WHERE rr.task_run_id=r.id
                    AND (rr.uncertain=1 OR rr.state='unknown')) AS outcome_unknown,
                CASE WHEN p.task_run_id IS NULL THEN 'unavailable'
                    WHEN NOT json_valid(p.delivery_state) THEN 'unknown'
                    WHEN EXISTS(SELECT 1 FROM json_tree(p.delivery_state)
                        WHERE key='phase' AND value='unknown') THEN 'unknown'
                    WHEN p.delivered=1 THEN 'delivered'
                    WHEN EXISTS(SELECT 1 FROM json_tree(p.delivery_state)
                        WHERE key='phase' AND value='sending') THEN 'inflight'
                    ELSE 'pending' END AS observed_delivery
             FROM runtime_tasks t JOIN task_runs r ON r.id=t.current_run_id
             JOIN runtime_attempts a ON a.run_id=r.id
             LEFT JOIN task_payloads p ON p.task_run_id=r.id WHERE {TASK_OWNER}
             ORDER BY CASE WHEN r.status NOT IN ('completed','failed','cancelled') THEN 0
                WHEN r.status='failed' OR outcome_unknown
                    OR observed_delivery IN ('unknown','inflight') THEN 1
                ELSE 2 END,r.id DESC LIMIT ?2"
        ))?;
        Ok(stmt
            .query_map(params![scope, LIST_LIMIT], |r| {
                let status = run_status(&r.get::<_, String>(3)?);
                let cancel_requested = r.get(8)?;
                let approval_id = r.get(9)?;
                let wait_kind = r
                    .get::<_, Option<String>>(10)?
                    .map(|raw| match raw.as_str() {
                        "approval" => "approval",
                        "dependency" => "dependency",
                        _ => "unknown",
                    });
                let outcome_unknown = r.get(11)?;
                let delivery = match r.get::<_, String>(12)?.as_str() {
                    "unknown" => "unknown",
                    "delivered" => "delivered",
                    "inflight" => "inflight",
                    "pending" => "pending",
                    _ => "unavailable",
                };
                Ok(StatusTask {
                    task_id: r.get(0)?,
                    run_id: r.get(1)?,
                    attempt: r.get(2)?,
                    status,
                    created_at: timestamp(r.get(4)?),
                    started_at: timestamp(r.get(5)?),
                    finished_at: timestamp(r.get(6)?),
                    progress_recorded: r.get(7)?,
                    cancel_requested,
                    approval_id,
                    wait_kind,
                    attention: outcome_unknown
                        || cancel_requested
                        || approval_id.is_some()
                        || wait_kind.is_some()
                        || matches!(
                            status,
                            "failed"
                                | "unknown"
                                | "awaiting_approval"
                                | "waiting_event"
                                | "waiting_remote"
                        )
                        || matches!(delivery, "unknown" | "inflight"),
                    outcome_unknown,
                    delivery,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn status_schedules(&self, scope: &str) -> Result<Vec<StatusSchedule>> {
        let mut stmt = self.conn.prepare(
            "SELECT s.id,s.revision,s.state,s.next_due,s.timing,
                EXISTS(SELECT 1 FROM runtime_schedule_runs link
                    JOIN task_runs r ON r.id=link.task_run_id
                    WHERE link.schedule_id=s.id AND r.session_id=s.session_id AND (
                        r.status NOT IN ('completed','cancelled') OR
                        (r.status='cancelled' AND r.started_at IS NOT NULL) OR EXISTS(
                            SELECT 1 FROM task_payloads p
                            WHERE p.task_run_id=r.id AND (
                                NOT json_valid(p.delivery_state) OR
                                CASE WHEN json_valid(p.delivery_state) THEN EXISTS(
                                    SELECT 1 FROM json_tree(p.delivery_state)
                                    WHERE key='phase' AND value IN ('sending','unknown'))
                                ELSE 0 END)))),
                (SELECT r.id FROM runtime_schedule_runs link
                    JOIN task_runs r ON r.id=link.task_run_id
                    WHERE link.schedule_id=s.id AND r.session_id=s.session_id
                    ORDER BY r.id DESC LIMIT 1),
                (SELECT r.status FROM runtime_schedule_runs link
                    JOIN task_runs r ON r.id=link.task_run_id
                    WHERE link.schedule_id=s.id AND r.session_id=s.session_id
                    ORDER BY r.id DESC LIMIT 1)
             FROM runtime_schedules s WHERE s.session_id=?1
             ORDER BY CASE s.state WHEN 'active' THEN 0 WHEN 'paused' THEN 1 ELSE 2 END,
                s.next_due IS NULL,s.next_due,s.id LIMIT ?2",
        )?;
        Ok(stmt
            .query_map(params![scope, LIST_LIMIT], |r| {
                Ok(StatusSchedule {
                    id: r.get(0)?,
                    revision: r.get(1)?,
                    state: match r.get::<_, String>(2)?.as_str() {
                        "active" => "active",
                        "paused" => "paused",
                        "cancelled" => "cancelled",
                        "done" => "done",
                        _ => "unknown",
                    },
                    next_due: r
                        .get::<_, Option<i64>>(3)?
                        .and_then(|due| DateTime::from_timestamp(due, 0))
                        .map(|due| due.to_rfc3339()),
                    timing: timing(&r.get::<_, String>(4)?),
                    blocked: r.get(5)?,
                    latest_run_id: r.get(6)?,
                    latest_status: r.get::<_, Option<String>>(7)?.map(|raw| run_status(&raw)),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

#[cfg(test)]
#[path = "status_tests.rs"]
mod tests;
