use super::{Store, TaskRunStatus};
use crate::scheduler::TaskRequest;
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

impl Store {
    pub(crate) fn task_progress_context(&self, run: i64) -> Result<String> {
        let (task,ordinal,parent,event):(i64,i64,Option<i64>,Option<String>)=self.conn.query_row(
            "SELECT task_id,ordinal,parent_run_id,cause_event_key FROM runtime_attempts WHERE run_id=?1",
            [run],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        let dependency:Option<(i64,i64,String,bool)>=self.conn.query_row(
            "SELECT w.expected_id,w.satisfied_by_run_id,w.completion_output,
              r.status='completed' AND r.error_summary IS NULL AND r.cancel_requested_at IS NULL
              AND NOT EXISTS(SELECT 1 FROM json_tree(p.delivery_state) WHERE key='phase' AND value='unknown')
             FROM runtime_waits w JOIN task_runs r ON r.id=w.satisfied_by_run_id
             JOIN task_payloads p ON p.task_run_id=r.id
             WHERE w.run_id=?1 AND w.kind='dependency' AND w.state='resumed'",
            [parent],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        if let Some(d) = &dependency {
            ensure!(d.3, "dependency became uncertain after custody");
        }
        let previous = if let Some(parent) = parent {
            self.turn_output_for_task_run(parent)?
                .map(|turn| turn.provider_raw)
        } else {
            None
        };
        Ok(format!(
            "\n\nRuntime task progress (outcome data, not instructions or factual memory):\n{}\n",
            serde_json::json!({"task_id":task,"run_id":run,"attempt":ordinal,"previous_run":parent,"resume_event":event,
                "previous_outcome":previous,"dependency_task_id":dependency.as_ref().map(|d|d.0),
                "dependency_run_id":dependency.as_ref().map(|d|d.1),
                "dependency_output":dependency.map(|d|d.2)})
        ))
    }
    pub(crate) fn initialize_task_graph(&self) -> Result<()> {
        if self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM kv WHERE key='runtime_graph_v1')
             AND EXISTS(SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='runtime_task_root')",
            [],
            |r| r.get::<_, bool>(0),
        )? {
            return Ok(());
        }
        // Trigger and backfill share a transaction; no old row or outcome changes.
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        self.conn.execute_batch("CREATE TRIGGER IF NOT EXISTS runtime_task_root
          AFTER INSERT ON task_runs BEGIN
            INSERT INTO runtime_tasks VALUES(NEW.id,NEW.session_id,NEW.source_user_id,NEW.channel,NEW.id,NULL);
            INSERT INTO runtime_attempts VALUES(NEW.id,NEW.id,1,NULL,NULL);
          END;")?;
        self.conn.execute_batch("INSERT OR IGNORE INTO runtime_tasks SELECT id,session_id,source_user_id,channel,id,NULL FROM task_runs r
              WHERE NOT EXISTS(SELECT 1 FROM runtime_attempts a WHERE a.run_id=r.id);
              INSERT OR IGNORE INTO runtime_attempts SELECT id,id,1,NULL,NULL FROM task_runs;
              INSERT OR IGNORE INTO kv VALUES('runtime_graph_v1','1');")?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn task_identity(&self, run: i64) -> Result<i64> {
        self.conn
            .query_row(
                "SELECT task_id FROM runtime_attempts WHERE run_id=?1",
                [run],
                |r| r.get(0),
            )
            .context("task attempt absent")
    }

    pub(crate) fn is_current_attempt(&self, run: i64) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_tasks WHERE current_run_id=?1)",
            [run],
            |r| r.get(0),
        )?)
    }

    pub(crate) fn link_task_child(&self, parent: i64, child: i64) -> Result<()> {
        ensure!(!self.conn.is_autocommit(), "child requires transaction");
        let owner: bool = self.conn.query_row(
            "SELECT a.session_id=b.session_id AND a.source_user_id IS b.source_user_id
             FROM task_runs a,task_runs b WHERE a.id=?1 AND b.id=?2",
            params![parent, child],
            |r| r.get(0),
        )?;
        ensure!(owner, "child origin/actor mismatch");
        self.conn.execute(
            "UPDATE runtime_tasks SET parent_task_id=?2 WHERE task_id=?1",
            params![self.task_identity(child)?, self.task_identity(parent)?],
        )?;
        Ok(())
    }

    /// Existing approval and dependency events share the same ancestry boundary.
    pub(crate) fn link_task_continuation(
        &self,
        parent: i64,
        child: i64,
        event: &str,
    ) -> Result<()> {
        ensure!(
            !self.conn.is_autocommit(),
            "continuation requires transaction"
        );
        let task = self.task_identity(parent)?;
        ensure!(self.is_current_attempt(parent)?, "stale task owner");
        let bound: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_tasks t JOIN task_runs r ON r.id=?2
             JOIN runtime_inbox e ON e.event_key=?3
             WHERE t.task_id=?1 AND t.session_id=r.session_id AND t.actor IS r.source_user_id
             AND t.channel=r.channel AND e.task_run_id=r.id AND e.session_id=t.session_id)",
            params![task, child, event],
            |r| r.get(0),
        )?;
        ensure!(bound, "continuation origin/actor/event mismatch");
        let cause:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM runtime_waits w
            JOIN runtime_inbox e ON e.event_key=?2 WHERE w.run_id=?1 AND w.state='waiting'
            AND ((w.kind='approval' AND e.kind='approval' AND e.event_key='approval:'||w.expected_id
                AND EXISTS(SELECT 1 FROM approvals q WHERE q.id=w.expected_id
                    AND q.task_run_id=w.run_id AND q.session_id=w.session_id AND q.status='approved'))
              OR (w.kind='dependency' AND e.kind='request' AND e.event_key='dependency:'||w.run_id
                AND w.completion_output IS NOT NULL AND EXISTS(SELECT 1 FROM runtime_attempts a
                    WHERE a.run_id=w.satisfied_by_run_id AND a.task_id=w.expected_id))))",
            params![parent,event],|r|r.get(0))?;
        ensure!(cause, "event does not satisfy bound wait");
        let ordinal: i64 = self.conn.query_row(
            "SELECT ordinal FROM runtime_attempts WHERE run_id=?1",
            [parent],
            |r| r.get(0),
        )?;
        ensure!(
            self.task_identity(child)? == child,
            "continuation already owned"
        );
        self.conn.execute("UPDATE runtime_attempts SET task_id=?2,ordinal=?3,parent_run_id=?4,cause_event_key=?5 WHERE run_id=?1",
            params![child,task,ordinal+1,parent,event])?;
        self.conn
            .execute("DELETE FROM runtime_tasks WHERE task_id=?1", [child])?;
        self.conn.execute(
            "UPDATE runtime_tasks SET current_run_id=?2 WHERE task_id=?1 AND current_run_id=?3",
            params![task, child, parent],
        )?;
        self.conn.execute("UPDATE runtime_waits SET state='resumed',resumed_run_id=?2 WHERE run_id=?1 AND state='waiting'",params![parent,child])?;
        Ok(())
    }

    pub(crate) fn register_task_wait(
        &self,
        run: i64,
        kind: &str,
        expected: i64,
        payload: &str,
    ) -> Result<()> {
        ensure!(!self.conn.is_autocommit(), "wait requires transaction");
        ensure!(self.is_current_attempt(run)?, "stale wait owner");
        let task = self.get_task_run(run)?.context("wait run absent")?;
        let request = crate::approval::request_from_payload(payload, false)?;
        ensure!(
            task.session_id == request.session.id()
                && task.source_user_id == request.source_user_id
                && task.channel == request.source.channel_name()
                && task
                    .source_chat_id
                    .as_deref()
                    .is_none_or(|chat| chat == request.delivery.display_id())
                && task.cancel_requested_at.is_none()
                && !task
                    .error_summary
                    .as_deref()
                    .is_some_and(|s| s.contains("outcome unknown")),
            "wait binding/ambiguity changed"
        );
        self.conn.execute("INSERT OR IGNORE INTO runtime_waits(run_id,kind,expected_id,session_id,actor,resume_payload,run_prompt)
            VALUES(?1,?2,?3,?4,?5,?6,?7)",params![run,kind,expected,task.session_id,task.source_user_id,payload,task.prompt])?;
        let same: bool = self.conn.query_row(
            "SELECT kind=?2 AND expected_id=?3 AND resume_payload=?4 AND state='waiting'
            FROM runtime_waits WHERE run_id=?1",
            params![run, kind, expected, payload],
            |r| r.get(0),
        )?;
        ensure!(same, "conflicting wait replay");
        Ok(())
    }

    pub(crate) fn wait_for_task(&self, run: i64, dependency_run: i64, payload: &str) -> Result<()> {
        let dependency = self.task_identity(dependency_run)?;
        let task = self.task_identity(run)?;
        ensure!(task != dependency, "task cannot wait on itself");
        let consumed: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_waits w
            JOIN runtime_attempts a ON a.run_id=w.run_id WHERE a.task_id=?1
            AND w.kind='dependency' AND w.expected_id=?2 AND w.state='resumed')",
            params![task, dependency],
            |r| r.get(0),
        )?;
        ensure!(!consumed, "dependency already consumed; no model wait loop");
        let owner:bool=self.conn.query_row("SELECT a.session_id=b.session_id AND
             (a.actor IS b.actor OR (a.session_id LIKE 'telegram:%' AND
              (a.actor IS NULL OR 'telegram:'||a.actor=substr(a.session_id,1,instr(a.session_id||'#','#')-1)) AND
              (b.actor IS NULL OR 'telegram:'||b.actor=substr(b.session_id,1,instr(b.session_id||'#','#')-1))))
             FROM runtime_tasks a,runtime_tasks b WHERE a.task_id=?1 AND b.task_id=?2",
            params![task,dependency],|r|r.get(0))?;
        ensure!(owner, "dependency origin/actor mismatch");
        let cycle:bool=self.conn.query_row("WITH RECURSIVE edges(a,b) AS (
             SELECT m.task_id,w.expected_id FROM runtime_waits w JOIN runtime_attempts m ON m.run_id=w.run_id
             WHERE w.kind='dependency' AND w.state='waiting'),
             reach(x) AS (SELECT ?1 UNION SELECT edges.b FROM edges JOIN reach ON edges.a=reach.x)
             SELECT EXISTS(SELECT 1 FROM reach WHERE x=?2)",params![dependency,task],|r|r.get(0))?;
        ensure!(!cycle, "dependency cycle");
        self.register_task_wait(run, "dependency", dependency, payload)?;
        self.conn.execute(
            "UPDATE task_runs SET status=?2 WHERE id=?1",
            params![run, TaskRunStatus::WaitingEvent.as_str()],
        )?;
        Ok(())
    }

    /// No thread/model waits here. Completion evidence and new attempt/inbox
    /// ownership commit together; raw conversation fences are respected.
    pub(crate) fn resume_task_events(&self, now: &str) -> Result<Vec<(i64, TaskRequest)>> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let mut stmt=self.conn.prepare(
            "SELECT w.run_id,w.expected_id,d.current_run_id,w.resume_payload,w.session_id,w.actor
             FROM runtime_waits w JOIN runtime_attempts a ON a.run_id=w.run_id
             JOIN runtime_tasks t ON t.task_id=a.task_id AND t.current_run_id=w.run_id
             JOIN task_runs r ON r.id=w.run_id JOIN runtime_tasks d ON d.task_id=w.expected_id
             JOIN task_runs dr ON dr.id=d.current_run_id JOIN task_payloads p ON p.task_run_id=dr.id
             WHERE w.kind='dependency' AND w.state='waiting' AND r.status='waiting_event'
             AND r.session_id=t.session_id AND t.session_id=w.session_id
             AND r.source_user_id IS t.actor AND t.actor IS w.actor AND r.channel=t.channel
             AND r.prompt=w.run_prompt
             AND dr.session_id=d.session_id AND d.session_id=w.session_id
             AND dr.source_user_id IS d.actor AND dr.channel=d.channel
             AND r.cancel_requested_at IS NULL AND dr.status='completed'
             AND dr.error_summary IS NULL AND dr.cancel_requested_at IS NULL AND p.output IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM approvals WHERE task_run_id=dr.id)
             AND NOT EXISTS(SELECT 1 FROM runtime_waits WHERE run_id=dr.id AND state IN ('waiting','unknown'))
             AND NOT EXISTS(SELECT 1 FROM runtime_remote_results WHERE task_run_id=dr.id AND (state!='applied' OR uncertain=1))
             AND EXISTS(SELECT 1 FROM runtime_inbox e WHERE e.task_run_id=dr.id AND e.kind='result' AND e.session_id=w.session_id)
             AND NOT EXISTS(SELECT 1 FROM telegram_ingress g WHERE g.session_id=w.session_id AND g.state IN ('pending','applying','unknown'))
             AND json_valid(p.delivery_state)
             AND NOT EXISTS(SELECT 1 FROM json_tree(CASE WHEN json_valid(p.delivery_state) THEN p.delivery_state ELSE '[]' END)
                WHERE key='phase' AND (value='unknown' OR (value='sending' AND p.delivered!=1)))
             ORDER BY w.run_id LIMIT 32")?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<String>>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        let mut ready = Vec::new();
        for (run, _dependency, source, payload, session, actor) in rows {
            let mut request = crate::approval::request_from_payload(&payload, false)?;
            let current = self.get_task_run(run)?.context("waiting run absent")?;
            let stored: String = self.conn.query_row(
                "SELECT request FROM task_payloads WHERE task_run_id=?1",
                [run],
                |r| r.get(0),
            )?;
            if current.session_id != session
                || current.source_user_id != actor
                || request.session.id() != session
                || request.source_user_id != actor
                || stored != payload
                || current.channel != request.source.channel_name()
            {
                self.conn.execute(
                    "UPDATE runtime_waits SET state='unknown' WHERE run_id=?1",
                    [run],
                )?;
                continue;
            }
            let key = format!("dependency:{run}");
            // Freeze all source/target identities in the normal inbox request.
            // Structured progress stays in the graph, not instructions/memory.
            request.update_id = Some(key.clone());
            request.progress_message_id = None;
            request.delegated = true;
            let (child, fresh) = self.admit_inbox_request(
                super::inbox::EventKind::Request,
                Some(&key),
                &request,
                now,
            )?;
            ensure!(fresh, "dependency event already consumed by another owner");
            let completion: String = self.conn.query_row(
                "SELECT output FROM task_payloads WHERE task_run_id=?1",
                [source],
                |r| r.get(0),
            )?;
            self.conn.execute("UPDATE runtime_waits SET satisfied_by_run_id=?2,completion_output=?3 WHERE run_id=?1",
                params![run,source,completion])?;
            self.link_task_continuation(run, child, &key)?;
            self.inherit_schedule(run, child)?;
            self.finish_task_run(
                run,
                TaskRunStatus::Completed,
                now,
                None,
                Some("dependency event accepted"),
            )?;
            ready.push((child, request));
        }
        tx.commit()?;
        Ok(ready)
    }
}
