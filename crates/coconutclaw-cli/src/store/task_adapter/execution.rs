//! Deliberate mock dispatch only. No provider, scheduler or operational admission.
use super::proposals::MockSigner;
use super::*;
use coconutclaw_sync::synthetic::membership::domain::Outcome;
use iroh::Signature;

pub(super) const CANCEL: &str = "CoconutClaw mock cancel v1:";

/// Not serializable or cloneable: reopening a Store cannot restore permission
/// to invoke a mock. A persisted start without this token recovers Unknown.
pub(super) struct MockDispatch {
    assignment: u64,
    run: i64,
    start: Vec<u8>,
}

/// Durable invocation claim, not publication permission or a new attempt.
pub(super) struct ClaimedMock {
    assignment: u64,
    run: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cancellation {
    origin: String,
    assignment: u64,
    assignment_hash: String,
    source_run: i64,
    runner: EndpointId,
    attempt: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignedCancellation {
    command: Cancellation,
    signature: Signature,
}

fn cancel_bytes(command: &Cancellation) -> Result<Vec<u8>> {
    let mut bytes = CANCEL.as_bytes().to_vec();
    bytes.extend(serde_json::to_vec(command)?);
    Ok(bytes)
}

pub(super) fn verify_cancellation(
    origin: &BoundOrigin,
    history: &History,
    text: &str,
    assignment: u64,
) -> Result<bool> {
    let text = text
        .strip_prefix(CANCEL)
        .context("not signed cancellation")?;
    let signed: SignedCancellation = serde_json::from_str(text)?;
    ensure!(
        serde_json::to_string(&signed)? == text,
        "noncanonical cancellation"
    );
    origin
        .origin
        .core
        .verify(&cancel_bytes(&signed.command)?, &signed.signature)?;
    let c = &signed.command;
    ensure!(c.origin == origin.hash()?, "foreign cancellation origin");
    if c.assignment != assignment {
        return Ok(false);
    }
    let bytes = history
        .export()
        .into_iter()
        .find(|b| SignedEvent::parse(b).is_ok_and(|s| s.event.sequence == assignment))
        .context("cancel assignment absent")?;
    let Action::Assign { request, .. } = SignedEvent::parse(&bytes)?.event.action else {
        anyhow::bail!("not assignment")
    };
    let delegation: Delegation = serde_json::from_str(&request)?;
    let job = history.jobs.get(&assignment).context("cancel job absent")?;
    ensure!(
        c.assignment_hash == blake3::hash(&bytes).to_string()
            && c.source_run == delegation.run
            && c.runner == job.runner
            && c.attempt == 1,
        "cancellation assignment mismatch"
    );
    Ok(true)
}

fn terminal(
    outcome: Outcome,
    cancelled: bool,
    already_unknown: bool,
) -> (TaskRunStatus, Option<&'static str>) {
    if outcome == Outcome::Unknown
        || already_unknown
        || (cancelled && outcome != Outcome::Cancelled)
    {
        (
            TaskRunStatus::Failed,
            Some(
                "mock outcome unknown; preserve cancellation/ambiguity; do not automatically retry",
            ),
        )
    } else {
        (
            match outcome {
                Outcome::Completed => TaskRunStatus::Completed,
                Outcome::Failed => TaskRunStatus::Failed,
                Outcome::Cancelled => TaskRunStatus::Cancelled,
                Outcome::Unknown => unreachable!(),
            },
            None,
        )
    }
}

impl TaskAdapter<'_> {
    pub(super) fn assigned(
        &self,
        peer: &mut ScopedPeer,
        assignment: u64,
    ) -> Result<(History, Vec<u8>, Delegation)> {
        let history = self.history(peer)?;
        let bytes = history
            .export()
            .into_iter()
            .find(|b| SignedEvent::parse(b).is_ok_and(|s| s.event.sequence == assignment))
            .context("assignment absent")?;
        let signed = SignedEvent::parse(&bytes)?;
        let Action::Assign {
            request,
            attempt: 1,
            ..
        } = &signed.event.action
        else {
            anyhow::bail!("not an attempt-1 assignment")
        };
        let delegation: Delegation = serde_json::from_str(request)?;
        ensure!(
            serde_json::to_string(&delegation)? == *request,
            "noncanonical assignment request"
        );
        let request = self.request(&delegation.payload)?;
        ensure!(
            delegation.run > delegation.parent
                && delegation.parent > 0
                && request.update_id
                    == Some(format!(
                        "delegation:{}:{}",
                        delegation.parent, delegation.index
                    )),
            "source identity mismatch"
        );
        Ok((history, bytes, delegation))
    }

    pub(super) fn incoming_run(
        &self,
        assignment: u64,
        bytes: &[u8],
        delegation: &Delegation,
    ) -> Result<i64> {
        let (id, event, source): (i64, Vec<u8>, i64) = self.store.conn.query_row(
            "SELECT run,event,source_run FROM adapter_incoming WHERE assignment=?1",
            [assignment],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        ensure!(
            event == bytes && source == delegation.run,
            "incoming binding conflict"
        );
        let mut request = self.request(&delegation.payload)?;
        request.session =
            SessionKey::local(&format!("sync:{}/{}", self.origin.hash()?, assignment));
        request.update_id = Some(format!("sync:{}/{}", self.origin.hash()?, assignment));
        request.source = TaskSource::Local;
        request.source_user_id = None;
        request.delivery = DeliveryTarget::Stdout;
        request.input.channel = "local".into();
        self.validate_task(id, &request)?;
        Ok(id)
    }

    pub(super) fn source_run(&self, bytes: &[u8], delegation: &Delegation) -> Result<i64> {
        let saved: Vec<u8> = self.store.conn.query_row(
            "SELECT event FROM adapter_outgoing WHERE run=?1",
            [delegation.run],
            |r| r.get(0),
        )?;
        ensure!(saved == bytes, "returned assignment substitution");
        self.validate_task(delegation.run, &self.request(&delegation.payload)?)?;
        Ok(delegation.run)
    }

    fn validate_task(&self, id: i64, request: &TaskRequest) -> Result<()> {
        let run = self.store.get_task_run(id)?.context("task absent")?;
        let payload: String = self.store.conn.query_row(
            "SELECT request FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| r.get(0),
        )?;
        ensure!(
            run.session_id == request.session.id()
                && run.update_id == request.update_id
                && run.prompt == request.input.user_text
                && run.channel == request.source.channel_name()
                && payload == build_resume_payload(request, &request.input)?,
            "task request changed"
        );
        Ok(())
    }

    async fn signed_next(
        &self,
        history: &History,
        runner: &impl MockSigner,
        action: Action,
    ) -> Result<Vec<u8>> {
        let expected = history.next(self.origin, runner.public(), action.clone())?;
        let bytes = runner.sign(self.origin, history, action).await?;
        ensure!(
            SignedEvent::parse(&bytes)?.event == expected,
            "runner signed a different challenge"
        );
        let mut events = history.export();
        events.push(bytes.clone());
        History::replay(
            self.origin,
            self.origin.origin.owner,
            self.origin.origin.namespace,
            events,
        )?;
        Ok(bytes)
    }

    /// Cancellation is independently signed intent, not a linear domain event:
    /// it must not fork the runner's Start/Result chain when a result is delayed.
    pub(super) async fn request_cancel(
        &self,
        peer: &mut ScopedPeer,
        assignment: u64,
        core: &SecretKey,
    ) -> Result<Record> {
        let (history, bytes, delegation) = self.assigned(peer, assignment)?;
        ensure!(
            core.public() == self.origin.origin.core,
            "cancellation signer is not core"
        );
        let id = self.source_run(&bytes, &delegation)?;
        let job = &history.jobs[&assignment];
        let command = Cancellation {
            origin: self.origin.hash()?,
            assignment,
            assignment_hash: blake3::hash(&bytes).to_string(),
            source_run: id,
            runner: job.runner,
            attempt: job.attempt,
        };
        let signed = SignedCancellation {
            signature: core.sign(&cancel_bytes(&command)?),
            command,
        };
        let text = format!("{CANCEL}{}", serde_json::to_string(&signed)?);
        let tx = self.store.conn.unchecked_transaction()?;
        let prior: Option<String> = self
            .store
            .conn
            .query_row(
                "SELECT command FROM adapter_cancel WHERE assignment=?1",
                [assignment],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            ensure!(prior == text, "cancellation conflict");
        } else {
            let run = self.store.get_task_run(id)?.context("task absent")?;
            ensure!(
                run.finished_at.is_none(),
                "cannot newly cancel a settled local task"
            );
            self.store
                .mark_task_run_cancel_requested(id, &chrono::Utc::now().to_rfc3339())?;
            self.store.conn.execute(
                "INSERT INTO adapter_cancel VALUES(?1,?2)",
                params![assignment, text],
            )?;
        }
        tx.commit()?;
        peer.publish(&text, b"mock cancellation intent, never exclusion proof")
            .await
    }

    pub(super) fn apply_cancellation(&self, peer: &mut ScopedPeer, assignment: u64) -> Result<()> {
        // history() repairs/revalidates ALL receipts under current policy first.
        let (history, bytes, delegation) = self.assigned(peer, assignment)?;
        let id = self.incoming_run(assignment, &bytes, &delegation)?;
        let mut selected = None;
        for record in peer.received()? {
            if !record.text.starts_with(CANCEL) {
                continue;
            }
            if !verify_cancellation(self.origin, &history, &record.text, assignment)? {
                continue;
            }
            if let Some(previous) = &selected {
                ensure!(previous == &record.text, "conflicting cancellation");
            }
            selected = Some(record.text);
        }
        if let Some(text) = selected {
            let tx = self.store.conn.unchecked_transaction()?;
            let prior: Option<String> = self
                .store
                .conn
                .query_row(
                    "SELECT command FROM adapter_cancel WHERE assignment=?1",
                    [assignment],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(prior) = prior {
                ensure!(prior == text, "persisted cancellation conflict");
            } else {
                self.store
                    .mark_task_run_cancel_requested(id, &chrono::Utc::now().to_rfc3339())?;
                self.store.conn.execute(
                    "INSERT INTO adapter_cancel VALUES(?1,?2)",
                    params![assignment, text],
                )?;
            }
            tx.commit()?;
        }
        Ok(())
    }

    pub(super) async fn start_mock(
        &self,
        peer: &mut ScopedPeer,
        assignment: u64,
        runner: &impl MockSigner,
    ) -> Result<(MockDispatch, Record)> {
        let (history, bytes, delegation) = self.assigned(peer, assignment)?;
        ensure!(
            history.jobs[&assignment].runner == runner.public(),
            "wrong mock runner"
        );
        let id = self.incoming_run(assignment, &bytes, &delegation)?;
        self.apply_cancellation(peer, assignment)?;
        let start = self
            .signed_next(&history, runner, Action::Start { assignment })
            .await?;
        self.persist_start(assignment, id, &start)?;
        let record = peer.publish_domain(self.origin, &start).await?;
        Ok((
            MockDispatch {
                assignment,
                run: id,
                start,
            },
            record,
        ))
    }

    fn persist_start(&self, assignment: u64, id: i64, start: &[u8]) -> Result<()> {
        let tx = self.store.conn.unchecked_transaction()?;
        let run = self.store.get_task_run(id)?.context("task absent")?;
        ensure!(
            run.started_at.is_none()
                && run.finished_at.is_none()
                && matches!(run.status.as_str(), "waiting_remote" | "cancel_requested"),
            "already started/uncertain; recover without execution"
        );
        self.store.conn.execute(
            "INSERT INTO adapter_execution(assignment,start) VALUES(?1,?2)",
            params![assignment, start],
        )?;
        self.store
            .update_task_run_started(id, &chrono::Utc::now().to_rfc3339())?;
        if let Some(cancelled) = run.cancel_requested_at {
            self.store.mark_task_run_cancel_requested(id, &cancelled)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Recover durable remote responses without issuing invocation permission.
    pub(super) fn restore_start(
        &self,
        peer: &mut ScopedPeer,
        assignment: u64,
        start: &[u8],
    ) -> Result<()> {
        let (history, bytes, delegation) = self.assigned(peer, assignment)?;
        ensure!(
            history.export().contains(&start.to_vec())
                && matches!(SignedEvent::parse(start)?.event.action,Action::Start{assignment:a} if a==assignment),
            "remote start mismatch"
        );
        let id = self.incoming_run(assignment, &bytes, &delegation)?;
        let saved: Option<Vec<u8>> = self
            .store
            .conn
            .query_row(
                "SELECT start FROM adapter_execution WHERE assignment=?1",
                [assignment],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(saved) = saved {
            ensure!(saved == start, "remote start conflict");
        } else {
            self.persist_start(assignment, id, start)?;
        }
        Ok(())
    }

    pub(super) fn restore_result(
        &self,
        peer: &mut ScopedPeer,
        assignment: u64,
        result: &[u8],
    ) -> Result<()> {
        let (history, bytes, delegation) = self.assigned(peer, assignment)?;
        ensure!(
            history.export().contains(&result.to_vec())
                && matches!(SignedEvent::parse(result)?.event.action,Action::Result{assignment:a,..} if a==assignment),
            "remote result mismatch"
        );
        let id = self.incoming_run(assignment, &bytes, &delegation)?;
        let saved: Option<Vec<u8>> = self.store.conn.query_row(
            "SELECT result FROM adapter_execution WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        if let Some(saved) = saved {
            ensure!(saved == result, "remote result conflict");
        } else {
            self.save_mock_result(assignment, id, result)?;
        }
        Ok(())
    }

    pub(super) fn interrupted_return(
        &self,
        peer: &mut ScopedPeer,
        assignment: u64,
        source: bool,
    ) -> Result<()> {
        let (_, bytes, delegation) = self.assigned(peer, assignment)?;
        let id = if source {
            self.source_run(&bytes, &delegation)?
        } else {
            self.incoming_run(assignment, &bytes, &delegation)?
        };
        self.store.finish_task_run(
            id,
            TaskRunStatus::Failed,
            &chrono::Utc::now().to_rfc3339(),
            Some("mock outcome unknown; interrupted proposal acceptance; no automatic retry"),
            None,
        )?;
        Ok(())
    }

    fn save_mock_result(&self, assignment: u64, id: i64, bytes: &[u8]) -> Result<()> {
        let signed = SignedEvent::parse(bytes)?;
        let Action::Result {
            assignment: bound,
            outcome,
            output,
        } = signed.event.action
        else {
            anyhow::bail!("not a result")
        };
        ensure!(bound == assignment, "result assignment mismatch");
        let tx = self.store.conn.unchecked_transaction()?;
        let run = self.store.get_task_run(id)?.context("task absent")?;
        let (status, error) = terminal(
            outcome,
            run.cancel_requested_at.is_some(),
            run.error_summary
                .as_deref()
                .is_some_and(|s| s.contains("outcome unknown")),
        );
        self.store.finish_task_run(
            id,
            status,
            &chrono::Utc::now().to_rfc3339(),
            error,
            Some(&output),
        )?;
        self.store.conn.execute(
            "UPDATE task_payloads SET output=?2 WHERE task_run_id=?1",
            params![id, output],
        )?;
        ensure!(self.store.conn.execute(
            "UPDATE adapter_execution SET result=?2,claimed=1 WHERE assignment=?1 AND result IS NULL", params![assignment,bytes],
        )? == 1, "result already persisted");
        tx.commit()?;
        Ok(())
    }

    pub(super) fn claim_mock(
        &self,
        peer: &mut ScopedPeer,
        runner: &impl MockSigner,
        dispatch: MockDispatch,
    ) -> Result<ClaimedMock> {
        let (history, bytes, delegation) = self.assigned(peer, dispatch.assignment)?;
        ensure!(
            history.jobs[&dispatch.assignment].runner == runner.public(),
            "wrong mock runner"
        );
        ensure!(
            history.jobs[&dispatch.assignment].started
                && history.jobs[&dispatch.assignment].result.is_none(),
            "dispatch no longer uncompleted"
        );
        let id = self.incoming_run(dispatch.assignment, &bytes, &delegation)?;
        ensure!(id == dispatch.run, "dispatch task mismatch");
        self.apply_cancellation(peer, dispatch.assignment)?;
        let tx = self.store.conn.unchecked_transaction()?;
        let saved: Vec<u8> = self.store.conn.query_row(
            "SELECT start FROM adapter_execution WHERE assignment=?1",
            [dispatch.assignment],
            |r| r.get(0),
        )?;
        ensure!(
            saved == dispatch.start && history.export().contains(&saved),
            "start not durably published"
        );
        let run = self.store.get_task_run(id)?.context("task absent")?;
        ensure!(
            run.finished_at.is_none()
                && matches!(run.status.as_str(), "running" | "cancel_requested"),
            "started task became uncertain; recover without executing"
        );
        ensure!(self.store.conn.execute(
            "UPDATE adapter_execution SET claimed=1 WHERE assignment=?1 AND claimed=0 AND result IS NULL",
            [dispatch.assignment],
        )? == 1, "mock invocation already claimed");
        tx.commit()?;
        Ok(ClaimedMock {
            assignment: dispatch.assignment,
            run: id,
        })
    }

    /// Allocate the Result only when the coordinator accepts the outcome,
    /// against the current head, after any intervening lifecycle events.
    pub(super) async fn finish_claimed(
        &self,
        peer: &mut ScopedPeer,
        runner: &impl MockSigner,
        claim: ClaimedMock,
        outcome: Outcome,
        output: String,
    ) -> Result<Record> {
        let (history, bytes, delegation) = self.assigned(peer, claim.assignment)?;
        let job = &history.jobs[&claim.assignment];
        ensure!(
            job.runner == runner.public() && job.started && job.result.is_none(),
            "claim no longer uncompleted"
        );
        let id = self.incoming_run(claim.assignment, &bytes, &delegation)?;
        ensure!(id == claim.run, "claimed task mismatch");
        self.apply_cancellation(peer, claim.assignment)?;
        let (start, claimed, result): (Vec<u8>, bool, Option<Vec<u8>>) =
            self.store.conn.query_row(
                "SELECT start,claimed,result FROM adapter_execution WHERE assignment=?1",
                [claim.assignment],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
        ensure!(
            claimed && result.is_none() && history.export().contains(&start),
            "claim conflict"
        );
        let bytes = self
            .signed_next(
                &history,
                runner,
                Action::Result {
                    assignment: claim.assignment,
                    outcome,
                    output,
                },
            )
            .await?;
        self.save_mock_result(claim.assignment, id, &bytes)?;
        peer.publish_domain(self.origin, &bytes).await
    }

    pub(super) async fn complete_mock<F>(
        &self,
        peer: &mut ScopedPeer,
        runner: &SecretKey,
        dispatch: MockDispatch,
        executor: F,
    ) -> Result<Record>
    where
        F: FnOnce(&Store, i64) -> Result<(Outcome, String)>,
    {
        let claim = self.claim_mock(peer, runner, dispatch)?;
        let run = self.store.get_task_run(claim.run)?.context("task absent")?;
        let (outcome, output) = if run.cancel_requested_at.is_some() {
            (
                Outcome::Cancelled,
                "mock cancelled before executor invocation".into(),
            )
        } else {
            executor(self.store, claim.run).unwrap_or((
                Outcome::Unknown,
                "mock executor failed; outcome unknown".into(),
            ))
        };
        self.finish_claimed(peer, runner, claim, outcome, output)
            .await
    }

    /// Replays exact persisted bytes, or settles a started ambiguity as Unknown.
    /// Never invokes a mock, even when the execution claim had not been written.
    pub(super) async fn recover_result(
        &self,
        peer: &mut ScopedPeer,
        assignment: u64,
        runner: &impl MockSigner,
    ) -> Result<Record> {
        let (history, bytes, delegation) = self.assigned(peer, assignment)?;
        ensure!(
            history.jobs[&assignment].runner == runner.public(),
            "wrong recovery runner"
        );
        let id = self.incoming_run(assignment, &bytes, &delegation)?;
        let (start, result): (Vec<u8>, Option<Vec<u8>>) = self.store.conn.query_row(
            "SELECT start,result FROM adapter_execution WHERE assignment=?1",
            [assignment],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        peer.publish_domain(self.origin, &start).await?;
        let result = if let Some(result) = result {
            result
        } else {
            let history = self.history(peer)?;
            let (outcome, output) = runner.recovery_outcome(assignment).await?.unwrap_or((
                Outcome::Unknown,
                "mock interrupted after durable start; outcome unknown; no automatic retry".into(),
            ));
            let result = self
                .signed_next(
                    &history,
                    runner,
                    Action::Result {
                        assignment,
                        outcome,
                        output,
                    },
                )
                .await?;
            self.save_mock_result(assignment, id, &result)?;
            result
        };
        let saved = SignedEvent::parse(&result)?;
        ensure!(
            saved.event.actor == runner.public()
                && matches!(saved.event.action, Action::Result { assignment: a, .. } if a == assignment),
            "persisted result binding mismatch"
        );
        peer.publish_domain(self.origin, &result).await
    }

    pub(super) fn apply_start(&self, peer: &mut ScopedPeer, assignment: u64) -> Result<i64> {
        let (history, bytes, delegation) = self.assigned(peer, assignment)?;
        let id = self.source_run(&bytes, &delegation)?;
        let start = history
            .export()
            .into_iter()
            .find(|b| {
                SignedEvent::parse(b).is_ok_and(|s|
            matches!(s.event.action, Action::Start { assignment: a } if a == assignment)
        )
            })
            .context("returned start absent")?;
        let tx = self.store.conn.unchecked_transaction()?;
        let prior: Option<Vec<u8>> = self
            .store
            .conn
            .query_row(
                "SELECT start FROM adapter_returns WHERE assignment=?1",
                [assignment],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            ensure!(prior == start, "returned start conflict");
        } else {
            let run = self.store.get_task_run(id)?.context("source task absent")?;
            ensure!(
                run.started_at.is_none()
                    && run.finished_at.is_none()
                    && matches!(run.status.as_str(), "waiting_remote" | "cancel_requested"),
                "source task no longer reserved"
            );
            self.store
                .update_task_run_started(id, &chrono::Utc::now().to_rfc3339())?;
            if let Some(cancelled) = run.cancel_requested_at {
                self.store.mark_task_run_cancel_requested(id, &cancelled)?;
            }
            self.store.conn.execute(
                "INSERT INTO adapter_returns(assignment,start) VALUES(?1,?2)",
                params![assignment, start],
            )?;
        }
        tx.commit()?;
        Ok(id)
    }

    pub(super) fn apply_result(&self, peer: &mut ScopedPeer, assignment: u64) -> Result<i64> {
        let (history, bytes, delegation) = self.assigned(peer, assignment)?;
        let id = self.source_run(&bytes, &delegation)?;
        let result = history
            .export()
            .into_iter()
            .find(|b| {
                SignedEvent::parse(b).is_ok_and(|s|
            matches!(s.event.action, Action::Result { assignment: a, .. } if a == assignment)
        )
            })
            .context("returned result absent")?;
        let signed = SignedEvent::parse(&result)?;
        let Action::Result {
            outcome, output, ..
        } = signed.event.action
        else {
            unreachable!()
        };
        let start = history
            .export()
            .into_iter()
            .find(|b| {
                SignedEvent::parse(b).is_ok_and(
                    |s| matches!(s.event.action,Action::Start{assignment:a} if a==assignment),
                )
            })
            .context("returned start absent")?;
        let tx = rusqlite::Transaction::new_unchecked(
            &self.store.conn,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        self.store.conn.execute(
            "INSERT OR IGNORE INTO adapter_returns(assignment,start) VALUES(?1,?2)",
            params![assignment, start],
        )?;
        let prior: Option<Vec<u8>> = self.store.conn.query_row(
            "SELECT result FROM adapter_returns WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        if let Some(prior) = prior {
            ensure!(prior == result, "returned result conflict");
        }
        let saved: Vec<u8> = self.store.conn.query_row(
            "SELECT start FROM adapter_returns WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        ensure!(saved == start, "returned start conflict");
        let (status, error) = terminal(outcome, false, false);
        self.store.custody_runtime_result(
            id,
            &start,
            &result,
            (status, error.is_some()),
            &output,
            &chrono::Utc::now().to_rfc3339(),
        )?;
        self.store.conn.execute(
            "UPDATE adapter_returns SET result=?2 WHERE assignment=?1",
            params![assignment, result],
        )?;
        tx.commit()?;
        // Custody survives a projection cut. Normal poll/startup use this exact
        // production reducer too; no provider or second notification sender.
        self.store
            .apply_runtime_results(&chrono::Utc::now().to_rfc3339())?;
        Ok(id)
    }
}
