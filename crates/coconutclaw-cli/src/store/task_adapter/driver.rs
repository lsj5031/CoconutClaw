//! One owned publisher for one disposable origin. Async mocks never own Stores,
//! peers or keys. This is not a distributed queue or live runner admission.
use super::execution::ClaimedMock;
use super::proposals::{MockSigner, PROPOSAL_RECORD, RunnerClient, SignedProposal};
use super::*;
use coconutclaw_sync::synthetic::membership::domain::Outcome;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

const MAX_TASKS: usize = 4;
const MAX_RUNNING: usize = 2;
#[cfg(unix)]
mod multihost;

#[derive(Debug)]
struct Completion {
    bytes: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct OutcomeSender {
    assignment: u64,
    channel: mpsc::Sender<Completion>,
    runner: RunnerClient,
}

impl OutcomeSender {
    pub(super) async fn finish(&self, outcome: Outcome, output: &str) -> Result<()> {
        let bytes = self
            .runner
            .report(self.assignment, outcome, output.into())
            .await?;
        self.channel
            .send(Completion { bytes })
            .await
            .map_err(|_| anyhow::anyhow!("mock driver stopped"))
    }
}

pub(super) struct MockWork {
    pub prompt: String,
    pub cancellation: watch::Receiver<bool>,
    pub result: OutcomeSender,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Step {
    Applied(u64),
    Duplicate(u64),
    Stopped(u64),
}

#[cfg(target_os = "linux")]
pub(super) struct ProcessRelease {
    release: Option<tokio::sync::oneshot::Sender<()>>,
    cancelled: tokio::sync::oneshot::Receiver<Result<()>>,
}
#[cfg(target_os = "linux")]
impl ProcessRelease {
    pub(super) fn release(self) -> Result<()> {
        if let Some(release) = self.release {
            release
                .send(())
                .map_err(|_| anyhow::anyhow!("process work no longer pending"))?;
        }
        Ok(())
    }
    pub(super) async fn cancelled(&mut self) -> Result<()> {
        (&mut self.cancelled)
            .await
            .context("child cancellation acknowledgment lost")?
    }
}

/// Ownership of the sole transport publisher and both SQLite connections is
/// exclusive. Only this coordinator allocates sequences. The runner actor signs
/// current-head challenges; callbacks submit proposals, not final domain events.
pub(super) struct RuntimeDriver {
    pub source: Store,
    pub target: Store,
    pub publisher: ScopedPeer,
    pub mirror: ScopedPeer,
    pub origin: BoundOrigin,
    pub session: SessionKey,
    core: SecretKey,
    pub runner: RunnerClient,
    ready: bool,
    claims: BTreeMap<u64, ClaimedMock>,
    signals: BTreeMap<u64, watch::Sender<bool>>,
    running: BTreeSet<u64>,
    tasks: JoinSet<u64>,
    stopped: VecDeque<u64>,
    sender: mpsc::Sender<Completion>,
    receiver: mpsc::Receiver<Completion>,
    // Disposable fault cut, never compiled into the normal executable.
    cut_after_remote_result: bool,
}

impl RuntimeDriver {
    /// Parent future only orchestrates child controls and forwards signed bytes.
    /// The child claims, executes, journals and signs the mock outcome.
    #[cfg(target_os = "linux")]
    pub(super) async fn admit_process(
        &mut self,
        assignment: u64,
        mode: super::proposals::process::Mode,
    ) -> Result<ProcessRelease> {
        use super::proposals::process::Mode;
        let client = self.runner.clone();
        let sender = self.sender.clone();
        let (started, receive) = tokio::sync::oneshot::channel::<Result<()>>();
        let (release, mut gate) = tokio::sync::oneshot::channel();
        let (cancelled, cancel_ack) = tokio::sync::oneshot::channel();
        self.admit(assignment, move |mut work| async move {
            let notice=if mode.is_provider() {
                match client.provider_notice(assignment,mode==Mode::RealCodex) {
                    Ok(notice)=>Some(notice),
                    Err(error)=>{let _=started.send(Err(error));return;}
                }
            } else {None};
            if let Err(error) = client.begin(assignment, mode).await {
                let _ = started.send(Err(error));
                return;
            }
            let _ = started.send(Ok(()));
            if let Some(notice)=notice {
                let ready=notice.wait();tokio::pin!(ready);
                let mut cancelled=Some(cancelled);let mut released=false;
                loop {
                    tokio::select! {
                        biased;
                        changed=work.cancellation.changed(),if cancelled.is_some()=>{
                            if changed.is_err(){return;}
                            let result=client.cancel_work(assignment).await;
                            let failed=result.is_err();
                            let _=cancelled.take().expect("guarded cancellation sender").send(result);
                            if failed{return;}
                        }
                        release=&mut gate,if !released=>{
                            if release.is_err(){
                                let _=client.cancel_work(assignment).await;
                                if (&mut ready).await.is_ok() {
                                    let _=client.abandon_provider(assignment).await;
                                }
                                return;
                            }
                            if client.release_provider(assignment).await.is_err(){return;}
                            released=true;
                        }
                        result=&mut ready=>{
                            if result.is_err(){return;}
                            if let Ok(bytes)=client.finish(assignment).await {
                                let _=sender.send(Completion{bytes}).await;
                            }
                            return;
                        }
                    }
                }
            }
            tokio::select! {
                biased;
                changed=work.cancellation.changed()=>{
                    if changed.is_err() {return;}
                    let result=client.cancel_work(assignment).await;
                    let failed=result.is_err();let _=cancelled.send(result);
                    if failed {return;}
                    if mode==Mode::Late && gate.await.is_err() {return;}
                }
                released=&mut gate=>{if released.is_err() {return;}}
            }
            if *work.cancellation.borrow() && client.cancel_work(assignment).await.is_err() {
                return;
            }
            if let Ok(bytes) = client.finish(assignment).await {
                let _ = sender.send(Completion { bytes }).await;
            }
        })
        .await?;
        let began = match receive.await {
            Ok(value) => value,
            Err(_)
                if self.publisher.domain_history(&self.origin)?.jobs[&assignment]
                    .result
                    .as_ref()
                    .is_some_and(|(outcome, _)| *outcome == Outcome::Cancelled) =>
            {
                return Ok(ProcessRelease {
                    release: None,
                    cancelled: cancel_ack,
                });
            }
            Err(error) => Err(error.into()),
        };
        if let Err(error) = began {
            self.ready = false;
            self.claims.remove(&assignment);
            return Err(error);
        }
        Ok(ProcessRelease {
            release: Some(release),
            cancelled: cancel_ack,
        })
    }

    pub(super) fn new(
        origin: BoundOrigin,
        session: SessionKey,
        stores: (Store, Store),
        peers: (ScopedPeer, ScopedPeer),
        keys: (SecretKey, RunnerClient),
    ) -> Result<Self> {
        ensure!(
            keys.0.public() == origin.origin.core
                && origin.origin.runners.contains(&keys.1.public()),
            "driver fixture roles mismatch"
        );
        TaskAdapter::open(&stores.0, &origin, &session)?;
        TaskAdapter::open(&stores.1, &origin, &session)?;
        let (sender, receiver) = mpsc::channel(MAX_TASKS);
        Ok(Self {
            source: stores.0,
            target: stores.1,
            publisher: peers.0,
            mirror: peers.1,
            origin,
            session,
            core: keys.0,
            runner: keys.1,
            ready: false,
            claims: BTreeMap::new(),
            signals: BTreeMap::new(),
            running: BTreeSet::new(),
            tasks: JoinSet::new(),
            stopped: VecDeque::new(),
            sender,
            receiver,
            cut_after_remote_result: false,
        })
    }

    fn healthy(&mut self) -> Result<()> {
        ensure!(self.ready, "driver must reconcile before allocating events");
        let source = TaskAdapter::open(&self.source, &self.origin, &self.session)?
            .history(&mut self.publisher)?;
        let mirror = TaskAdapter::open(&self.target, &self.origin, &self.session)?
            .history(&mut self.mirror)?;
        ensure!(
            source.export() == mirror.export(),
            "coordinator prefix not acknowledged"
        );
        Ok(())
    }

    async fn transfer(&mut self, record: &Record) -> Result<()> {
        self.mirror
            .receive(self.publisher.addr(), record.origin.parse()?, &record.key())
            .await?;
        let source = self.publisher.domain_history(&self.origin)?.export();
        ensure!(
            self.mirror.domain_history(&self.origin)?.export() == source,
            "mirror history incomplete"
        );
        #[cfg(unix)]
        if let Some(remote) = self.runner.remote() {
            let locator = super::proposals::process::remote::Locator {
                addr: self.publisher.addr(),
                author: record.origin.parse()?,
                record: record.clone(),
            };
            remote.import(&mut self.publisher, locator).await?;
        }
        Ok(())
    }

    /// Explicit reconnect/restart reconciliation. Validate the WHOLE candidate
    /// prefix before replay. Orphaned started rows recover Unknown, not callbacks.
    pub(super) async fn reconcile(&mut self) -> Result<()> {
        self.ready = false;
        let mut events = TaskAdapter::open(&self.source, &self.origin, &self.session)?
            .history(&mut self.publisher)?
            .export();
        let published = events.clone();
        let remote = self.runner.saved().await?;
        events.extend(remote.iter().cloned());
        let outgoing: Vec<Vec<u8>> = self
            .source
            .conn
            .prepare("SELECT event FROM adapter_outgoing ORDER BY run")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        ensure!(
            outgoing.len() <= MAX_TASKS,
            "disposable task bound exceeded"
        );
        events.extend(outgoing.iter().cloned());
        let executions: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .target
            .conn
            .prepare("SELECT start,result FROM adapter_execution ORDER BY assignment")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        for (start, result) in executions {
            events.push(start);
            if let Some(result) = result {
                events.push(result);
            }
        }
        let history = History::replay(
            &self.origin,
            self.origin.origin.owner,
            self.origin.origin.namespace,
            events,
        )?;
        ensure!(
            history.pending_fence().is_none() && history.export().len() <= MAX_TASKS * 3,
            "driver suspended or outside bounded task history"
        );
        let assignments: BTreeSet<_> = outgoing
            .iter()
            .map(|b| SignedEvent::parse(b).map(|s| s.event.sequence))
            .collect::<Result<_>>()?;
        ensure!(
            assignments == history.jobs.keys().copied().collect(),
            "source assignment mapping unavailable"
        );
        self.publisher.flush().await?;
        // A missing predecessor can make a partial mirror unavailable. It stays
        // inert until the full prefix below validates; never select a fork.
        let received = self
            .mirror
            .domain_history(&self.origin)
            .map(|h| h.export())
            .unwrap_or_default();
        // Generic receipt is still inert. Validate once the complete prefix has
        // arrived, so a previously missing predecessor can be repaired safely.
        for bytes in history.export() {
            if !published.contains(&bytes) || !received.contains(&bytes) {
                let record = self.publisher.publish_domain(&self.origin, &bytes).await?;
                self.mirror
                    .receive(self.publisher.addr(), record.origin.parse()?, &record.key())
                    .await?;
            }
        }
        ensure!(
            self.mirror.domain_history(&self.origin)?.export() == history.export(),
            "mirror conflict or incomplete history"
        );
        for bytes in outgoing {
            let signed = SignedEvent::parse(&bytes)?;
            let assignment = signed.event.sequence;
            let source = TaskAdapter::open(&self.source, &self.origin, &self.session)?;
            let (_, current, delegation) = source.assigned(&mut self.publisher, assignment)?;
            source.source_run(&current, &delegation)?;
            let exists: bool = self.target.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM adapter_incoming WHERE assignment=?1)",
                [assignment],
                |r| r.get(0),
            )?;
            if !exists {
                TaskAdapter::open(&self.target, &self.origin, &self.session)?.accept(
                    &mut self.mirror,
                    assignment,
                    self.runner.public(),
                )?;
            }
            TaskAdapter::open(&self.target, &self.origin, &self.session)?.incoming_run(
                assignment,
                &current,
                &delegation,
            )?;
            for start in &remote {
                if matches!(SignedEvent::parse(start)?.event.action,Action::Start{assignment:a} if a==assignment)
                {
                    TaskAdapter::open(&self.target, &self.origin, &self.session)?.restore_start(
                        &mut self.publisher,
                        assignment,
                        start,
                    )?;
                }
            }
            let mut state: Option<Option<Vec<u8>>> = self
                .target
                .conn
                .query_row(
                    "SELECT result FROM adapter_execution WHERE assignment=?1",
                    [assignment],
                    |r| r.get(0),
                )
                .optional()?;
            ensure!(
                history.jobs[&assignment].started == state.is_some(),
                "execution correlation unavailable; do not re-admit"
            );
            let cancelled: bool = self.source.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM adapter_cancel WHERE assignment=?1)",
                [assignment],
                |r| r.get(0),
            )?;
            if cancelled {
                let record = TaskAdapter::open(&self.source, &self.origin, &self.session)?
                    .request_cancel(&mut self.publisher, assignment, &self.core)
                    .await?;
                self.transfer(&record).await?;
                TaskAdapter::open(&self.target, &self.origin, &self.session)?
                    .apply_cancellation(&mut self.mirror, assignment)?;
                if let Some(signal) = self.signals.get(&assignment) {
                    signal.send_replace(true);
                }
            }
            if let Some(result) = state.take() {
                if result.is_none() {
                    TaskAdapter::open(&self.source, &self.origin, &self.session)?
                        .apply_start(&mut self.publisher, assignment)?;
                    let final_response = remote.iter().find(|b| {
                        SignedEvent::parse(b).is_ok_and(|s|
                        matches!(s.event.action,Action::Result{assignment:a,..} if a==assignment))
                    });
                    if final_response.is_some()
                        || (!self.claims.contains_key(&assignment)
                            && self.runner.stored_proposal(assignment).await?.is_some())
                    {
                        // A report/signature is durable, but acceptance into the
                        // task lifecycle was interrupted. Retain late evidence
                        // without turning that ambiguity into completed work.
                        TaskAdapter::open(&self.target, &self.origin, &self.session)?
                            .interrupted_return(&mut self.publisher, assignment, false)?;
                        TaskAdapter::open(&self.source, &self.origin, &self.session)?
                            .interrupted_return(&mut self.publisher, assignment, true)?;
                    }
                    if let Some(bytes) = final_response {
                        TaskAdapter::open(&self.target, &self.origin, &self.session)?
                            .restore_result(&mut self.publisher, assignment, bytes)?;
                        self.claims.remove(&assignment);
                        state = Some(Some(bytes.clone()));
                    } else {
                        state = Some(result);
                    }
                } else {
                    state = Some(result);
                }
            }
            if let Some(result) = state {
                if result.is_none() && !self.claims.contains_key(&assignment) {
                    let record = TaskAdapter::open(&self.target, &self.origin, &self.session)?
                        .recover_result(&mut self.publisher, assignment, &self.runner)
                        .await?;
                    self.transfer(&record).await?;
                }
                if result.is_some() || !self.claims.contains_key(&assignment) {
                    TaskAdapter::open(&self.source, &self.origin, &self.session)?
                        .apply_result(&mut self.publisher, assignment)?;
                }
            }
        }
        self.ready = true;
        Ok(())
    }

    pub(super) async fn queue(&mut self, run: i64) -> Result<u64> {
        self.healthy()?;
        let prior: Option<Vec<u8>> = self
            .source
            .conn
            .query_row(
                "SELECT event FROM adapter_outgoing WHERE run=?1",
                [run],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(bytes) = prior {
            let assignment = SignedEvent::parse(&bytes)?.event.sequence;
            let source = TaskAdapter::open(&self.source, &self.origin, &self.session)?;
            let (_, current, delegation) = source.assigned(&mut self.publisher, assignment)?;
            source.source_run(&current, &delegation)?;
            return Ok(assignment);
        }
        let count: usize =
            self.source
                .conn
                .query_row("SELECT count(*) FROM adapter_outgoing", [], |r| r.get(0))?;
        ensure!(count < MAX_TASKS, "disposable task bound exceeded");
        self.ready = false;
        let record = TaskAdapter::open(&self.source, &self.origin, &self.session)?
            .export(&mut self.publisher, run, &self.core, self.runner.public())
            .await?;
        self.transfer(&record).await?;
        let bytes: Vec<u8> = self.source.conn.query_row(
            "SELECT event FROM adapter_outgoing WHERE run=?1",
            [run],
            |r| r.get(0),
        )?;
        let assignment = SignedEvent::parse(&bytes)?.event.sequence;
        TaskAdapter::open(&self.target, &self.origin, &self.session)?.accept(
            &mut self.mirror,
            assignment,
            self.runner.public(),
        )?;
        self.ready = true;
        Ok(assignment)
    }

    pub(super) async fn cancel(&mut self, assignment: u64) -> Result<()> {
        self.healthy()?;
        self.ready = false;
        let record = TaskAdapter::open(&self.source, &self.origin, &self.session)?
            .request_cancel(&mut self.publisher, assignment, &self.core)
            .await?;
        self.transfer(&record).await?;
        TaskAdapter::open(&self.target, &self.origin, &self.session)?
            .apply_cancellation(&mut self.mirror, assignment)?;
        if let Some(signal) = self.signals.get(&assignment) {
            signal.send_replace(true);
        }
        self.ready = true;
        Ok(())
    }

    pub(super) async fn admit<F, Fut>(&mut self, assignment: u64, executor: F) -> Result<()>
    where
        F: FnOnce(MockWork) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.healthy()?;
        ensure!(self.running.len() < MAX_RUNNING, "mock admission full");
        ensure!(
            !self.claims.contains_key(&assignment) && !self.running.contains(&assignment),
            "already admitted"
        );
        ensure!(
            self.publisher
                .domain_history(&self.origin)?
                .jobs
                .get(&assignment)
                .is_some_and(|job| !job.started && job.result.is_none()),
            "assignment already started/settled or absent; never re-admit"
        );
        self.ready = false;
        let target = TaskAdapter::open(&self.target, &self.origin, &self.session)?;
        let (dispatch, record) = target
            .start_mock(&mut self.publisher, assignment, &self.runner)
            .await?;
        self.transfer(&record).await?;
        TaskAdapter::open(&self.source, &self.origin, &self.session)?
            .apply_start(&mut self.publisher, assignment)?;
        let target = TaskAdapter::open(&self.target, &self.origin, &self.session)?;
        let claim = target.claim_mock(&mut self.publisher, &self.runner, dispatch)?;
        let (_, bytes, delegation) = target.assigned(&mut self.publisher, assignment)?;
        let id = target.incoming_run(assignment, &bytes, &delegation)?;
        let run = self.target.get_task_run(id)?.context("mock task absent")?;
        self.claims.insert(assignment, claim);
        self.ready = true;
        if run.cancel_requested_at.is_some() {
            self.local_outcome(
                assignment,
                Outcome::Cancelled,
                "mock cancelled before async invocation",
            )
            .await?;
            return Ok(());
        }
        let (signal, cancellation) = watch::channel(false);
        self.signals.insert(assignment, signal);
        self.running.insert(assignment);
        let future = executor(MockWork {
            prompt: run.prompt,
            cancellation,
            result: OutcomeSender {
                assignment,
                channel: self.sender.clone(),
                runner: self.runner.clone(),
            },
        });
        self.tasks.spawn(async move {
            future.await;
            assignment
        });
        Ok(())
    }

    async fn accept(&mut self, completion: Completion) -> Result<Step> {
        self.healthy()?;
        let history = self.publisher.domain_history(&self.origin)?;
        let proposal = SignedProposal::verify(&completion.bytes, &self.origin, &history)?;
        ensure!(
            proposal.runner == self.runner.public(),
            "wrong proposal runner"
        );
        // Existing scoped transport, runner-side peer to publication coordinator.
        // The proposal is inert metadata, not a sequenced application event.
        #[cfg(unix)]
        let remote_publication = if let Some(remote) = self.runner.remote() {
            Some(remote.publish(completion.bytes.clone()).await?)
        } else {
            None
        };
        #[cfg(not(unix))]
        let remote_publication: Option<((), ())> = None;
        #[cfg(unix)]
        let record = if let Some((locator, artifact)) = remote_publication {
            ensure!(
                locator.addr.id == self.runner.remote().context("remote absent")?.data_id()
                    && locator.record.text == PROPOSAL_RECORD,
                "wrong remote proposal publisher"
            );
            self.publisher
                .receive(locator.addr, locator.author, &locator.record.key())
                .await?;
            ensure!(
                self.publisher.received_artifact(&locator.record)? == completion.bytes,
                "proposal bytes differ from actual scoped receipt"
            );
            if let Some(artifact) = artifact {
                self.accept_remote_artifact(&proposal, artifact).await?;
            }
            locator.record
        } else {
            self.mirror
                .publish(PROPOSAL_RECORD, &completion.bytes)
                .await?
        };
        #[cfg(not(unix))]
        let record = self
            .mirror
            .publish(PROPOSAL_RECORD, &completion.bytes)
            .await?;
        if self.runner_is_local() {
            self.publisher
                .receive(self.mirror.addr(), record.origin.parse()?, &record.key())
                .await?;
        }
        ensure!(
            record.artifact.hash == blake3::hash(&completion.bytes).to_string()
                && self.publisher.received()?.contains(&record),
            "proposal receipt mismatch"
        );
        let completion = proposal;
        let saved: Option<Vec<u8>> = self.target.conn.query_row(
            "SELECT result FROM adapter_execution WHERE assignment=?1",
            [completion.assignment],
            |r| r.get(0),
        )?;
        if let Some(saved) = saved {
            let signed = SignedEvent::parse(&saved)?;
            ensure!(
                matches!(signed.event.action,Action::Result { assignment,outcome,ref output }
                if assignment == completion.assignment && outcome == completion.outcome && output == &completion.output),
                "conflicting mock outcome"
            );
            return Ok(Step::Duplicate(completion.assignment));
        }
        let claim = self
            .claims
            .remove(&completion.assignment)
            .context("no active execution claim")?;
        self.ready = false;
        let record = TaskAdapter::open(&self.target, &self.origin, &self.session)?
            .finish_claimed(
                &mut self.publisher,
                &self.runner,
                claim,
                completion.outcome,
                completion.output,
            )
            .await?;
        if self.cut_after_remote_result {
            self.cut_after_remote_result = false;
            anyhow::bail!(
                "owned fault cut: signed result durable before original Store acknowledgment"
            );
        }
        self.transfer(&record).await?;
        TaskAdapter::open(&self.source, &self.origin, &self.session)?
            .apply_result(&mut self.publisher, completion.assignment)?;
        self.ready = true;
        Ok(Step::Applied(completion.assignment))
    }

    fn runner_is_local(&self) -> bool {
        #[cfg(unix)]
        {
            self.runner.remote().is_none()
        }
        #[cfg(not(unix))]
        {
            true
        }
    }

    pub(super) async fn accept_proposal(&mut self, bytes: Vec<u8>) -> Result<Step> {
        self.accept(Completion { bytes }).await
    }

    async fn local_outcome(
        &mut self,
        assignment: u64,
        outcome: Outcome,
        output: &str,
    ) -> Result<Step> {
        let bytes = self
            .runner
            .report(assignment, outcome, output.into())
            .await?;
        self.accept_proposal(bytes).await
    }

    pub(super) async fn step(&mut self) -> Result<Step> {
        self.healthy()?;
        if let Ok(completion) = self.receiver.try_recv() {
            return self.accept(completion).await;
        }
        if !self.stopped.is_empty() {
            return self.settle_stopped().await;
        }
        ensure!(
            !self.running.is_empty() || !self.receiver.is_empty(),
            "mock driver idle"
        );
        tokio::select! {
            biased;
            Some(completion) = self.receiver.recv() => self.accept(completion).await,
            stopped = self.tasks.join_next(), if !self.running.is_empty() => {
                let assignment = match stopped.context("mock task absent")? {
                    Ok(assignment) => assignment,
                    Err(error) => {
                        // No callback may remain live while its claim is treated
                        // as orphaned. A failed driver settles conservatively on
                        // explicit reconciliation, never by invoking again.
                        self.ready=false;
                        self.tasks.shutdown().await;
                        self.claims.clear();
                        self.running.clear();
                        self.signals.clear();
                        return Err(error.into());
                    }
                };
                self.running.remove(&assignment);
                self.signals.remove(&assignment);
                self.stopped.push_back(assignment);
                // On a multithreaded runtime a send can complete after recv was
                // polled Pending but before this join became Ready. Joining
                // guarantees all awaited sends by that callback are now queued.
                if let Ok(completion) = self.receiver.try_recv() {
                    self.accept(completion).await
                } else { self.settle_stopped().await }
            }
        }
    }

    async fn settle_stopped(&mut self) -> Result<Step> {
        let assignment = self.stopped.pop_front().context("stopped mock absent")?;
        if self.claims.contains_key(&assignment) {
            if let Some(bytes) = self.runner.stored_proposal(assignment).await? {
                self.accept_proposal(bytes).await
            } else {
                self.local_outcome(
                    assignment,
                    Outcome::Unknown,
                    "async mock ended without result; outcome unknown",
                )
                .await
            }
        } else {
            Ok(Step::Stopped(assignment))
        }
    }

    pub(super) async fn shutdown(mut self) -> Result<()> {
        self.tasks.shutdown().await;
        let runner = self.runner.shutdown().await;
        let publisher = self.publisher.shutdown().await;
        let mirror = self.mirror.shutdown().await;
        runner.and(publisher).and(mirror)
    }
}
