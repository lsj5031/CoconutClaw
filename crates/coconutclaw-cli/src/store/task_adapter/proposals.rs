//! Disposable local runner boundary, not process/key custody or live admission.
//! Only the actor owns the runner key. Its client cannot sign arbitrary events.
use super::*;
use coconutclaw_sync::synthetic::membership::domain::{Event, Outcome};
use iroh::Signature;
use std::path::Path;
use tokio::sync::{mpsc, oneshot};
#[cfg(unix)]
pub(super) mod process;

const LABEL: &[u8] = b"CoconutClaw runner outcome proposal v1\0";
const MAX_PROPOSAL: usize = coconutclaw_sync::synthetic::MAX_ARTIFACT;
pub(super) const PROPOSAL_RECORD: &str = "CoconutClaw runner outcome proposal v1";

pub(super) trait MockSigner {
    fn public(&self) -> EndpointId;
    async fn sign(
        &self,
        origin: &BoundOrigin,
        history: &History,
        action: Action,
    ) -> Result<Vec<u8>>;
    async fn recovery_outcome(&self, _assignment: u64) -> Result<Option<(Outcome, String)>> {
        Ok(None)
    }
}

// Retain direct-key synchronous fixture callers, not the coordinated route.
impl MockSigner for SecretKey {
    fn public(&self) -> EndpointId {
        SecretKey::public(self)
    }
    async fn sign(
        &self,
        origin: &BoundOrigin,
        history: &History,
        action: Action,
    ) -> Result<Vec<u8>> {
        SignedEvent::sign(history.next(origin, self.public(), action)?, self)?.bytes()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Proposal {
    pub origin: String,
    pub scope: String,
    pub assignment: u64,
    pub assignment_hash: String,
    pub source_run: i64,
    pub parent: i64,
    pub index: u32,
    pub attempt: u64,
    pub runner: EndpointId,
    pub outcome: Outcome,
    pub output: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SignedProposal {
    pub proposal: Proposal,
    pub signature: Signature,
}

fn preimage(proposal: &Proposal) -> Result<Vec<u8>> {
    let mut bytes = LABEL.to_vec();
    bytes.extend(serde_json::to_vec(proposal)?);
    ensure!(bytes.len() <= MAX_PROPOSAL, "proposal exceeds bound");
    Ok(bytes)
}

impl SignedProposal {
    pub(super) fn sign(proposal: Proposal, key: &SecretKey) -> Result<Vec<u8>> {
        ensure!(proposal.runner == key.public(), "proposal signer mismatch");
        let signed = Self {
            signature: key.sign(&preimage(&proposal)?),
            proposal,
        };
        let bytes = serde_json::to_vec(&signed)?;
        ensure!(
            bytes.len() <= MAX_PROPOSAL,
            "proposal envelope exceeds bound"
        );
        Ok(bytes)
    }

    pub(super) fn verify(
        bytes: &[u8],
        origin: &BoundOrigin,
        history: &History,
    ) -> Result<Proposal> {
        ensure!(
            !bytes.is_empty() && bytes.len() <= MAX_PROPOSAL,
            "invalid proposal size"
        );
        let signed: Self = serde_json::from_slice(bytes)?;
        ensure!(
            serde_json::to_vec(&signed)? == bytes,
            "noncanonical proposal"
        );
        let p = &signed.proposal;
        p.runner.verify(&preimage(p)?, &signed.signature)?;
        let expected = proposal(origin, history, p.assignment, p.outcome, p.output.clone())?;
        ensure!(p == &expected, "proposal identity/binding mismatch");
        // Size only, NOT a signed domain record. Reserve the largest sequence
        // width in this four-task driver, so an escaped payload cannot become
        // unpublishable merely because another task completes first.
        let mut event = history.next(
            origin,
            p.runner,
            Action::Result {
                assignment: p.assignment,
                outcome: p.outcome,
                output: p.output.clone(),
            },
        )?;
        event.sequence = 12;
        SignedEvent {
            event,
            signature: signed.signature,
        }
        .bytes()?;
        Ok(signed.proposal)
    }
}

fn proposal(
    origin: &BoundOrigin,
    history: &History,
    assignment: u64,
    outcome: Outcome,
    output: String,
) -> Result<Proposal> {
    ensure!(history.pending_fence().is_none(), "runner suspended");
    let job = history
        .jobs
        .get(&assignment)
        .context("proposal assignment absent")?;
    ensure!(
        job.started && job.attempt == 1,
        "proposal not started attempt1"
    );
    ensure!(
        !output.is_empty()
            && output.len() <= 2048
            && !output
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t'),
        "invalid proposal output"
    );
    let bytes = history
        .export()
        .into_iter()
        .find(|b| SignedEvent::parse(b).is_ok_and(|s| s.event.sequence == assignment))
        .context("assignment absent")?;
    let signed = SignedEvent::parse(&bytes)?;
    let Action::Assign { request, .. } = signed.event.action else {
        anyhow::bail!("not assignment")
    };
    let d: Delegation = serde_json::from_str(&request)?;
    ensure!(
        serde_json::to_string(&d)? == request && d.parent > 0 && d.run > d.parent,
        "invalid proposal delegation"
    );
    Ok(Proposal {
        origin: origin.hash()?,
        scope: origin.origin.scope.clone(),
        assignment,
        assignment_hash: blake3::hash(&bytes).to_string(),
        source_run: d.run,
        parent: d.parent,
        index: d.index,
        attempt: job.attempt,
        runner: job.runner,
        outcome,
        output,
    })
}

enum Request {
    Sign {
        history: Vec<Vec<u8>>,
        action: Action,
        reply: oneshot::Sender<Result<Vec<u8>>>,
    },
    Report {
        assignment: u64,
        outcome: Outcome,
        output: String,
        reply: oneshot::Sender<Result<Vec<u8>>>,
    },
    Saved(oneshot::Sender<Result<Vec<Vec<u8>>>>),
    Proposal(u64, oneshot::Sender<Result<Option<Vec<u8>>>>),
    Stop(oneshot::Sender<()>),
}

#[derive(Clone)]
pub(super) struct RunnerClient {
    public: EndpointId,
    origin: String,
    route: Route,
}

#[derive(Clone)]
enum Route {
    Actor(mpsc::Sender<Request>),
    #[cfg(unix)]
    Process(std::sync::Arc<process::ProcessClient>),
    #[cfg(unix)]
    Remote(std::sync::Arc<process::remote::Client>),
}

struct Runner {
    key: SecretKey,
    origin: BoundOrigin,
    sql: Connection,
}

impl RunnerClient {
    /// Call outside coordinator construction; key moves into the owned actor.
    pub(super) fn spawn(origin: BoundOrigin, key: SecretKey, path: &Path) -> Result<Self> {
        let runner = Runner::open(origin, key, path)?;
        let (channel, mut receiver) = mpsc::channel(4);
        let client = Self {
            public: runner.key.public(),
            origin: runner.origin.hash()?,
            route: Route::Actor(channel),
        };
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                match request {
                    Request::Sign {
                        history,
                        action,
                        reply,
                    } => {
                        let _ = reply.send(runner.sign(history, action));
                    }
                    Request::Report {
                        assignment,
                        outcome,
                        output,
                        reply,
                    } => {
                        let _ = reply.send(runner.report(assignment, outcome, output));
                    }
                    Request::Saved(reply) => {
                        let _ = reply.send(runner.saved());
                    }
                    Request::Proposal(a, reply) => {
                        let _ = reply.send(runner.stored_proposal(a));
                    }
                    Request::Stop(reply) => {
                        drop(runner);
                        let _ = reply.send(());
                        return;
                    }
                }
            }
        });
        Ok(client)
    }

    fn actor(&self) -> Result<&mpsc::Sender<Request>> {
        match &self.route {
            Route::Actor(channel) => Ok(channel),
            #[cfg(unix)]
            Route::Process(_) | Route::Remote(_) => anyhow::bail!("not an actor route"),
        }
    }

    pub(super) async fn report(
        &self,
        assignment: u64,
        outcome: Outcome,
        output: String,
    ) -> Result<Vec<u8>> {
        ensure!(output.len() <= 2048, "proposal output exceeds bound");
        #[cfg(unix)]
        if matches!(&self.route, Route::Process(_) | Route::Remote(_)) {
            return self
                .control(process::Command::Disposition {
                    assignment,
                    outcome,
                    output,
                })
                .await?
                .bytes();
        }
        let (reply, receive) = oneshot::channel();
        self.actor()?
            .send(Request::Report {
                assignment,
                outcome,
                output,
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("runner disconnected"))?;
        receive.await.context("runner report response lost")?
    }

    pub(super) async fn saved(&self) -> Result<Vec<Vec<u8>>> {
        #[cfg(unix)]
        if matches!(&self.route, Route::Process(_) | Route::Remote(_)) {
            return self.control(process::Command::Saved).await?.events();
        }
        let (reply, receive) = oneshot::channel();
        self.actor()?
            .send(Request::Saved(reply))
            .await
            .map_err(|_| anyhow::anyhow!("runner disconnected"))?;
        receive.await.context("runner journal response lost")?
    }

    pub(super) async fn stored_proposal(&self, assignment: u64) -> Result<Option<Vec<u8>>> {
        #[cfg(unix)]
        if matches!(&self.route, Route::Process(_) | Route::Remote(_)) {
            return self
                .control(process::Command::Proposal { assignment })
                .await?
                .proposal();
        }
        let (reply, receive) = oneshot::channel();
        self.actor()?
            .send(Request::Proposal(assignment, reply))
            .await
            .map_err(|_| anyhow::anyhow!("runner disconnected"))?;
        receive.await.context("runner proposal response lost")?
    }

    pub(super) async fn shutdown(&self) -> Result<()> {
        #[cfg(unix)]
        if let Route::Process(p) = &self.route {
            return p.shutdown().await;
        }
        #[cfg(unix)]
        if let Route::Remote(p) = &self.route {
            p.call(process::Command::Stop).await?;
            return Ok(());
        }
        let (reply, receive) = oneshot::channel();
        self.actor()?
            .send(Request::Stop(reply))
            .await
            .map_err(|_| anyhow::anyhow!("runner disconnected"))?;
        receive.await.context("runner stop response lost")
    }

    /// Fault fixture: drop the receiver, then use a FIFO journal barrier.
    pub(super) async fn lose_signing_response(
        &self,
        history: &History,
        action: Action,
    ) -> Result<()> {
        let (reply, receive) = oneshot::channel();
        drop(receive);
        self.actor()?
            .send(Request::Sign {
                history: history.export(),
                action,
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("runner disconnected"))?;
        self.saved().await?;
        Ok(())
    }
}

impl Runner {
    fn open(origin: BoundOrigin, key: SecretKey, path: &Path) -> Result<Self> {
        ensure!(
            origin.origin.runners.contains(&key.public()),
            "unprovisioned runner"
        );
        let sql = Connection::open(path)?;
        sql.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS binding(singleton INTEGER PRIMARY KEY CHECK(singleton=1),origin TEXT NOT NULL,runner TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS responses(assignment INTEGER PRIMARY KEY,start BLOB NOT NULL,proposal BLOB,final BLOB);")?;
        sql.execute(
            "INSERT OR IGNORE INTO binding VALUES(1,?1,?2)",
            params![origin.hash()?, key.public().to_string()],
        )?;
        let binding: (String, String) =
            sql.query_row("SELECT origin,runner FROM binding", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        ensure!(
            binding == (origin.hash()?, key.public().to_string()),
            "runner journal binding conflict"
        );
        Ok(Self { key, origin, sql })
    }
}

impl MockSigner for RunnerClient {
    fn public(&self) -> EndpointId {
        self.public
    }
    async fn sign(
        &self,
        origin: &BoundOrigin,
        history: &History,
        action: Action,
    ) -> Result<Vec<u8>> {
        ensure!(origin.hash()? == self.origin, "foreign signing origin");
        #[cfg(unix)]
        if matches!(&self.route, Route::Process(_) | Route::Remote(_)) {
            return self
                .control(process::Command::Sign {
                    history: history.export(),
                    action,
                })
                .await?
                .bytes();
        }
        let (reply, receive) = oneshot::channel();
        self.actor()?
            .send(Request::Sign {
                history: history.export(),
                action,
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("runner disconnected"))?;
        receive.await.context("runner signing response lost")?
    }
    async fn recovery_outcome(&self, assignment: u64) -> Result<Option<(Outcome, String)>> {
        self.stored_proposal(assignment)
            .await?
            .map(|bytes| {
                let signed: SignedProposal = serde_json::from_slice(&bytes)?;
                Ok((signed.proposal.outcome, signed.proposal.output))
            })
            .transpose()
    }
}

impl Runner {
    fn stored_proposal(&self, assignment: u64) -> Result<Option<Vec<u8>>> {
        Ok(self
            .sql
            .query_row(
                "SELECT proposal FROM responses WHERE assignment=?1",
                [assignment],
                |r| r.get(0),
            )
            .optional()?
            .flatten())
    }

    fn saved(&self) -> Result<Vec<Vec<u8>>> {
        let rows: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .sql
            .prepare("SELECT start,final FROM responses ORDER BY assignment")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        ensure!(rows.len() <= 4, "runner task bound exceeded");
        Ok(rows
            .into_iter()
            .flat_map(|(start, result)| std::iter::once(start).chain(result))
            .collect())
    }

    fn history(&self, mut prefix: Vec<Vec<u8>>) -> Result<History> {
        ensure!(prefix.len() <= 12, "runner history exceeds bound");
        let history = History::replay(
            &self.origin,
            self.origin.origin.owner,
            self.origin.origin.namespace,
            std::mem::take(&mut prefix),
        )?;
        ensure!(history.pending_fence().is_none(), "runner suspended");
        Ok(history)
    }

    fn report(&self, assignment: u64, outcome: Outcome, output: String) -> Result<Vec<u8>> {
        // The report binds to the exact signed assignment and Start accepted by
        // this runner. Journal the whole observed prefix, never an invented job.
        let start: Vec<u8> = self.sql.query_row(
            "SELECT start FROM responses WHERE assignment=?1",
            [assignment],
            |r| r.get(0),
        )?;
        let event = SignedEvent::parse(&start)?;
        let prefix: Vec<Vec<u8>> = self
            .sql
            .query_row(
                "SELECT prefix FROM runner_prefix WHERE assignment=?1",
                [assignment],
                |r| r.get::<_, String>(0),
            )
            .map(|json| serde_json::from_str(&json))??;
        let mut prefix = prefix;
        prefix.push(start);
        let history = self.history(prefix)?;
        ensure!(
            event.event.actor == self.key.public(),
            "runner start identity conflict"
        );
        let p = proposal(&self.origin, &history, assignment, outcome, output)?;
        let bytes = SignedProposal::sign(p, &self.key)?;
        SignedProposal::verify(&bytes, &self.origin, &history)?;
        // Keep the first observed outcome. Conflicting signed candidates can be
        // disclosed/refused, but cannot replace it in the final signing handshake.
        self.sql.execute(
            "UPDATE responses SET proposal=?2 WHERE assignment=?1 AND proposal IS NULL",
            params![assignment, bytes],
        )?;
        Ok(bytes)
    }

    fn sign(&self, prefix: Vec<Vec<u8>>, action: Action) -> Result<Vec<u8>> {
        let history = self.history(prefix.clone())?;
        let assignment = match &action {
            Action::Start { assignment } | Action::Result { assignment, .. } => *assignment,
            _ => anyhow::bail!("runner refuses non-task signing"),
        };
        let job = history
            .jobs
            .get(&assignment)
            .context("runner assignment absent")?;
        ensure!(
            job.runner == self.key.public() && job.attempt == 1,
            "wrong assigned runner/attempt"
        );
        let event: Event = history.next(&self.origin, self.key.public(), action.clone())?;
        let acknowledged = history.export();
        let mut cached = None;
        for response in self.saved()? {
            let signed = SignedEvent::parse(&response)?;
            if !acknowledged.contains(&response) {
                ensure!(
                    signed.event == event,
                    "previous runner signing response still reserves the history head"
                );
                cached = Some(response);
            }
        }
        let bytes = if let Some(bytes) = cached {
            bytes
        } else {
            SignedEvent::sign(event, &self.key)?.bytes()?
        };
        let mut extended = history.export();
        extended.push(bytes.clone());
        self.history(extended)?;
        let tx = self.sql.unchecked_transaction()?;
        match action {
            Action::Start { .. } => {
                self.sql.execute_batch("CREATE TABLE IF NOT EXISTS runner_prefix(assignment INTEGER PRIMARY KEY,prefix TEXT NOT NULL)")?;
                let saved: Option<Vec<u8>> = self
                    .sql
                    .query_row(
                        "SELECT start FROM responses WHERE assignment=?1",
                        [assignment],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(saved) = saved {
                    ensure!(saved == bytes, "different signing head already reserved");
                } else {
                    let count: usize =
                        self.sql
                            .query_row("SELECT count(*) FROM responses", [], |r| r.get(0))?;
                    ensure!(count < 4, "runner task bound exceeded");
                    self.sql.execute(
                        "INSERT INTO responses(assignment,start) VALUES(?1,?2)",
                        params![assignment, bytes],
                    )?;
                    self.sql.execute(
                        "INSERT INTO runner_prefix VALUES(?1,?2)",
                        params![assignment, serde_json::to_string(&prefix)?],
                    )?;
                }
            }
            Action::Result {
                outcome,
                ref output,
                ..
            } => {
                let start: Vec<u8> = self.sql.query_row(
                    "SELECT start FROM responses WHERE assignment=?1",
                    [assignment],
                    |r| r.get(0),
                )?;
                ensure!(
                    history.export().contains(&start),
                    "runner start not in signing prefix"
                );
                let saved = self.stored_proposal(assignment)?;
                let reported = if let Some(saved) = saved {
                    SignedProposal::verify(&saved, &self.origin, &history)?
                } else {
                    ensure!(outcome == Outcome::Unknown, "no observed runner outcome");
                    let p = proposal(&self.origin, &history, assignment, outcome, output.clone())?;
                    let signed = SignedProposal::sign(p.clone(), &self.key)?;
                    self.sql.execute(
                        "UPDATE responses SET proposal=?2 WHERE assignment=?1",
                        params![assignment, signed],
                    )?;
                    p
                };
                ensure!(
                    reported.outcome == outcome && reported.output == *output,
                    "challenge differs from runner proposal"
                );
                let saved: Option<Vec<u8>> = self.sql.query_row(
                    "SELECT final FROM responses WHERE assignment=?1",
                    [assignment],
                    |r| r.get(0),
                )?;
                if let Some(saved) = saved {
                    ensure!(saved == bytes, "different signing head already reserved");
                } else {
                    self.sql.execute(
                        "UPDATE responses SET final=?2 WHERE assignment=?1",
                        params![assignment, bytes],
                    )?;
                }
            }
            _ => unreachable!(),
        }
        tx.commit()?;
        Ok(bytes)
    }
}
