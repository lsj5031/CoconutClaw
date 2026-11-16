use super::*;
use crate::store::task_adapter::proposals::process::{
    Mode,
    provider::host::{self, Input, Manifest},
    remote::{LocalRequest, Locator},
};

impl RuntimeDriver {
    /// Same admission/claim/outcome path, with a separate remote completion
    /// stream. No callback computes the provider result or owns runner keys.
    pub(in crate::store::task_adapter) async fn admit_remote(
        &mut self,
        assignment: u64,
        mode: Mode,
    ) -> Result<()> {
        let runner = self.runner.clone();
        ensure!(runner.remote().is_some(), "not a remote route");
        let sender = self.sender.clone();
        let (began, receive) = tokio::sync::oneshot::channel();
        self.admit(assignment, move |mut work| async move {
            if let Err(error) = runner.begin(assignment, mode).await {
                let _ = began.send(Err(error));
                return;
            }
            let _ = began.send(Ok(()));
            if mode == Mode::HostCodex {
                let ready = runner.remote().expect("checked remote route").wait(assignment);
                tokio::pin!(ready);
                tokio::select! {
                    result = &mut ready => { if result.is_err() { return; } }
                    changed = work.cancellation.changed() => {
                        if changed.is_err() || runner.cancel_work(assignment).await.is_err() { return; }
                        if ready.await.is_err() { return; }
                    }
                }
            }
            if let Ok(bytes) = runner.finish(assignment).await {
                let _ = sender.send(Completion { bytes }).await;
            }
        }).await?;
        if let Err(error) = receive.await.context("remote Begin acknowledgment lost")? {
            self.ready = false;
            self.claims.remove(&assignment);
            return Err(error);
        }
        Ok(())
    }

    /// Select only while waiting for a completion. Once selected, acceptance
    /// runs to completion, never cancellation-drops a signing/publication future.
    pub(in crate::store::task_adapter) async fn step_remote_control(
        &mut self,
        control: &mut mpsc::Receiver<LocalRequest>,
    ) -> Result<Step> {
        self.healthy()?;
        loop {
            tokio::select! {
                biased;
                Some(completion) = self.receiver.recv() => return self.accept(completion).await,
                Some(request) = control.recv() => {
                    let value = match request.value["command"].as_str() {
                        Some("cancel") => match self.cancel(1).await {
                            Ok(()) => serde_json::json!({"cancel_requested":true}),
                            Err(error) => serde_json::json!({"ok":false,"error":error.to_string()}),
                        },
                        Some("status") => serde_json::json!({
                            "running":true,"provider":self.runner.provider_evidence().await?
                        }),
                        _ => serde_json::json!({"ok":false,"error":"while running use status/cancel; stop after settlement"}),
                    };
                    let _ = request.reply.send(value);
                }
                Some(stopped) = self.tasks.join_next() => {
                    let assignment = stopped?;
                    self.running.remove(&assignment);
                    self.signals.remove(&assignment);
                    if let Ok(completion) = self.receiver.try_recv() {
                        return self.accept(completion).await;
                    }
                    self.stopped.push_back(assignment);
                    return self.settle_stopped().await;
                }
            }
        }
    }

    pub(super) async fn accept_remote_artifact(
        &mut self,
        proposal: &super::super::proposals::Proposal,
        locator: Locator,
    ) -> Result<()> {
        let remote = self.runner.remote().context("not a remote route")?;
        ensure!(
            locator.addr.id == remote.data_id() && locator.record.text == host::ARTIFACT,
            "wrong remote artifact publisher"
        );
        let manifest: Manifest = serde_json::from_str(&proposal.output)?;
        self.publisher
            .receive(locator.addr, locator.author, &locator.record.key())
            .await?;
        let bytes = self.publisher.received_artifact(&locator.record)?;
        let input = Input {
            nonce: remote.run_id().into(),
            a: 17,
            b: 25,
        };
        host::verify_artifact(
            &manifest,
            proposal.assignment,
            &input,
            remote.hostname(),
            &bytes,
        )?;
        self.source.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS acceptance_artifact(
                assignment INTEGER PRIMARY KEY,bytes BLOB NOT NULL,manifest TEXT NOT NULL);",
        )?;
        self.source.conn.execute(
            "INSERT OR IGNORE INTO acceptance_artifact VALUES(?1,?2,?3)",
            params![proposal.assignment, bytes, proposal.output],
        )?;
        let saved: (Vec<u8>, String) = self.source.conn.query_row(
            "SELECT bytes,manifest FROM acceptance_artifact WHERE assignment=?1",
            [proposal.assignment],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            saved == (bytes, proposal.output.clone()),
            "artifact replay conflict"
        );
        Ok(())
    }
}
