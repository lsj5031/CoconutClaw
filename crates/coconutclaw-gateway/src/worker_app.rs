use std::cell::OnceCell;

use futures_util::StreamExt;
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use worker::*;

use crate::executor_wire::{self, Event, Evidence, evidence_bytes, request_bytes};
use crate::ledger::{Delivery, Ledger, Receipt, Reply, ScheduleInput, conflict};
use crate::protocol::{Agent, BODY_LIMIT, Command, Fixture, INSTANCE, telegram_input, valid_text};

#[wasm_bindgen(inline_js = "
export async function verify(key, signature, message) {
    const signatureCopy = signature.slice();
    const messageCopy = message.slice();
    const publicKey = await crypto.subtle.importKey('raw', key, 'Ed25519', false, ['verify']);
    return crypto.subtle.verify('Ed25519', publicKey, signatureCopy, messageCopy);
}
export async function mockSend(body) {
    return fetch('https://telegram.invalid/sendMessage', {
        method: 'POST', body, headers: { 'content-type': 'application/json' },
        redirect: 'manual', signal: AbortSignal.timeout(1500)
    });
}
")]
extern "C" {
    fn verify(key: &[u8], signature: &[u8], message: &[u8]) -> js_sys::Promise;
    #[wasm_bindgen(js_name = mockSend)]
    fn mock_send(body: &str) -> js_sys::Promise;
}

#[derive(Clone, Copy, PartialEq)]
enum LocalPhase {
    Protocol,
    Executor,
}

impl LocalPhase {
    fn from_env(env: &Env) -> Option<Self> {
        match env.var("PHASE").ok()?.to_string().as_str() {
            "phase1-local" => Some(Self::Protocol),
            "phase2-local" => Some(Self::Executor),
            _ => None,
        }
    }

    fn instance(self) -> &'static str {
        match self {
            Self::Protocol => INSTANCE,
            Self::Executor => executor_wire::INSTANCE,
        }
    }
}

fn authorized(req: &Request, env: &Env, header: &str, binding: &str) -> bool {
    match (req.headers().get(header), env.var(binding)) {
        (Ok(Some(actual)), Ok(expected)) => {
            let expected = expected.to_string();
            !expected.is_empty() && bool::from(actual.as_bytes().ct_eq(expected.as_bytes()))
        }
        _ => false,
    }
}

fn now() -> i64 {
    js_sys::Date::now() as i64
}

fn unavailable() -> Result<Response> {
    Reply::error(503, "unavailable").response()
}

fn local_route(req: &Request, env: &Env) -> Result<Option<LocalPhase>> {
    let url = req.url()?;
    Ok((req.method() == Method::Post
        && url.scheme() == "https"
        && url.host_str() == Some("127.0.0.1")
        && url.query().is_none()
        && matches!(url.path(), "/v1/telegram" | "/v1/agent" | "/v1/fixture"))
    .then(|| LocalPhase::from_env(env))
    .flatten())
}

#[event(fetch)]
pub async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let Some(phase) = local_route(&req, &env)? else {
        return Reply::error(404, "not_found").response();
    };
    match forward(req, env, phase).await {
        Ok(response) => Ok(response),
        Err(_) => unavailable(),
    }
}

async fn forward(req: Request, env: Env, phase: LocalPhase) -> Result<Response> {
    env.durable_object("BOTS")?
        .get_by_name(phase.instance())?
        .fetch_with_request(req)
        .await
}

#[durable_object]
pub struct BotLedger {
    state: State,
    env: Env,
    ledger: OnceCell<Ledger>,
}

impl DurableObject for BotLedger {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            ledger: OnceCell::new(),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let Some(phase) = local_route(&req, &self.env)? else {
            return Reply::error(404, "not_found").response();
        };
        match self.handle(req, phase).await {
            Ok(reply) => reply.response(),
            Err(_) => unavailable(),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        if LocalPhase::from_env(&self.env) != Some(LocalPhase::Protocol) {
            return Reply::ok(json!({})).response();
        }
        let storage = self.state.storage();
        let result = async {
            self.ledger(&storage)?;
            let (_, alarm) = Ledger::transaction(&storage, |ledger| ledger.wake(&storage, now()))?;
            settle_alarm(alarm).await?;
            storage.sync().await
        }
        .await;
        result.map_err(|_| worker::Error::from("alarm storage unavailable"))?;
        Reply::ok(json!({})).response()
    }
}

impl BotLedger {
    fn ledger(&self, storage: &Storage) -> Result<&Ledger> {
        if self.ledger.get().is_none() {
            self.ledger
                .set(Ledger::open(storage)?)
                .map_err(|_| "storage already initialized")?;
        }
        self.ledger
            .get()
            .ok_or_else(|| "storage unavailable".into())
    }

    async fn handle(&self, mut req: Request, phase: LocalPhase) -> Result<Reply> {
        let executor = phase == LocalPhase::Executor;
        let path = req.path();
        let auth = match path.as_str() {
            "/v1/telegram" => authorized(
                &req,
                &self.env,
                "x-telegram-bot-api-secret-token",
                "WEBHOOK_SECRET",
            ),
            "/v1/fixture" => authorized(&req, &self.env, "x-fixture-secret", "FIXTURE_SECRET"),
            _ => true,
        };
        if !auth {
            return Ok(Reply::error(401, "unauthorized"));
        }
        let bytes = match bounded_body(&mut req).await {
            Ok(bytes) => bytes,
            Err(reply) => return Ok(reply),
        };
        let fault = if authorized(&req, &self.env, "x-fixture-secret", "FIXTURE_SECRET") {
            req.headers().get("x-fixture-fault")?
        } else {
            None
        };
        let storage = self.state.storage();
        let (reply, drop_reply) = match path.as_str() {
            "/v1/telegram" => {
                let Ok(update) = serde_json::from_slice::<Value>(&bytes) else {
                    return Ok(bad_input());
                };
                let Some(id) = update["update_id"]
                    .as_i64()
                    .filter(|id| (0..=i32::MAX as i64).contains(id))
                else {
                    return Ok(bad_input());
                };
                let input = telegram_input(&update)
                    .map(|(route, text)| (route.chat_id, route.message_thread_id, text));
                self.ledger(&storage)?;
                let reply = Ledger::transaction(&storage, |ledger| {
                    ledger.ingest(id, input, fault.as_deref() == Some("rollback"))
                })?;
                (reply, fault.as_deref() == Some("after_commit"))
            }
            "/v1/agent" => {
                let Ok(agent) = serde_json::from_slice::<Agent>(&bytes) else {
                    return Ok(bad_input());
                };
                let Some(key) = self.verify_agent(&req, &agent, &bytes, phase).await else {
                    return Ok(Reply::error(401, "unauthorized"));
                };
                if !agent.command.available(executor) {
                    return Ok(Reply::error(404, "not_found"));
                }
                if !valid_command(&agent) {
                    return Ok(bad_input());
                }
                let evidence = if let Command::Report { proof } = &agent.command {
                    let Ok(evidence) = serde_json::from_str::<Evidence>(&proof.payload) else {
                        return Ok(bad_input());
                    };
                    if !valid_attempt(evidence.attempt)
                        || matches!(&evidence.event, Event::Result { text } if !valid_text(text))
                    {
                        return Ok(bad_input());
                    }
                    if !signed_by(&key, &proof.signature, &evidence_bytes(&proof.payload)).await {
                        return Ok(Reply::error(401, "unauthorized"));
                    }
                    Some(evidence)
                } else {
                    None
                };
                let ledger = self.ledger(&storage)?;
                if let Some(reply) = ledger.read_agent(&agent, now(), phase.instance())? {
                    return Ok(reply);
                }
                let drop_reply = fault.as_deref() == Some("after_commit")
                    && matches!(
                        agent.command,
                        Command::Start { .. } | Command::Report { .. }
                    );
                let reply = Ledger::transaction(&storage, |ledger| {
                    ledger.mutate_agent(agent, now(), executor, evidence)
                })?;
                (reply, drop_reply)
            }
            "/v1/fixture" => {
                let Ok(fixture) = serde_json::from_slice(&bytes) else {
                    return Ok(bad_input());
                };
                if executor
                    && matches!(
                        fixture,
                        Fixture::Schedule { .. } | Fixture::Disable { .. } | Fixture::Wake { .. }
                    )
                {
                    return Ok(Reply::error(404, "not_found"));
                }
                let drop_receipt = fault.as_deref() == Some("drop_receipt");
                self.ledger(&storage)?;
                (
                    self.fixture(&storage, fixture, drop_receipt, executor)
                        .await?,
                    false,
                )
            }
            _ => return Ok(Reply::error(404, "not_found")),
        };
        storage.sync().await?;
        if drop_reply {
            Ok(Reply::error(503, "fixture_after_commit"))
        } else {
            Ok(reply)
        }
    }

    async fn verify_agent(
        &self,
        req: &Request,
        agent: &Agent,
        bytes: &[u8],
        phase: LocalPhase,
    ) -> Option<Vec<u8>> {
        if agent.at_ms.abs_diff(now()) > 30_000
            || (phase == LocalPhase::Executor && agent.device.name() != executor_wire::DEVICE)
        {
            return None;
        }
        let binding = if phase == LocalPhase::Executor {
            "EXECUTOR_ALPHA_KEY".to_owned()
        } else {
            format!("DEVICE_{}_KEY", agent.device.name().to_ascii_uppercase())
        };
        let key = self
            .env
            .var(&binding)
            .ok()
            .and_then(|var| hex::decode(var.to_string()).ok())
            .filter(|key| key.len() == 32)?;
        let signature = req.headers().get("x-agent-signature").ok().flatten()?;
        signed_by(&key, &signature, &request_bytes(phase.instance(), bytes))
            .await
            .then_some(key)
    }

    async fn fixture(
        &self,
        storage: &Storage,
        fixture: Fixture,
        drop_receipt: bool,
        executor: bool,
    ) -> Result<Reply> {
        match fixture {
            Fixture::Cancel { task } => Ledger::transaction(storage, |ledger| ledger.cancel(&task)),
            Fixture::Disconnect { attempt } => {
                Ledger::transaction(storage, |ledger| ledger.disconnect(attempt))
            }
            Fixture::RecoverSend { task } => {
                Ledger::transaction(storage, |ledger| ledger.recover_send(&task, executor))
            }
            Fixture::Send { task } => {
                let Some(send) =
                    Ledger::transaction(storage, |ledger| ledger.reserve_send(&task, now()))?
                else {
                    return Ok(conflict());
                };
                storage.sync().await?;
                let receipt = send_telegram(&send).await.unwrap_or(Receipt::Unknown);
                if drop_receipt {
                    return Ok(Reply::error(503, "fixture_dropped_receipt"));
                }
                Ledger::transaction(storage, |ledger| {
                    ledger.finish_send(&task, receipt, send.sends, now(), executor)
                })
            }
            other => {
                let (reply, alarm) = Ledger::transaction(storage, |ledger| match other {
                    Fixture::Schedule {
                        version,
                        first_at,
                        interval_ms,
                        count,
                        message_thread_id,
                        prompt,
                        pin,
                    } if (1..=32).contains(&version)
                        && (1000..=60_000).contains(&interval_ms)
                        && (1..=3).contains(&count)
                        && message_thread_id.is_none_or(|id| id > 0)
                        && valid_text(&prompt) =>
                    {
                        ledger.configure_schedule(
                            storage,
                            ScheduleInput {
                                version,
                                first_at,
                                interval_ms,
                                count,
                                thread: message_thread_id,
                                prompt,
                                pin,
                            },
                            now(),
                        )
                    }
                    Fixture::Disable { version } => ledger.disable(storage, version),
                    Fixture::Wake { at_ms } if at_ms.abs_diff(now()) <= 300_000 => {
                        ledger.wake(storage, at_ms)
                    }
                    _ => Ok((bad_input(), None)),
                })?;
                settle_alarm(alarm).await?;
                Ok(reply)
            }
        }
    }
}

fn bad_input() -> Reply {
    Reply::error(400, "bad_input")
}

async fn signed_by(key: &[u8], signature: &str, bytes: &[u8]) -> bool {
    let mut decoded = [0; 64];
    if hex::decode_to_slice(signature, &mut decoded).is_err() {
        return false;
    }
    JsFuture::from(verify(key, &decoded, bytes))
        .await
        .is_ok_and(|result| result.as_bool() == Some(true))
}

fn valid_attempt(attempt: i64) -> bool {
    (1..=i32::MAX as i64).contains(&attempt)
}

fn valid_command(agent: &Agent) -> bool {
    match &agent.command {
        Command::Hello | Command::Next => agent.sequence == 0,
        Command::Status { attempt } => agent.sequence == 0 && valid_attempt(*attempt),
        _ if !(1..=i32::MAX as i64).contains(&agent.sequence) => false,
        Command::Claim { task } => !task.is_empty() && task.len() <= 64,
        Command::Start { attempt }
        | Command::Started { attempt }
        | Command::Cancelled { attempt, .. } => valid_attempt(*attempt),
        Command::Result { attempt, text } => valid_attempt(*attempt) && valid_text(text),
        Command::Report { .. } => true,
    }
}

async fn settle_alarm(alarm: Option<js_sys::Promise>) -> Result<()> {
    if let Some(alarm) = alarm {
        JsFuture::from(alarm).await?;
    }
    Ok(())
}

async fn bounded_body(req: &mut Request) -> std::result::Result<Vec<u8>, Reply> {
    if req
        .headers()
        .get("content-length")
        .ok()
        .flatten()
        .and_then(|size| size.parse::<usize>().ok())
        .is_some_and(|size| size > BODY_LIMIT)
    {
        return Err(Reply::error(413, "body_limit"));
    }
    collect_body(req.stream().map_err(|_| bad_input())?).await
}

async fn collect_body(mut stream: ByteStream) -> std::result::Result<Vec<u8>, Reply> {
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| bad_input())?;
        if chunk.len() > BODY_LIMIT - body.len() {
            return Err(Reply::error(413, "body_limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn send_telegram(delivery: &Delivery) -> Result<Receipt> {
    let mut body = json!({"chat_id": delivery.chat_id, "text": delivery.text});
    if let Some(thread) = delivery.thread_id {
        body["message_thread_id"] = json!(thread);
    }
    let mut response = Response::from(
        JsFuture::from(mock_send(&body.to_string()))
            .await?
            .dyn_into::<web_sys::Response>()?,
    );
    let bytes = collect_body(response.stream()?)
        .await
        .map_err(|_| "invalid mock receipt")?;
    let value = serde_json::from_slice::<Value>(&bytes)?;
    Ok(match response.status_code() {
        200 if value["ok"] == true => match value["result"]["message_id"].as_i64() {
            Some(id) if id > 0 => Receipt::Delivered(id),
            _ => Receipt::Unknown,
        },
        429 if value["ok"] == false && value["error_code"] == 429 => {
            match value["parameters"]["retry_after"].as_i64() {
                Some(seconds @ 1..=10) => Receipt::Retry(seconds * 1000),
                _ => Receipt::Rejected,
            }
        }
        400 | 403 if value["ok"] == false && value["error_code"] == response.status_code() => {
            Receipt::Rejected
        }
        _ => Receipt::Unknown,
    })
}
