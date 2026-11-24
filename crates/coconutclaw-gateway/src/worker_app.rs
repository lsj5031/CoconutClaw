use std::cell::OnceCell;
use std::num::NonZeroU32;

use futures_util::StreamExt;
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use worker::*;

use crate::executor_wire::{self, Event, Evidence, evidence_bytes_for, request_bytes};
use crate::ledger::{
    Delivery, Execution, Ledger, LocalAuthority, Receipt, Reply, ScheduleInput, conflict,
};
use crate::protocol::{Agent, BODY_LIMIT, Command, Fixture, INSTANCE, telegram_input, valid_text};
use crate::runtime_authority::{Authority, Pin, nonce};

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
    Pilot,
    Cutover,
}

const PILOT_INSTANCE: &str = "hosted-synthetic-pilot";
const PILOT_UPDATE: i64 = 23001;
const PILOT_TASK: &str = "telegram:23001";
const PILOT_TOPIC: i32 = 37;
const PILOT_PROMPT: &str = "synthetic pilot";
const PILOT_RESULT: &str = "pilot: SYNTHETIC PILOT";

impl LocalPhase {
    fn from_env(env: &Env) -> Option<Self> {
        match env.var("PHASE").ok()?.to_string().as_str() {
            "phase1-local" => Some(Self::Protocol),
            "phase2-local" => Some(Self::Executor),
            "phase2-pilot" => Some(Self::Pilot),
            "phase2-cutover" => Some(Self::Cutover),
            _ => None,
        }
    }

    fn instance(self) -> &'static str {
        match self {
            Self::Protocol => INSTANCE,
            Self::Executor => executor_wire::INSTANCE,
            Self::Pilot | Self::Cutover => PILOT_INSTANCE,
        }
    }

    fn executor(self) -> bool {
        self != Self::Protocol
    }

    fn authority_pin(self, env: &Env) -> std::result::Result<Option<NonZeroU32>, Reply> {
        if self != Self::Executor {
            return Ok(None);
        }
        env.var("LOCAL_GENERATION")
            .ok()
            .and_then(|var| var.to_string().parse().ok())
            .map(Some)
            .ok_or_else(inactive_generation)
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

pub(crate) fn now() -> i64 {
    js_sys::Date::now() as i64
}

fn unavailable() -> Result<Response> {
    Reply::error(503, "unavailable").response()
}

fn inactive_generation() -> Reply {
    Reply::error(503, "inactive_generation")
}

fn route(req: &Request, env: &Env) -> Result<Option<LocalPhase>> {
    let Some(phase) = LocalPhase::from_env(env) else {
        return Ok(None);
    };
    let url = req.url()?;
    if req.method() != Method::Post || url.scheme() != "https" || url.query().is_some() {
        return Ok(None);
    }
    let valid = if phase == LocalPhase::Cutover {
        let Some(pin) = Pin::read(env) else {
            return Ok(None);
        };
        url.host_str() == Some(&pin.hostname)
            && url.port().is_none()
            && matches!(url.path(), "/v1/telegram" | "/v1/agent" | "/v1/cancel")
    } else if phase == LocalPhase::Pilot {
        matches!(
            url.path(),
            "/v1/pilot/telegram" | "/v1/pilot/agent" | "/v1/pilot/fixture"
        )
    } else {
        url.host_str() == Some("127.0.0.1")
            && matches!(url.path(), "/v1/telegram" | "/v1/agent" | "/v1/fixture")
    };
    Ok(valid.then_some(phase))
}

#[event(fetch)]
pub async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let Some(phase) = route(&req, &env)? else {
        return Reply::error(404, "not_found").response();
    };
    if let Err(reply) = phase.authority_pin(&env) {
        return reply.response();
    }
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
    initialized: OnceCell<()>,
    incarnation: OnceCell<String>,
}

impl DurableObject for BotLedger {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            initialized: OnceCell::new(),
            incarnation: OnceCell::new(),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let Some(phase) = route(&req, &self.env)? else {
            return Reply::error(404, "not_found").response();
        };
        let pin = match phase.authority_pin(&self.env) {
            Ok(pin) => pin,
            Err(reply) => return reply.response(),
        };
        if self.state.id()
            != self
                .env
                .durable_object("BOTS")?
                .id_from_name(phase.instance())?
        {
            return Reply::error(404, "not_found").response();
        }
        let reply = if phase == LocalPhase::Cutover {
            self.handle_cutover(req).await
        } else {
            self.handle(req, phase, pin).await
        };
        match reply {
            Ok(reply) => reply.response(),
            Err(_) => unavailable(),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        let Some(phase) = LocalPhase::from_env(&self.env) else {
            return Reply::ok(json!({})).response();
        };
        let Ok(pin) = phase.authority_pin(&self.env) else {
            return Reply::ok(json!({})).response();
        };
        let bots = self.env.durable_object("BOTS")?;
        if self.state.id() != bots.id_from_name(phase.instance())? {
            return Reply::ok(json!({})).response();
        }
        let storage = self.state.storage();
        let result = async {
            if phase == LocalPhase::Cutover {
                return self.cutover_alarm(&storage).await;
            }
            let ledger = self.ledger(&storage, phase, "alarm")?;
            if let Some(pin) = pin
                && (ledger.legacy_cutover_held()?
                    || !Self::local_authority(&ledger, pin)?
                        .is_some_and(|authority| authority.active == 1))
            {
                return Ok(());
            }
            match phase {
                LocalPhase::Protocol => {
                    let (_, alarm) =
                        ledger.transaction(&storage, || ledger.wake(&storage, now()))?;
                    settle_alarm(alarm).await?;
                    storage.sync().await
                }
                LocalPhase::Executor => self.deliver(&storage, &ledger, None).await,
                LocalPhase::Pilot => {
                    let alarm = ledger
                        .transaction(&storage, || ledger.deliver_synthetic(&storage, now()))?;
                    settle_alarm(alarm).await?;
                    storage.sync().await
                }
                LocalPhase::Cutover => unreachable!(),
            }
        }
        .await;
        result.map_err(|_| worker::Error::from("alarm storage unavailable"))?;
        Reply::ok(json!({})).response()
    }
}

impl BotLedger {
    fn local_authority(ledger: &Ledger, pin: NonZeroU32) -> Result<Option<LocalAuthority>> {
        let durable = ledger.local_authority()?;
        Ok((pin.get() == durable.generation).then_some(durable))
    }

    fn ledger(&self, storage: &Storage, phase: LocalPhase, scope: &'static str) -> Result<Ledger> {
        let measure = phase == LocalPhase::Executor
            && self
                .env
                .var("LOCAL_SQL_METRICS")
                .is_ok_and(|var| var.to_string() == "1");
        let ledger = Ledger::new(storage, measure.then_some(scope));
        if self.initialized.get().is_none() {
            ledger.initialize()?;
            if phase == LocalPhase::Pilot {
                ledger.initialize_synthetic_delivery()?;
            } else if matches!(phase, LocalPhase::Executor | LocalPhase::Cutover) {
                ledger.initialize_local_cutover()?;
            }
            if phase == LocalPhase::Cutover {
                ledger.initialize_cutover_custody()?;
            }
            self.initialized
                .set(())
                .map_err(|_| "storage already initialized")?;
        }
        Ok(ledger)
    }

    async fn deliver(
        &self,
        storage: &Storage,
        ledger: &Ledger,
        authority: Option<&Authority<'_>>,
    ) -> Result<()> {
        let (delivery, alarm) =
            ledger.transaction(storage, || ledger.wake_delivery(storage, now()))?;
        settle_alarm(alarm).await?;
        storage.sync().await?;
        if let Some(delivery) = delivery {
            let receipt = if let Some(authority) = authority {
                let receipt = authority.send(&delivery).await;
                if authority.check().await.is_none() {
                    return Ok(());
                }
                receipt
            } else {
                send_telegram(&delivery).await.unwrap_or(Receipt::Unknown)
            };
            let alarm = ledger.transaction(storage, || {
                ledger.settle_delivery(storage, delivery, receipt, now())
            })?;
            settle_alarm(alarm).await?;
            storage.sync().await?;
        }
        Ok(())
    }

    fn cutover_authority(&self, runner: Option<String>) -> Option<Authority<'_>> {
        let authority = Authority::read(&self.env, self.incarnation.get_or_init(nonce), runner)?;
        (authority.pin().object_id == self.state.id().to_string()).then_some(authority)
    }

    async fn cutover_alarm(&self, storage: &Storage) -> Result<()> {
        let Some(authority) = self.cutover_authority(None) else {
            return Ok(());
        };
        if authority.check().await.is_none() {
            return Ok(());
        }
        let pin = authority.pin();
        let ledger = self.ledger(storage, LocalPhase::Cutover, "alarm")?;
        if ledger
            .cutover_authority(pin.generation.get(), &pin.custody_digest)?
            .is_some()
        {
            self.deliver(storage, &ledger, Some(&authority)).await?;
        }
        Ok(())
    }

    async fn handle_cutover(&self, mut req: Request) -> Result<Reply> {
        let path = req.path();
        let runner = match path.as_str() {
            "/v1/agent" => {
                let Some(runner) = req.headers().get("x-executor-incarnation")? else {
                    return Ok(inactive_generation());
                };
                Some(runner)
            }
            "/v1/telegram" => {
                if !authorized(
                    &req,
                    &self.env,
                    "x-telegram-bot-api-secret-token",
                    "WEBHOOK_SECRET",
                ) {
                    return Ok(Reply::error(401, "unauthorized"));
                }
                None
            }
            "/v1/cancel" => {
                if !authorized(
                    &req,
                    &self.env,
                    "x-cutover-operator",
                    "CUTOVER_CANCEL_SECRET",
                ) {
                    return Ok(Reply::error(401, "unauthorized"));
                }
                None
            }
            _ => return Ok(Reply::error(404, "not_found")),
        };
        let Some(authority) = self.cutover_authority(runner) else {
            return Ok(inactive_generation());
        };
        if authority.check().await.is_none() {
            return Ok(inactive_generation());
        }
        let bytes = match bounded_body(&mut req).await {
            Ok(bytes) => bytes,
            Err(reply) => return Ok(reply),
        };
        let pin = authority.pin();
        let instance = pin.agent_instance();
        let agent = if path == "/v1/agent" {
            match self
                .decode_agent(
                    &req,
                    &bytes,
                    LocalPhase::Cutover,
                    &instance,
                    Some(&pin.executor_key),
                )
                .await
            {
                Ok(agent) => Some(agent),
                Err(reply) => return Ok(reply),
            }
        } else {
            None
        };
        let Some(valid_until) = authority.check().await else {
            return Ok(inactive_generation());
        };
        let storage = self.state.storage();
        let ledger = self.ledger(&storage, LocalPhase::Cutover, "cutover")?;
        let Some(custody) = ledger.cutover_authority(pin.generation.get(), &pin.custody_digest)?
        else {
            return Ok(Reply::error(503, "legacy_cutover_held"));
        };
        let mut start = None;
        let (reply, alarm) = if let Some((agent, evidence)) = agent {
            start = match agent.command {
                Command::Start { attempt } => Some(attempt),
                _ => None,
            };
            let status = match agent.command {
                Command::Status { attempt } => Some(attempt),
                _ => None,
            };
            if start
                .or(status)
                .or_else(|| evidence.as_ref().map(|proof| proof.attempt))
                .is_some_and(|attempt| attempt <= custody.attempt_floor)
            {
                return Ok(conflict());
            }
            if let Some(reply) = ledger.read_agent(&agent, now(), &instance)? {
                return Ok(reply.authority_deadline(valid_until));
            }
            ledger.transaction(&storage, || {
                let at = now();
                if !ledger.take_pilot_request(at, false)? {
                    return Ok((ledger.pilot_limit(at)?, None));
                }
                ledger.mutate_agent(
                    agent,
                    at,
                    Some(Execution::Cutover {
                        instance: &instance,
                        valid_until,
                    }),
                    evidence,
                    &storage,
                )
            })?
        } else if path == "/v1/telegram" {
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
            (
                ledger.transaction(&storage, || {
                    let at = now();
                    if !ledger.take_pilot_request(at, false)? {
                        return ledger.pilot_limit(at);
                    }
                    ledger.ingest(id, input, false, Some(at))
                })?,
                None,
            )
        } else {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Cancel {
                task: String,
            }
            let Ok(cancel) = serde_json::from_slice::<Cancel>(&bytes) else {
                return Ok(bad_input());
            };
            if cancel.task.is_empty() || cancel.task.len() > 64 {
                return Ok(bad_input());
            }
            (
                ledger.transaction(&storage, || ledger.cancel(&cancel.task))?,
                None,
            )
        };
        settle_alarm(alarm).await?;
        storage.sync().await?;
        let Some(valid_until) = authority.check().await else {
            return Ok(inactive_generation());
        };
        if let Some(attempt) = start
            && !ledger.grant_current(attempt, now())?
        {
            return Ok(conflict());
        }
        Ok(reply.authority_deadline(valid_until))
    }

    async fn handle(
        &self,
        mut req: Request,
        phase: LocalPhase,
        pin: Option<NonZeroU32>,
    ) -> Result<Reply> {
        let executor = phase.executor();
        let pilot = phase == LocalPhase::Pilot;
        let path = req.path();
        let operation = if pilot {
            path.strip_prefix("/v1/pilot/")
        } else {
            path.strip_prefix("/v1/")
        };
        let (scope, auth) = match operation {
            Some("telegram") => (
                "telegram",
                pilot
                    || authorized(
                        &req,
                        &self.env,
                        "x-telegram-bot-api-secret-token",
                        "WEBHOOK_SECRET",
                    ),
            ),
            Some("fixture") => (
                "fixture",
                pilot || authorized(&req, &self.env, "x-fixture-secret", "FIXTURE_SECRET"),
            ),
            Some("agent") => ("agent", true),
            _ => return Ok(Reply::error(404, "not_found")),
        };
        let storage = self.state.storage();
        let ledger = self.ledger(&storage, phase, scope)?;
        let authority = if let Some(pin) = pin {
            if ledger.legacy_cutover_held()? {
                return Ok(Reply::error(503, "legacy_cutover_held"));
            }
            let Some(authority) = Self::local_authority(&ledger, pin)? else {
                return Ok(inactive_generation());
            };
            Some(authority)
        } else {
            None
        };
        let frozen = authority
            .as_ref()
            .is_some_and(|authority| authority.active == 0);
        let instance = authority.as_ref().map_or_else(
            || phase.instance().to_owned(),
            |authority| executor_wire::fixture_instance(authority.generation),
        );
        if frozen && scope == "telegram" {
            return Ok(Reply::error(503, "frozen_generation"));
        }
        if executor {
            let at = now();
            let (admitted, alarm) = ledger.transaction(&storage, || {
                let admitted = ledger.take_pilot_request(at, false)?;
                let alarm = if admitted && !frozen {
                    ledger.bootstrap_delivery(&storage, at)?
                } else {
                    None
                };
                Ok((admitted, alarm))
            })?;
            settle_alarm(alarm).await?;
            if !admitted {
                return ledger.pilot_limit(at);
            }
        }
        if !auth {
            return Ok(Reply::error(401, "unauthorized"));
        }
        let bytes = match bounded_body(&mut req).await {
            Ok(bytes) => bytes,
            Err(reply) => return Ok(reply),
        };
        let fault = if !pilot && authorized(&req, &self.env, "x-fixture-secret", "FIXTURE_SECRET") {
            req.headers().get("x-fixture-fault")?
        } else {
            None
        };
        let (reply, drop_reply) = match scope {
            "telegram" => {
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
                if pilot
                    && (id != PILOT_UPDATE || input != Some((321, Some(PILOT_TOPIC), PILOT_PROMPT)))
                {
                    return Ok(bad_input());
                }
                let reply = ledger.transaction(&storage, || {
                    ledger.ingest(
                        id,
                        input,
                        fault.as_deref() == Some("rollback"),
                        executor.then(now),
                    )
                })?;
                (reply, fault.as_deref() == Some("after_commit"))
            }
            "agent" => {
                let (agent, evidence) = match self
                    .decode_agent(&req, &bytes, phase, &instance, None)
                    .await
                {
                    Ok(agent) => agent,
                    Err(reply) => return Ok(reply),
                };
                if let Some(authority) = authority {
                    if frozen {
                        match agent.command {
                            Command::Next => return Ok(Reply::ok(json!({"task":null}))),
                            Command::Hello | Command::Status { .. } | Command::Report { .. } => (),
                            _ => return Ok(Reply::error(503, "frozen_generation")),
                        }
                    }
                    let attempt = match agent.command {
                        Command::Start { attempt } => Some(attempt),
                        _ => evidence.as_ref().map(|evidence| evidence.attempt),
                    };
                    if attempt.is_some_and(|attempt| attempt <= authority.attempt_floor) {
                        return Ok(conflict());
                    }
                }
                if let Some(reply) = ledger.read_agent(&agent, now(), &instance)? {
                    return Ok(reply);
                }
                let drop_reply = fault.as_deref() == Some("after_commit")
                    && matches!(
                        agent.command,
                        Command::Start { .. } | Command::Report { .. }
                    );
                let execution = match phase {
                    LocalPhase::Protocol => None,
                    LocalPhase::Executor => Some(Execution::Local {
                        instance: &instance,
                        reconcile_only: frozen,
                    }),
                    LocalPhase::Pilot => Some(Execution::HostedPilot),
                    LocalPhase::Cutover => unreachable!(),
                };
                let (reply, alarm) = ledger.transaction(&storage, || {
                    ledger.mutate_agent(agent, now(), execution, evidence, &storage)
                })?;
                settle_alarm(alarm).await?;
                (reply, drop_reply)
            }
            "fixture" => {
                let Ok(fixture) = serde_json::from_slice(&bytes) else {
                    return Ok(bad_input());
                };
                if frozen && !matches!(fixture, Fixture::DeliveryStatus { .. }) {
                    return Ok(Reply::error(503, "frozen_generation"));
                }
                if pilot {
                    return match fixture {
                        Fixture::DeliveryStatus { task } if task == PILOT_TASK => {
                            ledger.synthetic_delivery_status(&task)
                        }
                        _ => Ok(Reply::error(404, "not_found")),
                    };
                }
                if executor
                    && matches!(
                        fixture,
                        Fixture::Schedule { .. }
                            | Fixture::Disable { .. }
                            | Fixture::Wake { .. }
                            | Fixture::Send { .. }
                            | Fixture::RecoverSend { .. }
                    )
                {
                    return Ok(Reply::error(404, "not_found"));
                }
                let drop_receipt = fault.as_deref() == Some("drop_receipt");
                (
                    self.fixture(&ledger, &storage, fixture, drop_receipt)
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

    async fn decode_agent(
        &self,
        req: &Request,
        bytes: &[u8],
        phase: LocalPhase,
        instance: &str,
        cutover_key: Option<&str>,
    ) -> std::result::Result<(Agent, Option<Evidence>), Reply> {
        let unauthorized = || Reply::error(401, "unauthorized");
        let agent = serde_json::from_slice::<Agent>(bytes).map_err(|_| bad_input())?;
        if agent.at_ms.abs_diff(now()) > 30_000
            || (phase.executor() && agent.device.name() != executor_wire::DEVICE)
        {
            return Err(unauthorized());
        }
        let key = match cutover_key {
            Some(key) => key.to_owned(),
            None => {
                let binding = match phase {
                    LocalPhase::Pilot => "PILOT_ALPHA_KEY".to_owned(),
                    LocalPhase::Executor => "EXECUTOR_ALPHA_KEY".to_owned(),
                    LocalPhase::Protocol => {
                        format!("DEVICE_{}_KEY", agent.device.name().to_ascii_uppercase())
                    }
                    LocalPhase::Cutover => return Err(unauthorized()),
                };
                self.env
                    .var(&binding)
                    .map_err(|_| unauthorized())?
                    .to_string()
            }
        };
        let mut decoded = [0; 32];
        hex::decode_to_slice(key, &mut decoded).map_err(|_| unauthorized())?;
        let signature = req
            .headers()
            .get("x-agent-signature")
            .ok()
            .flatten()
            .ok_or_else(unauthorized)?;
        if !signed_by(&decoded, &signature, &request_bytes(instance, bytes)).await {
            return Err(unauthorized());
        }
        if !agent.command.available(phase.executor()) {
            return Err(Reply::error(404, "not_found"));
        }
        if !valid_command(&agent) {
            return Err(bad_input());
        }
        let evidence = if let Command::Report { proof } = &agent.command {
            let evidence =
                serde_json::from_str::<Evidence>(&proof.payload).map_err(|_| bad_input())?;
            if !valid_attempt(evidence.attempt)
                || matches!(&evidence.event, Event::Result { text }
                    if !valid_text(text) || (phase == LocalPhase::Pilot && text != PILOT_RESULT))
            {
                return Err(bad_input());
            }
            if !signed_by(
                &decoded,
                &proof.signature,
                &evidence_bytes_for(instance, &proof.payload),
            )
            .await
            {
                return Err(unauthorized());
            }
            Some(evidence)
        } else {
            None
        };
        Ok((agent, evidence))
    }

    async fn fixture(
        &self,
        ledger: &Ledger,
        storage: &Storage,
        fixture: Fixture,
        drop_receipt: bool,
    ) -> Result<Reply> {
        match fixture {
            Fixture::Cancel { task } => ledger.transaction(storage, || ledger.cancel(&task)),
            Fixture::Disconnect { attempt } => {
                ledger.transaction(storage, || ledger.disconnect(attempt))
            }
            Fixture::DeliveryStatus { task } => ledger.delivery_state(&task, true),
            Fixture::RecoverSend { task } => {
                ledger.transaction(storage, || ledger.recover_send(&task, false))
            }
            Fixture::Send { task } => {
                let Some(send) =
                    ledger.transaction(storage, || ledger.reserve_send(&task, now()))?
                else {
                    return Ok(conflict());
                };
                storage.sync().await?;
                let receipt = send_telegram(&send).await.unwrap_or(Receipt::Unknown);
                if drop_receipt {
                    return Ok(Reply::error(503, "fixture_dropped_receipt"));
                }
                ledger.transaction(storage, || {
                    ledger.finish_send(&task, receipt, send.sends, now())?;
                    ledger.delivery_state(&task, false)
                })
            }
            other => {
                let (reply, alarm) = ledger.transaction(storage, || match other {
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

pub(crate) async fn collect_body(mut stream: ByteStream) -> std::result::Result<Vec<u8>, Reply> {
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
                Some(seconds @ 1..=2_147_483_647) => Receipt::RateLimited(seconds * 1000),
                _ => Receipt::Rejected,
            }
        }
        400 | 403 if value["ok"] == false && value["error_code"] == response.status_code() => {
            Receipt::Rejected
        }
        _ => Receipt::Unknown,
    })
}
