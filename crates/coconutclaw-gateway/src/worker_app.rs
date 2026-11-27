use std::cell::{Cell, OnceCell};
use std::num::NonZeroU32;
use std::time::Duration;

use coconutclaw_telegram::{PrivateRoute, private_route};
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use worker::*;

use crate::executor_wire::{self, Event, Evidence, evidence_bytes_for, request_bytes};
use crate::ledger::{
    Delivery, DirectGeneration, Execution, Ledger, LocalAuthority, Receipt, Reply, ScheduleInput,
    conflict,
};
use crate::protocol::{
    Agent, BODY_LIMIT, Command, Device, Fixture, INSTANCE, PLACEMENT_INSTANCE, Phase,
    PlacementAliases, TelegramTask, telegram_input, valid_text,
};
use crate::runtime_authority::{Authority, Pin, Runner, nonce};

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
    Placement,
    Executor,
    Pilot,
    Cutover,
    Production,
    EnrolledPlacement,
    PairedPlacement,
}

const RUNNER_PRESENCE_MS: i64 = 15_000;
const PILOT_INSTANCE: &str = "hosted-synthetic-pilot";
const PRODUCTION_INSTANCE: &str = "bigfur-production";
const PRODUCTION_HOST: &str = "bigfur.liu.nz";
const PRODUCTION_IDENTITY: &str = "/v1/production/identity";
const PRODUCTION_INGRESS_HANDOFF: &str = "/v1/production/ingress-handoff";
const PILOT_UPDATE: i64 = 23001;
const PILOT_TASK: &str = "telegram:23001";
const PILOT_TOPIC: i32 = 37;
const PILOT_PROMPT: &str = "synthetic pilot";
const PILOT_RESULT: &str = "pilot: SYNTHETIC PILOT";

fn production_object_name(generation: NonZeroU32) -> String {
    if generation.get() == 1 {
        PRODUCTION_INSTANCE.to_owned()
    } else {
        format!("{PRODUCTION_INSTANCE}:{generation}")
    }
}

impl LocalPhase {
    fn from_env(env: &Env) -> Option<Self> {
        match env.var("PHASE").ok()?.to_string().as_str() {
            "phase1-local" => Some(Self::Protocol),
            "phase1-placement-local" => Some(Self::Placement),
            "phase2-local" => Some(Self::Executor),
            "phase2-pilot" => Some(Self::Pilot),
            "phase2-cutover" => Some(Self::Cutover),
            "phase2-production" => Some(Self::Production),
            "phase2-placement-local" => Some(Self::EnrolledPlacement),
            "phase2-pairing-local" => Some(Self::PairedPlacement),
            _ => None,
        }
    }

    fn instance(self) -> &'static str {
        match self {
            Self::Protocol => INSTANCE,
            Self::Placement => PLACEMENT_INSTANCE,
            Self::Executor => executor_wire::INSTANCE,
            Self::Pilot | Self::Cutover => PILOT_INSTANCE,
            Self::Production => PRODUCTION_INSTANCE,
            Self::EnrolledPlacement => "enrolled-placement-fixture",
            Self::PairedPlacement => "paired-placement-fixture",
        }
    }

    fn object_name(self, env: &Env) -> Option<String> {
        match self {
            Self::Production => Some(production_object_name(Pin::read(env)?.generation)),
            Self::EnrolledPlacement | Self::PairedPlacement => Some(format!(
                "{}:{}",
                self.instance(),
                Pin::read(env)?.generation
            )),
            _ => Some(self.instance().to_owned()),
        }
    }

    fn executor(self) -> bool {
        matches!(
            self,
            Self::Executor
                | Self::Pilot
                | Self::Cutover
                | Self::Production
                | Self::EnrolledPlacement
                | Self::PairedPlacement
        )
    }

    fn controlled(self) -> bool {
        self == Self::Cutover || self.direct()
    }

    fn direct(self) -> bool {
        matches!(
            self,
            Self::Production | Self::EnrolledPlacement | Self::PairedPlacement
        )
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

struct TelegramScope {
    chat: i64,
    thread: i32,
}

impl TelegramScope {
    fn read(env: &Env) -> Option<Self> {
        let scope = env.secret("PRODUCTION_TELEGRAM_SCOPE").ok()?.to_string();
        let (chat, thread) = scope.split_once(':')?;
        let chat = chat
            .parse()
            .ok()
            .filter(|id| (1..(1_i64 << 52)).contains(id))?;
        let thread = thread.parse().ok().filter(|id| *id > 0)?;
        Some(Self { chat, thread })
    }

    fn text<'a>(&self, update: &'a Value) -> Option<&'a str> {
        let message = update.get("message")?;
        let route = private_route(message).ok()?;
        let text = message["text"].as_str()?;
        (message["chat"]["type"] == "private"
            && route.chat_id == self.chat
            && route.message_thread_id == Some(self.thread)
            && message["from"]["id"].as_i64() == Some(self.chat)
            && message["from"]["is_bot"] == false
            && !text.trim_start().starts_with('/')
            && valid_text(text))
        .then_some(text)
    }

    fn generation(&self, pin: &Pin) -> DirectGeneration {
        DirectGeneration {
            generation: pin.generation.get(),
            digest: pin.custody_digest.clone(),
            chat_id: self.chat,
            thread_id: self.thread,
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
    let valid = if phase.controlled() {
        if phase == LocalPhase::Production {
            if url.host_str() != Some(PRODUCTION_HOST) || url.port().is_some() {
                return Ok(None);
            }
            if url.path() == PRODUCTION_IDENTITY {
                return Ok(Some(phase));
            }
        }
        let Some(pin) = Pin::read(env) else {
            return Ok(None);
        };
        url.host_str() == Some(&pin.hostname)
            && url.port().is_none()
            && (matches!(url.path(), "/v1/telegram" | "/v1/agent" | "/v1/cancel")
                || (pin.paired() && url.path() == "/v1/status")
                || (phase == LocalPhase::Production && url.path() == PRODUCTION_INGRESS_HANDOFF))
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
    if phase == LocalPhase::Production && req.path() == PRODUCTION_IDENTITY {
        if !authorized(&req, &env, "x-cutover-operator", "CUTOVER_CANCEL_SECRET") {
            return Reply::error(401, "unauthorized").response();
        }
        let generation = match req.headers().get("x-cutover-generation")? {
            Some(value) => match value.parse() {
                Ok(generation) => generation,
                Err(_) => return bad_input().response(),
            },
            None => NonZeroU32::MIN,
        };
        let bots = env.durable_object("BOTS")?;
        let object_id = bots.id_from_name(&production_object_name(generation))?;
        return Reply::ok(json!({"objectId": object_id.to_string()})).response();
    }
    if let Err(reply) = phase.authority_pin(&env) {
        return reply.response();
    }
    match forward(req, env, phase).await {
        Ok(response) => Ok(response),
        Err(_) => unavailable(),
    }
}

async fn forward(req: Request, env: Env, phase: LocalPhase) -> Result<Response> {
    let Some(name) = phase.object_name(&env) else {
        return inactive_generation().response();
    };
    env.durable_object("BOTS")?
        .get_by_name(&name)?
        .fetch_with_request(req)
        .await
}

#[durable_object]
pub struct BotLedger {
    state: State,
    env: Env,
    initialized: OnceCell<()>,
    incarnation: OnceCell<String>,
    runner_seen: [Cell<Option<i64>>; 3],
}

impl DurableObject for BotLedger {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            initialized: OnceCell::new(),
            incarnation: OnceCell::new(),
            runner_seen: Default::default(),
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
        if !self.routed_here(phase)? {
            return Reply::error(404, "not_found").response();
        }
        let reply = if phase.direct() && req.path() == "/v1/telegram" {
            self.handle_direct_telegram(req, phase).await
        } else if phase == LocalPhase::Production && req.path() == PRODUCTION_INGRESS_HANDOFF {
            self.freeze_production_ingress(req).await
        } else if phase.controlled() {
            self.handle_cutover(req, phase).await
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
        if !self.routed_here(phase)? {
            return Reply::ok(json!({})).response();
        }
        let storage = self.state.storage();
        let result = async {
            if phase.controlled() {
                return self.cutover_alarm(&storage, phase).await;
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
                LocalPhase::Placement => Ok(()),
                LocalPhase::Pilot => {
                    let alarm = ledger
                        .transaction(&storage, || ledger.deliver_synthetic(&storage, now()))?;
                    settle_alarm(alarm).await?;
                    storage.sync().await
                }
                LocalPhase::Cutover
                | LocalPhase::Production
                | LocalPhase::EnrolledPlacement
                | LocalPhase::PairedPlacement => {
                    unreachable!()
                }
            }
        }
        .await;
        result.map_err(|_| worker::Error::from("alarm storage unavailable"))?;
        Reply::ok(json!({})).response()
    }
}

impl BotLedger {
    fn routed_here(&self, phase: LocalPhase) -> Result<bool> {
        let Some(name) = phase.object_name(&self.env) else {
            return Ok(false);
        };
        let bots = self.env.durable_object("BOTS")?;
        Ok(self.state.id() == bots.id_from_name(&name)?)
    }

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

    fn cutover_authority(&self, runner: Option<Runner>) -> Option<Authority<'_>> {
        let authority = Authority::read(&self.env, self.incarnation.get_or_init(nonce), runner)?;
        (authority.pin().object_id == self.state.id().to_string()).then_some(authority)
    }

    async fn cutover_alarm(&self, storage: &Storage, phase: LocalPhase) -> Result<()> {
        let Some(authority) = self.cutover_authority(None) else {
            return Ok(());
        };
        let ledger = self.controlled_ledger(storage, phase)?;
        if self
            .attempt_floor(&ledger, phase, authority.pin())?
            .is_none()
        {
            return Ok(());
        }
        match authority.delivery_allowed().await {
            Ok(true) => self.deliver(storage, &ledger, Some(&authority)).await?,
            Ok(false) => {}
            Err(_) => {
                let alarm =
                    ledger.transaction(storage, || ledger.defer_unsent_delivery(storage, now()))?;
                settle_alarm(alarm).await?;
                storage.sync().await?;
            }
        }
        Ok(())
    }

    fn controlled_ledger(&self, storage: &Storage, phase: LocalPhase) -> Result<Ledger> {
        if phase.direct() {
            Ok(Ledger::new(storage, None))
        } else {
            self.ledger(storage, phase, "cutover")
        }
    }

    fn attempt_floor(&self, ledger: &Ledger, phase: LocalPhase, pin: &Pin) -> Result<Option<i64>> {
        if phase.direct() {
            let Some(scope) = TelegramScope::read(&self.env) else {
                return Ok(None);
            };
            Ok((ledger.direct_generation()? == Some(scope.generation(pin))).then_some(0))
        } else {
            Ok(ledger
                .cutover_authority(pin.generation.get(), &pin.custody_digest)?
                .map(|custody| custody.attempt_floor))
        }
    }

    async fn freeze_production_ingress(&self, req: Request) -> Result<Reply> {
        if !authorized(
            &req,
            &self.env,
            "x-cutover-operator",
            "CUTOVER_CANCEL_SECRET",
        ) {
            return Ok(Reply::error(401, "unauthorized"));
        }
        let (Some(pin), Some(scope)) = (Pin::read(&self.env), TelegramScope::read(&self.env))
        else {
            return Ok(inactive_generation());
        };
        if pin.object_id != self.state.id().to_string() {
            return Ok(inactive_generation());
        }
        let generation = scope.generation(&pin);
        let storage = self.state.storage();
        let ledger = Ledger::new(&storage, None);
        let reply = ledger.transaction(&storage, || {
            match ledger.direct_generation()? {
                Some(current) if current == generation => {}
                None if ledger.empty_schema()? => ledger.initialize_direct(generation)?,
                _ => return Ok(inactive_generation()),
            }
            Ok(Reply::ok(json!({
                "generation": pin.generation,
                "objectId": pin.object_id,
                "custodyDigest": pin.custody_digest,
                "updateIds": ledger.freeze_ingress()?,
            })))
        })?;
        storage.sync().await?;
        Ok(reply)
    }

    async fn handle_direct_telegram(&self, mut req: Request, phase: LocalPhase) -> Result<Reply> {
        if !authorized(
            &req,
            &self.env,
            "x-telegram-bot-api-secret-token",
            "WEBHOOK_SECRET",
        ) {
            return Ok(Reply::error(401, "unauthorized"));
        }
        let (Some(pin), Some(scope)) = (Pin::read(&self.env), TelegramScope::read(&self.env))
        else {
            return Ok(inactive_generation());
        };
        if pin.object_id != self.state.id().to_string() {
            return Ok(inactive_generation());
        }
        let bytes = match select(
            std::pin::pin!(bounded_body(&mut req)),
            Delay::from(Duration::from_secs(2)),
        )
        .await
        {
            Either::Left((Ok(bytes), _)) => bytes,
            Either::Left((Err(reply), _)) => return Ok(reply),
            Either::Right(_) => return Ok(Reply::error(408, "body_timeout")),
        };
        let Some((id, update)) = decode_update(&bytes) else {
            return Ok(bad_input());
        };
        let generation = scope.generation(&pin);
        let text = scope.text(&update);
        let storage = self.state.storage();
        let ledger = Ledger::new(&storage, None);
        if text.is_some()
            && ledger.direct_generation()?.as_ref() == Some(&generation)
            && ledger.has_update(id)?
        {
            storage.sync().await?;
            return Ok(Reply::ok(json!({"disposition": "duplicate"})));
        }
        let mut permit_deadline = None;
        let aliases = if pin.paired() && text.is_some() {
            let Some(authority) = self.cutover_authority(None) else {
                return Ok(inactive_generation());
            };
            let permit = if phase == LocalPhase::Production {
                authority.ingress_permit(id).await
            } else {
                authority.permit().await
            };
            let Some(permit) = permit else {
                return Ok(inactive_generation());
            };
            if permit.duplicate == Some(true) {
                return Ok(Reply::ok(json!({"disposition": "duplicate"})));
            }
            permit_deadline = Some(permit.valid_until);
            let Some(aliases) = permit.routes() else {
                return Ok(inactive_generation());
            };
            Some(aliases)
        } else if phase == LocalPhase::EnrolledPlacement {
            let raw = self
                .env
                .var("LOCAL_PLACEMENT_ALIASES")
                .map_or_else(|_| String::new(), |value| value.to_string());
            let Some(aliases) = PlacementAliases::parse(&raw) else {
                return Ok(Reply::error(503, "invalid_placement_aliases"));
            };
            Some(aliases)
        } else {
            None
        };
        let reply = ledger.transaction(&storage, || {
            let at = now();
            let selection = text
                .and_then(|text| match &aliases {
                    Some(aliases) => aliases.select(text),
                    None => Some((Device::Alpha, text)),
                })
                .filter(|(device, _)| pin.paired() || pin.key(*device).is_some());
            let initialized = match ledger.direct_generation()? {
                Some(current) if current == generation => {
                    if text.is_some() && ledger.has_update(id)? {
                        return Ok(Reply::ok(json!({"disposition": "duplicate"})));
                    }
                    true
                }
                None if ledger.empty_schema()? => false,
                _ => return Ok(inactive_generation()),
            };
            if phase == LocalPhase::Production && text.is_some() && ledger.ingress_frozen()? {
                return Ok(inactive_generation());
            }
            if permit_deadline.is_some_and(|deadline| at >= deadline) {
                return Ok(inactive_generation());
            }
            let Some((device, text)) = selection else {
                return Ok(Reply::ok(json!({"disposition": "ignored", "task": null})));
            };
            if !initialized {
                ledger.initialize_direct(generation)?;
            }
            if !ledger.take_pilot_request(at, false)? {
                return ledger.pilot_limit(at);
            }
            ledger.ingest(
                id,
                Some(TelegramTask {
                    route: PrivateRoute {
                        chat_id: scope.chat,
                        message_thread_id: Some(scope.thread),
                    },
                    prompt: text,
                    pin: Some(device),
                }),
                false,
                Some(at),
            )
        })?;
        storage.sync().await?;
        Ok(reply)
    }

    async fn handle_cutover(&self, mut req: Request, phase: LocalPhase) -> Result<Reply> {
        let path = req.path();
        let runner = match path.as_str() {
            "/v1/agent" => {
                let Some(incarnation) = req.headers().get("x-executor-incarnation")? else {
                    return Ok(inactive_generation());
                };
                let placement = match req.headers().get("x-executor-placement")? {
                    Some(name) => match Device::named(&name) {
                        Some(device) => Some(device),
                        None => return Ok(inactive_generation()),
                    },
                    None => None,
                };
                Some(Runner {
                    incarnation,
                    placement,
                })
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
            "/v1/cancel" | "/v1/status" => {
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
        let Some(permit) = authority.permit().await else {
            return Ok(inactive_generation());
        };
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
                    phase,
                    &instance,
                    Some((&authority, permit.key.as_deref())),
                )
                .await
            {
                Ok(agent) => Some(agent),
                Err(reply) => return Ok(reply),
            }
        } else {
            None
        };
        let Some(current) = authority
            .permit()
            .await
            .filter(|current| current.same_runner(&permit))
        else {
            return Ok(inactive_generation());
        };
        let permit_deadline = current.valid_until;
        let storage = self.state.storage();
        let ledger = self.controlled_ledger(&storage, phase)?;
        let Some(attempt_floor) = self.attempt_floor(&ledger, phase, pin)? else {
            return Ok(if phase.direct() {
                inactive_generation()
            } else {
                Reply::error(503, "legacy_cutover_held")
            });
        };
        let mut start = None;
        let (reply, alarm) = if let Some((agent, evidence)) = agent {
            if now() >= permit_deadline
                || (current.reconcile_only
                    && !matches!(
                        agent.command,
                        Command::Hello | Command::Status { .. } | Command::Report { .. }
                    ))
            {
                return Ok(inactive_generation());
            }
            start = match agent.command {
                Command::Start { attempt } => Some((agent.device, attempt)),
                _ => None,
            };
            let status = match agent.command {
                Command::Status { attempt } => Some(attempt),
                _ => None,
            };
            if start
                .map(|(_, attempt)| attempt)
                .or(status)
                .or_else(|| evidence.as_ref().map(|proof| proof.attempt))
                .is_some_and(|attempt| attempt <= attempt_floor)
            {
                return Ok(conflict());
            }
            if pin.paired() && !current.reconcile_only {
                let seen = &self.runner_seen[agent.device as usize];
                let signed_at = agent.at_ms.min(now());
                seen.set(Some(seen.get().map_or(signed_at, |at| at.max(signed_at))));
            }
            if let Some(reply) = ledger.read_agent(&agent, now(), &instance, true)? {
                let reply = if current.reconcile_only {
                    reply.reconcile_only()
                } else {
                    reply
                };
                return Ok(reply.authority_deadline(permit_deadline));
            }
            ledger.transaction(&storage, || {
                let at = now();
                if at >= permit_deadline {
                    return Ok((inactive_generation(), None));
                }
                if !ledger.take_pilot_request(at, false)? {
                    return Ok((ledger.pilot_limit(at)?, None));
                }
                ledger.mutate_agent(
                    agent,
                    at,
                    Some(Execution::Cutover {
                        instance: &instance,
                        valid_until: permit_deadline,
                        placement: authority.runner().and_then(|runner| runner.placement),
                        reconcile_only: current.reconcile_only,
                    }),
                    evidence,
                    &storage,
                    true,
                )
            })?
        } else if path == "/v1/telegram" {
            let Some((id, update)) = decode_update(&bytes) else {
                return Ok(bad_input());
            };
            let input = telegram_input(&update).map(|mut task| {
                task.pin = Some(Device::Alpha);
                task
            });
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
            struct TaskQuery {
                task: String,
            }
            let Ok(query) = serde_json::from_slice::<TaskQuery>(&bytes) else {
                return Ok(bad_input());
            };
            if query.task.is_empty() || query.task.len() > 64 {
                return Ok(bad_input());
            }
            if path == "/v1/status" {
                let Some(task) = ledger.placed_task(&query.task)? else {
                    return Ok(Reply::error(404, "not_found"));
                };
                let online = current.has_route(task.placement)
                    && self.runner_seen[task.placement as usize]
                        .get()
                        .is_some_and(|seen| now().saturating_sub(seen) < RUNNER_PRESENCE_MS);
                return Ok(Reply::ok(json!({
                    "placement": task.placement,
                    "runner": if online { "Online runner" } else { "Offline runner" },
                    "attempt_phase": task.attempt_phase,
                    "outcome": (task.attempt_phase == Some(Phase::Unknown)).then_some("Unknown attempt outcome"),
                })));
            }
            (
                ledger.transaction(&storage, || {
                    if now() >= permit_deadline {
                        return Ok(inactive_generation());
                    }
                    ledger.cancel(&query.task)
                })?,
                None,
            )
        };
        settle_alarm(alarm).await?;
        storage.sync().await?;
        let Some(final_permit) = authority
            .permit()
            .await
            .filter(|current| current.same_runner(&permit))
        else {
            return Ok(inactive_generation());
        };
        if let Some((device, attempt)) = start
            && !ledger.grant_current(device, attempt, permit_deadline, now())?
        {
            return Ok(conflict());
        }
        Ok(reply.authority_deadline(final_permit.valid_until))
    }

    async fn handle(
        &self,
        mut req: Request,
        phase: LocalPhase,
        pin: Option<NonZeroU32>,
    ) -> Result<Reply> {
        let executor = phase.executor();
        let pilot = phase == LocalPhase::Pilot;
        let placement = phase == LocalPhase::Placement;
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
                let Some((id, update)) = decode_update(&bytes) else {
                    return Ok(bad_input());
                };
                let input = if placement {
                    let raw = self
                        .env
                        .var("LOCAL_PLACEMENT_ALIASES")
                        .map_or_else(|_| String::new(), |var| var.to_string());
                    let Some(aliases) = PlacementAliases::parse(&raw) else {
                        return Ok(Reply::error(503, "invalid_placement_aliases"));
                    };
                    aliases.input(&update)
                } else {
                    telegram_input(&update)
                };
                if pilot
                    && (id != PILOT_UPDATE
                        || !input.as_ref().is_some_and(|task| {
                            task.route.message_thread_id == Some(PILOT_TOPIC)
                                && task.prompt == PILOT_PROMPT
                        }))
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
                if let Some(reply) = ledger.read_agent(&agent, now(), &instance, placement)? {
                    return Ok(reply);
                }
                let drop_reply = fault.as_deref() == Some("after_commit")
                    && matches!(
                        agent.command,
                        Command::Start { .. } | Command::Report { .. }
                    );
                let execution = match phase {
                    LocalPhase::Protocol | LocalPhase::Placement => None,
                    LocalPhase::Executor => Some(Execution::Local {
                        instance: &instance,
                        reconcile_only: frozen,
                    }),
                    LocalPhase::Pilot => Some(Execution::HostedPilot),
                    LocalPhase::Cutover
                    | LocalPhase::Production
                    | LocalPhase::EnrolledPlacement
                    | LocalPhase::PairedPlacement => {
                        unreachable!()
                    }
                };
                let (reply, alarm) = ledger.transaction(&storage, || {
                    ledger.mutate_agent(agent, now(), execution, evidence, &storage, placement)
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
                if placement
                    && matches!(
                        fixture,
                        Fixture::Schedule { .. } | Fixture::Disable { .. } | Fixture::Wake { .. }
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
        authority: Option<(&Authority<'_>, Option<&str>)>,
    ) -> std::result::Result<(Agent, Option<Evidence>), Reply> {
        let unauthorized = || Reply::error(401, "unauthorized");
        let agent = serde_json::from_slice::<Agent>(bytes).map_err(|_| bad_input())?;
        let scoped = authority
            .and_then(|(authority, _)| authority.runner())
            .and_then(|runner| {
                runner
                    .placement
                    .map(|placement| (placement, &runner.incarnation))
            });
        if agent.at_ms.abs_diff(now()) > 30_000
            || (phase.executor()
                && agent.device != scoped.map_or(Device::Alpha, |(device, _)| device))
        {
            return Err(unauthorized());
        }
        let key = match authority {
            Some((authority, key)) => key
                .or_else(|| authority.pin().key(agent.device))
                .ok_or_else(unauthorized)?
                .to_owned(),
            None => {
                let binding = match phase {
                    LocalPhase::Pilot => "PILOT_ALPHA_KEY".to_owned(),
                    LocalPhase::Executor => "EXECUTOR_ALPHA_KEY".to_owned(),
                    LocalPhase::Protocol | LocalPhase::Placement => {
                        device_key_binding(agent.device)
                    }
                    LocalPhase::Cutover
                    | LocalPhase::Production
                    | LocalPhase::EnrolledPlacement
                    | LocalPhase::PairedPlacement => {
                        return Err(unauthorized());
                    }
                };
                self.env
                    .var(&binding)
                    .map_err(|_| unauthorized())?
                    .to_string()
            }
        };
        let mut decoded = [0; 32];
        hex::decode_to_slice(key, &mut decoded).map_err(|_| unauthorized())?;
        if phase == LocalPhase::Placement
            && Device::ALL.iter().any(|&device| {
                device.name() != agent.device.name()
                    && self.env.var(&device_key_binding(device)).is_ok_and(|key| {
                        let mut other = [0; 32];
                        hex::decode_to_slice(key.to_string(), &mut other).is_ok()
                            && other == decoded
                    })
            })
        {
            return Err(unauthorized());
        }
        let signature = req
            .headers()
            .get("x-agent-signature")
            .ok()
            .flatten()
            .ok_or_else(unauthorized)?;
        let message = match scoped {
            Some((device, incarnation)) => {
                executor_wire::enrolled_request_bytes(instance, device.name(), incarnation, bytes)
            }
            None => request_bytes(instance, bytes),
        };
        if !signed_by(&decoded, &signature, &message).await {
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
            let message = match scoped {
                Some((device, incarnation)) => executor_wire::enrolled_evidence_bytes(
                    instance,
                    device.name(),
                    incarnation,
                    &proof.payload,
                ),
                None => evidence_bytes_for(instance, &proof.payload),
            };
            if !signed_by(&decoded, &proof.signature, &message).await {
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

fn device_key_binding(device: Device) -> String {
    format!("DEVICE_{}_KEY", device.name().to_ascii_uppercase())
}

fn decode_update(bytes: &[u8]) -> Option<(i64, Value)> {
    let update: Value = serde_json::from_slice(bytes).ok()?;
    let id = update["update_id"]
        .as_i64()
        .filter(|id| (0..=i32::MAX as i64).contains(id))?;
    Some((id, update))
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
