use std::num::NonZeroU32;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use wasm_bindgen::{JsCast, prelude::wasm_bindgen};
use wasm_bindgen_futures::JsFuture;
use worker::{Env, Response, Result, Url, web_sys};

use crate::ledger::{Delivery, Receipt};
use crate::protocol::Device;
use crate::protocol::{PairedRoute, PlacementAliases};
use crate::worker_app::{collect_body, now};

const PERMIT_MS: i64 = 2_000;

#[wasm_bindgen(inline_js = "
export function nonce() {
    return crypto.randomUUID();
}
export function askAuthority(url, bearer, body) {
    return fetch(url, {
        method: 'POST', body, redirect: 'manual', cache: 'no-store',
        headers: { 'authorization': 'Bearer ' + bearer, 'content-type': 'application/json' },
        signal: AbortSignal.timeout(1500)
    });
}
")]
extern "C" {
    pub fn nonce() -> String;
    #[wasm_bindgen(js_name = askAuthority)]
    fn ask_authority(url: &str, bearer: &str, body: &str) -> js_sys::Promise;
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Target {
    pin: Pin,
    version_id: String,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Pin {
    pub generation: NonZeroU32,
    revision: String,
    pub hostname: String,
    pub object_id: String,
    pub custody_digest: String,
    telegram_credential_version: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    executor_key: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    executor_keys: Option<ExecutorKeys>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pairing: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ExecutorKeys {
    alpha: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    beta: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    gamma: Option<String>,
}

fn present<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
struct Version {
    id: String,
}

impl Pin {
    pub fn read(env: &Env) -> Option<Self> {
        let pin: Self = serde_json::from_str(&env.var("CUTOVER_TARGET").ok()?.to_string()).ok()?;
        let host = Url::parse(&format!("https://{}", pin.hostname)).ok()?;
        let phase = env.var("PHASE").ok()?.to_string();
        let valid_keys = match (
            &pin.executor_key,
            &pin.executor_keys,
            pin.pairing.as_deref(),
        ) {
            (Some(key), None, None) => {
                !matches!(
                    phase.as_str(),
                    "phase2-placement-local" | "phase2-pairing-local"
                ) && pin.hostname != "127.0.0.1"
                    && lower_hex(key, 64)
            }
            (None, Some(keys), None) => {
                phase == "phase2-placement-local"
                    && pin.hostname == "127.0.0.1"
                    && lower_hex(&keys.alpha, 64)
                    && keys
                        .beta
                        .iter()
                        .chain(&keys.gamma)
                        .all(|key| lower_hex(key, 64) && key != &keys.alpha)
                    && (keys.beta.is_none() || keys.beta != keys.gamma)
            }
            (None, None, Some("fixture-v1")) => {
                phase == "phase2-pairing-local"
                    && pin.hostname == "127.0.0.1"
                    && pin.generation == NonZeroU32::MIN
            }
            (None, None, Some("production-v1")) => {
                phase == "phase2-production" && pin.hostname == "bigfur.liu.nz"
            }
            _ => false,
        };
        (host.host_str() == Some(&pin.hostname)
            && host.as_str() == format!("https://{}/", pin.hostname)
            && pin.hostname.contains('.')
            && reference(&pin.telegram_credential_version)
            && lower_hex(&pin.revision, 40)
            && lower_hex(&pin.object_id, 64)
            && lower_hex(&pin.custody_digest, 64)
            && valid_keys)
            .then_some(pin)
    }

    pub fn key(&self, device: Device) -> Option<&str> {
        match (&self.executor_keys, device) {
            (Some(keys), Device::Alpha) => Some(&keys.alpha),
            (Some(keys), Device::Beta) => keys.beta.as_deref(),
            (Some(keys), Device::Gamma) => keys.gamma.as_deref(),
            (None, Device::Alpha) => self.executor_key.as_deref(),
            (None, _) => None,
        }
    }

    pub fn agent_instance(&self) -> String {
        format!("bigfur:{}:{}", self.generation, self.object_id)
    }

    pub fn paired(&self) -> bool {
        self.pairing.is_some()
    }
}

fn reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._/-".contains(&byte))
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub struct Authority<'a> {
    target: Target,
    object_incarnation: &'a str,
    runner: Option<Runner>,
    origin: Url,
    bearer: String,
}

pub struct Runner {
    pub incarnation: String,
    pub placement: Option<Device>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Permit {
    nonce: String,
    target: Target,
    high_water: NonZeroU32,
    object_incarnation: String,
    runner_incarnation: Option<String>,
    placement: Option<Device>,
    pub valid_until: i64,
    #[serde(default, deserialize_with = "present")]
    pub key: Option<String>,
    #[serde(default, deserialize_with = "present")]
    routes: Option<Vec<PairedRoute>>,
    #[serde(default)]
    pub reconcile_only: bool,
}

impl Permit {
    pub fn routes(self) -> Option<PlacementAliases> {
        PlacementAliases::paired(self.routes?)
    }

    pub fn has_route(&self, device: Device) -> bool {
        self.routes
            .as_ref()
            .is_some_and(|routes| routes.iter().any(|route| route.placement == device))
    }

    pub fn same_runner(&self, previous: &Self) -> bool {
        self.key == previous.key && self.reconcile_only == previous.reconcile_only
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SendReply {
    nonce: String,
    message_id: i64,
}

impl<'a> Authority<'a> {
    pub fn read(env: &Env, object_incarnation: &'a str, runner: Option<Runner>) -> Option<Self> {
        let origin = Url::parse(&env.var("CUTOVER_AUTHORITY_ORIGIN").ok()?.to_string()).ok()?;
        let bearer = env.secret("CUTOVER_AUTHORITY_BEARER").ok()?.to_string();
        let version: Version = env.object_var("CF_VERSION_METADATA").ok()?;
        if origin.scheme() != "https"
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || !lower_hex(&bearer, 64)
            || !reference(&version.id)
            || runner
                .as_ref()
                .is_some_and(|runner| !lower_hex(&runner.incarnation, 64))
        {
            return None;
        }
        let pin = Pin::read(env)?;
        if runner.as_ref().is_some_and(|runner| {
            runner.placement.is_some() != (pin.executor_keys.is_some() || pin.pairing.is_some())
                || (pin.pairing.is_none()
                    && pin.key(runner.placement.unwrap_or(Device::Alpha)).is_none())
        }) {
            return None;
        }
        Some(Self {
            target: Target {
                pin,
                version_id: version.id,
            },
            object_incarnation,
            runner,
            origin,
            bearer,
        })
    }

    pub fn pin(&self) -> &Pin {
        &self.target.pin
    }

    pub fn runner(&self) -> Option<&Runner> {
        self.runner.as_ref()
    }

    async fn call<T: DeserializeOwned>(
        &self,
        path: &str,
        nonce: &str,
        delivery: Value,
    ) -> Result<T> {
        let url = self.origin.join(path)?;
        let mut request = json!({
            "nonce": nonce,
            "target": self.target,
            "objectIncarnation": self.object_incarnation,
            "runnerIncarnation": self.runner().map(|runner| &runner.incarnation),
            "delivery": delivery,
        });
        if let Some(placement) = self.runner().and_then(|runner| runner.placement) {
            request["placement"] = json!(placement);
        }
        let mut response = Response::from(
            JsFuture::from(ask_authority(
                url.as_str(),
                &self.bearer,
                &request.to_string(),
            ))
            .await?
            .dyn_into::<web_sys::Response>()?,
        );
        if response.status_code() != 200 {
            return Err("authority denied".into());
        }
        let bytes = collect_body(response.stream()?)
            .await
            .map_err(|_| "invalid authority response")?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn permit(&self) -> Option<Permit> {
        let nonce = nonce();
        let started = now();
        let permit: Permit = self.call("/v1/check", &nonce, Value::Null).await.ok()?;
        let enrollment = match (self.pin().pairing.is_some(), self.runner().is_some()) {
            (false, _) => permit.key.is_none() && permit.routes.is_none(),
            (true, true) => {
                permit.routes.is_none() && permit.key.as_ref().is_some_and(|key| lower_hex(key, 64))
            }
            (true, false) => permit.key.is_none() && permit.routes.is_some(),
        };
        (enrollment
            && permit.nonce == nonce
            && permit.target == self.target
            && (if permit.reconcile_only {
                self.runner().is_some()
                    && self.pin().hostname == "bigfur.liu.nz"
                    && permit.high_water >= self.pin().generation
            } else {
                permit.high_water == self.pin().generation
            })
            && permit.object_incarnation == self.object_incarnation
            && permit.runner_incarnation.as_ref()
                == self.runner().map(|runner| &runner.incarnation)
            && permit.placement == self.runner().and_then(|runner| runner.placement)
            && permit.valid_until <= started + PERMIT_MS
            && permit.valid_until > now())
        .then_some(permit)
    }

    pub async fn check(&self) -> Option<i64> {
        Some(self.permit().await?.valid_until)
    }

    pub async fn send(&self, delivery: &Delivery) -> Receipt {
        let nonce = nonce();
        let sent = self
            .call::<SendReply>(
                "/v1/send",
                &nonce,
                json!({
                    "task": delivery.task,
                    "sendNumber": delivery.sends,
                    "chatId": delivery.chat_id,
                    "messageThreadId": delivery.thread_id,
                    "text": delivery.text,
                }),
            )
            .await;
        match sent {
            Ok(reply) if reply.nonce == nonce && reply.message_id > 0 => {
                Receipt::Delivered(reply.message_id)
            }
            _ => Receipt::Unknown,
        }
    }
}
