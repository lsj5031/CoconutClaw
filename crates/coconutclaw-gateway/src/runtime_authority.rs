use std::num::NonZeroU32;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use wasm_bindgen::{JsCast, prelude::wasm_bindgen};
use wasm_bindgen_futures::JsFuture;
use worker::{Env, Response, Result, Url, web_sys};

use crate::ledger::{Delivery, Receipt};
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
    pub executor_key: String,
}

#[derive(Deserialize)]
struct Version {
    id: String,
}

impl Pin {
    pub fn read(env: &Env) -> Option<Self> {
        let pin: Self = serde_json::from_str(&env.var("CUTOVER_TARGET").ok()?.to_string()).ok()?;
        let host = Url::parse(&format!("https://{}", pin.hostname)).ok()?;
        (host.host_str() == Some(&pin.hostname)
            && host.as_str() == format!("https://{}/", pin.hostname)
            && pin.hostname.contains('.')
            && reference(&pin.telegram_credential_version)
            && lower_hex(&pin.revision, 40)
            && lower_hex(&pin.object_id, 64)
            && lower_hex(&pin.custody_digest, 64)
            && lower_hex(&pin.executor_key, 64))
        .then_some(pin)
    }

    pub fn agent_instance(&self) -> String {
        format!("bigfur:{}:{}", self.generation, self.object_id)
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
    pub runner_incarnation: Option<String>,
    origin: Url,
    bearer: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Permit {
    nonce: String,
    target: Target,
    high_water: NonZeroU32,
    object_incarnation: String,
    runner_incarnation: Option<String>,
    valid_until: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SendReply {
    nonce: String,
    message_id: i64,
}

impl<'a> Authority<'a> {
    pub fn read(env: &Env, object_incarnation: &'a str, runner: Option<String>) -> Option<Self> {
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
            || runner.as_deref().is_some_and(|value| !lower_hex(value, 64))
        {
            return None;
        }
        Some(Self {
            target: Target {
                pin: Pin::read(env)?,
                version_id: version.id,
            },
            object_incarnation,
            runner_incarnation: runner,
            origin,
            bearer,
        })
    }

    pub fn pin(&self) -> &Pin {
        &self.target.pin
    }

    async fn call<T: DeserializeOwned>(
        &self,
        path: &str,
        nonce: &str,
        delivery: Value,
    ) -> Result<T> {
        let url = self.origin.join(path)?;
        let request = json!({
            "nonce": nonce,
            "target": self.target,
            "objectIncarnation": self.object_incarnation,
            "runnerIncarnation": self.runner_incarnation,
            "delivery": delivery,
        });
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

    pub async fn check(&self) -> Option<i64> {
        let nonce = nonce();
        let started = now();
        let permit: Permit = self.call("/v1/check", &nonce, Value::Null).await.ok()?;
        (permit.nonce == nonce
            && permit.target == self.target
            && permit.high_water == self.pin().generation
            && permit.object_incarnation == self.object_incarnation
            && permit.runner_incarnation == self.runner_incarnation
            && permit.valid_until <= started + PERMIT_MS
            && permit.valid_until > now())
        .then_some(permit.valid_until)
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
