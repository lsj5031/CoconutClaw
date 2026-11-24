use crate::store::Store;
use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use serde_json::{Value, json};

pub(crate) mod media;
pub(crate) mod telegram;

pub(crate) fn cleanup_attachment_path(path: &std::path::Path) {
    if let Err(err) = std::fs::remove_file(path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("failed to remove attachment {}: {err:#}", path.display());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryTarget {
    Telegram {
        chat_id: String,
        thread_id: Option<String>,
    },
    Stdout,
}
impl DeliveryTarget {
    pub(crate) fn display_id(&self) -> &str {
        match self {
            Self::Telegram { chat_id, .. } => chat_id,
            Self::Stdout => "local",
        }
    }

    pub(crate) fn telegram_route(&self) -> Option<String> {
        match self {
            Self::Telegram { chat_id, thread_id } => Some(
                crate::session::SessionKey {
                    platform: crate::session::SessionPlatform::Telegram,
                    root_id: chat_id.clone(),
                    thread_id: thread_id.clone(),
                }
                .id(),
            ),
            Self::Stdout => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskSource {
    Telegram,
    Local,
}
impl TaskSource {
    pub(crate) fn channel_name(&self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
            Self::Local => "local",
        }
    }
}

pub(crate) fn parse_delivery_target(raw: Option<&str>) -> Option<DeliveryTarget> {
    let value: Value = serde_json::from_str(raw?).ok()?;
    match value.get("kind")?.as_str()? {
        "telegram" => {
            let chat_id = value.get("chat_id")?.as_str()?;
            if chat_id.is_empty() || !chat_id.bytes().all(|ch| ch.is_ascii_digit()) {
                return None;
            }
            let thread_id = match value.get("message_thread_id") {
                None | Some(Value::Null) => None,
                Some(Value::String(raw)) => Some(raw.clone()),
                Some(Value::Number(number)) => Some(number.as_i64()?.to_string()),
                _ => return None,
            };
            let target = DeliveryTarget::Telegram {
                chat_id: chat_id.to_string(),
                thread_id,
            };
            crate::telegram::telegram_route_params(&target.telegram_route()?).ok()?;
            Some(target)
        }
        "stdout" => Some(DeliveryTarget::Stdout),
        _ => None,
    }
}
pub(crate) fn serialize_delivery_target(target: &DeliveryTarget) -> String {
    match target {
        DeliveryTarget::Telegram { chat_id, thread_id } => {
            json!({"kind":"telegram","chat_id":chat_id,"message_thread_id":thread_id}).to_string()
        }
        DeliveryTarget::Stdout => json!({"kind":"stdout"}).to_string(),
    }
}

#[derive(Debug, Default)]
pub(crate) struct DeliveryState {
    completed_ops: Vec<String>,
    pub(crate) text_plan: Option<Value>,
    pub(crate) operations: serde_json::Map<String, Value>,
}
impl DeliveryState {
    pub(crate) fn from_json(raw: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(raw)?;
        let (completed_ops, text_plan) = if value.is_array() {
            (serde_json::from_value(value)?, None)
        } else {
            (
                serde_json::from_value(value.get("completed_ops").cloned().unwrap_or(json!([])))?,
                value.get("text_plan").filter(|v| !v.is_null()).cloned(),
            )
        };
        Ok(Self {
            completed_ops,
            text_plan,
            operations: value_operations(raw)?,
        })
    }
    pub(crate) fn has_telegram_op(&self, op: &str) -> bool {
        self.completed_ops.iter().any(|s| s == op)
    }
    pub(crate) fn mark_telegram_op(&mut self, op: impl Into<String>) {
        let op = op.into();
        if !self.has_telegram_op(&op) {
            self.completed_ops.push(op);
        }
    }
}

pub(crate) fn unstarted_delivery(raw: &str) -> Result<Option<String>> {
    let mut value: Value = serde_json::from_str(raw)?;
    let empty = json!({"completed_ops":[],"operations":{},"text_plan":null});
    if value == json!([]) || value == empty {
        return Ok(Some(raw.into()));
    }
    let plan = &value["text_plan"];
    if let Some(markdown) = plan["markdown"].as_str()
        && *plan == json!({"kind":"rich","markdown":markdown,"phase":"ready","attempts":0})
    {
        value["text_plan"] = Value::Null;
        if value == empty {
            return Ok(Some(value.to_string()));
        }
    }
    Ok(None)
}
#[derive(Clone, Copy)]
pub(crate) struct TaskDispatch<'a> {
    pub(crate) task_id: DeliveryKey,
    pub(crate) output: &'a str,
    pub(crate) progress_message_id: Option<&'a str>,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum DeliveryKey {
    Task(i64),
    Telegram(i64),
    Notification(i64),
}

pub(crate) fn reserve_telegram_delivery(
    store: &Store,
    key: DeliveryKey,
    state: &DeliveryState,
) -> Result<bool> {
    let tx = rusqlite::Transaction::new_unchecked(
        &store.conn,
        rusqlite::TransactionBehavior::Immediate,
    )?;
    if store.legacy_admission_frozen()? {
        return Ok(false);
    }
    persist_delivery_state(store, key, state)?;
    tx.commit()?;
    Ok(true)
}

pub(crate) fn persist_delivery_state(
    store: &Store,
    key: DeliveryKey,
    state: &DeliveryState,
) -> Result<()> {
    if let DeliveryKey::Notification(id) = key {
        return store.persist_notification_delivery(id,
            &json!({"completed_ops":state.completed_ops,"text_plan":state.text_plan,"operations":state.operations}).to_string(),
            !state.has_telegram_op("telegram:text") && state.text_plan.as_ref().is_some_and(|p|p["phase"]=="sending"));
    }
    let (sql, id) = match key {
        DeliveryKey::Task(id) => (
            "UPDATE task_payloads SET delivery_state=?2 WHERE task_run_id=?1",
            id,
        ),
        DeliveryKey::Telegram(id) => (
            "UPDATE telegram_ingress SET delivery_state=?2 WHERE seq=?1",
            id,
        ),
        DeliveryKey::Notification(_) => unreachable!("handled above"),
    };
    anyhow::ensure!(store.conn.execute(
        sql,
        rusqlite::params![
            id,
            json!({"completed_ops":state.completed_ops,"text_plan":state.text_plan,"operations":state.operations}).to_string()
        ],
    )? == 1,"delivery owner missing; no external effect authorized");
    Ok(())
}

/// Shares the scheduler's existing delivery lock and native durable sender.
/// Freeze bounded candidates and release the SQLite read before any HTTP.
pub(crate) fn deliver_notifications(cfg: &RuntimeConfig, store: &Store) -> Result<()> {
    if store.legacy_admission_frozen()? {
        return Ok(());
    }
    let rows=store.conn.prepare(
        "SELECT id,target,output,delivery_state FROM runtime_notifications WHERE disposition='pending' ORDER BY id LIMIT 32")?
        .query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut client = None;
    for (id, target, output, raw) in rows {
        let state = DeliveryState::from_json(&raw)?;
        let uncertain = notification_delivery_ambiguous(store, id)?;
        let disposition = if uncertain {
            "unknown"
        } else if state.has_telegram_op("telegram:text") {
            "sent"
        } else if !store.notification_is_current(id)? {
            "superseded"
        } else if state
            .text_plan
            .as_ref()
            .is_some_and(|p| p["attempts"].as_u64().is_some_and(|n| n >= 3))
        {
            "suspended"
        } else {
            let Some(target) = parse_delivery_target(Some(&target)) else {
                continue;
            };
            let DeliveryTarget::Telegram { chat_id, thread_id } = &target else {
                continue;
            };
            if !crate::util::is_allowed_chat(cfg, Some(chat_id))
                || (thread_id.is_some() && cfg.telegram_topics_enabled == Some(false))
            {
                continue;
            }
            if client.is_none() {
                client = Some(crate::telegram::build_telegram_client(cfg)?);
            }
            match dispatch_task_output(
                store,
                cfg,
                client.as_ref(),
                &target,
                TaskDispatch {
                    task_id: DeliveryKey::Notification(id),
                    output: &output,
                    progress_message_id: None,
                },
                &raw,
            ) {
                Ok(true) => "sent",
                Ok(false) | Err(_) => {
                    if notification_delivery_ambiguous(store, id)? {
                        "unknown"
                    } else {
                        continue;
                    }
                }
            }
        };
        store.conn.execute(
            "UPDATE runtime_notifications SET disposition=?2 WHERE id=?1",
            rusqlite::params![id, disposition],
        )?;
    }
    Ok(())
}

fn notification_delivery_ambiguous(store: &Store, id: i64) -> Result<bool> {
    Ok(store.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM runtime_notifications n,json_tree(n.delivery_state) j
         WHERE n.id=?1 AND j.key='phase' AND (j.value='unknown' OR (j.value='sending'
           AND NOT EXISTS(SELECT 1 FROM json_each(n.delivery_state,'$.completed_ops') WHERE value='telegram:text'))))",
        [id],|r|r.get(0))?)
}
pub(crate) fn dispatch_task_output(
    store: &Store,
    cfg: &RuntimeConfig,
    client: Option<&Client>,
    target: &DeliveryTarget,
    request: TaskDispatch<'_>,
    raw_state: &str,
) -> Result<bool> {
    match target {
        DeliveryTarget::Stdout => {
            println!("{}", request.output);
            Ok(true)
        }
        DeliveryTarget::Telegram { .. } => {
            let client = client.ok_or_else(|| anyhow::anyhow!("Telegram client missing"))?;
            let mut state = DeliveryState::from_json(raw_state)?;
            telegram::dispatch_task_output(
                store,
                cfg,
                client,
                request,
                &mut state,
                target.telegram_route().as_deref(),
            )
        }
    }
}

fn value_operations(raw: &str) -> Result<serde_json::Map<String, Value>> {
    let value: Value = serde_json::from_str(raw)?;
    Ok(value
        .get("operations")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
