//! Button-first local controls. Only authenticated ingress can create these wire plans.
use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use rusqlite::{OptionalExtension, params};
use serde_json::json;

use crate::{scheduler::SessionScheduler, session::SessionKey, store::Store};

pub(crate) const PREFIX: &str = "COCONUT_UI_V1:";
// Product interaction lifetimes, not evidence of runtime liveness.
const BUTTON_LIFETIME: i64 = 24 * 60 * 60;
const INPUT_LIFETIME: i64 = 15 * 60;

#[derive(Debug)]
pub(crate) struct Message {
    pub(crate) text: String,
    pub(crate) keyboard: serde_json::Value,
    pub(crate) edit: Option<String>,
    pub(crate) callback_id: Option<String>,
}

fn epoch() -> i64 {
    chrono::Utc::now().timestamp()
}

fn token(store: &Store) -> Result<String> {
    Ok(store
        .conn
        .query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?)
}

fn button(
    store: &Store,
    session: &SessionKey,
    label: &str,
    action: &str,
    navigation_only: bool,
) -> Result<serde_json::Value> {
    let id = token(store)?;
    let label = match (label, action.split_once(':')) {
        ("Use remote", Some(("node-select", id))) => {
            format!("Use {}", crate::nodes::display_name(store, id)?)
        }
        (
            "Run details" | "Schedule details" | "Cancel run" | "Approve" | "Reject",
            Some((_, id)),
        ) => {
            format!("{label} #{id}")
        }
        _ => label.to_string(),
    };
    store.conn.execute(
        "INSERT INTO telegram_ui_controls(token,session_id,actor,action,expires_at,navigation_only) VALUES(?1,?2,?3,?4,?5,?6)",
        params![id, session.id(), session.root_id, action, epoch() + BUTTON_LIFETIME,navigation_only],
    )?;
    Ok(json!({"text":label,"callback_data":format!("ui:{id}")}))
}

fn controls(
    store: &Store,
    session: &SessionKey,
    actions: &[(&str, String)],
    navigation_only: bool,
) -> Result<serde_json::Value> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut item_row = false;
    for (label, action) in actions {
        let kind = action.split(':').next().unwrap_or("");
        let starts_item = matches!(kind, "run" | "schedule")
            && matches!(*label, "Run details" | "Schedule details");
        let continues_item = matches!(kind, "cancel" | "approve" | "reject")
            && matches!(*label, "Cancel run" | "Approve" | "Reject");
        if !row.is_empty() && (starts_item || item_row != continues_item) {
            rows.push(std::mem::take(&mut row));
        }
        item_row = starts_item || continues_item;
        row.push(button(store, session, label, action, navigation_only)?);
        if !item_row && row.len() == 2 {
            rows.push(std::mem::take(&mut row));
        }
    }
    if !row.is_empty() {
        rows.push(row);
    }
    Ok(json!({"inline_keyboard":rows}))
}

fn encoded(
    text: String,
    keyboard: serde_json::Value,
    edit: Option<String>,
    callback_id: Option<String>,
) -> Result<String> {
    let mut units = 0;
    let mut bounded = String::new();
    for ch in text.chars() {
        units += ch.len_utf16();
        if units > 3500 {
            break;
        }
        bounded.push(ch);
    }
    if bounded.len() < text.len() {
        bounded.push_str("\n\nMessage shortened to fit Telegram. Some text is not shown.");
    }
    Ok(format!(
        "{PREFIX}{}",
        json!({"text":bounded,"keyboard":keyboard,"edit":edit,"callback_id":callback_id})
    ))
}

pub(crate) fn decode(output: &str) -> Option<Message> {
    let value: serde_json::Value = serde_json::from_str(output.strip_prefix(PREFIX)?).ok()?;
    Some(Message {
        text: value.get("text")?.as_str()?.into(),
        keyboard: value.get("keyboard")?.clone(),
        edit: value.get("edit")?.as_str().map(str::to_string),
        callback_id: value
            .get("callback_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    })
}

pub(crate) fn runtime_control(store: &Store, session: &SessionKey, text: &str) -> Result<bool> {
    let mut parts = text.splitn(2, char::is_whitespace);
    if parts.next().and_then(|p| p.split('@').next()) != Some("/button") {
        return Ok(false);
    }
    let id = parts.next().unwrap_or("").trim();
    let action: Option<String> = store.conn.query_row(
        "SELECT action FROM telegram_ui_controls WHERE token=?1 AND session_id=?2 AND actor=?3 AND consumed=0 AND expires_at>?4",
        params![id,session.id(),session.root_id,epoch()], |r|r.get(0)).optional()?;
    Ok(action.is_some_and(|a| {
        a.starts_with("cancel:") || a.starts_with("approve:") || a.starts_with("reject:")
    }))
}

/// Observation may pass an older Unknown, but never repeats its effect.
pub(crate) fn observation_control(
    store: &Store,
    session: &SessionKey,
    command: &str,
    arg: &str,
) -> Result<bool> {
    Ok(match command {
        "/start" | "/help" | "/status" | "/nodes" | "/tasks" | "/schedules" | "/notifications" => {
            true
        }
        "/memory" => {
            arg.is_empty()
                || matches!(
                    arg.split_whitespace().next(),
                    Some("list" | "search" | "get")
                )
        }
        "/preferences" => arg.is_empty() || arg == "list",
        "/button" => {
            let action:Option<String>=store.conn.query_row(
                "SELECT action FROM telegram_ui_controls WHERE token=?1 AND session_id=?2 AND actor=?3 AND expires_at>?4 AND consumed=0",
                params![arg,session.id(),session.root_id,epoch()],|r|r.get(0)).optional()?;
            action.is_none_or(|a| {
                a == "home"
                    || a == "help"
                    || a == "status"
                    || a == "nodes"
                    || a == "tasks"
                    || a == "schedules"
                    || a == "memory"
                    || a == "preferences"
                    || a.starts_with("run:")
                    || a.starts_with("schedule:")
                    || a.starts_with("tasks-page:")
                    || a.starts_with("schedules-page:")
                    || a.starts_with("notifications:")
            })
        }
        _ => false,
    })
}

fn callback_message(store: &Store, update: Option<&str>, token: &str) -> Result<Option<String>> {
    let navigation: bool = store.conn.query_row(
        "SELECT coalesce((SELECT navigation_only FROM telegram_ui_controls WHERE token=?1),0)",
        [token],
        |r| r.get(0),
    )?;
    if !navigation {
        return Ok(None);
    }
    let raw: Option<String> = store
        .conn
        .query_row(
            "SELECT payload FROM telegram_ingress WHERE update_id=?1",
            [update],
            |r| r.get(0),
        )
        .optional()?;
    Ok(raw
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .and_then(|v| {
            v["callback_query"]["message"]["message_id"]
                .as_i64()
                .map(|id| id.to_string())
        }))
}

const HELP: &str = "Home shows tasks and settings for this conversation. Reports are saved snapshots, not live updates. New snapshot sends a fresh report without changing the old one.\n\nA task is a piece of work; a run is one attempt. Cancel requests a stop, it does not confirm one. Approve or Reject answers a pending approval request.\n\nCommands: /start, /help, /status, /nodes, /fresh (new context, keeps memory), /tasks [run ID], /cancel [run ID], /memory, /preferences, /notifications, /schedules, /approve ID, /reject ID, /reconcile.\n\nUnknown means the outcome is unconfirmed, not that nothing happened. Check Tasks and the conversation before doing the work again. There is no automatic retry. Recovery help explains how to check a delivery receipt.\n\nMemory and preference guides ask you to send /input with a code. Other messages stay ordinary conversation. /input cancel closes the guide. Guides expire after 15 minutes; starting a new guide replaces the old one.";

/// `None` means ordinary conversation, never an unrecognized slash command.
pub(crate) fn handle(
    cfg: &RuntimeConfig,
    store: &Store,
    scheduler: &SessionScheduler,
    session: &SessionKey,
    update: Option<&str>,
    text: &str,
) -> Result<Option<String>> {
    let mut words = text.splitn(2, char::is_whitespace);
    let command = words
        .next()
        .and_then(|s| crate::telegram::command_token(s, cfg.telegram_bot_username.as_deref()))
        .unwrap_or("");
    let arg = words.next().unwrap_or("").trim();
    let mut action = match command {
        "/start" => "home".to_string(),
        "/help" => "help".to_string(),
        "/status" => "status".to_string(),
        "/nodes" => "nodes".to_string(),
        "/tasks" if arg.is_empty() => "tasks".to_string(),
        "/tasks" if arg.parse::<i64>().is_ok_and(|id| id > 0) => format!("run:{arg}"),
        "/schedules" => "schedules".to_string(),
        "/memory" if arg.is_empty() => "memory".to_string(),
        "/preferences" if arg.is_empty() || arg == "list" => "preferences".to_string(),
        "/notifications" => format!("notifications:{arg}"),
        "/button" => {
            let saved: Option<String> = store.conn.query_row(
                "SELECT action FROM telegram_ui_controls WHERE token=?1 AND session_id=?2 AND actor=?3 AND expires_at>?4 AND consumed=0",
                params![arg,session.id(),session.root_id,epoch()], |r|r.get(0)).optional()?;
            saved.unwrap_or_else(|| "invalid".into())
        }
        "/input" => "input".to_string(),
        _ => return Ok(None),
    };
    let mut notice = None;
    if action == "input" {
        if arg == "cancel" {
            store.conn.execute(
                "DELETE FROM telegram_ui_inputs WHERE session_id=?1 AND actor=?2",
                params![session.id(), session.root_id],
            )?;
            notice = Some("Input cancelled. No memory or preference was changed.".to_string());
        } else {
            let (code, value) = arg.split_once(char::is_whitespace).unwrap_or((arg, ""));
            let pending: Option<(String,String,i64)> = store.conn.query_row(
                "SELECT token,kind,expires_at FROM telegram_ui_inputs WHERE session_id=?1 AND actor=?2",
                params![session.id(),session.root_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
            notice = Some(match pending {
                Some((saved,kind,expires)) if expires > epoch() && saved == code && !value.trim().is_empty() => {
                    let value = value.trim();
                    let response = if let Some(operation)=match kind.as_str() {
                        "memory"=>Some("remember"),"memory-search"=>Some("search"),"memory-get"=>Some("get"),
                        "memory-promote"=>Some("promote"),"memory-correct"=>Some("correct"),_=>None,
                    } {
                        store.memory_command(&session.id(), &format!("/memory {operation} {value}"),update,&crate::iso_now(&cfg.timezone))?
                    } else if matches!(kind.as_str(),"preferences"|"preferences-unset") {
                        store.preferences_command(session,&format!("{} {value}",if kind=="preferences"{"set"}else{"unset"}),update,&crate::iso_now(&cfg.timezone))?
                    } else {
                        "Input kind unavailable. Start again from Home.".into()
                    };
                    store.conn.execute("DELETE FROM telegram_ui_inputs WHERE session_id=?1",[session.id()])?;
                    response
                }
                Some((_,_,expires)) if expires <= epoch() => {
                    store.conn.execute("DELETE FROM telegram_ui_inputs WHERE session_id=?1",[session.id()])?;
                    "Input expired. Open Memory or Preferences to start again.".into()
                }
                _ => "No matching input. Use the exact /input code shown in the current guide, or /input cancel. Ordinary messages are not used as input.".into(),
            });
        }
        action = "home".into();
    }
    let observed_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let displayed_at = crate::wording::time(Some(&observed_at), &cfg.timezone);
    let mut edit = None;
    let navigation_only = matches!(action.as_str(), "home" | "help") && notice.is_none();
    let mut actions: Vec<(&str, String)> = Vec::new();
    let mut report = false;
    let body = if let Some(kind) = action.strip_prefix("guide:").filter(|k| {
        matches!(
            *k,
            "memory"
                | "memory-search"
                | "memory-get"
                | "memory-promote"
                | "memory-correct"
                | "preferences"
                | "preferences-unset"
        )
    }) {
        let code = token(store)?;
        store.conn.execute(
            "INSERT INTO telegram_ui_inputs(session_id,actor,token,kind,expires_at) VALUES(?1,?2,?3,?4,?5)
             ON CONFLICT(session_id) DO UPDATE SET actor=excluded.actor,token=excluded.token,kind=excluded.kind,expires_at=excluded.expires_at",
            params![session.id(),session.root_id,code,kind,epoch()+INPUT_LIFETIME])?;
        store.conn.execute(
            "UPDATE telegram_ui_controls SET consumed=1 WHERE token=?1",
            [arg],
        )?;
        actions.push(("Cancel input", format!("input-cancel:{code}")));
        let (title, format) = match kind {
            "memory" => ("Remember", "text to remember"),
            "memory-search" => ("Search memory", "search text"),
            "memory-get" => ("Memory details", "memory ID"),
            "memory-promote" => ("Promote memory", "memory ID"),
            "memory-correct" => ("Correct memory", "active memory ID replacement text"),
            "preferences-unset" => ("Remove preference", "lowercase-key"),
            _ => ("Set preference", "lowercase-key preference text"),
        };
        format!(
            "{}\nSend /input {code} {format}\n\nExpires in 15 minutes. Other messages stay ordinary conversation. A new guide replaces this one; Cancel input closes it.",
            title
        )
    } else if let Some(code) = action.strip_prefix("input-cancel:") {
        let removed = store.conn.execute(
            "DELETE FROM telegram_ui_inputs WHERE session_id=?1 AND actor=?2 AND token=?3",
            params![session.id(), session.root_id, code],
        )?;
        store.conn.execute(
            "UPDATE telegram_ui_controls SET consumed=1 WHERE token=?1",
            [arg],
        )?;
        if removed == 1 {
            "Input cancelled. No memory or preference was changed.".into()
        } else {
            "That input was replaced or expired. The current input is unchanged.".into()
        }
    } else if action == "memory-undo" {
        store.conn.execute(
            "UPDATE telegram_ui_controls SET consumed=1 WHERE token=?1",
            [arg],
        )?;
        store.memory_command(
            &session.id(),
            "/memory undo",
            update,
            &crate::iso_now(&cfg.timezone),
        )?
    } else if let Some(node) = action.strip_prefix("node-select:") {
        let transaction = store
            .conn
            .is_autocommit()
            .then(|| store.conn.unchecked_transaction())
            .transpose()?;
        crate::nodes::selection(store, &session.id(), (node != "local").then_some(node))?;
        store.conn.execute(
            "UPDATE telegram_ui_controls SET consumed=1 WHERE token=?1",
            [arg],
        )?;
        if let Some(transaction) = transaction {
            transaction.commit()?;
        }
        format!(
            "New text tasks go to: {}.\nExisting tasks keep their assigned node. Remote tasks use that node's permitted workspace and do not switch to this computer if it is unreachable.",
            if node == "local" {
                "this computer (local)".into()
            } else {
                crate::nodes::display_name(store, node)?
            }
        )
    } else if action == "fresh" {
        store.insert_boundary_turn(
            &crate::iso_now(&cfg.timezone),
            &session.id(),
            update,
            "telegram",
        )?;
        store.conn.execute(
            "UPDATE telegram_ui_controls SET consumed=1 WHERE token=?1",
            [arg],
        )?;
        "New conversation started. Earlier messages are no longer included; memory and preferences are kept.".into()
    } else if let Some((operation, id)) = action
        .split_once(':')
        .filter(|(op, _)| matches!(*op, "cancel" | "approve" | "reject"))
    {
        let id = id.parse::<i64>()?;
        // Consume before runtime effects. A crash preserves Unknown, never replays
        // an approval or cancellation against a later attempt.
        store.conn.execute(
            "UPDATE telegram_ui_controls SET consumed=1 WHERE token=?1",
            [arg],
        )?;
        if operation == "cancel" {
            if scheduler.cancel_task_for_session(id, Some(&session.id()))? {
                format!(
                    "Cancel requested for run #{id}, not yet confirmed. Open Tasks to check its status."
                )
            } else {
                format!("Run #{id} is no longer cancellable in this conversation/topic.")
            }
        } else {
            scheduler
                .resolve_approval(id, operation, &session.id())?
                .unwrap_or_else(|| {
                    "Approval unavailable or already resolved in this conversation/topic.".into()
                })
        }
    } else {
        match action.as_str() {
            "home" => {
                edit = callback_message(store,update,arg)?;
                actions.extend([
                    ("Status","status"),("Tasks","tasks"),("Nodes","nodes"),("Scheduled tasks","schedules"),
                    ("Memory","memory"),("Preferences","preferences"),("Help","help"),("New conversation","fresh"),
                ].map(|(a,b)|(a,b.to_string())));
                notice.map(|text|format!("Input result at {displayed_at}\n\n{text}")).unwrap_or_else(||"Home\nChoose what to view or change in this conversation.".into())
            }
            "help" => { edit=callback_message(store,update,arg)?; actions.push(("Recovery help","recovery-help".into())); HELP.into() }
            "recovery-help" => "Check a delivery receipt\nIf a reply may have arrived but its receipt is unconfirmed, inspect the conversation first.\n\nUse /reconcile task RUN_ID for a task reply, or /reconcile ack INGRESS_ID for a command reply. If stored receipts prove delivery, you will receive a confirmation command. Copy it exactly to record that receipt.\n\nThis never runs work or sends a reply again. It cannot confirm an uncertain task outcome, command effect, or progress message. If the evidence has changed or is insufficient, the outcome stays unconfirmed. Keep the run or operation ID for local diagnosis.".into(),
            "memory" => {
                report=true;
                actions.push(("Remember","guide:memory".into()));
                actions.extend([("Search","guide:memory-search"),("Get memory","guide:memory-get"),("Promote","guide:memory-promote"),("Correct","guide:memory-correct"),("Undo last memory change","memory-undo")].map(|(label,action)|(label,action.into())));
                format!("Memory\n{}\nActive memories can be recalled. Candidates are not used until promoted. Use Promote to activate a candidate, or Correct to replace an active memory.\nOlder entries: /memory list before-ID.",store.memory_command(&session.id(),"/memory list",update,&crate::iso_now(&cfg.timezone))?)
            }
            "preferences" => {
                report=true;
                actions.push(("Set preference","guide:preferences".into()));
                actions.push(("Remove preference","guide:preferences-unset".into()));
                let value: serde_json::Value=serde_json::from_str(&store.preferences_context(&session.id())?)?;
                let lines=value.as_array().into_iter().flatten().map(|p|format!("{}: {}",p["name"].as_str().unwrap_or("?"),p["value"].as_str().unwrap_or(""))).collect::<Vec<_>>();
                format!("Preferences\n{}\nRemove with /preferences unset key.",if lines.is_empty(){"No conversation preferences.".into()}else{lines.join("\n")})
            }
            a if a.starts_with("notifications:") => {
                report=true;
                let raw=store.notifications_command(session,a.strip_prefix("notifications:").unwrap_or(""),update)?;
                if let Ok(rows)=serde_json::from_str::<Vec<serde_json::Value>>(&raw) {
                    let lines=rows.iter().map(|r|format!("Notification #{}: task #{} / run #{}\nReason: {}\nDelivery: {}{}",
                        r["id"],r["task_id"],r["run_id"],r["reason"].as_str().unwrap_or("unavailable"),
                        crate::wording::delivery(r["delivery"]["state"].as_str().unwrap_or("unavailable")),
                        if r["delivery"]["uncertain"]==true {" (no automatic retry)"}else if r["delivery"]["acknowledged"]==true {" (receipt recorded)"}else{""})).collect::<Vec<_>>();
                    format!("Notifications (up to 8)\n{}",if lines.is_empty(){"No task notifications recorded in this conversation.".into()}else{lines.join("\n\n")})
                } else {raw}
            }
            "invalid" => "This button is invalid, expired, used, or belongs to another conversation/topic. Open /start for current controls.".into(),
            _ => {
                report=true;
                render_snapshot(cfg,&observed_at,store,session,update,&action,&mut actions)?
            }
        }
    };
    if report {
        actions.push(("New snapshot", action.clone()));
    }
    if action != "home" && !actions.iter().any(|(_, a)| a == "home") {
        actions.push(("Home", "home".into()));
    }
    let text = if report {
        format!("Snapshot at {displayed_at}\nThis conversation only\n\n{body}")
    } else {
        body
    };
    Ok(Some(encoded(
        text,
        controls(store, session, &actions, navigation_only)?,
        edit,
        callback_id(store, update)?,
    )?))
}

fn callback_id(store: &Store, update: Option<&str>) -> Result<Option<String>> {
    let raw: Option<String> = store
        .conn
        .query_row(
            "SELECT payload FROM telegram_ingress WHERE update_id=?1",
            [update],
            |r| r.get(0),
        )
        .optional()?;
    Ok(raw
        .and_then(|r| serde_json::from_str::<serde_json::Value>(&r).ok())
        .and_then(|v| v["callback_query"]["id"].as_str().map(str::to_string)))
}

fn render_snapshot(
    cfg: &RuntimeConfig,
    observed_at: &str,
    store: &Store,
    session: &SessionKey,
    update: Option<&str>,
    action: &str,
    actions: &mut Vec<(&'static str, String)>,
) -> Result<String> {
    let snapshot = store.status_snapshot(session, observed_at)?;
    let mut lines = Vec::new();
    match action {
        "status" => {
            lines.push(format!("Status\n{} tasks, {} runs; {} schedules.\nMessages: {} waiting, {} processing, {} unconfirmed.\nLast received: {}\nBased on saved records, not a connection check.",snapshot.task_count,snapshot.run_count,snapshot.schedule_count,snapshot.transport.pending,snapshot.transport.applying,snapshot.transport.unknown,crate::wording::time(snapshot.transport.latest_received_at.as_deref(), &cfg.timezone)));
            if snapshot.transport.unknown > 0 {
                lines.push("Unconfirmed means a message or its reply may already have taken effect. Check Tasks and the conversation before repeating it; no automatic retry.".into());
            }
            let attention = snapshot.tasks.iter().filter(|t| t.attention).count();
            lines.push(format!("Need attention: {attention} of {} tasks shown. Open Tasks or Scheduled tasks for details. Message counts include this request.",snapshot.tasks.len()));
            actions.extend([
                ("Tasks", "tasks".into()),
                ("Scheduled tasks", "schedules".into()),
            ]);
        }
        "nodes" => {
            lines.push(crate::nodes::status(
                store,
                Some(&session.id()),
                &cfg.timezone,
            )?);
            actions.push(("Use local", "node-select:local".into()));
            for node in crate::nodes::choices(store)? {
                actions.push(("Use remote", format!("node-select:{node}")));
            }
        }
        a if a == "schedules" || a.starts_with("schedules-page:") => {
            let offset = a
                .strip_prefix("schedules-page:")
                .and_then(|p| p.parse::<usize>().ok())
                .unwrap_or(0)
                .min(50);
            lines.push(format!("Scheduled tasks: {} total; {} shown on this page.\nNext due is the planned time, not confirmation the task ran.",snapshot.schedule_count,snapshot.schedules.len().saturating_sub(offset).min(10)));
            if snapshot.schedules.is_empty() {
                lines.push("No scheduled tasks in this conversation.".into());
            }
            for s in snapshot.schedules.iter().skip(offset).take(10) {
                lines.push(format!(
                    "#{}: {} | next {} | latest run {}: {}{}",
                    s.id,
                    s.state,
                    crate::wording::time(s.next_due.as_deref(), &cfg.timezone),
                    s.latest_run_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "none".into()),
                    crate::wording::run_state(s.latest_status.unwrap_or("unavailable")),
                    if s.blocked {
                        " | attention: blocked"
                    } else {
                        ""
                    }
                ));
                actions.push(("Schedule details", format!("schedule:{}", s.id)));
            }
            if offset + 10 < snapshot.schedules.len() {
                actions.push(("Next page", format!("schedules-page:{}", offset + 10)));
            }
            if offset > 0 {
                actions.push((
                    "Previous page",
                    format!("schedules-page:{}", offset.saturating_sub(10)),
                ));
            }
            lines.push(format!(
                "Page {}. At most 50 schedules in each snapshot.",
                offset / 10 + 1
            ));
            lines.push("Create or change scheduled tasks locally with coconutclaw schedule. Unconfirmed outcomes are not retried automatically.".into());
        }
        a if a.starts_with("schedule:") => {
            let id = a
                .strip_prefix("schedule:")
                .and_then(|s| s.parse::<i64>().ok());
            if let Some(s) = snapshot.schedules.iter().find(|s| Some(s.id) == id) {
                lines.push(format!(
                    "Schedule #{} (version {}): {}\nNext due: {}\nNeeds attention: {}\nLatest run: {} ({})",
                    s.id,
                    s.revision,
                    s.state,
                    crate::wording::time(s.next_due.as_deref(), &cfg.timezone),
                    if s.blocked { "yes, inspect the latest run before changing it" } else { "no blocker recorded" },
                    s.latest_run_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "none".into()),
                    crate::wording::run_state(s.latest_status.unwrap_or("unavailable"))
                ));
                if let Some(t) = &s.timing {
                    lines.push(format!(
                        "{}\nTimezone: {}\nWhen clocks repeat: use the {} occurrence.\nWhen clocks skip a time: {}.\nMissed runs: {}.",
                        t.cadence, t.timezone, t.fold, if t.gap == "next-valid" { "use the next valid time" } else { "skip that occurrence" }, if t.missed == "skip" { "skip" } else { "combine into one run" }
                    ));
                } else {
                    lines.push("Timing unavailable.".into());
                }
            } else {
                lines.push(
                    "Schedule not shown. Open Scheduled tasks to check the latest list in this conversation."
                        .into(),
                );
            }
        }
        a if a == "tasks" || a.starts_with("tasks-page:") || a.starts_with("run:") => {
            let id = a.strip_prefix("run:").and_then(|s| s.parse::<i64>().ok());
            let offset = a
                .strip_prefix("tasks-page:")
                .and_then(|p| p.parse::<usize>().ok())
                .unwrap_or(0)
                .min(50);
            actions.push(("Notifications", "notifications:".into()));
            lines.push(format!(
                "Tasks: {} tasks, {} runs. Showing up to {} tasks.",
                snapshot.task_count,
                snapshot.run_count,
                snapshot.tasks.len()
            ));
            if id.is_none() {
                lines.push(format!(
                    "{} on this page.",
                    snapshot.tasks.len().saturating_sub(offset).min(10)
                ));
            }
            for t in snapshot
                .tasks
                .iter()
                .filter(|t| id.is_none_or(|id| t.run_id == id))
                .skip(offset)
                .take(if id.is_some() { 1 } else { 10 })
            {
                lines.push(format!(
                    "Task #{} / run #{} (attempt {}): {}\nProgress: {}. Delivery: {}.{}{}",
                    t.task_id,
                    t.run_id,
                    t.attempt,
                    crate::wording::run_state(if t.outcome_unknown { "unknown" } else { t.status }),
                    if t.progress_recorded {
                        "update recorded"
                    } else {
                        "no update recorded"
                    },
                    crate::wording::delivery(t.delivery),
                    if t.outcome_unknown {
                        " Unknown: Check the conversation and run evidence before doing this work again. No automatic retry."
                    } else {
                        ""
                    },
                    if t.attention {
                        " Attention required."
                    } else {
                        ""
                    }
                ));
                if let Some(wait) = t.wait_kind {
                    lines.push(format!(
                        "Waiting for: {}",
                        match wait {
                            "approval" => "your approval",
                            "dependency" => "another task",
                            _ => "an unconfirmed event, inspect run evidence",
                        }
                    ));
                }
                if id.is_some() {
                    lines.push(format!(
                        "Created: {}\nStarted: {}\nFinished: {}\nCancellation requested: {}",
                        crate::wording::time(t.created_at.as_deref(), &cfg.timezone),
                        crate::wording::time(t.started_at.as_deref(), &cfg.timezone),
                        crate::wording::time(t.finished_at.as_deref(), &cfg.timezone),
                        if t.cancel_requested {
                            "yes, this alone does not confirm a stop"
                        } else {
                            "no"
                        }
                    ));
                }
                if id.is_none() {
                    actions.push(("Run details", format!("run:{}", t.run_id)));
                }
                if matches!(
                    t.status,
                    "queued"
                        | "running"
                        | "awaiting_approval"
                        | "waiting_remote"
                        | "waiting_event"
                        | "cancel_requested"
                ) {
                    actions.push(("Cancel run", format!("cancel:{}", t.run_id)));
                }
                if let Some(approval) = t.approval_id {
                    actions.push(("Approve", format!("approve:{approval}")));
                    actions.push(("Reject", format!("reject:{approval}")));
                }
            }
            if snapshot.tasks.is_empty() {
                lines.push("No tasks in this conversation/topic.".into());
            }
            if let Some(id) = id.filter(|id| !snapshot.tasks.iter().any(|t| t.run_id == *id)) {
                let raw = store.task_status_command(session, &id.to_string(), update)?;
                if let Ok(row) = serde_json::from_str::<serde_json::Value>(&raw) {
                    let uncertain = store.get_task_run(id)?.is_some_and(|run| {
                        run.error_summary.as_deref().is_some_and(|e| e.contains("outcome unknown"))
                    }) || store.conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM runtime_remote_results WHERE task_run_id=?1 AND (uncertain=1 OR state='unknown'))",
                        [id], |r| r.get::<_, bool>(0),
                    )?;
                    lines.push(format!("Task #{} / requested run #{}: {}\nCurrent run: {}. Attempt: {}.\nThis run is outside the current list; open Tasks for current-run actions.",row["task_id"],id,crate::wording::run_state(if uncertain { "unknown" } else { row["status"].as_str().unwrap_or("unknown") }),row["current_run"],row["attempt"]));
                    if uncertain {
                        lines.push("Check the conversation and local run evidence before doing this work again. No automatic retry.".into());
                    }
                } else {
                    lines.push(raw);
                }
            }
            if id.is_none() {
                if offset + 10 < snapshot.tasks.len() {
                    actions.push(("Next page", format!("tasks-page:{}", offset + 10)));
                }
                if offset > 0 {
                    actions.push((
                        "Previous page",
                        format!("tasks-page:{}", offset.saturating_sub(10)),
                    ));
                }
                lines.push(format!("Page {}. At most 50 tasks in each snapshot; /tasks run-ID can inspect an older attempt.",offset/10+1));
            }
        }
        _ => lines.push("Unknown view. Open Home for current controls.".into()),
    }
    Ok(lines.join("\n\n"))
}
