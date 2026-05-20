use anyhow::Result;
use coconutclaw_config::RuntimeConfig;

use crate::context::append_memory_and_tasks;
use crate::markers::parse_markers;
use crate::scheduling::append_routing_for_task_run;
use crate::store::Store;

pub(crate) fn reconcile_pending_turn_side_effects(
    cfg: &RuntimeConfig,
    store: &mut Store,
) -> Result<usize> {
    let pending_turns = store.pending_turn_side_effects()?;
    let mut reconciled = 0usize;
    for mut turn in pending_turns {
        let markers = parse_markers(&turn.provider_raw);
        let (origin_session, delivery_target_json) =
            append_routing_for_task_run(store, turn.task_run_id)?;
        let effects = markers.to_effects();
        let append_outcome = append_memory_and_tasks(
            cfg,
            store,
            &turn.ts,
            Some(turn.id),
            &effects,
            origin_session.as_deref(),
            delivery_target_json.as_deref(),
        )?;
        if !append_outcome.schedule_feedback.is_empty() {
            if !turn.telegram_reply.trim().is_empty() {
                turn.telegram_reply.push_str("\n\n");
            }
            turn.telegram_reply
                .push_str(&append_outcome.schedule_feedback.join("\n"));
        }
        store.update_turn_reply_and_side_effects_by_id(
            turn.id,
            &turn.telegram_reply,
            &turn.voice_reply,
        )?;
        reconciled += 1;
    }
    Ok(reconciled)
}

pub(crate) fn recover_scheduled_task_output_from_task_run(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduled_task_id: i64,
    task_run_id: i64,
) -> Result<Option<String>> {
    let Some(mut turn) = store.turn_output_for_task_run(task_run_id)? else {
        return Ok(None);
    };
    if turn.status == crate::TurnStatus::Cancelled.to_string() {
        return Ok(None);
    }

    let markers = parse_markers(&turn.provider_raw);
    if !turn.side_effects_applied {
        let (origin_session, delivery_target_json) =
            append_routing_for_task_run(store, turn.task_run_id)?;
        let effects = markers.to_effects();
        let append_outcome = append_memory_and_tasks(
            cfg,
            store,
            &turn.ts,
            Some(turn.id),
            &effects,
            origin_session.as_deref(),
            delivery_target_json.as_deref(),
        )?;
        if !append_outcome.schedule_feedback.is_empty() {
            if !turn.telegram_reply.trim().is_empty() {
                turn.telegram_reply.push_str("\n\n");
            }
            turn.telegram_reply
                .push_str(&append_outcome.schedule_feedback.join("\n"));
        }
        store.update_turn_reply_and_side_effects_by_id(
            turn.id,
            &turn.telegram_reply,
            &turn.voice_reply,
        )?;
    }

    let mut effects = markers.to_effects();
    // Use stored (potentially post-processed) text replies; keep parsed non-text effects.
    if !turn.telegram_reply.trim().is_empty() {
        effects.retain(|e| !matches!(e, crate::markers::Effect::TelegramReply(_)));
        effects.push(crate::markers::Effect::TelegramReply(
            turn.telegram_reply.clone(),
        ));
    }
    if !turn.voice_reply.trim().is_empty() {
        effects.retain(|e| !matches!(e, crate::markers::Effect::VoiceReply(_)));
        effects.push(crate::markers::Effect::VoiceReply(turn.voice_reply.clone()));
    }
    let output = crate::markers::render_effects(&effects)
        .trim_end()
        .to_string();
    store.set_scheduled_task_pending_output(scheduled_task_id, &output)?;
    Ok(Some(output))
}
