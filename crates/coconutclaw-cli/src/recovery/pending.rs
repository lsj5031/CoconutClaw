use crate::{context::append_memory_and_tasks, markers::parse_markers, store::Store};
use anyhow::Result;
use coconutclaw_config::RuntimeConfig;

pub(crate) fn reconcile_pending_turn_side_effects(
    cfg: &RuntimeConfig,
    store: &mut Store,
) -> Result<usize> {
    let turns = store.pending_turn_side_effects()?;
    for turn in &turns {
        append_memory_and_tasks(
            cfg,
            store,
            &turn.ts,
            Some(turn.id),
            &parse_markers(&turn.provider_raw).to_effects(),
            None,
            None,
        )?;
        store.update_turn_reply_and_side_effects_by_id(turn.id, &turn.telegram_reply)?;
    }
    Ok(turns.len())
}
