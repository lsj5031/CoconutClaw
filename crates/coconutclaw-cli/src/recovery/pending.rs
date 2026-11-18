use crate::{context::append_memory_and_tasks, markers::CompiledOutput, store::Store};
use anyhow::Result;
use coconutclaw_config::RuntimeConfig;

pub(crate) fn reconcile_pending_turn_side_effects(
    cfg: &RuntimeConfig,
    store: &mut Store,
) -> Result<usize> {
    let turns = store.pending_turn_side_effects()?;
    for turn in &turns {
        let delegated = store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM turns t JOIN task_payloads p ON p.task_run_id=t.task_run_id
             WHERE t.id=?1 AND json_extract(p.request,'$.delegated')=1)",
            [turn.id],
            |r| r.get::<_, bool>(0),
        )?;
        let admitted = crate::delivery::media::admit_compiled(
            cfg,
            CompiledOutput::from_saved_turn(&turn.provider_raw, &turn.telegram_reply),
            delegated,
        );
        let reply = Some(admitted.reply())
            .filter(|reply| reply.starts_with("Output rejected:"))
            .unwrap_or(&turn.telegram_reply);
        append_memory_and_tasks(
            cfg,
            store,
            &turn.ts,
            Some(turn.id),
            &admitted.effects,
            None,
            None,
        )?;
        if reply.starts_with("Output rejected:") {
            store.conn.execute(
                "UPDATE turns SET provider_raw=?2 WHERE id=?1",
                rusqlite::params![turn.id, admitted.encode()],
            )?;
        }
        store.update_turn_reply_and_side_effects_by_id(turn.id, reply)?;
    }
    Ok(turns.len())
}
