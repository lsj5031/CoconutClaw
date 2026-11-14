use crate::{context::sync_managed_context_files, store::Store};
use anyhow::{Context, Result, bail};
use coconutclaw_config::RuntimeConfig;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(crate) fn install_shutdown_handler(
    scheduler: crate::scheduler::SessionScheduler,
) -> Result<Arc<AtomicBool>> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = shutdown.clone();
    ctrlc::set_handler(move || {
        flag.store(true, Ordering::SeqCst);
        scheduler.request_shutdown();
    })
    .context("failed to register shutdown signal handler")?;
    crate::telegram::set_runtime_shutdown(&shutdown);
    Ok(shutdown)
}

pub(crate) fn run_run(
    cfg: &RuntimeConfig,
    store: &mut Store,
    args: &crate::TurnArgs,
) -> Result<()> {
    if args.inject_text.is_some() || args.inject_file.is_some() {
        return super::run_once(cfg, store, args);
    }
    if crate::telegram::valid_telegram_token(cfg).is_none()
        || crate::telegram::valid_telegram_chat_id(cfg).is_none()
    {
        bail!("run requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID or TELEGRAM_CHAT_IDS");
    }
    let client = crate::telegram::build_telegram_client(cfg)?;
    let cancel_router = Some(crate::cancel::CancelRouter::start(cfg)?);
    let scheduler = crate::scheduler::SessionScheduler::new(cfg.clone(), cancel_router);
    let shutdown = install_shutdown_handler(scheduler.clone())?;
    let result = (|| {
        crate::telegram::register_bot_commands(&client, cfg)?;
        crate::recovery::reconcile_pending_turn_side_effects(cfg, store)?;
        sync_managed_context_files(cfg, store)?;
        scheduler.recover()?;
        crate::loops::restore_inflight_update(cfg, store, &scheduler, Some(&client))?;
        crate::loops::run_poll_loop(cfg, store, &scheduler, &client, &shutdown)
    })();
    let stopped = scheduler.shutdown();
    if shutdown.load(Ordering::SeqCst) {
        stopped
    } else {
        result.and(stopped)
    }
}
