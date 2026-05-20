use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use std::io::Write;

use crate::store::Store;
use crate::turn::process_turn;
use crate::types::QuotedMessage;

pub(crate) fn run_once(
    cfg: &RuntimeConfig,
    store: &mut Store,
    args: &crate::TurnArgs,
) -> Result<()> {
    let input = crate::turn::resolve_turn_input(
        cfg,
        store,
        None,
        args.inject_text.clone(),
        args.inject_file.clone(),
        "telegram",
    )?;
    let output = process_turn(
        cfg,
        store,
        input,
        &crate::delivery::TaskSource::Telegram,
        args.chat_id.clone(),
        None,
        None,
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        None,
    )?;
    print!("{output}");
    std::io::stdout().flush().ok();
    Ok(())
}
