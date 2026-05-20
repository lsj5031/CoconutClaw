use anyhow::Result;
use std::io::Write;

use coconutclaw_config::RuntimeConfig;

use crate::store::Store;
use crate::turn::process_turn;
use crate::types::{InputType, QuotedMessage, TurnInput};

pub(crate) fn run_heartbeat(cfg: &RuntimeConfig, store: &mut Store) -> Result<()> {
    let prompt = "Daily heartbeat for CoconutClaw. Summarize today, surface urgent tasks from TASKS/pending.md, and suggest next 1-3 actions.";
    let output = process_turn(
        cfg,
        store,
        TurnInput {
            input_type: InputType::Text,
            user_text: prompt.to_string(),
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "telegram".to_string(),
        },
        &crate::delivery::TaskSource::Telegram,
        crate::telegram::valid_telegram_chat_id(cfg).map(ToOwned::to_owned),
        None,
        None,
        &QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        None,
    )?;

    let client = crate::telegram::build_telegram_client(cfg)?;
    crate::telegram::dispatch_telegram_output(
        &client,
        cfg,
        crate::telegram::valid_telegram_chat_id(cfg),
        &output,
        None,
    )?;
    super::helpers::dispatch_slack_if_configured(cfg, &output);
    print!("{output}");
    std::io::stdout().flush().ok();
    Ok(())
}
