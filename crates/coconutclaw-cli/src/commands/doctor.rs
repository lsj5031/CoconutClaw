use anyhow::Result;
use coconutclaw_config::RuntimeConfig;

pub(crate) fn run_doctor(cfg: &RuntimeConfig, args: &crate::DoctorArgs) -> Result<()> {
    let report = serde_json::json!({
        "data_dir": cfg.instance_dir,
        "sqlite_db_path": cfg.sqlite_db_path,
        "config_file": cfg.config_file_path,
        "runtime": "codex",
        "transport": "telegram-polling",
        "checks": {
            "codex_bin": crate::command_exists(&cfg.codex.bin),
            "telegram_token": crate::telegram::valid_telegram_token(cfg).is_some(),
            "telegram_chat_id": crate::telegram::valid_telegram_chat_id(cfg).is_some(),
        }
    });
    if !args.json {
        println!("CoconutClaw doctor (local checks only, no authentication or network probes)");
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
