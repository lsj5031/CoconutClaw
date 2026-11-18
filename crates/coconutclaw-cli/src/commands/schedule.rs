use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use coconutclaw_config::RuntimeConfig;

use crate::{
    InputType, QuotedMessage, TurnInput,
    delivery::{DeliveryTarget, TaskSource},
    scheduler::TaskRequest,
    scheduling::{Fold, Gap, Missed, Policies, Timing},
    session::{SessionKey, SessionPlatform},
    store::Store,
};

#[derive(Args, Debug)]
pub(crate) struct ScheduleArgs {
    /// Conversation ID: local:name or telegram:private-chat-id#topic-id.
    #[arg(long)]
    session: String,
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand, Debug)]
enum Action {
    /// Create a scheduled task. Reusing a key does not create a second schedule.
    Create {
        #[arg(long)]
        key: String,
        #[command(flatten)]
        job: Job,
    },
    /// Edit future occurrences only; accepted/running work is unchanged.
    Edit {
        id: i64,
        #[command(flatten)]
        job: Job,
    },
    /// Show schedules and timing rules as JSON.
    List,
    /// Pause future runs without stopping already accepted tasks.
    Pause { id: i64 },
    /// Resume future runs using the saved missed-run policy.
    Resume { id: i64 },
    /// Cancel future occurrences; cancel accepted tasks separately.
    Cancel { id: i64 },
}

#[derive(Args, Debug)]
struct Job {
    #[arg(long)]
    prompt: String,
    /// RFC3339 timestamp with an explicit offset, whole seconds.
    #[arg(long, conflicts_with_all = ["daily", "weekly"])]
    at: Option<String>,
    /// Local HH:MM daily.
    #[arg(long, conflicts_with = "at")]
    daily: Option<String>,
    /// Selected weekdays, comma-separated mon,tue,wed,thu,fri,sat,sun.
    #[arg(long, conflicts_with_all = ["at", "daily"], requires = "time")]
    weekly: Option<String>,
    /// Weekly local HH:MM.
    #[arg(long, requires = "weekly")]
    time: Option<String>,
    /// When clocks repeat a time, choose the first or second occurrence.
    #[arg(long, value_enum, default_value = "first")]
    fold: Fold,
    /// When clocks skip a time, skip it or use the next valid time.
    #[arg(long, value_enum, default_value = "skip")]
    gap: Gap,
    /// Skip missed runs or combine them into one run (coalesce).
    #[arg(long, value_enum, default_value = "coalesce")]
    missed: Missed,
    /// Timezone name, for example Europe/London.
    #[arg(long)]
    timezone: String,
}

impl Job {
    fn timing(&self) -> Result<Timing> {
        Timing::parse_rule(
            self.at.as_deref(),
            self.daily.as_deref(),
            self.weekly.as_deref(),
            self.time.as_deref(),
            &self.timezone,
            Policies {
                fold: self.fold,
                gap: self.gap,
                missed: self.missed,
            },
        )
    }
}

pub(crate) fn run(
    cfg: &RuntimeConfig,
    store: &Store,
    args: &ScheduleArgs,
    now: DateTime<Utc>,
) -> Result<serde_json::Value> {
    let session = SessionKey::from_id(args.session.clone());
    ensure!(
        session.id() == args.session && !session.root_id.trim().is_empty(),
        "canonical session required"
    );
    match &args.action {
        Action::Create { key, job } => {
            let request = request(cfg, session, &job.prompt)?;
            let timing = job.timing()?;
            Ok(serde_json::json!({"schedule":store.create_schedule(key,&request,&timing,now)?}))
        }
        Action::Edit { id, job } => {
            let _ = request(cfg, session, &job.prompt)?;
            let timing = job.timing()?;
            store.edit_schedule(&args.session, *id, &job.prompt, &timing, now)?;
            Ok(serde_json::json!({"edited":id}))
        }
        Action::List => store.list_schedules(&args.session),
        Action::Pause { id } | Action::Resume { id } | Action::Cancel { id } => {
            let state = match args.action {
                Action::Pause { .. } => "paused",
                Action::Resume { .. } => "active",
                _ => "cancelled",
            };
            store.set_schedule_state(&args.session, *id, state)?;
            Ok(serde_json::json!({"schedule":id,"state":state}))
        }
    }
}

pub(crate) fn request(
    cfg: &RuntimeConfig,
    session: SessionKey,
    prompt: &str,
) -> Result<TaskRequest> {
    ensure!(
        !prompt.trim().is_empty() && prompt.len() <= 8192,
        "prompt must be 1..8192 bytes"
    );
    let (source, delivery) = match session.platform {
        SessionPlatform::Local => {
            ensure!(
                session.thread_id.is_none(),
                "local schedules do not have topics"
            );
            (TaskSource::Local, DeliveryTarget::Stdout)
        }
        SessionPlatform::Telegram => {
            crate::telegram::telegram_route_params(&session.id())?;
            ensure!(
                crate::util::is_allowed_chat(cfg, Some(&session.root_id)),
                "chat is not configured"
            );
            (
                TaskSource::Telegram,
                DeliveryTarget::Telegram {
                    chat_id: session.root_id.clone(),
                    thread_id: session.thread_id.clone(),
                },
            )
        }
    };
    Ok(TaskRequest {
        session,
        source: source.clone(),
        delegated: true,
        input: TurnInput {
            input_type: InputType::Text,
            user_text: prompt.into(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: source.channel_name().into(),
        },
        update_id: None,
        media: None,
        quoted: QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        delivery,
        source_user_id: None,
        progress_message_id: None,
    })
}
