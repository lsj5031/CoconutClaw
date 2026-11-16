//! Bounded calendar math only, separate from runtime clocks and admission.
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Datelike, LocalResult, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde_json::json;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Fold {
    #[default]
    First,
    Second,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Gap {
    #[default]
    Skip,
    NextValid,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Missed {
    #[default]
    Coalesce,
    Skip,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Policies {
    pub(crate) fold: Fold,
    pub(crate) gap: Gap,
    pub(crate) missed: Missed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Cadence {
    Once(DateTime<Utc>),
    Wall {
        time: NaiveTime,
        days: Option<Vec<u32>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Timing {
    cadence: Cadence,
    zone: Tz,
    policies: Policies,
}

impl Timing {
    pub(crate) fn parse(at: Option<&str>, daily: Option<&str>, zone: &str) -> Result<Self> {
        Self::parse_rule(at, daily, None, None, zone, Policies::default())
    }

    pub(crate) fn parse_rule(
        at: Option<&str>,
        daily: Option<&str>,
        weekly: Option<&str>,
        time: Option<&str>,
        zone: &str,
        policies: Policies,
    ) -> Result<Self> {
        let zone: Tz = zone
            .parse()
            .map_err(|_| anyhow::anyhow!("unknown IANA timezone"))?;
        ensure!(
            usize::from(at.is_some())
                + usize::from(daily.is_some())
                + usize::from(weekly.is_some())
                == 1,
            "supply exactly one of --at, --daily, --weekly"
        );
        ensure!(
            weekly.is_some() == time.is_some(),
            "--weekly requires --time HH:MM (weekly only)"
        );
        let cadence = if let Some(at) = at {
            let at = DateTime::parse_from_rfc3339(at)?.with_timezone(&Utc);
            ensure!(
                at.nanosecond() == 0,
                "due time must have whole-second precision"
            );
            ensure!(
                policies.fold == Fold::First && policies.gap == Gap::Skip,
                "fold/gap policies apply only to wall-clock recurrence"
            );
            Cadence::Once(at)
        } else {
            let time = daily.or(time).context("wall time missing")?;
            ensure!(time.len() == 5, "wall time must be HH:MM");
            let time =
                NaiveTime::parse_from_str(time, "%H:%M").context("wall time must be HH:MM")?;
            let days = weekly
                .map(|raw| -> Result<Vec<u32>> {
                    let names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
                    let mut days = Vec::new();
                    for name in raw.split(',') {
                        let day = names
                            .iter()
                            .position(|n| *n == name)
                            .context("weekdays must be comma-separated mon..sun")?
                            as u32;
                        ensure!(!days.contains(&day), "duplicate weekday");
                        days.push(day);
                    }
                    days.sort_unstable();
                    Ok(days)
                })
                .transpose()?;
            Cadence::Wall { time, days }
        };
        Ok(Self {
            cadence,
            zone,
            policies,
        })
    }

    pub(crate) fn encode(&self) -> String {
        let mut value = json!({
            "timezone": self.zone.name(),
            "fold": match self.policies.fold { Fold::First => "first", Fold::Second => "second" },
            "gap": match self.policies.gap { Gap::Skip => "skip", Gap::NextValid => "next-valid" },
            "missed": match self.policies.missed { Missed::Coalesce => "coalesce", Missed::Skip => "skip" },
        });
        match &self.cadence {
            Cadence::Once(at) => value["at"] = json!(at.to_rfc3339()),
            Cadence::Wall { time, days: None } => {
                value["daily"] = json!(time.format("%H:%M").to_string())
            }
            Cadence::Wall {
                time,
                days: Some(days),
            } => {
                let names = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
                value["weekly"] = json!(
                    days.iter()
                        .map(|d| names[*d as usize])
                        .collect::<Vec<_>>()
                        .join(",")
                );
                value["time"] = json!(time.format("%H:%M").to_string());
            }
        }
        value.to_string()
    }

    pub(crate) fn decode(raw: &str) -> Result<Self> {
        let v: serde_json::Value = serde_json::from_str(raw)?;
        // Legacy rows migrate on read, retaining their conservative semantics.
        let policies = Policies {
            fold: match v.get("fold").and_then(|v| v.as_str()) {
                None if v.get("fold").is_none() => Fold::First,
                Some("first") => Fold::First,
                Some("second") => Fold::Second,
                _ => bail!("invalid fold policy"),
            },
            gap: match v.get("gap").and_then(|v| v.as_str()) {
                None if v.get("gap").is_none() => Gap::Skip,
                Some("skip") => Gap::Skip,
                Some("next-valid") => Gap::NextValid,
                _ => bail!("invalid gap policy"),
            },
            missed: match v.get("missed").and_then(|v| v.as_str()) {
                None if v.get("missed").is_none() => Missed::Coalesce,
                Some("coalesce") => Missed::Coalesce,
                Some("skip") => Missed::Skip,
                _ => bail!("invalid missed policy"),
            },
        };
        for key in ["at", "daily", "weekly", "time"] {
            ensure!(v.get(key).is_none_or(|v| v.is_string()), "invalid {key}");
        }
        // Keep the original parser as the legacy/default entry point.
        if policies == Policies::default() && v.get("weekly").is_none() && v.get("time").is_none() {
            return Self::parse(
                v["at"].as_str(),
                v["daily"].as_str(),
                v["timezone"]
                    .as_str()
                    .context("schedule timezone missing")?,
            );
        }
        Self::parse_rule(
            v["at"].as_str(),
            v["daily"].as_str(),
            v["weekly"].as_str(),
            v["time"].as_str(),
            v["timezone"]
                .as_str()
                .context("schedule timezone missing")?,
            policies,
        )
    }

    pub(crate) fn first(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
        match self.cadence {
            Cadence::Once(at) => Ok(at),
            _ => self.wall_bound(now, true, true),
        }
    }

    pub(crate) fn occurrence(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
        match self.cadence {
            Cadence::Once(at) => Ok(at),
            _ => self.wall_bound(now, false, true),
        }
    }

    pub(crate) fn skip_missed(&self, due: i64, now: DateTime<Utc>) -> bool {
        self.policies.missed == Missed::Skip && due < now.timestamp()
    }

    pub(crate) fn next(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
        match self.cadence {
            Cadence::Once(_) => Ok(None),
            _ => self.wall_bound(now, true, false).map(Some),
        }
    }

    fn wall_bound(
        &self,
        now: DateTime<Utc>,
        forward: bool,
        inclusive: bool,
    ) -> Result<DateTime<Utc>> {
        let Cadence::Wall { time, ref days } = self.cadence else {
            unreachable!("wall math")
        };
        let mut date = now.with_timezone(&self.zone).date_naive();
        // At most 32 local dates, independent of downtime. Weekly rules need
        // at most 8 normally; this also spans a skipped DST/date transition.
        for _ in 0..32 {
            if days
                .as_ref()
                .is_none_or(|ds| ds.contains(&date.weekday().num_days_from_monday()))
            {
                let local = date.and_time(time);
                let mut result = self.zone.from_local_datetime(&local);
                if result == LocalResult::None && self.policies.gap == Gap::NextValid {
                    // First valid minute on THIS date, never invent an offset
                    // or move a skipped civil date onto another occurrence.
                    for minute in 1..1440 {
                        let Some(next) =
                            local.checked_add_signed(chrono::Duration::minutes(minute))
                        else {
                            break;
                        };
                        if next.date() != date {
                            break;
                        }
                        result = self.zone.from_local_datetime(&next);
                        if result != LocalResult::None {
                            break;
                        }
                    }
                }
                let candidate = match result {
                    LocalResult::None => None,
                    LocalResult::Single(t) => Some(t.with_timezone(&Utc)),
                    LocalResult::Ambiguous(a, b) => Some(
                        match self.policies.fold {
                            Fold::First => a.min(b),
                            Fold::Second => a.max(b),
                        }
                        .with_timezone(&Utc),
                    ),
                };
                if let Some(at) = candidate
                    && (if forward {
                        at > now || (inclusive && at == now)
                    } else {
                        at <= now
                    })
                {
                    return Ok(at);
                }
            }
            date = if forward {
                date.succ_opt()
            } else {
                date.pred_opt()
            }
            .context("schedule outside supported calendar range")?;
        }
        bail!("no occurrence within bounded 32-date search")
    }
}
