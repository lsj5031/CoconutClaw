//! Presentation only. Never use these labels as stored states or protocol values.
use chrono::DateTime;
use chrono_tz::Tz;

pub(crate) const CANCELLATION: &str = "Cancellation requested. Earlier work may already have taken effect. Check Tasks and local run evidence before repeating it.";

pub(crate) fn time(raw: Option<&str>, timezone: &str) -> String {
    let Some(time) = raw.and_then(|s| DateTime::parse_from_rfc3339(s).ok()) else {
        return "not recorded".into();
    };
    let zone = timezone.parse::<Tz>().unwrap_or(chrono_tz::UTC);
    format!(
        "{} ({zone})",
        time.with_timezone(&zone).format("%d %b %Y %H:%M %:z")
    )
}

pub(crate) fn run_state(state: &str) -> &str {
    match state {
        "awaiting_approval" => "waiting for approval",
        "waiting_remote" => "waiting for a remote result",
        "waiting_event" => "waiting for another task",
        "cancel_requested" => "cancellation requested, not confirmed",
        "unknown" => "outcome unconfirmed",
        "unavailable" => "not recorded",
        other => other,
    }
}

pub(crate) fn delivery(state: &str) -> &str {
    match state {
        "unknown" => "unconfirmed, check the conversation before sending again",
        "inflight" | "sending" => "send started, receipt not confirmed",
        "pending" | "ready" => "waiting to send",
        "sent" | "delivered" => "delivered",
        "unavailable" => "not recorded",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displayed_times_follow_configured_zone_and_disambiguate_dst() {
        let first = time(Some("2026-11-01T05:30:00Z"), "America/New_York");
        let second = time(Some("2026-11-01T06:30:00Z"), "America/New_York");
        assert!(first.contains("01 Nov 2026 01:30 -04:00"));
        assert!(second.contains("01 Nov 2026 01:30 -05:00"));
        assert!(first.contains("America/New_York"));
        assert_eq!(time(None, "UTC"), "not recorded");
        assert_eq!(time(Some("invalid"), "UTC"), "not recorded");
        assert!(time(Some("2026-11-01T05:30:00Z"), "invalid").contains("(UTC)"));
    }

    #[test]
    fn uncertainty_and_cancellation_are_not_completion() {
        assert!(run_state("cancel_requested").contains("not confirmed"));
        assert_ne!(run_state("unknown"), run_state("failed"));
        assert!(delivery("unknown").contains("check the conversation"));
        assert_ne!(delivery("inflight"), delivery("delivered"));
    }
}
