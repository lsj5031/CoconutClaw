//! Only disposable capabilities and resolved claim-bypass projections have a TTL.
//! Source receipts and immutable evidence stay online for exact replay.
use anyhow::Result;
use rusqlite::{Transaction, TransactionBehavior};

use super::Store;

const BATCH: i64 = 256;
const OPERATIONAL_RETENTION_SECONDS: i64 = 30 * 24 * 60 * 60;

impl Store {
    pub(crate) fn retain_operational_rows(&self, now: i64) -> Result<usize> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let mut removed = 0;
        for table in ["telegram_ui_inputs", "telegram_ui_controls"] {
            removed += tx.execute(
                &format!(
                    "DELETE FROM {table} WHERE rowid IN (
                       SELECT rowid FROM {table} WHERE expires_at<=?1 ORDER BY expires_at,rowid LIMIT ?2)"
                ),
                (now, BATCH),
            )?;
        }
        for table in [
            "telegram_ui_observations",
            "telegram_reconciliation_controls",
        ] {
            removed += tx.execute(
                &format!(
                    "DELETE FROM {table} WHERE seq IN (
                       SELECT p.seq FROM {table} p JOIN telegram_ingress e ON e.seq=p.seq
                       WHERE e.state='done' AND e.ack_phase='sent'
                         AND e.progress_state IN ('none','sent')
                         AND unixepoch(e.created_at)<=?1
                         AND CASE WHEN json_valid(e.delivery_state) THEN
                           NOT EXISTS(SELECT 1 FROM json_tree(e.delivery_state)
                             WHERE key='phase' AND value IN ('sending','unknown'))
                           ELSE 0 END
                       ORDER BY p.seq LIMIT ?2)"
                ),
                (now.saturating_sub(OPERATIONAL_RETENTION_SECONDS), BATCH),
            )?;
        }
        tx.commit()?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coconutclaw_config::RuntimeConfig;
    use rusqlite::params;

    #[test]
    fn retention_expires_capabilities_but_preserves_live_and_consumed_unexpired_controls() {
        let store = Store::open(&RuntimeConfig::test_config()).unwrap();
        for (token, expiry, consumed) in [
            ("expired", 99, 0),
            ("boundary", 100, 0),
            ("live", 101, 0),
            ("consumed", 101, 1),
        ] {
            store.conn.execute("INSERT INTO telegram_ui_controls VALUES(?1,'telegram:321','321','home',1,?2,?3)", params![token,expiry,consumed]).unwrap();
        }
        for (session, expiry) in [("expired", 99), ("boundary", 100), ("live", 101)] {
            store
                .conn
                .execute(
                    "INSERT INTO telegram_ui_inputs VALUES(?1,'321','token','memory',?2)",
                    params![session, expiry],
                )
                .unwrap();
        }
        assert_eq!(store.retain_operational_rows(100).unwrap(), 4);
        assert_eq!(store.conn.query_row("SELECT group_concat(token,',') FROM (SELECT token FROM telegram_ui_controls ORDER BY token)", [], |r|r.get::<_,String>(0)).unwrap(), "consumed,live");
        assert_eq!(
            store
                .conn
                .query_row("SELECT session_id FROM telegram_ui_inputs", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            "live"
        );
        assert_eq!(store.retain_operational_rows(100).unwrap(), 0);
    }

    #[test]
    fn retention_only_removes_resolved_projections_and_keeps_exact_replay_and_evidence() {
        let store = Store::open(&RuntimeConfig::test_config()).unwrap();
        let now = 2_000_000_000;
        let old = chrono::DateTime::from_timestamp(now - OPERATIONAL_RETENTION_SECONDS, 0)
            .unwrap()
            .to_rfc3339();
        let cases = [
            ("done", "sent", "none", "[]", old.as_str()),
            (
                "done",
                "sent",
                "sent",
                r#"[{"phase":"sent"}]"#,
                old.as_str(),
            ),
            ("pending", "sent", "none", "[]", old.as_str()),
            ("applying", "sent", "none", "[]", old.as_str()),
            ("unknown", "sent", "none", "[]", old.as_str()),
            ("done", "unknown", "none", "[]", old.as_str()),
            ("done", "ready", "none", "[]", old.as_str()),
            ("done", "sent", "unknown", "[]", old.as_str()),
            ("done", "sent", "sending", "[]", old.as_str()),
            (
                "done",
                "sent",
                "none",
                r#"[{"phase":"unknown"}]"#,
                old.as_str(),
            ),
            (
                "done",
                "sent",
                "none",
                r#"[{"phase":"sending"}]"#,
                old.as_str(),
            ),
            ("done", "sent", "none", "malformed", old.as_str()),
            ("done", "sent", "none", "[]", "unparseable"),
            ("done", "sent", "none", "[]", "2099-01-01T00:00:00+00:00"),
        ];
        for (i, (state, ack, progress, delivery, date)) in cases.iter().enumerate() {
            let id = (i + 1).to_string();
            let seq = store
                .admit_telegram(&id, &format!("raw-{id}"), Some("telegram:321"), date)
                .unwrap();
            store.conn.execute("UPDATE telegram_ingress SET state=?2,ack_phase=?3,progress_state=?4,delivery_state=?5,output='saved result' WHERE seq=?1", params![seq,state,ack,progress,delivery]).unwrap();
            store
                .conn
                .execute("INSERT INTO telegram_ui_observations VALUES(?1)", [seq])
                .unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO telegram_reconciliation_controls VALUES(?1)",
                    [seq],
                )
                .unwrap();
        }
        // Immutable reconciliation evidence may refer to a pruned projection.
        store.conn.execute("INSERT INTO runtime_reconciliation_offers VALUES('offer','telegram:321','321','ack',1,'snapshot','old')",[]).unwrap();
        store.conn.execute("INSERT INTO runtime_reconciliation_audit VALUES('offer',1,'durable-text-ack','old')",[]).unwrap();
        let snapshot = |table: &str| -> String {
            let mut stmt = store
                .conn
                .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                .unwrap();
            let width = stmt.column_count();
            format!(
                "{:?}",
                stmt.query_map([], |r| (0..width)
                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>())
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            )
        };
        let evidence = [
            "telegram_ingress",
            "runtime_reconciliation_offers",
            "runtime_reconciliation_audit",
        ];
        let before: Vec<_> = evidence.iter().map(|t| snapshot(t)).collect();
        assert_eq!(store.retain_operational_rows(now).unwrap(), 4);
        for table in [
            "telegram_ui_observations",
            "telegram_reconciliation_controls",
        ] {
            let remaining: Vec<i64> = store
                .conn
                .prepare(&format!("SELECT seq FROM {table} ORDER BY seq"))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert_eq!(remaining, (3..=cases.len() as i64).collect::<Vec<_>>());
        }
        assert_eq!(
            evidence.iter().map(|t| snapshot(t)).collect::<Vec<_>>(),
            before
        );
        assert_eq!(
            store
                .admit_telegram("1", "raw-1", Some("telegram:321"), "later")
                .unwrap(),
            1
        );
        assert!(
            store
                .admit_telegram("1", "conflicting", Some("telegram:321"), "later")
                .is_err()
        );
        assert!(!store.claim_telegram(1).unwrap());
        assert_eq!(
            store.telegram_outcome(1).unwrap().output.as_deref(),
            Some("saved result")
        );
        assert!(
            store
                .conn
                .execute("DELETE FROM runtime_reconciliation_audit", [])
                .is_err()
        );
        assert_eq!(store.retain_operational_rows(now).unwrap(), 0);
    }

    #[test]
    fn retention_is_bounded_and_progresses_across_batches() {
        let store = Store::open(&RuntimeConfig::test_config()).unwrap();
        for i in 0..(BATCH + 1) {
            store
                .conn
                .execute(
                    "INSERT INTO telegram_ui_controls VALUES(?1,'s','a','home',1,0,0)",
                    [i.to_string()],
                )
                .unwrap();
        }
        assert_eq!(store.retain_operational_rows(1).unwrap(), BATCH as usize);
        assert_eq!(store.retain_operational_rows(1).unwrap(), 1);
        assert_eq!(store.retain_operational_rows(1).unwrap(), 0);
    }

    #[test]
    fn retention_rolls_back_the_whole_batch_on_failure() {
        let store = Store::open(&RuntimeConfig::test_config()).unwrap();
        store
            .conn
            .execute_batch(
                "INSERT INTO telegram_ui_inputs VALUES('s','a','token','memory',0);
             INSERT INTO telegram_ui_controls VALUES('token','s','a','home',1,0,0);
             CREATE TRIGGER retention_test_failure BEFORE DELETE ON telegram_ui_controls
             BEGIN SELECT RAISE(ABORT,'synthetic retention failure'); END;",
            )
            .unwrap();
        assert!(store.retain_operational_rows(1).is_err());
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM telegram_ui_inputs", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM telegram_ui_controls", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        store
            .conn
            .execute_batch("DROP TRIGGER retention_test_failure")
            .unwrap();
        assert_eq!(store.retain_operational_rows(1).unwrap(), 2);
    }

    #[test]
    fn concurrent_retention_preserves_pending_custody_until_it_is_settled() {
        let cfg = RuntimeConfig::test_config();
        let store = Store::open(&cfg).unwrap();
        let seq = store
            .admit_telegram(
                "1",
                "raw",
                Some("telegram:321"),
                "2000-01-01T00:00:00+00:00",
            )
            .unwrap();
        store
            .conn
            .execute("INSERT INTO telegram_ui_observations VALUES(?1)", [seq])
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO telegram_reconciliation_controls VALUES(?1)",
                [seq],
            )
            .unwrap();
        let workers = |cfg: &RuntimeConfig| {
            (0..2)
                .map(|_| {
                    let cfg = cfg.clone();
                    std::thread::spawn(move || {
                        Store::connect(&cfg)
                            .unwrap()
                            .retain_operational_rows(2_000_000_000)
                            .unwrap()
                    })
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            workers(&cfg)
                .into_iter()
                .map(|w| w.join().unwrap())
                .sum::<usize>(),
            0
        );
        assert!(store.claim_telegram(seq).unwrap());
        assert_eq!(store.retain_operational_rows(2_000_000_000).unwrap(), 0);
        store
            .conn
            .execute(
                "UPDATE telegram_ingress SET state='done',ack_phase='unknown' WHERE seq=?1",
                [seq],
            )
            .unwrap();
        assert_eq!(store.retain_operational_rows(2_000_000_000).unwrap(), 0);
        store
            .conn
            .execute(
                "UPDATE telegram_ingress SET ack_phase='sent' WHERE seq=?1",
                [seq],
            )
            .unwrap();
        assert_eq!(
            workers(&cfg)
                .into_iter()
                .map(|w| w.join().unwrap())
                .sum::<usize>(),
            2
        );
        assert_eq!(
            store
                .admit_telegram("1", "raw", Some("telegram:321"), "later")
                .unwrap(),
            seq
        );
        assert!(!store.claim_telegram(seq).unwrap());
    }

    #[test]
    fn real_control_replay_after_retention_returns_saved_output_without_new_capabilities() {
        let mut cfg = RuntimeConfig::test_config();
        cfg.telegram_bot_token = None;
        cfg.codex.bin = "/nonexistent/synthetic-codex".into();
        let mut store = Store::open(&cfg).unwrap();
        let scheduler = crate::scheduler::SessionScheduler::new(cfg.clone(), None);
        let raw =
            r#"{"update_id":1,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":"/help"}}"#;
        let first =
            crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, raw).unwrap();
        assert!(first.output.is_some());
        let token: String = store
            .conn
            .query_row("SELECT token FROM telegram_ui_controls LIMIT 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        store
            .conn
            .execute_batch(
                "UPDATE telegram_ui_controls SET expires_at=0;
             UPDATE telegram_ingress SET created_at='2000-01-01T00:00:00+00:00',ack_phase='sent';",
            )
            .unwrap();
        assert!(
            store
                .retain_operational_rows(chrono::Utc::now().timestamp())
                .unwrap()
                > 0
        );
        let replay =
            crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, raw).unwrap();
        assert_eq!(replay.output, first.output);
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM telegram_ui_controls", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let expired = serde_json::json!({"update_id":2,"message":{"from":{"id":"321"},"chat":{"id":"321"},"text":format!("/button {token}")}}).to_string();
        let result =
            crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, &expired)
                .unwrap();
        assert!(result.output.unwrap().contains("invalid, expired, used"));
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        scheduler.shutdown().unwrap();
    }
}
