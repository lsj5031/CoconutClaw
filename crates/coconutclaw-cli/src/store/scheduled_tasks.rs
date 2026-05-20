use super::*;
use anyhow::Result;
use rusqlite::params;

impl Store {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn insert_scheduled_task(
        &self,
        ts: &str,
        source: &str,
        prompt: &str,
        schedule_time: &str,
        recurring: bool,
    ) -> Result<ScheduledTaskInsertResult> {
        self.insert_scheduled_task_with_target(
            ts,
            source,
            prompt,
            schedule_time,
            recurring,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_scheduled_task_with_target(
        &self,
        ts: &str,
        source: &str,
        prompt: &str,
        schedule_time: &str,
        recurring: bool,
        origin_session: Option<&str>,
        delivery_target: Option<&str>,
    ) -> Result<ScheduledTaskInsertResult> {
        let existing: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM scheduled_tasks
             WHERE source = ?1
               AND prompt = ?2
               AND schedule_time = ?3
               AND done = 0
               AND COALESCE(origin_session, '') = COALESCE(?4, '')
               AND (
                    (delivery_target IS NULL AND ?5 IS NULL)
                    OR delivery_target = ?5
               )",
            params![
                source,
                prompt,
                schedule_time,
                origin_session,
                delivery_target
            ],
            |row| row.get(0),
        )?;
        if existing > 0 {
            return Ok(ScheduledTaskInsertResult::Duplicate);
        }

        self.conn.execute(
            "INSERT INTO scheduled_tasks(ts, source, prompt, schedule_time, recurring, done, pending_output, delivery_state, origin_session, delivery_target)
             VALUES(?1, ?2, ?3, ?4, ?5, 0, NULL, NULL, ?6, ?7)",
            params![ts, source, prompt, schedule_time, recurring as i32, origin_session, delivery_target],
        )?;
        Ok(ScheduledTaskInsertResult::Inserted)
    }

    pub(crate) fn list_active_scheduled_tasks(&self) -> Result<Vec<ScheduledTask>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, source, prompt, schedule_time, recurring, last_run_ts, done, pending_output, delivery_state, origin_session, delivery_target
             FROM scheduled_tasks
             WHERE done = 0
             ORDER BY schedule_time ASC, id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(ScheduledTask {
                id: row.get(0)?,
                ts: row.get(1)?,
                source: row.get(2)?,
                prompt: row.get(3)?,
                schedule_time: row.get(4)?,
                recurring: row.get::<_, i32>(5)? != 0,
                last_run_ts: row.get(6)?,
                done: row.get::<_, i32>(7)? != 0,
                pending_output: row.get(8)?,
                delivery_state: row.get(9)?,
                origin_session: row.get(10)?,
                delivery_target: row.get(11)?,
            });
        }
        Ok(tasks)
    }

    pub(crate) fn get_scheduled_task(&self, id: i64) -> Result<Option<ScheduledTask>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, source, prompt, schedule_time, recurring, last_run_ts, done, pending_output, delivery_state, origin_session, delivery_target
             FROM scheduled_tasks
             WHERE id = ?1
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(ScheduledTask {
            id: row.get(0)?,
            ts: row.get(1)?,
            source: row.get(2)?,
            prompt: row.get(3)?,
            schedule_time: row.get(4)?,
            recurring: row.get::<_, i32>(5)? != 0,
            last_run_ts: row.get(6)?,
            done: row.get::<_, i32>(7)? != 0,
            pending_output: row.get(8)?,
            delivery_state: row.get(9)?,
            origin_session: row.get(10)?,
            delivery_target: row.get(11)?,
        }))
    }

    pub(crate) fn get_due_scheduled_tasks(
        &self,
        current_hhmm: &str,
        today: &str,
    ) -> Result<Vec<ScheduledTask>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, source, prompt, schedule_time, recurring, last_run_ts, done, pending_output, delivery_state, origin_session, delivery_target
             FROM scheduled_tasks
             WHERE schedule_time <= ?1
               AND done = 0
               AND (
                 (last_run_ts IS NOT NULL AND substr(last_run_ts, 1, 10) < ?2)
                 OR
                 (last_run_ts IS NULL AND (
                    substr(ts, 1, 10) < ?2 OR substr(ts, 12, 5) <= schedule_time
                 ))
               )
             ORDER BY schedule_time ASC",
        )?;
        let mut rows = stmt.query(params![current_hhmm, today])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(ScheduledTask {
                id: row.get(0)?,
                ts: row.get(1)?,
                source: row.get(2)?,
                prompt: row.get(3)?,
                schedule_time: row.get(4)?,
                recurring: row.get::<_, i32>(5)? != 0,
                last_run_ts: row.get(6)?,
                done: row.get::<_, i32>(7)? != 0,
                pending_output: row.get(8)?,
                delivery_state: row.get(9)?,
                origin_session: row.get(10)?,
                delivery_target: row.get(11)?,
            });
        }
        Ok(tasks)
    }

    pub(crate) fn mark_scheduled_task_executed(&self, id: i64, ts: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks
             SET last_run_ts = ?1,
                 done = CASE WHEN recurring = 0 THEN 1 ELSE 0 END,
                 pending_output = NULL,
                 delivery_state = NULL
             WHERE id = ?2",
            params![ts, id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn set_scheduled_task_pending_output(&self, id: i64, output: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks
             SET pending_output = ?1,
                 delivery_state = NULL
             WHERE id = ?2",
            params![output, id],
        )?;
        Ok(())
    }

    pub(crate) fn set_scheduled_task_delivery_state(
        &self,
        id: i64,
        delivery_state: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks SET delivery_state = ?1 WHERE id = ?2",
            params![delivery_state, id],
        )?;
        Ok(())
    }

    pub(crate) fn set_scheduled_task_routing(
        &self,
        id: i64,
        origin_session: Option<&str>,
        delivery_target: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks
             SET origin_session = COALESCE(origin_session, ?2),
                 delivery_target = COALESCE(delivery_target, ?3)
             WHERE id = ?1",
            params![id, origin_session, delivery_target],
        )?;
        Ok(())
    }

    pub(crate) fn reconcile_scheduled_tasks_from_completed_runs(
        &self,
        fallback_ts: &str,
    ) -> Result<usize> {
        self.conn.execute(
            "UPDATE scheduled_tasks
             SET last_run_ts = COALESCE(
                    (
                        SELECT finished_at
                        FROM task_runs
                        WHERE scheduled_task_id = scheduled_tasks.id
                          AND status = 'completed'
                          AND finished_at IS NOT NULL
                        ORDER BY id DESC
                        LIMIT 1
                    ),
                    ?1
                 ),
                 done = CASE WHEN recurring = 0 THEN 1 ELSE 0 END,
                 pending_output = NULL
             WHERE done = 0
               AND pending_output IS NOT NULL
               AND EXISTS (
                    SELECT 1
                    FROM task_runs
                    WHERE scheduled_task_id = scheduled_tasks.id
                      AND status = 'completed'
               )",
            params![fallback_ts],
        )?;
        Ok(self.conn.changes() as usize)
    }
}
