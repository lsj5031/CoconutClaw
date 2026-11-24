use std::cell::Cell;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use wasm_bindgen::prelude::*;
use worker::{Response, Result, SqlStorage, SqlStorageValue, Storage};

use crate::executor_wire::{Event, Evidence, Grant, SignedEvidence, Status};
use crate::protocol::{Agent, CancelEvidence, Command, Device, LEASE_MS, Phase, START_MS};

#[wasm_bindgen(inline_js = "
export function transaction(storage, callback) { storage.transactionSync(callback); }
export function stageAlarm(storage, at) {
    return at ? storage.setAlarm(at) : storage.deleteAlarm();
}
")]
extern "C" {
    #[wasm_bindgen(catch)]
    fn transaction(
        storage: &JsValue,
        callback: &js_sys::Function,
    ) -> std::result::Result<(), JsValue>;
    #[wasm_bindgen(catch, js_name = stageAlarm)]
    fn stage_alarm(storage: &JsValue, at: f64) -> std::result::Result<js_sys::Promise, JsValue>;
}

pub struct Reply(u16, Value);

impl Reply {
    pub fn ok(body: Value) -> Self {
        Self(200, body)
    }

    pub fn error(status: u16, code: &'static str) -> Self {
        Self(status, json!({"error": code}))
    }

    pub fn response(self) -> Result<Response> {
        let mut response = Response::from_json(&self.1)?.with_status(self.0);
        if let Some(wait) = self.1["retry_after_seconds"].as_i64() {
            response
                .headers_mut()
                .set("retry-after", &wait.to_string())?;
        }
        Ok(response)
    }
}

pub fn conflict() -> Reply {
    Reply::error(409, "state_conflict")
}

#[derive(Deserialize)]
struct Task {
    id: String,
    chat_id: i64,
    thread_id: Option<i32>,
    prompt: String,
}

#[derive(Deserialize)]
struct Attempt {
    id: i64,
    task: String,
    device: String,
    lease_until: i64,
    start_until: Option<i64>,
    phase: Phase,
    cancel_requested: i32,
    started_evidence: i32,
}

#[derive(Deserialize)]
struct Schedule {
    version: i64,
    first_at: i64,
    next_at: i64,
    interval_ms: i64,
    run_count: i64,
    remaining: i64,
    chat_id: i64,
    thread_id: Option<i32>,
    prompt: String,
    pin: Option<String>,
}

#[derive(Deserialize)]
pub struct Delivery {
    pub task: String,
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub text: String,
    pub sends: i64,
}

pub enum Receipt {
    Delivered(i64),
    Rejected,
    RateLimited(i64),
    Unknown,
}

const DAY_MS: i64 = 86_400_000;
const REQUEST_LIMIT: i64 = 10_000;
const INGRESS_LIMIT: i64 = 9_800;
const TASK_LIMIT: i64 = 20;
const SEND_INTERVAL_MS: i64 = 4_000;
const RECEIPT_DEADLINE_MS: i64 = 5_000;

pub struct Ledger {
    sql: SqlStorage,
    meter: Option<Meter>,
}

struct Meter {
    scope: &'static str,
    started: f64,
    rows: Cell<(usize, usize)>,
}

impl Drop for Meter {
    fn drop(&mut self) {
        let (read, written) = self.rows.get();
        worker::console_log!(
            "pilot_sql {} {} {} {}",
            self.scope,
            read,
            written,
            js_sys::Date::now() - self.started
        );
    }
}

type Bind = SqlStorageValue;

#[derive(Deserialize)]
struct Number {
    value: i64,
}

impl Ledger {
    pub fn new(storage: &Storage, measure: Option<&'static str>) -> Self {
        Self {
            sql: storage.sql(),
            meter: measure.map(|scope| Meter {
                scope,
                started: js_sys::Date::now(),
                rows: Cell::default(),
            }),
        }
    }

    pub fn initialize(&self) -> Result<()> {
        for query in include_str!("schema.sql").split_terminator(';') {
            if !query.trim().is_empty() {
                self.write(query, [])?;
            }
        }
        Ok(())
    }

    pub fn initialize_synthetic_delivery(&self) -> Result<()> {
        self.write(
            "CREATE TABLE IF NOT EXISTS synthetic_delivery (
                slot INTEGER PRIMARY KEY CHECK (slot=1),
                task TEXT NOT NULL REFERENCES outbox(task),
                chat_id INTEGER NOT NULL,
                message_thread_id INTEGER NOT NULL,
                text TEXT NOT NULL
            ) STRICT",
            [],
        )
    }

    pub fn transaction<T>(&self, storage: &Storage, run: impl FnOnce() -> Result<T>) -> Result<T> {
        let mut output = None;
        let mut run = Some(run);
        let mut callback = || -> std::result::Result<(), JsValue> {
            let result = run.take().ok_or(JsValue::NULL)?().map_err(JsValue::from)?;
            output = Some(result);
            Ok(())
        };
        let closure = ScopedClosure::borrow_mut_assert_unwind_safe(&mut callback);
        transaction(storage.as_raw().as_ref(), closure.as_ref().unchecked_ref())?;
        drop(closure);
        output.ok_or_else(|| "missing transaction result".into())
    }

    fn write<const N: usize>(&self, query: &str, values: [Bind; N]) -> Result<()> {
        let cursor = self.sql.exec(query, Some(values.into()))?;
        self.record_sql(&cursor);
        Ok(())
    }

    fn record_sql(&self, cursor: &worker::SqlCursor) {
        if let Some(meter) = &self.meter {
            let (read, written) = meter.rows.get();
            meter
                .rows
                .set((read + cursor.rows_read(), written + cursor.rows_written()));
        }
    }

    fn row<T: DeserializeOwned, const N: usize>(
        &self,
        query: &str,
        values: [Bind; N],
    ) -> Result<Option<T>> {
        let cursor = self.sql.exec(query, Some(values.into()))?;
        let rows = cursor.to_array::<T>()?;
        self.record_sql(&cursor);
        Ok(rows.into_iter().last())
    }

    fn number<const N: usize>(&self, query: &str, values: [Bind; N]) -> Result<Option<i64>> {
        Ok(self.row::<Number, N>(query, values)?.map(|row| row.value))
    }

    pub fn take_pilot_request(&self, now: i64, alarm: bool) -> Result<bool> {
        let day = now.div_euclid(DAY_MS);
        let limit = if alarm { REQUEST_LIMIT } else { INGRESS_LIMIT };
        Ok(self
            .number(
                "INSERT INTO pilot_budget(slot,day,requests,tasks) VALUES(1,?1,1,0)
             ON CONFLICT(slot) DO UPDATE SET day=?1,
               requests=CASE WHEN day=?1 THEN requests+1 ELSE 1 END,
               tasks=CASE WHEN day=?1 THEN tasks ELSE 0 END
             WHERE day<?1 OR (day=?1 AND requests<?2)
             RETURNING requests AS value",
                [day.into(), limit.into()],
            )?
            .is_some())
    }

    fn pilot_reset_at(&self) -> Result<i64> {
        let day = self
            .number("SELECT day AS value FROM pilot_budget", [])?
            .ok_or("missing pilot day")?;
        Ok((day + 1) * DAY_MS)
    }

    pub fn pilot_limit(&self, now: i64) -> Result<Reply> {
        let wait = (self.pilot_reset_at()? - now + 999) / 1000;
        Ok(Reply(
            429,
            json!({
                "error": "pilot_request_limit", "retry_after_seconds": wait.clamp(1, 86_400)
            }),
        ))
    }

    fn take_pilot_task(&self, now: i64) -> Result<bool> {
        Ok(self
            .number(
                "UPDATE pilot_budget SET tasks=tasks+1 WHERE slot=1 AND day=? AND tasks<?
             RETURNING tasks AS value",
                [now.div_euclid(DAY_MS).into(), TASK_LIMIT.into()],
            )?
            .is_some())
    }

    pub fn ingest(
        &self,
        update: i64,
        input: Option<(i64, Option<i32>, &str)>,
        fail: bool,
        pilot_now: Option<i64>,
    ) -> Result<Reply> {
        if self
            .number(
                "SELECT sequence AS value FROM inbox WHERE update_id=?",
                [update.into()],
            )?
            .is_some()
        {
            return Ok(Reply::ok(json!({"disposition": "duplicate"})));
        }
        if self.number(
            "SELECT sequence AS value FROM inbox ORDER BY sequence DESC LIMIT 1",
            [],
        )? >= Some(128)
        {
            return Ok(Reply::error(503, "inbox_capacity"));
        }
        if let Some(now) = pilot_now
            && input.is_some()
            && !self.take_pilot_task(now)?
        {
            return Ok(Reply::error(429, "pilot_task_limit"));
        }
        let disposition = if input.is_some() { "task" } else { "ignored" };
        self.write(
            "INSERT INTO inbox(update_id,disposition) VALUES(?,?)",
            [update.into(), disposition.into()],
        )?;
        let task = if let Some((chat, thread, text)) = input {
            let id = format!("telegram:{update}");
            self.insert_task(&id, chat, thread, text, None)?;
            Some(id)
        } else {
            None
        };
        if fail {
            return Err("fixture rollback".into());
        }
        Ok(Reply::ok(json!({"disposition": disposition, "task": task})))
    }

    fn insert_task(
        &self,
        id: &str,
        chat: i64,
        thread: Option<i32>,
        prompt: &str,
        pin: Option<&str>,
    ) -> Result<()> {
        self.write(
            "INSERT INTO tasks(id,chat_id,thread_id,prompt,pin) VALUES(?,?,?,?,?)",
            [
                id.into(),
                chat.into(),
                thread.into(),
                prompt.into(),
                pin.into(),
            ],
        )
    }

    fn candidate(&self, device: Device, now: i64, id: Option<&str>) -> Result<Option<Task>> {
        self.row(
            "SELECT t.id,t.chat_id,t.thread_id,t.prompt FROM tasks t
             LEFT JOIN attempts a ON t.current_attempt=a.id
             WHERE t.cancelled=0 AND (t.pin IS NULL OR t.pin=?1)
             AND (?2 IS NULL OR t.id=?2)
             AND (a.id IS NULL OR (a.phase='claimed' AND a.lease_until<=?3))
             ORDER BY t.sequence LIMIT 1",
            [device.name().into(), id.into(), now.into()],
        )
    }

    pub fn read_agent(&self, agent: &Agent, now: i64, instance: &str) -> Result<Option<Reply>> {
        match agent.command {
            Command::Hello => Ok(Some(Reply::ok(json!({
                "protocol": 1, "instance": instance, "device": agent.device, "capabilities": ["text"],
                "last_sequence": self.last_sequence(agent.device)?
            })))),
            Command::Next => {
                let task = self.candidate(agent.device, now, None)?.map(|task| task.id);
                Ok(Some(Reply::ok(json!({"task": task}))))
            }
            Command::Status { attempt } => {
                let row = self.row::<Attempt, 2>(
                    "SELECT * FROM attempts WHERE id=? AND device=?",
                    [attempt.into(), agent.device.name().into()],
                )?;
                Ok(Some(match row {
                    Some(row) => Reply::ok(json!(Status {
                        attempt,
                        phase: if row.phase == Phase::Claimed && row.lease_until <= now {
                            Phase::Expired
                        } else {
                            row.phase
                        },
                        start_until: row.start_until,
                        cancel_requested: row.cancel_requested != 0,
                    })),
                    None => conflict(),
                }))
            }
            _ => Ok(None),
        }
    }

    fn last_sequence(&self, device: Device) -> Result<i64> {
        Ok(self
            .number(
                "SELECT sequence AS value FROM agent_sequences WHERE device=?",
                [device.name().into()],
            )?
            .unwrap_or(0))
    }

    pub fn mutate_agent(
        &self,
        agent: Agent,
        now: i64,
        executor: bool,
        evidence: Option<Evidence>,
        storage: &Storage,
        single_attempt: bool,
    ) -> Result<(Reply, Option<js_sys::Promise>)> {
        let mut alarm = None;
        let device = agent.device;
        if self.last_sequence(device)? >= agent.sequence {
            return Ok((Reply::error(409, "replay"), None));
        }
        self.write(
            "INSERT INTO agent_sequences(device,sequence) VALUES(?,?) ON CONFLICT(device) DO UPDATE SET sequence=excluded.sequence",
            [device.name().into(), agent.sequence.into()],
        )?;
        let sequence = agent.sequence;
        let reply = match agent.command {
            Command::Claim { .. }
                if single_attempt
                    && self
                        .number("SELECT id AS value FROM attempts LIMIT 1", [])?
                        .is_some() =>
            {
                Ok(conflict())
            }
            Command::Claim { task } => self.claim(device, now, &task, executor),
            Command::Start { attempt } => self.update_attempt(device, attempt, |row| {
                if row.cancel_requested != 0 {
                    return Ok(conflict());
                }
                let until = match row.phase {
                    Phase::Claimed if row.lease_until > now => {
                        let until = now + if executor { 5000 } else { START_MS };
                        self.write(
                            "UPDATE attempts SET phase='authorized',start_until=? WHERE id=?",
                            [until.into(), attempt.into()],
                        )?;
                        until
                    }
                    Phase::Authorized if row.start_until.is_some_and(|until| until > now) => {
                        row.start_until.ok_or("missing start deadline")?
                    }
                    _ => return Ok(conflict()),
                };
                Ok(Reply::ok(if executor {
                    json!(Grant {
                        attempt,
                        start_until: until,
                        request_sequence: sequence,
                    })
                } else {
                    json!({"phase":"authorized", "attempt":attempt, "start_until":until})
                }))
            }),
            Command::Report { proof } => {
                let (reply, queued) =
                    self.report(device, proof, evidence.ok_or("missing verified evidence")?)?;
                if queued {
                    alarm = self.arm_delivery(storage, now, now)?;
                }
                Ok(reply)
            }
            command => self.apply_evidence(device, command),
        }?;
        Ok((reply, alarm))
    }

    fn report(
        &self,
        device: Device,
        proof: SignedEvidence,
        evidence: Evidence,
    ) -> Result<(Reply, bool)> {
        let attempt = evidence.attempt;
        let event = evidence.event.name();
        let mut queued = false;
        let reply = self.update_attempt(device, attempt, |row| {
            if let Some(stored) = self.row::<SignedEvidence, 2>(
                "SELECT payload,signature FROM executor_evidence WHERE attempt=? AND event=?",
                [attempt.into(), event.into()],
            )? {
                return Ok(
                    if stored.payload == proof.payload && stored.signature == proof.signature {
                        Reply::ok(json!({"event": event}))
                    } else {
                        conflict()
                    },
                );
            }
            let reply = match evidence.event {
                Event::Started => self.apply_evidence(device, Command::Started { attempt })?,
                Event::Result { text } if row.started_evidence != 0 => {
                    queued = row.phase.has_grant();
                    self.apply_evidence(device, Command::Result { attempt, text })?
                }
                Event::Cancelled {
                    evidence: CancelEvidence::BeforeStart,
                } if row.phase == Phase::CancelledBeforeStart => Reply::ok(json!({})),
                Event::Cancelled { evidence }
                    if matches!(evidence, CancelEvidence::BeforeStart)
                        || row.started_evidence != 0 =>
                {
                    self.apply_evidence(device, Command::Cancelled { attempt, evidence })?
                }
                Event::Unknown if row.phase.has_grant() => {
                    self.set_phase(attempt, "unknown")?;
                    Reply::ok(json!({}))
                }
                _ => conflict(),
            };
            if reply.0 != 200 {
                return Ok(reply);
            }
            self.write(
                "INSERT INTO executor_evidence(attempt,event,payload,signature) VALUES(?,?,?,?)",
                [
                    attempt.into(),
                    event.into(),
                    proof.payload.into(),
                    proof.signature.into(),
                ],
            )?;
            Ok(Reply::ok(json!({"event": event})))
        })?;
        queued &= reply.0 == 200;
        Ok((reply, queued))
    }

    fn apply_evidence(&self, device: Device, command: Command) -> Result<Reply> {
        match command {
            Command::Started { attempt } => self.update_attempt(device, attempt, |row| {
                if !row.phase.has_grant() {
                    return Ok(conflict());
                }
                self.set_phase(attempt, "started")?;
                Ok(Reply::ok(
                    json!({"phase":"started", "cancel_requested": row.cancel_requested != 0}),
                ))
            }),
            Command::Result { attempt, text } => self.update_attempt(device, attempt, |row| {
                if row.phase == Phase::Completed {
                    #[derive(Deserialize)]
                    struct Message {
                        text: String,
                    }
                    let stored = self.row::<Message, 1>(
                        "SELECT text FROM outbox WHERE task=?",
                        [row.task.as_str().into()],
                    )?;
                    if !stored.is_some_and(|message| message.text == text) {
                        return Ok(conflict());
                    }
                } else if row.phase.has_grant() {
                    self.write(
                        "INSERT INTO outbox(task,text,phase) VALUES(?,?,'pending')",
                        [row.task.into(), text.into()],
                    )?;
                    self.set_phase(attempt, "completed")?;
                } else {
                    return Ok(conflict());
                }
                Ok(Reply::ok(json!({"phase":"completed"})))
            }),
            Command::Cancelled { attempt, evidence } => {
                self.update_attempt(device, attempt, |row| {
                    let phase = match evidence {
                        CancelEvidence::BeforeStart
                            if matches!(
                                row.phase,
                                Phase::Authorized | Phase::Unknown | Phase::CancelledBeforeStart
                            ) =>
                        {
                            "cancelled_before_start"
                        }
                        CancelEvidence::ProcessExited
                            if row.phase.has_grant() || row.phase == Phase::CancelledAfterExit =>
                        {
                            "cancelled_after_exit"
                        }
                        _ => return Ok(conflict()),
                    };
                    if row.cancel_requested == 0 {
                        return Ok(conflict());
                    }
                    if phase == "cancelled_before_start" && row.started_evidence != 0 {
                        return Ok(conflict());
                    }
                    self.set_phase(attempt, phase)?;
                    Ok(Reply::ok(json!({"phase":phase})))
                })
            }
            _ => Err("unexpected evidence command".into()),
        }
    }

    fn claim(&self, device: Device, now: i64, id: &str, executor: bool) -> Result<Reply> {
        let candidate = self.candidate(device, now, Some(id))?;
        let task = if let Some(task) = candidate {
            task
        } else {
            let Some(row) = self.current_attempt(id)? else {
                return Ok(conflict());
            };
            if row.device != device.name() || row.phase != Phase::Claimed || row.lease_until <= now
            {
                return Ok(conflict());
            }
            let task = self
                .row(
                    "SELECT id,chat_id,thread_id,prompt FROM tasks WHERE id=?",
                    [id.into()],
                )?
                .ok_or("missing claimed task")?;
            return Ok(Self::claim_reply(task, row.id, row.lease_until));
        };
        if self.number(
            "SELECT id AS value FROM attempts ORDER BY id DESC LIMIT 1",
            [],
        )? >= Some(512)
        {
            return Ok(Reply::error(503, "attempt_capacity"));
        }
        self.write(
            "UPDATE attempts SET phase='expired' WHERE id=(SELECT current_attempt FROM tasks WHERE id=?)",
            [id.into()],
        )?;
        let lease = now + if executor { 5000 } else { LEASE_MS };
        let attempt = self.number(
            "INSERT INTO attempts(task,device,lease_until,phase) VALUES(?,?,?,'claimed') RETURNING id AS value",
            [id.into(), device.name().into(), lease.into()],
        )?.ok_or("missing attempt")?;
        self.write(
            "UPDATE tasks SET current_attempt=? WHERE id=?",
            [attempt.into(), id.into()],
        )?;
        Ok(Self::claim_reply(task, attempt, lease))
    }

    fn claim_reply(task: Task, attempt: i64, lease_until: i64) -> Reply {
        Reply::ok(json!({
            "phase":"claimed", "task":task.id, "attempt":attempt, "lease_until":lease_until,
            "chat_id":task.chat_id, "message_thread_id":task.thread_id, "prompt":task.prompt
        }))
    }

    fn current_attempt(&self, task: &str) -> Result<Option<Attempt>> {
        self.row(
            "SELECT a.* FROM tasks t JOIN attempts a ON t.current_attempt=a.id WHERE t.id=?",
            [task.into()],
        )
    }

    fn update_attempt(
        &self,
        device: Device,
        attempt: i64,
        update: impl FnOnce(Attempt) -> Result<Reply>,
    ) -> Result<Reply> {
        let row = self.row::<Attempt, 1>(
            "SELECT a.* FROM attempts a JOIN tasks t ON t.current_attempt=a.id WHERE a.id=?",
            [attempt.into()],
        )?;
        match row {
            Some(row) if row.device == device.name() => update(row),
            _ => Ok(conflict()),
        }
    }

    fn set_phase(&self, attempt: i64, phase: &str) -> Result<()> {
        self.write(
            "UPDATE attempts SET phase=?1,started_evidence=MAX(started_evidence,?1='started') WHERE id=?2",
            [phase.into(), attempt.into()],
        )
    }

    pub fn cancel(&self, task: &str) -> Result<Reply> {
        let Some(row) = self.current_attempt(task)? else {
            if self
                .number(
                    "SELECT sequence AS value FROM tasks WHERE id=?",
                    [task.into()],
                )?
                .is_none()
            {
                return Ok(conflict());
            }
            self.write("UPDATE tasks SET cancelled=1 WHERE id=?", [task.into()])?;
            return Ok(Reply::ok(json!({"phase":"cancelled_before_start"})));
        };
        let phase = match row.phase {
            Phase::Claimed => {
                self.set_phase(row.id, "cancelled_before_start")?;
                "cancelled_before_start"
            }
            Phase::CancelledBeforeStart => "cancelled_before_start",
            Phase::CancelledAfterExit => "cancelled_after_exit",
            Phase::Completed => "completed",
            _ => {
                self.write(
                    "UPDATE attempts SET cancel_requested=1 WHERE id=?",
                    [row.id.into()],
                )?;
                "cancel_requested"
            }
        };
        Ok(Reply::ok(json!({"phase":phase})))
    }

    pub fn disconnect(&self, attempt: i64) -> Result<Reply> {
        let row = self.row::<Attempt, 1>(
            "SELECT * FROM attempts WHERE id=? AND phase IN ('authorized','started','unknown')",
            [attempt.into()],
        )?;
        if row.is_none() {
            return Ok(conflict());
        }
        self.set_phase(attempt, "unknown")?;
        Ok(Reply::ok(json!({"phase":"unknown"})))
    }

    pub fn reserve_send(&self, task: &str, now: i64) -> Result<Option<Delivery>> {
        let delivery = self.row::<Delivery, 2>(
            "SELECT o.task,t.chat_id,t.thread_id,o.text,o.sends+1 AS sends FROM outbox o
             JOIN tasks t ON o.task=t.id WHERE o.task=? AND o.phase='pending' AND o.ready_at<=? AND o.sends<2",
            [task.into(), now.into()],
        )?;
        if delivery.is_some() {
            self.write(
                "UPDATE outbox SET phase='in_flight',sends=sends+1 WHERE task=?",
                [task.into()],
            )?;
        }
        Ok(delivery)
    }

    pub fn finish_send(&self, task: &str, receipt: Receipt, sends: i64, now: i64) -> Result<()> {
        let (phase, ready_at) = match receipt {
            Receipt::Delivered(message_id) => {
                self.write(
                    "INSERT INTO delivery_receipts(task,message_id)
                     SELECT task,? FROM outbox WHERE task=? AND phase='in_flight' AND sends=?",
                    [message_id.into(), task.into(), sends.into()],
                )?;
                ("delivered", 0)
            }
            Receipt::Rejected => ("failed", 0),
            Receipt::RateLimited(delay) if sends == 1 && delay <= 10_000 => {
                ("pending", now + delay)
            }
            Receipt::RateLimited(_) => ("failed", 0),
            Receipt::Unknown => ("unknown", 0),
        };
        self.write(
            "UPDATE outbox SET phase=?,ready_at=? WHERE task=? AND phase='in_flight' AND sends=?",
            [phase.into(), ready_at.into(), task.into(), sends.into()],
        )
    }

    pub fn bootstrap_delivery(
        &self,
        storage: &Storage,
        now: i64,
    ) -> Result<Option<js_sys::Promise>> {
        if self
            .number("SELECT not_before AS value FROM pilot_delivery", [])?
            .is_some()
        {
            return Ok(None);
        }
        self.write("INSERT INTO pilot_delivery VALUES(1,0)", [])?;
        self.arm_delivery(storage, now, now)
    }

    pub fn wake_delivery(
        &self,
        storage: &Storage,
        now: i64,
    ) -> Result<(Option<Delivery>, Option<js_sys::Promise>)> {
        let (delivery, until) = self.take_delivery(now)?;
        Ok((delivery, self.arm_delivery(storage, now, until)?))
    }

    fn take_delivery(&self, now: i64) -> Result<(Option<Delivery>, i64)> {
        if !self.take_pilot_request(now, true)? {
            return Ok((None, self.pilot_reset_at()?));
        }
        let flight = self.in_flight_at()?;
        let delivery = if flight.is_some_and(|deadline| deadline <= now) {
            self.write(
                "UPDATE outbox SET phase='unknown',ready_at=0 WHERE rowid=(
                    SELECT rowid FROM outbox WHERE phase='in_flight' AND ready_at<=?
                    ORDER BY ready_at LIMIT 1
                 )",
                [now.into()],
            )?;
            None
        } else if flight.is_none() && self.send_not_before()? <= now {
            let next = self.row::<Delivery, 1>(
                "SELECT o.task,t.chat_id,t.thread_id,o.text,o.sends+1 AS sends FROM outbox o
                 JOIN tasks t ON o.task=t.id WHERE o.phase='pending' AND o.ready_at<=? AND o.sends<2
                 ORDER BY o.ready_at,o.rowid LIMIT 1",
                [now.into()],
            )?;
            if let Some(delivery) = &next {
                self.write(
                    "UPDATE outbox SET phase='in_flight',sends=sends+1,ready_at=? WHERE task=?",
                    [
                        (now + RECEIPT_DEADLINE_MS).into(),
                        delivery.task.as_str().into(),
                    ],
                )?;
                self.delay_sends_until(now + SEND_INTERVAL_MS)?;
            }
            next
        } else {
            None
        };
        Ok((delivery, now))
    }

    pub fn settle_delivery(
        &self,
        storage: &Storage,
        delivery: Delivery,
        receipt: Receipt,
        now: i64,
    ) -> Result<Option<js_sys::Promise>> {
        if let Receipt::RateLimited(delay) = receipt {
            self.delay_sends_until(now + delay)?;
        }
        self.finish_send(&delivery.task, receipt, delivery.sends, now)?;
        self.arm_delivery(storage, now, now)
    }

    pub fn deliver_synthetic(
        &self,
        storage: &Storage,
        now: i64,
    ) -> Result<Option<js_sys::Promise>> {
        let (delivery, until) = self.take_delivery(now)?;
        let Some(delivery) = delivery else {
            return self.arm_delivery(storage, now, until);
        };
        self.write(
            "INSERT INTO synthetic_delivery VALUES(1,?,?,?,?)",
            [
                delivery.task.as_str().into(),
                delivery.chat_id.into(),
                delivery.thread_id.into(),
                delivery.text.as_str().into(),
            ],
        )?;
        self.settle_delivery(storage, delivery, Receipt::Delivered(1), now)
    }

    pub fn synthetic_delivery_status(&self, task: &str) -> Result<Reply> {
        let mut status = self.delivery_state(task, true)?;
        if let Some(delivery) = self.row::<Value, 1>(
            "SELECT chat_id,message_thread_id,text FROM synthetic_delivery WHERE task=?",
            [task.into()],
        )? {
            status.1["delivery"] = delivery;
        }
        Ok(status)
    }

    fn delay_sends_until(&self, at: i64) -> Result<()> {
        self.write(
            "UPDATE pilot_delivery SET not_before=max(not_before,?) WHERE slot=1",
            [at.into()],
        )
    }

    fn send_not_before(&self) -> Result<i64> {
        self.number("SELECT not_before AS value FROM pilot_delivery", [])?
            .ok_or_else(|| "missing pilot delivery state".into())
    }

    fn in_flight_at(&self) -> Result<Option<i64>> {
        self.number(
            "SELECT ready_at AS value FROM outbox WHERE phase='in_flight' ORDER BY ready_at LIMIT 1",
            [],
        )
    }

    fn arm_delivery(
        &self,
        storage: &Storage,
        now: i64,
        until: i64,
    ) -> Result<Option<js_sys::Promise>> {
        let due = if let Some(at) = self.in_flight_at()? {
            Some(at)
        } else if let Some(at) = self.number(
            "SELECT ready_at AS value FROM outbox WHERE phase='pending' ORDER BY ready_at LIMIT 1",
            [],
        )? {
            Some(at.max(self.send_not_before()?))
        } else {
            None
        };
        let at = due.map_or(0, |at| at.max(now + 100).max(until));
        Ok(Some(Self::alarm(storage, at)?))
    }

    pub fn recover_send(&self, task: &str, include_receipt: bool) -> Result<Reply> {
        self.write(
            "UPDATE outbox SET phase='unknown' WHERE task=? AND phase='in_flight'",
            [task.into()],
        )?;
        self.delivery_state(task, include_receipt)
    }

    pub fn delivery_state(&self, task: &str, include_receipt: bool) -> Result<Reply> {
        #[derive(Deserialize)]
        struct DeliveryState {
            phase: String,
            sends: i64,
            message_id: Option<i64>,
        }
        Ok(
            match self.row::<DeliveryState, 1>(
                "SELECT phase,sends,message_id FROM outbox LEFT JOIN delivery_receipts USING(task) WHERE task=?",
                [task.into()],
            )? {
                Some(row) => {
                    let mut body = json!({"phase":row.phase,"sends":row.sends});
                    if include_receipt && let Some(id) = row.message_id {
                        body["message_id"] = json!(id);
                    }
                    Reply::ok(body)
                }
                None => conflict(),
            },
        )
    }

    pub fn configure_schedule(
        &self,
        storage: &Storage,
        schedule: ScheduleInput,
        now: i64,
    ) -> Result<(Reply, Option<js_sys::Promise>)> {
        let previous = self.row::<Schedule, 0>("SELECT * FROM schedule", [])?;
        if let Some(row) = &previous
            && row.version == schedule.version
            && row.first_at == schedule.first_at
            && row.interval_ms == schedule.interval_ms
            && row.run_count == schedule.count
            && row.thread_id == schedule.thread
            && row.prompt == schedule.prompt
            && row.pin.as_deref() == schedule.pin.map(Device::name)
        {
            return Ok((Reply::ok(json!({"version":schedule.version})), None));
        }
        if schedule.version != previous.map_or(1, |schedule| schedule.version + 1) {
            return Ok((conflict(), None));
        }
        if !(now + 500..=now + 60_000).contains(&schedule.first_at) {
            return Ok((Reply::error(400, "bad_input"), None));
        }
        self.write(
            "INSERT OR REPLACE INTO schedule(slot,version,first_at,next_at,interval_ms,run_count,remaining,chat_id,thread_id,prompt,pin)
             VALUES(1,?1,?2,?2,?3,?4,?4,321,?5,?6,?7)",
            [schedule.version.into(), schedule.first_at.into(), schedule.interval_ms.into(), schedule.count.into(),
                schedule.thread.into(), schedule.prompt.into(), schedule.pin.map(Device::name).into()],
        )?;
        Ok((
            Reply::ok(json!({"version":schedule.version})),
            Some(Self::alarm(storage, schedule.first_at)?),
        ))
    }

    pub fn disable(
        &self,
        storage: &Storage,
        version: i64,
    ) -> Result<(Reply, Option<js_sys::Promise>)> {
        if self.number("SELECT version AS value FROM schedule", [])? != Some(version) {
            return Ok((conflict(), None));
        }
        self.write("UPDATE schedule SET remaining=0 WHERE remaining>0", [])?;
        Ok((
            Reply::ok(json!({"version":version})),
            Some(Self::alarm(storage, 0)?),
        ))
    }

    pub fn wake(&self, storage: &Storage, now: i64) -> Result<(Reply, Option<js_sys::Promise>)> {
        let Some(schedule) =
            self.row::<Schedule, 0>("SELECT * FROM schedule WHERE remaining>0", [])?
        else {
            return Ok((Reply::ok(json!({"task":null})), None));
        };
        if schedule.next_at > now {
            return Ok((
                Reply::ok(json!({"task":null})),
                Some(Self::alarm(storage, schedule.next_at)?),
            ));
        }
        let due = (1 + (now - schedule.next_at) / schedule.interval_ms).min(schedule.remaining);
        let instant = schedule.next_at + (due - 1) * schedule.interval_ms;
        let remaining = schedule.remaining - due;
        let next = instant + schedule.interval_ms;
        let id = format!("schedule:fixture:{instant}");
        let mut task = None;
        if now - instant <= 60_000
            && self
                .number(
                    "SELECT scheduled_at AS value FROM occurrences WHERE scheduled_at=?",
                    [instant.into()],
                )?
                .is_none()
        {
            self.insert_task(
                &id,
                schedule.chat_id,
                schedule.thread_id,
                &schedule.prompt,
                schedule.pin.as_deref(),
            )?;
            self.write(
                "INSERT INTO occurrences(scheduled_at,version,task) VALUES(?,?,?)",
                [instant.into(), schedule.version.into(), id.as_str().into()],
            )?;
            task = Some(id);
        }
        self.write(
            "UPDATE schedule SET next_at=?,remaining=?",
            [next.into(), remaining.into()],
        )?;
        let alarm_at = if remaining > 0 { next } else { 0 };
        Ok((
            Reply::ok(json!({"task":task})),
            Some(Self::alarm(storage, alarm_at)?),
        ))
    }

    fn alarm(storage: &Storage, at: i64) -> Result<js_sys::Promise> {
        Ok(stage_alarm(storage.as_raw().as_ref(), at as f64)?)
    }
}

pub struct ScheduleInput {
    pub version: i64,
    pub first_at: i64,
    pub interval_ms: i64,
    pub count: i64,
    pub thread: Option<i32>,
    pub prompt: String,
    pub pin: Option<Device>,
}
