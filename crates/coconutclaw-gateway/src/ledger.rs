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
        Ok(Response::from_json(&self.1)?.with_status(self.0))
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
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub text: String,
    pub sends: i64,
}

pub enum Receipt {
    Delivered(i64),
    Rejected,
    Retry(i64),
    Unknown,
}

pub struct Ledger(SqlStorage);

type Bind = SqlStorageValue;

#[derive(Deserialize)]
struct Number {
    value: i64,
}

impl Ledger {
    pub fn open(storage: &Storage) -> Result<Self> {
        let ledger = Self(storage.sql());
        for query in include_str!("schema.sql").split_terminator(';') {
            if !query.trim().is_empty() {
                ledger.write(query, [])?;
            }
        }
        Ok(ledger)
    }

    pub fn transaction<T>(storage: &Storage, run: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        let ledger = Self(storage.sql());
        let mut output = None;
        let mut run = Some(run);
        let mut callback = || -> std::result::Result<(), JsValue> {
            let result = run.take().ok_or(JsValue::NULL)?(&ledger).map_err(JsValue::from)?;
            output = Some(result);
            Ok(())
        };
        let closure = ScopedClosure::borrow_mut_assert_unwind_safe(&mut callback);
        transaction(storage.as_raw().as_ref(), closure.as_ref().unchecked_ref())?;
        drop(closure);
        output.ok_or_else(|| "missing transaction result".into())
    }

    fn write<const N: usize>(&self, query: &str, values: [Bind; N]) -> Result<()> {
        self.0.exec(query, Some(values.into()))?;
        Ok(())
    }

    fn row<T: DeserializeOwned, const N: usize>(
        &self,
        query: &str,
        values: [Bind; N],
    ) -> Result<Option<T>> {
        Ok(self
            .0
            .exec(query, Some(values.into()))?
            .to_array::<T>()?
            .pop())
    }

    fn number<const N: usize>(&self, query: &str, values: [Bind; N]) -> Result<Option<i64>> {
        Ok(self.row::<Number, N>(query, values)?.map(|row| row.value))
    }

    pub fn ingest(
        &self,
        update: i64,
        input: Option<(i64, Option<i32>, &str)>,
        fail: bool,
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
    ) -> Result<Reply> {
        let device = agent.device;
        if self.last_sequence(device)? >= agent.sequence {
            return Ok(Reply::error(409, "replay"));
        }
        self.write(
            "INSERT INTO agent_sequences(device,sequence) VALUES(?,?) ON CONFLICT(device) DO UPDATE SET sequence=excluded.sequence",
            [device.name().into(), agent.sequence.into()],
        )?;
        let sequence = agent.sequence;
        match agent.command {
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
                self.report(device, proof, evidence.ok_or("missing verified evidence")?)
            }
            command => self.apply_evidence(device, command),
        }
    }

    fn report(&self, device: Device, proof: SignedEvidence, evidence: Evidence) -> Result<Reply> {
        let attempt = evidence.attempt;
        let event = evidence.event.name();
        self.update_attempt(device, attempt, |row| {
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
        })
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
            "SELECT t.chat_id,t.thread_id,o.text,o.sends+1 AS sends FROM outbox o
             JOIN tasks t ON o.task=t.id WHERE o.task=? AND o.phase='pending' AND o.ready_at<=?",
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

    pub fn finish_send(
        &self,
        task: &str,
        receipt: Receipt,
        sends: i64,
        now: i64,
        include_receipt: bool,
    ) -> Result<Reply> {
        let (phase, ready_at) = match receipt {
            Receipt::Delivered(message_id) => {
                self.write(
                    "INSERT INTO delivery_receipts(task,message_id)
                     SELECT task,? FROM outbox WHERE task=? AND phase='in_flight'",
                    [message_id.into(), task.into()],
                )?;
                ("delivered", 0)
            }
            Receipt::Rejected => ("failed", 0),
            Receipt::Retry(delay) if sends == 1 => ("pending", now + delay),
            Receipt::Retry(_) => ("failed", 0),
            Receipt::Unknown => ("unknown", 0),
        };
        self.write(
            "UPDATE outbox SET phase=?,ready_at=? WHERE task=? AND phase='in_flight'",
            [phase.into(), ready_at.into(), task.into()],
        )?;
        self.delivery_state(task, include_receipt)
    }

    pub fn recover_send(&self, task: &str, include_receipt: bool) -> Result<Reply> {
        self.write(
            "UPDATE outbox SET phase='unknown' WHERE task=? AND phase='in_flight'",
            [task.into()],
        )?;
        self.delivery_state(task, include_receipt)
    }

    fn delivery_state(&self, task: &str, include_receipt: bool) -> Result<Reply> {
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
