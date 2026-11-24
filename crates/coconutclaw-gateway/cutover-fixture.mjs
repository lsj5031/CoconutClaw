import assert from "node:assert/strict";
import { createHash, verify } from "node:crypto";
import { readFileSync } from "node:fs";

const version = "coconutclaw-synthetic-cutover-v1";
const digest = payload => createHash("sha256").update(payload).digest("hex");
const integer = (value, max) => Number.isSafeInteger(value) && value > 0 && value <= max;
const text = value => typeof value === "string" && value.trim().length > 0;

export function seedCutoverSource(source) {
    source.exec(readFileSync(new URL("../../sql/schema.sql", import.meta.url), "utf8"));
    source.exec(`
            INSERT INTO task_runs(id,session_id,channel,source_chat_id,source_thread_id,update_id,
                prompt,status,created_at,error_summary)
            VALUES
                (23,'telegram:321#37','telegram','telegram:321#37','37','201',
                 'synthetic continuation','completed','2026-01-01',NULL),
                (24,'telegram:321#37','telegram','telegram:321#37','37',NULL,
                 'synthetic continuation','failed','2026-01-01','runtime restarted; outcome unknown, do not automatically retry'),
                (25,'telegram:321#41','telegram','telegram:321#41','41','203',
                 'synthetic running task','running','2026-01-01',NULL),
                (26,'telegram:321#53','telegram','telegram:321#53','53','202',
                 'synthetic task with a send in flight','completed','2026-01-01',NULL),
                (27,'telegram:321','telegram','321',NULL,'204',
                 'synthetic task with a pending send','completed','2026-01-01',NULL);
            INSERT INTO runtime_tasks(task_id,session_id,channel,current_run_id)
            SELECT id,session_id,channel,CASE id WHEN 23 THEN 24 ELSE id END FROM task_runs WHERE id<>24;
            INSERT INTO runtime_attempts(run_id,task_id,ordinal,parent_run_id)
            VALUES (23,23,1,NULL),(24,23,2,23),(25,25,1,NULL),(26,26,1,NULL),(27,27,1,NULL);
            INSERT INTO telegram_ingress(seq,update_id,payload,session_id,state,created_at)
            SELECT id-22,update_id,json_object('update_id',CAST(update_id AS INTEGER),'message',
                json_patch('{"chat":{"id":321,"type":"private"},"from":{"id":321}}',
                    json_object('message_thread_id',CAST(source_thread_id AS INTEGER),'text',prompt))),
                session_id,'done',created_at
            FROM task_runs WHERE update_id IS NOT NULL;
            INSERT INTO task_payloads(task_run_id,request,output,delivery_state) VALUES
                (26,'{}','TELEGRAM_REPLY: synthetic in-flight reply',
                 '{"completed_ops":[],"operations":{},"text_plan":{"kind":"rich","phase":"sending","attempts":1,"markdown":"synthetic in-flight reply"}}'),
                (27,'{}','TELEGRAM_REPLY: synthetic pending reply',
                 '{"completed_ops":[],"operations":{},"text_plan":{"kind":"rich","phase":"ready","attempts":0,"markdown":"synthetic pending reply"}}');
    `);
}

export function cutoverSnapshot(source) {
    const tasks = source.prepare(`
            SELECT t.task_id AS taskId, a.run_id AS runId, g.seq AS sequence, g.payload AS ingress,
                   root.session_id AS session, r.status, r.error_summary AS error, p.delivery_state AS delivery
            FROM runtime_tasks t
            JOIN task_runs root ON root.id=t.task_id
            JOIN runtime_attempts a ON a.run_id=t.current_run_id AND a.task_id=t.task_id
            JOIN task_runs r ON r.id=a.run_id
            JOIN telegram_ingress g ON g.update_id=root.update_id
            LEFT JOIN task_payloads p ON p.task_run_id=r.id
            WHERE g.state='done'
            ORDER BY g.seq
    `).all().map(row => ({
        ...row,
        ingress: JSON.parse(row.ingress),
        delivery: JSON.parse(row.delivery),
    }));
    const payload = JSON.stringify({ version, tasks });
    return JSON.stringify({ sha256: digest(payload), payload });
}

function validate(snapshot) {
    const { sha256, payload } = JSON.parse(snapshot);
    assert.ok(typeof payload === "string" && sha256 === digest(payload), "fixture checksum mismatch");
    const fixture = JSON.parse(payload);
    assert.equal(fixture.version, version, "unsupported fixture version");
    const { tasks } = fixture;
    assert.ok(Array.isArray(tasks) && tasks.length > 0 && tasks.length < 128, "invalid fixture task count");
    for (const key of ["taskId", "runId", "sequence"]) {
        assert.equal(new Set(tasks.map(task => task[key])).size, tasks.length, `duplicate fixture ${key}`);
    }
    assert.equal(new Set(tasks.map(task => task.ingress.update_id)).size, tasks.length, "duplicate fixture update");
    const rows = tasks.map(({ taskId, runId, sequence, ingress, session, status, error, delivery }) => {
        const { update_id: update, message } = ingress;
        const { chat, message_thread_id: thread = null, text: prompt } = message;
        assert.ok(integer(taskId, 511) && integer(runId, 511) && integer(sequence, 127)
            && integer(update, 2 ** 31 - 1) && text(prompt), "invalid fixture identity or prompt");
        assert.ok(chat.type === "private" && chat.id === 321
            && (thread === null || integer(thread, 2 ** 31 - 1)), "invalid fixture route");
        assert.equal(session, `telegram:${chat.id}${thread === null ? "" : `#${thread}`}`, "conflicting fixture route");
        const completed = status === "completed";
        if (completed) {
            const { completed_ops, operations, text_plan } = delivery;
            assert.deepEqual(completed_ops, []);
            assert.deepEqual(operations, {});
            assert.ok(text(text_plan.markdown)
                && text_plan.kind === "rich"
                && ((text_plan.phase === "sending" && text_plan.attempts === 1)
                    || (text_plan.phase === "ready" && text_plan.attempts === 0)), "unsupported fixture delivery");
        } else {
            assert.ok(status === "running" || (status === "failed" && error?.includes("outcome unknown")),
                "unsupported fixture task status");
            assert.equal(delivery, null);
        }
        return {
            task: `legacy:${taskId}`, runId, sequence, update, chat: chat.id, thread, prompt,
            attemptPhase: completed ? "completed" : "unknown", plan: delivery?.text_plan,
        };
    });
    assert.ok(rows.every((row, index) => index === 0 || row.sequence > rows[index - 1].sequence),
        "fixture sequence must increase");
    return { tasks, rows };
}

function transaction(db, apply) {
    db.exec("BEGIN IMMEDIATE");
    try {
        const result = apply();
        db.exec("COMMIT");
        return result;
    } catch (error) {
        db.exec("ROLLBACK");
        throw error;
    }
}

function insertGatewayRows(db, rows) {
    const inbox = db.prepare("INSERT INTO inbox(sequence,update_id,disposition) VALUES(?,?,'task')");
    const task = db.prepare("INSERT INTO tasks(sequence,id,chat_id,thread_id,prompt) VALUES(?,?,?,?,?)");
    const attempt = db.prepare("INSERT INTO attempts(id,task,device,lease_until,phase) VALUES(?,?,'legacy-fixture',0,?)");
    const current = db.prepare("UPDATE tasks SET current_attempt=? WHERE id=?");
    const outbox = db.prepare("INSERT INTO outbox(task,text,phase,sends) VALUES(?,?,'unknown',?)");
    for (const row of rows) {
        inbox.run(row.sequence, row.update);
        task.run(row.sequence, row.task, row.chat, row.thread, row.prompt);
        attempt.run(row.runId, row.task, row.attemptPhase);
        current.run(row.runId, row.task);
        if (row.plan) outbox.run(row.task, row.plan.markdown, row.plan.attempts);
    }
}

export function importCutoverFixture(db, snapshot) {
    const { rows } = validate(snapshot);
    return transaction(db, () => {
        db.exec(`CREATE TABLE IF NOT EXISTS synthetic_cutover(
            slot INTEGER PRIMARY KEY CHECK(slot=1), snapshot TEXT NOT NULL, baseline TEXT
        ) STRICT`);
        const imported = db.prepare("SELECT snapshot FROM synthetic_cutover WHERE slot=1").get();
        if (imported) {
            assert.equal(imported.snapshot, snapshot, "conflicting fixture snapshot");
            return "already imported";
        }
        for (const table of ["inbox", "tasks", "attempts", "outbox", "schedule"]) {
            assert.equal(db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get(), undefined, "fixture destination must be empty");
        }
        insertGatewayRows(db, rows);
        db.prepare("INSERT INTO synthetic_cutover(slot,snapshot) VALUES(1,?)").run(snapshot);
        return "imported";
    });
}

export function importCutoverDelta(db, baseline, frozen) {
    const before = validate(baseline);
    const after = validate(frozen);
    assert.ok(after.rows.length > before.rows.length, "missing fixture delta");
    assert.deepEqual(after.tasks.slice(0, before.tasks.length), before.tasks, "fixture delta must append");
    return transaction(db, () => {
        const imported = db.prepare("SELECT snapshot,baseline FROM synthetic_cutover WHERE slot=1").get();
        if (imported?.baseline === baseline && imported.snapshot === frozen) return "already imported";
        assert.ok(imported?.snapshot === baseline && imported.baseline === null, "wrong fixture baseline");
        insertGatewayRows(db, after.rows.slice(before.rows.length));
        db.prepare("UPDATE synthetic_cutover SET snapshot=?,baseline=? WHERE slot=1").run(frozen, baseline);
        return "imported";
    });
}

function legacyIngress(db, ingress, state) {
    const { update_id: update, message: { chat, message_thread_id: thread } } = ingress;
    const session = `telegram:${chat.id}${thread == null ? "" : `#${thread}`}`;
    const { changes } = db.prepare(`
        INSERT INTO telegram_ingress(update_id,payload,session_id,state,created_at)
        VALUES(?,?,?,?,'2026-01-01') ON CONFLICT(update_id) DO NOTHING
    `).run(String(update), JSON.stringify(ingress), session, state);
    return { session, changes };
}

export function admitLegacyFixtureUpdate(db, ingress) {
    return legacyIngress(db, ingress, "pending").changes === 1 ? "accepted" : "duplicate";
}

function insertLegacyTask(db, taskId, ingress, status, result = null) {
    const { session, changes } = legacyIngress(db, ingress, "done");
    assert.equal(changes, 1, "legacy update already recorded");
    const { chat, message_thread_id: thread, text: prompt } = ingress.message;
    db.prepare(`INSERT INTO task_runs(id,session_id,channel,source_chat_id,source_thread_id,update_id,
        prompt,status,created_at,result_summary) VALUES(?,?,'telegram',?,?,?,?,?,'2026-01-01',?)
    `).run(taskId, session, String(chat.id), thread == null ? null : String(thread),
        String(ingress.update_id), prompt, status, result);
    db.prepare("INSERT INTO runtime_tasks(task_id,session_id,channel,current_run_id) VALUES(?,?,'telegram',?)")
        .run(taskId, session, taskId);
    db.prepare("INSERT INTO runtime_attempts(run_id,task_id,ordinal) VALUES(?,?,1)").run(taskId, taskId);
}

export function appendRunningFixtureTask(db, taskId, ingress) {
    transaction(db, () => insertLegacyTask(db, taskId, ingress, "running"));
}

export function returnSettledFixtureTask(db, frozen, taskId, ingress, receipt, publicKey) {
    const { update_id: update, message } = ingress;
    assert.ok(receipt && receipt.task === `telegram:${update}` && receipt.update_id === update
        && receipt.chat_id === Number(message.chat.id)
        && receipt.thread_id === (message.message_thread_id ?? null) && receipt.prompt === message.text
        && receipt.attempt_phase === "completed" && receipt.delivery_phase === "delivered"
        && receipt.sends === 1 && integer(receipt.message_id, Number.MAX_SAFE_INTEGER),
    "return requires a settled, matching update");
    const signed = `coconutclaw-evidence-v1\nlocal-executor-fixture\nalpha\n${receipt.payload}`;
    assert.ok(verify(null, Buffer.from(signed), publicKey, Buffer.from(receipt.signature, "hex")),
        "invalid return result signature");
    assert.deepEqual(JSON.parse(receipt.payload), {
        attempt: receipt.attempt, event: { kind: "result", text: receipt.text },
    }, "return result does not match delivery");
    const record = JSON.stringify({ frozen: JSON.parse(frozen).sha256, ingress, ...receipt,
        key: publicKey.export({ type: "spki", format: "der" }).toString("hex") });
    return transaction(db, () => {
        db.exec(`CREATE TABLE IF NOT EXISTS synthetic_return(
            task_run_id INTEGER PRIMARY KEY REFERENCES task_runs(id), record TEXT NOT NULL
        ) STRICT`);
        const previous = db.prepare("SELECT record FROM synthetic_return WHERE task_run_id=?").get(taskId);
        if (previous) {
            assert.equal(previous.record, record, "conflicting fixture return");
            return "already imported";
        }
        assert.equal(cutoverSnapshot(db), frozen, "legacy fixture changed after freeze");
        insertLegacyTask(db, taskId, ingress, "completed", receipt.text);
        db.prepare("INSERT INTO task_payloads(task_run_id,request,output,delivered) VALUES(?,'{}',?,1)")
            .run(taskId, `TELEGRAM_REPLY: ${receipt.text}`);
        db.prepare("INSERT INTO synthetic_return VALUES(?,?)").run(taskId, record);
        return "imported";
    });
}
