import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";

const version = "coconutclaw-synthetic-cutover-v1";
const digest = payload => createHash("sha256").update(payload).digest("hex");
const integer = (value, max) => Number.isSafeInteger(value) && value > 0 && value <= max;
const text = value => typeof value === "string" && value.trim().length > 0;

export function cutoverSnapshot() {
    const source = new DatabaseSync(":memory:");
    try {
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
    } finally {
        source.close();
    }
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
    return tasks.map(({ taskId, runId, sequence, ingress, session, status, error, delivery }) => {
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
}

export function importCutoverFixture(db, snapshot) {
    const tasks = validate(snapshot);
    db.exec("BEGIN IMMEDIATE");
    try {
        db.exec("CREATE TABLE IF NOT EXISTS synthetic_cutover(slot INTEGER PRIMARY KEY CHECK(slot=1), snapshot TEXT NOT NULL) STRICT");
        const imported = db.prepare("SELECT snapshot FROM synthetic_cutover WHERE slot=1").get();
        if (imported) {
            assert.equal(imported.snapshot, snapshot, "conflicting fixture snapshot");
        } else {
            for (const table of ["inbox", "tasks", "attempts", "outbox", "schedule"]) {
                assert.equal(db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get(), undefined, "fixture destination must be empty");
            }
            const inbox = db.prepare("INSERT INTO inbox(sequence,update_id,disposition) VALUES(?,?,'task')");
            const task = db.prepare("INSERT INTO tasks(sequence,id,chat_id,thread_id,prompt) VALUES(?,?,?,?,?)");
            const attempt = db.prepare("INSERT INTO attempts(id,task,device,lease_until,phase) VALUES(?,?,'legacy-fixture',0,?)");
            const current = db.prepare("UPDATE tasks SET current_attempt=? WHERE id=?");
            const outbox = db.prepare("INSERT INTO outbox(task,text,phase,sends) VALUES(?,?,'unknown',?)");
            for (const row of tasks) {
                inbox.run(row.sequence, row.update);
                task.run(row.sequence, row.task, row.chat, row.thread, row.prompt);
                attempt.run(row.runId, row.task, row.attemptPhase);
                current.run(row.runId, row.task);
                if (row.plan) outbox.run(row.task, row.plan.markdown, row.plan.attempts);
            }
            db.prepare("INSERT INTO synthetic_cutover VALUES(1,?)").run(snapshot);
        }
        db.exec("COMMIT");
        return imported ? "already imported" : "imported";
    } catch (error) {
        db.exec("ROLLBACK");
        throw error;
    }
}
