import assert from "node:assert/strict";
import { createHash, verify } from "node:crypto";
import { readFileSync } from "node:fs";

const version = "coconutclaw-synthetic-cutover-v2";
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
            INSERT INTO runtime_schedules(id,session_id,creation_key,creation_spec,request,timing,state,next_due)
            VALUES(7,'telegram:321#37','fixture','{}','{}','{}','active',1000);
    `);
}

export function cutoverSnapshot(source) {
    const tasks = source.prepare(`
            SELECT t.task_id AS taskId, a.run_id AS runId, g.seq AS sequence, g.payload AS ingress,
                   root.session_id AS session, r.status, r.error_summary AS error, p.delivery_state AS delivery,
                   coalesce(p.delivered,0) AS delivered
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
    const schedules = source.prepare(`
        SELECT id,session_id,revision,state,next_due FROM runtime_schedules ORDER BY id
    `).all();
    const payload = JSON.stringify({ version, tasks, schedules });
    return JSON.stringify({ sha256: digest(payload), payload });
}

function validate(snapshot) {
    const { sha256, payload } = JSON.parse(snapshot);
    assert.ok(typeof payload === "string" && sha256 === digest(payload), "fixture checksum mismatch");
    const fixture = JSON.parse(payload);
    assert.equal(fixture.version, version, "unsupported fixture version");
    const { tasks, schedules } = fixture;
    assert.ok(Array.isArray(tasks) && tasks.length > 0 && tasks.length < 128, "invalid fixture task count");
    assert.ok(Array.isArray(schedules) && schedules.every((schedule, index) =>
        integer(schedule.id, 511) && (index === 0 || schedule.id > schedules[index - 1].id)
        && schedule.session_id === "telegram:321#37" && integer(schedule.revision, 511)
        && ["active", "paused", "done", "cancelled"].includes(schedule.state)
        && (schedule.next_due === null || integer(schedule.next_due, Number.MAX_SAFE_INTEGER))),
    "invalid fixture schedules");
    for (const key of ["taskId", "runId", "sequence"]) {
        assert.equal(new Set(tasks.map(task => task[key])).size, tasks.length, `duplicate fixture ${key}`);
    }
    assert.equal(new Set(tasks.map(task => task.ingress.update_id)).size, tasks.length, "duplicate fixture update");
    const rows = tasks.map(({ taskId, runId, sequence, ingress, session, status, error, delivery, delivered }) => {
        const { update_id: update, message } = ingress;
        const { chat, message_thread_id: thread = null, text: prompt } = message;
        assert.ok(integer(taskId, 511) && integer(runId, 511) && integer(sequence, 127)
            && integer(update, 2 ** 31 - 1) && text(prompt), "invalid fixture identity or prompt");
        assert.ok(chat.type === "private" && chat.id === 321
            && (thread === null || integer(thread, 2 ** 31 - 1)), "invalid fixture route");
        assert.equal(session, `telegram:${chat.id}${thread === null ? "" : `#${thread}`}`, "conflicting fixture route");
        const completed = status === "completed";
        assert.ok(delivered === 0 || (completed && delivered === 1), "invalid fixture delivery flag");
        if (completed) {
            const { completed_ops, operations, text_plan } = delivery;
            assert.deepEqual(completed_ops, []);
            assert.deepEqual(operations, {});
            assert.ok(text(text_plan.markdown)
                && text_plan.kind === "rich"
                && ((text_plan.phase === "sending" && text_plan.attempts === 1)
                    || (text_plan.phase === "ready" && text_plan.attempts === 0)
                    || (text_plan.phase === "done" && text_plan.attempts === 1
                        && integer(text_plan.messageId, Number.MAX_SAFE_INTEGER))), "unsupported fixture delivery");
            assert.equal(delivered, Number(text_plan.phase === "done"), "conflicting fixture delivery flag");
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
    return { tasks, rows, schedules };
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

export function freezeGatewayFixture(db, generation) {
    return transaction(db, () => {
        db.exec("INSERT OR IGNORE INTO local_authority VALUES(1,1,0,1)");
        const authority = db.prepare("SELECT generation,active FROM local_authority").get();
        assert.equal(authority.generation, generation, "wrong fixture generation");
        if (authority.active === 0) return "already frozen";
        db.exec(`
            UPDATE local_authority SET active=0;
            UPDATE outbox SET phase='unknown' WHERE phase IN ('pending','in_flight');
            UPDATE attempts SET phase='unknown' WHERE phase IN ('claimed','authorized','started');
        `);
        return "frozen";
    });
}

export function advanceGatewayFixture(db, generation, publicKey, active) {
    return transaction(db, () => {
        const previous = db.prepare("SELECT generation,attempt_floor,active FROM local_authority").get();
        if (generation > 1 && previous.generation === generation) {
            assert.equal(previous.active, Number(active), "conflicting fixture generation replay");
            return "already advanced";
        }
        assert.equal(previous.active, 0, "freeze the fixture generation first");
        assert.equal(generation, previous.generation + 1, "fixture generation must advance exactly once");
        const instance = previous.generation === 1 ? "local-executor-fixture" : `local-executor-fixture:${previous.generation}`;
        const evidence = db.prepare("SELECT event,payload,signature FROM executor_evidence WHERE attempt=?");
        const grants = db.prepare("SELECT id, device FROM attempts WHERE id>? AND start_until IS NOT NULL ORDER BY id")
            .all(previous.attempt_floor);
        for (const { id, device } of grants) {
            assert.equal(device, "alpha", "unexpected fixture grant device");
            const receipts = evidence.all(id);
            const terminal = receipts.filter(receipt => receipt.event !== "started");
            assert.equal(terminal.length, 1, `attempt ${id} requires signed terminal evidence`);
            if (terminal[0].event === "result") {
                assert.ok(receipts.some(receipt => receipt.event === "started"), `attempt ${id} requires signed Start`);
            }
            for (const receipt of receipts) {
                const signed = `coconutclaw-evidence-v1\n${instance}\n${device}\n${receipt.payload}`;
                assert.ok(verify(null, Buffer.from(signed), publicKey, Buffer.from(receipt.signature, "hex")),
                    `invalid signed evidence for attempt ${id}`);
                const payload = JSON.parse(receipt.payload);
                assert.equal(payload.attempt, id, "evidence attempt mismatch");
                assert.equal(payload.event.kind, receipt.event, "evidence event mismatch");
            }
        }
        db.prepare(`UPDATE local_authority SET generation=?,active=?,
            attempt_floor=(SELECT coalesce(max(id),0) FROM attempts)`).run(generation, Number(active));
        return "advanced";
    });
}

function insertGatewayRows(db, rows) {
    const inbox = db.prepare("INSERT INTO inbox(sequence,update_id,disposition) VALUES(?,?,'task')");
    const task = db.prepare("INSERT INTO tasks(sequence,id,chat_id,thread_id,prompt) VALUES(?,?,?,?,?)");
    const attempt = db.prepare("INSERT INTO attempts(id,task,device,lease_until,phase) VALUES(?,?,'legacy-fixture',0,?)");
    const current = db.prepare("UPDATE tasks SET current_attempt=? WHERE id=?");
    for (const row of rows) {
        inbox.run(row.sequence, row.update);
        task.run(row.sequence, row.task, row.chat, row.thread, row.prompt);
        attempt.run(row.runId, row.task, row.attemptPhase);
        current.run(row.runId, row.task);
        projectDelivery(db, row);
    }
}

function projectDelivery(db, { task, plan }) {
    if (!plan) return;
    const delivered = plan.phase === "done";
    db.prepare(`INSERT INTO outbox(task,text,phase,sends) VALUES(?,?,?,?)
        ON CONFLICT(task) DO UPDATE SET text=excluded.text,phase=excluded.phase,sends=excluded.sends`)
        .run(task, plan.markdown, delivered ? "delivered" : "unknown", plan.attempts);
    if (delivered) {
        db.prepare("INSERT INTO delivery_receipts(task,message_id) VALUES(?,?)").run(task, plan.messageId);
    }
}

export function importCutoverFixture(db, snapshot) {
    const { rows } = validate(snapshot);
    return transaction(db, () => {
        db.exec(`CREATE TABLE IF NOT EXISTS synthetic_cutover(
            slot INTEGER PRIMARY KEY CHECK(slot=1), snapshot TEXT NOT NULL, baseline TEXT
        ) STRICT`);
        const imported = db.prepare("SELECT snapshot,baseline FROM synthetic_cutover WHERE slot=1").get();
        if (imported) {
            assert.equal(imported.baseline ?? imported.snapshot, snapshot, "conflicting fixture snapshot");
            return "already imported";
        }
        for (const table of ["inbox", "tasks", "attempts", "outbox", "schedule"]) {
            assert.equal(db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get(), undefined, "fixture destination must be empty");
        }
        insertGatewayRows(db, rows);
        db.prepare("INSERT INTO synthetic_cutover(slot,snapshot) VALUES(1,?)").run(snapshot);
        db.exec("INSERT INTO local_authority VALUES(1,1,0,0)");
        return "imported";
    });
}

export function importCutoverDelta(db, baseline, frozen) {
    const before = validate(baseline);
    const after = validate(frozen);
    assert.notEqual(baseline, frozen, "missing fixture delta");
    assert.ok(after.rows.length >= before.rows.length, "fixture delta removed tasks");
    const changed = [];
    for (const [index, prior] of before.tasks.entries()) {
        const next = after.tasks[index];
        const { delivery, delivered, ...identity } = prior;
        const { delivery: nextDelivery, delivered: nextDelivered, ...nextIdentity } = next;
        assert.deepEqual(nextIdentity, identity, "fixture delta changed stable task data");
        if (JSON.stringify(prior) === JSON.stringify(next)) continue;
        assert.ok(delivered === 0 && nextDelivered === 1
            && delivery?.text_plan.phase === "sending"
            && delivery.text_plan.markdown === nextDelivery.text_plan.markdown,
        "unsupported fixture delivery transition");
        changed.push(after.rows[index]);
    }
    for (const [index, previous] of before.schedules.entries()) {
        const next = after.schedules[index];
        assert.ok(next?.id === previous.id && next.session_id === previous.session_id
            && next.revision >= previous.revision
            && (next.next_due === null || previous.next_due === null || next.next_due >= previous.next_due),
        "fixture schedule regressed");
    }
    return transaction(db, () => {
        const imported = db.prepare("SELECT snapshot,baseline FROM synthetic_cutover WHERE slot=1").get();
        if (imported?.baseline === baseline && imported.snapshot === frozen) return "already imported";
        assert.ok(imported?.snapshot === baseline && imported.baseline === null, "wrong fixture baseline");
        for (const row of changed) projectDelivery(db, row);
        insertGatewayRows(db, after.rows.slice(before.rows.length));
        db.prepare("UPDATE synthetic_cutover SET snapshot=?,baseline=? WHERE slot=1").run(frozen, baseline);
        assert.equal(db.prepare("UPDATE local_authority SET active=1 WHERE generation=1 AND active=0").run().changes, 1);
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
    return transaction(db, () => {
        assert.equal(db.prepare("SELECT singleton FROM legacy_admission_freeze").get(), undefined, "legacy is frozen");
        return legacyIngress(db, ingress, "pending").changes === 1 ? "accepted" : "duplicate";
    });
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

function validateReturn(ingress, receipt, publicKey) {
    const { update_id: update, message } = ingress;
    assert.ok(receipt && receipt.task === `telegram:${update}` && receipt.update_id === update
        && receipt.chat_id === Number(message.chat.id)
        && receipt.thread_id === (message.message_thread_id ?? null) && receipt.prompt === message.text
        && integer(receipt.attempt, 512), "return update does not match stable identity");
    const delivered = receipt.attempt_phase === "completed" && receipt.delivery_phase === "delivered"
        && receipt.sends === 1 && integer(receipt.message_id, Number.MAX_SAFE_INTEGER);
    const held = receipt.attempt_phase === "unknown" && receipt.delivery_phase === null
        && receipt.sends === null && receipt.message_id === null;
    assert.ok(delivered || held, "return requires a settled or held effect");
    for (const [payload, signature] of [
        [receipt.start_payload, receipt.start_signature], [receipt.payload, receipt.signature],
    ]) {
        const signed = `coconutclaw-evidence-v1\nlocal-executor-fixture\nalpha\n${payload}`;
        assert.ok(verify(null, Buffer.from(signed), publicKey, Buffer.from(signature, "hex")),
            "invalid return evidence signature");
    }
    assert.deepEqual(JSON.parse(receipt.start_payload), {
        attempt: receipt.attempt, event: { kind: "started" },
    }, "return Start does not match attempt");
    const proof = JSON.parse(receipt.payload);
    assert.ok(text(proof.event.text), "invalid return result text");
    assert.deepEqual(proof, {
        attempt: receipt.attempt, event: { kind: "result", text: delivered ? receipt.text : proof.event.text },
    }, "return result does not match delivery");
    return delivered;
}

export function returnFixtureTasks(db, frozen, entries, publicKey) {
    assert.ok(entries.length > 0, "empty fixture return");
    for (const field of [entry => entry.taskId, entry => entry.receipt.task, entry => entry.receipt.attempt]) {
        assert.equal(new Set(entries.map(field)).size, entries.length, "duplicate returned identity");
    }
    const anchor = JSON.parse(frozen).sha256;
    const key = publicKey.export({ type: "spki", format: "der" }).toString("hex");
    const records = entries.map(({ taskId, ingress, receipt }) => ({
        taskId, ingress, receipt, delivered: validateReturn(ingress, receipt, publicKey),
        record: JSON.stringify({ frozen: anchor, ingress, ...receipt, key }),
    }));
    return transaction(db, () => {
        db.exec(`CREATE TABLE IF NOT EXISTS synthetic_return(
            task_run_id INTEGER PRIMARY KEY REFERENCES task_runs(id), task TEXT NOT NULL UNIQUE, record TEXT NOT NULL
        ) STRICT`);
        const lookup = db.prepare("SELECT task_run_id,record FROM synthetic_return WHERE task=?");
        const previous = records.map(({ receipt }) => lookup.get(receipt.task));
        if (previous.some(Boolean)) {
            for (const [index, { taskId, record }] of records.entries()) {
                assert.equal(previous[index]?.task_run_id, taskId, "conflicting fixture return");
                assert.equal(previous[index].record, record, "conflicting fixture return");
            }
            assert.equal(db.prepare("SELECT count(*) AS total FROM synthetic_return").get().total, records.length,
                "incomplete fixture return replay");
            return "already imported";
        }
        assert.equal(cutoverSnapshot(db), frozen, "legacy fixture changed after freeze");
        for (const { taskId, ingress, receipt, delivered, record } of records) {
            insertLegacyTask(db, taskId, ingress, delivered ? "completed" : "failed", receipt.text);
            if (delivered) {
                db.prepare("INSERT INTO task_payloads(task_run_id,request,output,delivered) VALUES(?,'{}',?,1)")
                    .run(taskId, `TELEGRAM_REPLY: ${receipt.text}`);
            } else {
                db.prepare("UPDATE task_runs SET error_summary=? WHERE id=?")
                    .run("effect outcome unknown at handoff; do not automatically retry", taskId);
            }
            db.prepare("INSERT INTO synthetic_return VALUES(?,?,?)").run(taskId, receipt.task, record);
        }
        assert.equal(db.prepare("DELETE FROM legacy_admission_freeze").run().changes, 1, "legacy must be frozen");
        return "imported";
    });
}
