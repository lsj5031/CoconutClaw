import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";

const command = (...args) => spawnSync(process.execPath, [join(import.meta.dirname, "operator-query.mjs"), ...args],
    { encoding: "utf8" });

function readFixture(context, seed) {
    const directory = mkdtempSync(join(tmpdir(), "bigfur-query-test-"));
    const path = join(directory, "ledger.sqlite");
    const writer = new DatabaseSync(path, { enableForeignKeyConstraints: false });
    writer.exec(readFileSync(join(import.meta.dirname, "src/schema.sql"), "utf8"));
    writer.exec(seed);
    writer.close();
    const reader = new DatabaseSync(path, { readOnly: true });
    context.after(() => {
        reader.close();
        rmSync(directory, { recursive: true, force: true });
    });
    return (...args) => {
        const result = command(...args);
        assert.equal(result.status, 0, result.stderr);
        const queries = JSON.parse(result.stdout);
        assert.equal(queries.length, 1);
        const [{ sql, params = [] }] = queries;
        return { ...reader.prepare(sql).get(...params) };
    };
}

test("task status projects execution, delivery and receipt evidence independently", context => {
    const read = readFixture(context, `
        INSERT INTO tasks(id, chat_id, thread_id, prompt, pin, current_attempt) VALUES
            ('telegram:1', 12345, 6789, 'synthetic prompt', 'alpha', 1),
            ('telegram:2', 12345, 6789, 'synthetic prompt', 'beta', 2),
            ('telegram:3', 12345, 6789, 'synthetic prompt', 'alpha', 3),
            ('telegram:4', 12345, 6789, 'synthetic prompt', 'alpha', 4);
        INSERT INTO attempts(id, task, device, lease_until, start_until, phase) VALUES
            (1, 'telegram:1', 'synthetic device', 1700000000000, 1700000005000, 'unknown'),
            (2, 'telegram:2', 'synthetic device', 1700000000000, 1700000005000, 'completed'),
            (3, 'telegram:3', 'synthetic device', 1700000000000, 1700000005000, 'completed'),
            (4, 'telegram:4', 'synthetic device', 1700000000000, 1700000005000, 'completed');
        INSERT INTO executor_evidence VALUES (1, 'unknown', 'synthetic untrusted payload', 'synthetic signature');
        INSERT INTO outbox(task, text, phase, sends) VALUES
            ('telegram:2', 'synthetic result', 'unknown', 1),
            ('telegram:3', 'synthetic result', 'delivered', 2),
            ('telegram:4', 'synthetic result', 'delivered', 1);
        INSERT INTO delivery_receipts VALUES ('telegram:3', 54321);
    `);
    assert.deepEqual(read("task-status", "telegram:1"), {
        task_evidence: "present", execution: "unknown", attempt: 1, cancel_requested: 0,
        claim_lease_expires_at_ms: null, start_grant_expires_at_ms: null,
        delivery: "no_outbox", send_reservations: null, delivery_receipt_recorded: null,
        delivery_ready_at_ms: null, delivery_expires_at_ms: null,
    });
    assert.deepEqual(read("task-status", "telegram:2"), {
        task_evidence: "present", execution: "completed", attempt: 2, cancel_requested: 0,
        claim_lease_expires_at_ms: null, start_grant_expires_at_ms: null,
        delivery: "unknown", send_reservations: 1, delivery_receipt_recorded: 0,
        delivery_ready_at_ms: null, delivery_expires_at_ms: null,
    });
    const delivered = read("task-status", "telegram:3");
    assert.equal(delivered.delivery, "delivered");
    assert.equal(delivered.delivery_receipt_recorded, 1);
    assert.deepEqual(Object.keys(delivered), Object.keys(read("task-status", "telegram:2")));
    assert.doesNotMatch(JSON.stringify(delivered), /synthetic|12345|6789|54321/);
    const withoutReceipt = read("task-status", "telegram:4");
    assert.equal(withoutReceipt.delivery, "delivered");
    assert.equal(withoutReceipt.delivery_receipt_recorded, 0);
});

test("task status distinguishes missing evidence and reports only stored deadlines", context => {
    const read = readFixture(context, `
        INSERT INTO tasks(id, chat_id, prompt, current_attempt, cancelled) VALUES
            ('telegram:1', 1, '', NULL, 0), ('telegram:2', 1, '', NULL, 1),
            ('telegram:3', 1, '', 999, 0), ('telegram:4', 1, '', 4, 0),
            ('telegram:5', 1, '', 5, 0), ('telegram:6', 1, '', 6, 0),
            ('telegram:7', 1, '', 7, 0);
        INSERT INTO attempts(id, task, device, lease_until, start_until, phase) VALUES
            (4, 'telegram:4', 'alpha', 1700000000000, NULL, 'claimed'),
            (5, 'telegram:5', 'alpha', 1700000000000, 1700000005000, 'authorized'),
            (6, 'telegram:6', 'alpha', 1700000000000, 1700000005000, 'completed'),
            (7, 'telegram:7', 'alpha', 1700000000000, 1700000005000, 'completed');
        INSERT INTO outbox VALUES
            ('telegram:6', '', 'in_flight', 1, 1700000010000), ('telegram:7', '', 'pending', 1, 1700000020000);
    `);
    const absent = read("task-status", "telegram:0");
    assert.equal(absent.task_evidence, "absent");
    assert.equal(absent.execution, null);
    assert.equal(absent.delivery, null);
    assert.equal(read("task-status", "telegram:1").execution, "no_attempt");
    assert.equal(read("task-status", "telegram:2").execution, "cancelled_before_start");
    assert.equal(read("task-status", "telegram:3").execution, "missing_attempt");
    const claimed = read("task-status", "telegram:4");
    const authorized = read("task-status", "telegram:5");
    assert.equal(claimed.claim_lease_expires_at_ms, 1700000000000);
    assert.equal(claimed.start_grant_expires_at_ms, null);
    assert.equal(authorized.claim_lease_expires_at_ms, null);
    assert.equal(authorized.start_grant_expires_at_ms, 1700000005000);
    assert.equal(read("task-status", "telegram:6").delivery_expires_at_ms, 1700000010000);
    assert.equal(read("task-status", "telegram:7").delivery_ready_at_ms, 1700000020000);
});

test("webhook health reports retained commits and freeze evidence without update identifiers", context => {
    const direct = `
        CREATE TABLE direct_generation (slot INTEGER PRIMARY KEY, generation INTEGER);
        INSERT INTO direct_generation VALUES (1, 2);
    `;
    const empty = readFixture(context, direct);
    assert.deepEqual(empty("webhook-health"), {
        ledger_generation: 2, ingress_frozen: 0, committed_updates: 0,
        last_committed_sequence: null, last_committed_disposition: null,
    });
    const frozen = readFixture(context, `${direct}
        CREATE TABLE direct_ingress_frozen (slot INTEGER PRIMARY KEY);
        INSERT INTO inbox(update_id, disposition) VALUES (91000, 'task'), (81000, 'task');
    `);
    assert.deepEqual(frozen("webhook-health"), {
        ledger_generation: 2, ingress_frozen: 1, committed_updates: 2,
        last_committed_sequence: 2, last_committed_disposition: "task",
    });
    const otherLedger = readFixture(context, "");
    assert.throws(() => otherLedger("webhook-health"), /no such table: direct_generation/);
});

test("operator query rejects task content, SQL and out-of-range update IDs as arguments", () => {
    for (const args of [["task-status", "telegram:1' UNION SELECT prompt"], ["task-status", "telegram:2147483648"],
        ["task-status", "telegram:01"], ["task-status"], ["webhook-health", "synthetic-payload"]]) {
        const result = command(...args);
        assert.equal(result.status, 1);
        assert.equal(result.stdout, "");
        assert.doesNotMatch(result.stderr, /UNION|synthetic-payload/);
    }
});
