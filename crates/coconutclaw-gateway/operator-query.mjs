import { updateId } from "./authority-preflight.mjs";

const taskStatus = `
SELECT CASE WHEN t.id IS NULL THEN 'absent' ELSE 'present' END AS task_evidence,
       CASE
           WHEN t.id IS NULL THEN NULL
           WHEN t.current_attempt IS NULL AND t.cancelled = 1 THEN 'cancelled_before_start'
           WHEN t.current_attempt IS NULL THEN 'no_attempt'
           WHEN a.id IS NULL THEN 'missing_attempt'
           ELSE a.phase
       END AS execution,
       a.id AS attempt,
       a.cancel_requested,
       CASE WHEN a.phase = 'claimed' THEN a.lease_until END AS claim_lease_expires_at_ms,
       CASE WHEN a.phase = 'authorized' THEN a.start_until END AS start_grant_expires_at_ms,
       CASE WHEN t.id IS NOT NULL THEN COALESCE(o.phase, 'no_outbox') END AS delivery,
       o.sends AS send_reservations,
       CASE WHEN o.task IS NOT NULL THEN r.task IS NOT NULL END AS delivery_receipt_recorded,
       CASE WHEN o.phase = 'pending' AND o.ready_at > 0 THEN o.ready_at END AS delivery_ready_at_ms,
       CASE WHEN o.phase = 'in_flight' AND o.ready_at > 0 THEN o.ready_at END AS delivery_expires_at_ms
FROM (SELECT ? AS id) AS requested
LEFT JOIN tasks t ON t.id = requested.id
LEFT JOIN attempts a ON a.id = t.current_attempt AND a.task = t.id
LEFT JOIN outbox o ON o.task = t.id
LEFT JOIN delivery_receipts r ON r.task = o.task`;

const webhookHealth = `
SELECT (SELECT generation FROM direct_generation WHERE slot = 1) AS ledger_generation,
       EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'direct_ingress_frozen') AS ingress_frozen,
       (SELECT COUNT(*) FROM inbox) AS committed_updates,
       latest.sequence AS last_committed_sequence,
       latest.disposition AS last_committed_disposition
FROM (SELECT 1) AS snapshot
LEFT JOIN (SELECT sequence, disposition FROM inbox ORDER BY sequence DESC LIMIT 1) AS latest ON true`;

const [command, task, ...extra] = process.argv.slice(2);
if (command === "webhook-health" && task === undefined) {
    console.log(JSON.stringify([{ sql: webhookHealth }]));
} else if (command === "task-status" && extra.length === 0 &&
    /^telegram:(?:0|[1-9]\d{0,9})$/.test(task ?? "") && updateId(Number(task.slice("telegram:".length)))) {
    console.log(JSON.stringify([{ sql: taskStatus, params: [task] }]));
} else {
    console.error("Usage: node operator-query.mjs task-status telegram:<update-id> | webhook-health");
    process.exitCode = 1;
}
