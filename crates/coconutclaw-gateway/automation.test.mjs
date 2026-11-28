import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { controlWorkerFixture, credentialVersion, operatorBearer } from "./control-worker-fixture.mjs";
import { snapshotDigest } from "./control-migration.mjs";
import { authorityFixture } from "./runtime-authority-fixture.mjs";

const config = {
    repository: "fixture/project", zone: "UTC", time: "09:00", gap: "skip", fold: "earlier",
    chatId: 321, messageThreadId: 37,
};
const metadata = { runs: [{ name: "Build", status: "success", sha: "a".repeat(40) }] };
const digest = "CI digest · fixture/project\nBuild · success · aaaaaaa";
const start = "2026-07-10T08:00:00Z";
const due = "2026-07-10T09:00:00Z";

async function expect(response, status = 200) {
    const result = await response;
    const text = await result.text();
    assert.equal(result.status, status, text);
    return text ? JSON.parse(text) : undefined;
}

async function fixture(context, clock = start, automation = true) {
    const root = await mkdtemp(join(tmpdir(), "automation-"));
    const control = await controlWorkerFixture(context, join(root, "worker"), { automation });
    const legacy = await authorityFixture(join(root, "legacy.sqlite"));
    legacy.publish({
        pin: {
            generation: 1, revision: "e".repeat(40), hostname: "bigfur.liu.nz", objectId: "b".repeat(64),
            custodyDigest: "c".repeat(64), telegramCredentialVersion: credentialVersion, pairing: "production-v1",
        },
        versionId: "legacy",
    }, null);
    await legacy.close();
    context.after(() => rm(root, { recursive: true, force: true }));
    const source = legacy.snapshot();
    await expect(control.operator("import", { source, sourceSha256: snapshotDigest(source), gatewayVersion: "fixture" }));
    const api = (action, body, status) => expect(control.operator(`automation/${action}`, body), status);
    const command = (action, value, status) => expect(control.command({ action, value }), status);
    const time = value => command("clock", value === null ? null : Date.parse(value));
    const restart = async (value = clock, sender = true) => {
        await control.restart(sender);
        if (automation) await time(value);
    };
    await restart();
    control.onRead = () => Response.json(metadata);
    return {
        control, api, time, restart, command, wake: status => command("wake", undefined, status),
        save: (additional = {}, version = 0, id = "daily") => api("save", { id, version, config: { ...config, ...additional } }),
        gate: (frozen = false, status = 200) => api("freeze", { generation: 1, frozen }, status),
        resume: (version = 1, backfill = false, id = "daily") => api("resume", { id, version, backfill }),
        once: (key, status, revision = 1, id = "daily") => api("run-once", { id, revision, key }, status),
        history: async (id = "daily") => (await api("history", { id })).runs,
    };
}

test("private operator endpoints require a bearer and fixture bindings", async context => {
    const { control, save, once, gate, api } = await fixture(context);
    const path = "automation/dry-run";
    await expect(control.runtime(`operator/${path}`, { config, metadata }, operatorBearer), 404);
    await expect(control.operator(path, { config, metadata }, "0".repeat(64)), 401);
    await api("save", { id: "daily", version: 0, config: { ...config, repository: "other/repo" } }, 400);
    await api("dry-run", { config, metadata: { runs: [{ ...metadata.runs[0], name: "bad\nline" }] } }, 400);
    assert.deepEqual(await api("dry-run", { config, metadata }), { config, digest });
    assert.equal((await save()).paused, 1);
    await once("frozen-by-default", 503);
    await gate();
    assert.deepEqual(control.reads, []);
    assert.deepEqual(control.sends, []);
    const disabled = await fixture(context, start, false);
    await disabled.api("dry-run", { config, metadata }, 404);
});

test("preview resolves gaps, both fold choices, and a half-hour fold", async context => {
    const { save, api, time } = await fixture(context, "2026-03-07T12:00:00Z");
    await save({ zone: "America/New_York", time: "02:30" });
    const spring = await api("preview", { id: "daily" });
    assert.deepEqual(spring.gaps, ["2026-03-08T02:30"]);
    assert.equal(spring.schedule.config.messageThreadId, 37);
    assert.deepEqual(spring.times[0], { local: "2026-03-09T02:30", instant: Date.parse("2026-03-09T06:30:00Z") });
    await time("2026-11-01T00:00:00Z");
    await save({ zone: "America/New_York", time: "01:30" }, 1);
    const earlier = await api("preview", { id: "daily" });
    assert.equal(earlier.times[0].instant, Date.parse("2026-11-01T05:30:00Z"));
    assert.equal(earlier.times[1].instant, Date.parse("2026-11-02T06:30:00Z"));
    await save({ zone: "America/New_York", time: "01:30", fold: "later" }, 2);
    assert.equal((await api("preview", { id: "daily" })).times[0].instant, Date.parse("2026-11-01T06:30:00Z"));
    await time("2026-04-04T12:00:00Z");
    await save({ zone: "Australia/Lord_Howe", time: "01:45", fold: "later" }, 3);
    assert.equal((await api("preview", { id: "daily" })).times[0].instant, Date.parse("2026-04-04T15:15:00Z"));
    await api("save", { id: "daily", version: 4, config: { ...config, zone: "+01:00" } }, 400);
});

test("admission rollback and concurrent wakes preserve the occurrence and its quota slot", async context => {
    const { save, gate, resume, time, command, wake, history, api, control } = await fixture(context);
    await save();
    await gate();
    await resume();
    assert.ok(await command("pending-alarm"));
    await time(due);
    await command("fault", "admission");
    await wake(503);
    assert.deepEqual(await history(), []);
    assert.equal((await api("preview", { id: "daily" })).schedule.next_at, Date.parse(due));
    assert.ok(await command("pending-alarm"));
    assert.equal(control.sends.length, 0);
    await command("fault", "alarm");
    await wake(503);
    assert.deepEqual(await history(), []);
    assert.equal((await api("preview", { id: "daily" })).schedule.next_at, Date.parse(due));
    await Promise.all([wake(), wake(), wake()]);
    await wake();
    const [run, ...extra] = await history();
    assert.deepEqual(extra, []);
    assert.equal(run.run_key, `occurrence:1:${Date.parse(due)}`);
    assert.equal(run.quota_day, Math.floor(Date.parse(due) / 86_400_000));
    assert.equal(run.message_id, 1);
    assert.equal(control.reads.length, 1);
    assert.deepEqual(control.sends, [{ chat_id: 321, message_thread_id: 37, text: digest }]);
});

test("restart preserves admitted input across edit and manual replay", async context => {
    const { save, gate, once, restart, wake, history, control, api, command } = await fixture(context);
    await save();
    await gate();
    const [first, replay] = await Promise.all([once("manual-attempt"), once("manual-attempt")]);
    assert.deepEqual(first, replay);
    assert.equal(first.run_key, "manual:manual-attempt");
    await once("different", 409);
    assert.ok(await command("pending-alarm"));
    await restart();
    assert.ok(await command("pending-alarm"));
    const edit = await save({ chatId: 654, messageThreadId: 90 }, 1);
    assert.equal(edit.paused, 1);
    await api("pause", { id: "daily", version: 1 }, 409);
    await wake();
    const receipt = await once("manual-attempt");
    assert.equal(receipt.delivery, "sent");
    assert.equal(receipt.revision, 1);
    await once("manual-attempt", 409, 2);
    await once("new-revision", 200, 2);
    await wake();
    assert.deepEqual(control.sends.map(payload => [payload.chat_id, payload.message_thread_id]), [[321, 37], [654, 90]]);
    assert.equal((await history()).length, 2);
});

test("pause wins before admission and resume bounds backfill after a long sleep", async context => {
    const { save, gate, resume, time, wake, api, history, control } = await fixture(context);
    await save();
    await gate();
    await resume();
    await time(due);
    await api("pause", { id: "daily", version: 2 });
    await wake();
    assert.deepEqual(await history(), []);
    await time("2026-07-11T10:00:00Z");
    const resumed = await resume(3);
    assert.equal(resumed.next_at, Date.parse("2026-07-12T09:00:00Z"));
    assert.equal((await history())[0].reason, "resume_next_future");
    await api("pause", { id: "daily", version: 4 });
    await time("2026-07-20T10:00:00Z");
    await resume(5, true);
    await wake();
    assert.equal(control.sends.length, 0);
    assert.ok((await history()).some(run => run.reason === "catch_up_expired"));
    await wake();
    await wake();
    await wake();
    const runs = (await history()).filter(run => run.delivery === "sent");
    assert.deepEqual(runs.map(run => run.due_at).sort(), [
        Date.parse("2026-07-19T09:00:00Z"), Date.parse("2026-07-20T09:00:00Z"),
    ]);
    assert.equal(control.sends.length, 2);
});

test("a sent digest with a lost receipt holds the schedule across wakes, edit and restart", async context => {
    const { save, gate, resume, time, wake, history, control, restart, once, api, command } = await fixture(context);
    await save();
    await gate();
    await resume();
    await time(due);
    let entered;
    const sending = new Promise(resolve => { entered = resolve; });
    let release;
    const held = new Promise(resolve => { release = resolve; });
    control.onSend = async () => {
        entered();
        await held;
        return Response.json({ ok: true, result: { message_id: 19 } });
    };
    await command("fault", "sent");
    const waking = wake(503);
    await sending;
    try {
        assert.equal((await history())[0].delivery, "unknown");
        await api("pause", { id: "daily", version: 2 });
        await gate(true, 503);
        await expect(control.operator("retire", { generation: 1 }), 503);
        await Promise.all([wake(), wake()]);
    } finally {
        release();
    }
    await waking;
    await restart("2026-07-11T08:00:00Z");
    await save({ messageThreadId: 70 }, 3);
    await once("held", 409, 2);
    await resume(4);
    await time("2026-07-11T09:00:00Z");
    await wake();
    const runs = await history();
    assert.equal(runs[0].reason, "unresolved");
    assert.equal(runs[1].delivery, "unknown");
    assert.equal(runs[1].config.messageThreadId, 37);
    assert.equal(control.sends.length, 1);
});

test("freeze, retirement and missing sender block new admissions and known-unsent rows", async context => {
    const { save, gate, resume, time, wake, history, control, once, restart } = await fixture(context);
    await save();
    await gate();
    await resume();
    await gate(true);
    await once("blocked", 503);
    await time(due);
    await wake();
    assert.equal((await history())[0].reason, "control_unavailable");
    await gate();
    await restart(due, false);
    await once("sender-absent", 503);
    await restart(due);
    await once("prior-to-retirement");
    await expect(control.operator("retire", { generation: 1 }));
    await wake();
    assert.equal((await once("prior-to-retirement")).execution, "failed");
    await once("after-retirement", 503);
    assert.deepEqual(control.reads, []);
    assert.deepEqual(control.sends, []);
});

test("a freeze during metadata allows the read to finish but blocks the send", async context => {
    const { save, gate, once, wake, control } = await fixture(context);
    await save();
    await gate();
    let entered;
    const reading = new Promise(resolve => { entered = resolve; });
    let release;
    const held = new Promise(resolve => { release = resolve; });
    control.onRead = async () => {
        entered();
        await held;
        return Response.json(metadata);
    };
    await once("ready-while-frozen");
    const waking = wake();
    await reading;
    try {
        await gate(true);
    } finally {
        release();
    }
    await waking;
    const run = await once("ready-while-frozen");
    assert.equal(run.execution, "ready");
    assert.equal(run.delivery, "blocked");
    await gate();
    await wake();
    assert.equal(control.sends.length, 0);
});

test("a failed reservation rolls back and a lost Telegram response is held", async context => {
    const { save, gate, once, wake, command, restart, control } = await fixture(context);
    await save();
    await gate();
    await once("receipt-lost");
    await command("fault", "reserve");
    await wake(503);
    assert.equal((await once("receipt-lost")).delivery, "waiting");
    assert.equal(control.sends.length, 0);
    await restart();
    control.onSend = () => new Response(null, { status: 502 });
    await wake();
    assert.equal((await once("receipt-lost")).delivery, "unknown");
    await restart();
    await wake();
    await once("no-retry", 409);
    assert.equal(control.reads.length, 1);
    assert.equal(control.sends.length, 1);
});

test("each wake advances at most four schedules and sends at most one digest", async context => {
    const { save, gate, resume, time, wake, history, control, command } = await fixture(context);
    await gate();
    const ids = ["alpha", "bravo", "charlie", "delta", "echo"];
    for (const id of ids) {
        await save({}, 0, id);
        await resume(1, false, id);
    }
    await time(due);
    await wake();
    assert.deepEqual(await Promise.all(ids.map(async id => (await history(id)).length)), [1, 1, 1, 1, 0]);
    assert.equal(control.sends.length, 1);
    assert.ok(await command("pending-alarm"));
    await wake();
    assert.deepEqual(await Promise.all(ids.map(async id => (await history(id)).length)), [1, 1, 1, 1, 1]);
    assert.equal(control.sends.length, 2);
});

test("read failure is terminal and quota covers manual and scheduled admissions", async context => {
    const { save, gate, resume, once, wake, time, history, control } = await fixture(context);
    await save();
    await save({}, 0, "other");
    await gate();
    await resume(1, false, "other");
    control.onRead = () => Response.json({ runs: [{ ...metadata.runs[0], detail: "unscoped" }] });
    await once("bad-metadata");
    await wake();
    assert.equal((await once("bad-metadata")).reason, "metadata_unavailable");
    assert.equal(control.sends.length, 0);
    control.onRead = () => Response.json(metadata);
    for (let index = 0; index < 7; index++) {
        await once(`run-${index}`);
        await wake();
    }
    await once("over-budget", 429);
    await time(due);
    await wake();
    assert.equal((await history("other"))[0].reason, "quota");
    assert.equal(control.sends.length, 7);
    await time("2026-07-11T00:00:00Z");
    await once("new-day");
    await wake();
    assert.equal(control.sends.length, 8);
});

test("the installed Durable Object alarm dispatches after restart", async context => {
    const { gate, save, once, restart, control, command } = await fixture(context, null);
    await save();
    await gate();
    await command("delay-alarm", 1500);
    await once("real-alarm");
    assert.equal(control.sends.length, 0);
    const alarm = await command("pending-alarm");
    await restart(null);
    assert.equal(await command("pending-alarm"), alarm);
    for (let index = 0; index < 80 && !control.sends.length; index++) await sleep(50);
    await sleep(100);
    assert.equal((await once("real-alarm")).delivery, "sent");
    assert.equal(control.sends.length, 1);
});
