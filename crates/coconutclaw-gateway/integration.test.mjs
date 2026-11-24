import assert from "node:assert/strict";
import { generateKeyPairSync, sign } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { request } from "node:https";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { readBuildOutput } from "@cloudflare/build-output-utils";
import { Log, LogLevel, Miniflare } from "miniflare";

const { workers: { default: { bundleDir, config } } } = await readBuildOutput(import.meta.dirname);
const { mainModule, modules } = config.manifest;
const moduleTypes = { esm: "ESModule", wasm: "CompiledWasm" };
const workerModules = [mainModule, ...Object.keys(modules).filter(name => name !== mainModule)].map(name => ({
    path: join(bundleDir, name),
    type: moduleTypes[modules[name].type],
}));
const variables = {};
const durableObjects = {};
for (const [name, binding] of Object.entries(config.env)) {
    if (binding.type === "text") {
        variables[name] = binding.value;
    } else {
        assert.equal(binding.type, "durable-object");
        assert.equal(binding.worker, config.name);
        durableObjects[name] = {
            className: binding.exportName,
            useSQLite: config.exports[binding.exportName].storage === "sqlite",
        };
    }
}
const webhook = { "x-telegram-bot-api-secret-token": "synthetic-webhook" };
const operator = { "x-fixture-secret": "synthetic-operator" };
const keys = Object.fromEntries(["alpha", "beta"].map(device => [device, generateKeyPairSync("ed25519")]));
const signingPrefix = "coconutclaw-v1\nlocal-fixture\nPOST\n/v1/agent\n";

async function runFixture(context, { enabled = true } = {}) {
    const persistence = await mkdtemp(join(import.meta.dirname, "build", "sqlite-"));
    const sends = [];
    const logs = [];
    const log = new Log(LogLevel.INFO);
    log.log = message => logs.push(message);
    const sequence = { alpha: 0, beta: 0 };
    let service;
    let url;
    let calls = 0;
    async function restart() {
        await service?.dispose();
        service = new Miniflare({
            host: "127.0.0.1",
            port: 0,
            https: true,
            cf: false,
            liveReload: false,
            log,
            handleRuntimeStdio(stdout, stderr) {
                for (const stream of [stdout, stderr]) {
                    stream.setEncoding("utf8");
                    stream.on("data", chunk => logs.push(chunk));
                }
            },
            modules: workerModules,
            modulesRoot: bundleDir,
            compatibilityDate: config.compatibilityDate,
            compatibilityFlags: config.compatibilityFlags,
            bindings: {
                ...variables,
                ...(enabled ? { PHASE: "phase1-local" } : {}),
                WEBHOOK_SECRET: webhook["x-telegram-bot-api-secret-token"],
                FIXTURE_SECRET: operator["x-fixture-secret"],
                ...Object.fromEntries(Object.entries(keys).map(([device, pair]) => [
                    `DEVICE_${device.toUpperCase()}_KEY`,
                    pair.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex"),
                ])),
            },
            durableObjects,
            durableObjectsPersist: persistence,
            outboundService: async req => {
                assert.equal(req.url, "https://telegram.invalid/sendMessage");
                assert.equal(req.method, "POST");
                const body = await req.json();
                sends.push(body);
                assert.ok(sends.length <= 8);
                if (body.text === "mock lost response") return new Response(null, { status: 502 });
                if (body.text === "mock 429") {
                    return Response.json({
                        ok: false, error_code: 429, parameters: { retry_after: 1 },
                    }, { status: 429 });
                }
                return Response.json({ ok: true, result: { message_id: sends.length } });
            },
        });
        url = await service.ready;
        assert.equal(url.protocol, "https:");
        assert.equal(url.hostname, "127.0.0.1");
    }
    context.after(async () => {
        await service?.dispose();
        await rm(persistence, { recursive: true, force: true });
        assert.doesNotMatch(logs.join(""), /synthetic-webhook|synthetic-operator|sensitive synthetic task/);
    });
    await restart();

    async function post(path, body, headers, status = 200, chunked = false) {
        assert.ok(++calls <= 72, "test request budget");
        const bytes = typeof body === "string" ? body : JSON.stringify(body);
        const response = await new Promise((resolve, reject) => {
            const req = request(new URL(`/v1/${path}`, url), {
                method: "POST",
                rejectUnauthorized: false,
                timeout: 5000,
                headers: {
                    "content-type": "application/json",
                    ...(chunked ? {} : { "content-length": Buffer.byteLength(bytes) }),
                    ...headers,
                },
            }, async res => {
                try {
                    const chunks = [];
                    for await (const chunk of res) chunks.push(chunk);
                    resolve({ status: res.statusCode, bytes: Buffer.concat(chunks).toString() });
                } catch (error) {
                    reject(error);
                }
            });
            req.on("error", reject);
            req.on("timeout", () => req.destroy(new Error("loopback request timed out")));
            if (chunked) req.write(bytes.slice(0, 9000));
            req.end(chunked ? bytes.slice(9000) : bytes);
        });
        assert.ok([status].flat().includes(response.status), `${path} HTTP ${response.status} ${response.bytes}`);
        return JSON.parse(response.bytes);
    }

    function signed(device, command, { at = Date.now(), prefix = signingPrefix } = {}) {
        const mutating = !["hello", "next"].includes(command.op);
        const body = JSON.stringify({
            device, at_ms: at, sequence: mutating ? ++sequence[device] : 0, command,
        });
        const signature = sign(null, Buffer.from(prefix + body), keys[device].privateKey).toString("hex");
        return { body, headers: { "x-agent-signature": signature } };
    }

    const wire = (packet, status) => post("agent", packet.body, packet.headers, status);
    const agent = (device, op, fields, status) => wire(signed(device, { op, ...fields }), status);
    const fixture = (op, fields, status, fault) => post("fixture", { op, ...fields }, {
        ...operator, ...(fault ? { "x-fixture-fault": fault } : {}),
    }, status);
    return { agent, fixture, post, restart, sends, signed, wire };
}

function update(id, topic = 7, text = `synthetic task ${id}`) {
    return {
        update_id: id,
        message: { chat: { id: "321", type: "private" }, from: { id: 321 },
            message_thread_id: topic, text },
    };
}

const timeout = { timeout: 20_000 };

test("checked-in phase gate disables webhook, agent and fixture routes", timeout, async context => {
    const client = await runFixture(context, { enabled: false });
    await client.post("telegram", update(41), webhook, 404);
    await client.agent("alpha", "hello", {}, 404);
    await client.fixture("wake", { at_ms: Date.now() }, 404);
    assert.deepEqual(client.sends, []);
});

test("webhook admission, rollback, durable ack after restart, and late same-topic update", timeout, async context => {
    const { agent, post, restart } = await runFixture(context);
    await post("telegram", update(41), {}, 401);
    await post("telegram", "a".repeat(16385), webhook, 413);
    await post("telegram", "a".repeat(16385), webhook, 413, true);
    await post("telegram", { update_id: "41" }, webhook, 400);
    const concurrently = await Promise.all([post("telegram", update(41), webhook), post("telegram", update(41), webhook)]);
    assert.deepEqual(concurrently.map(result => result.disposition).sort(), ["duplicate", "task"]);
    await post("telegram", update(39), { ...webhook, ...operator, "x-fixture-fault": "rollback" }, 503);
    await post("telegram", update(38, 8, "sensitive synthetic task"), {
        ...webhook, ...operator, "x-fixture-fault": "after_commit",
    }, 503);
    await restart();
    assert.equal((await post("telegram", update(41), webhook)).disposition, "duplicate");
    assert.equal((await post("telegram", update(38), webhook)).disposition, "duplicate");
    assert.equal((await post("telegram", update(39), webhook)).disposition, "task");
    assert.equal((await post("telegram", update(39, 9, "different duplicate"), webhook)).disposition, "duplicate");
    const wrongChat = update(42);
    wrongChat.message.from.id = 322;
    assert.equal((await post("telegram", wrongChat, webhook)).disposition, "ignored");
    assert.equal((await post("telegram", update(42), webhook)).disposition, "duplicate");
    for (const [id, prompt] of [[43, ""], [44, "x".repeat(4097)], [45, " /cancel"]]) {
        assert.equal((await post("telegram", update(id, 7, prompt), webhook)).disposition, "ignored");
    }
    for (const [id, route] of [[41, 7], [38, 8], [39, 7]]) {
        const task = `telegram:${id}`;
        assert.deepEqual(await agent("alpha", "next"), { task });
        const claim = await agent("alpha", "claim", { task });
        assert.equal(claim.chat_id, 321);
        assert.equal(claim.message_thread_id, route);
        assert.equal(claim.prompt, id === 38 ? "sensitive synthetic task" : `synthetic task ${id}`);
    }
    assert.deepEqual(await agent("alpha", "next"), { task: null });
});

test("signed handshake, lease transfer, lost Start reply, and cancellation evidence", timeout, async context => {
    const { agent, fixture, post, restart, signed, wire } = await runFixture(context);
    await wire(signed("alpha", { op: "hello" }, { prefix: signingPrefix.replace("local-fixture", "other") }), 401);
    await wire(signed("alpha", { op: "hello" }, { at: Date.now() - 60_000 }), 401);
    const tampered = signed("alpha", { op: "hello" });
    await wire({ ...tampered, body: tampered.body.replace("hello", "next") }, 401);
    assert.deepEqual(await agent("alpha", "hello"), {
        protocol: 1, instance: "local-fixture", device: "alpha", capabilities: ["text"], last_sequence: 0,
    });
    await post("telegram", update(51), webhook);
    await post("telegram", update(52), webhook);
    const task = "telegram:51";
    const packet = signed("alpha", { op: "claim", task });
    const { attempt, lease_until } = await wire(packet);
    assert.equal((await wire(packet, 409)).error, "replay");
    assert.equal((await agent("alpha", "claim", { task })).attempt, attempt);
    await agent("beta", "claim", { task }, 409);
    await sleep(Math.max(0, lease_until - Date.now() + 15));
    await agent("alpha", "start", { attempt }, 409);
    const replacement = (await agent("beta", "claim", { task })).attempt;
    assert.notEqual(replacement, attempt);
    await agent("alpha", "start", { attempt }, 409);
    await agent("alpha", "start", { attempt: replacement }, 409);
    const lostGrant = signed("beta", { op: "start", attempt: replacement });
    assert.deepEqual(await post("agent", lostGrant.body, {
        ...lostGrant.headers, ...operator, "x-fixture-fault": "after_commit",
    }, 503), { error: "fixture_after_commit" });
    const lostReplyAt = Date.now();
    await sleep(100);
    const grant = await agent("beta", "start", { attempt: replacement });
    assert.ok(grant.start_until <= lostReplyAt + 1500, "retry preserves the original grant");
    assert.deepEqual(await agent("beta", "start", { attempt: replacement }), grant);
    assert.equal((await fixture("disconnect", { attempt: replacement })).phase, "unknown");
    await restart();
    assert.equal((await wire(packet, 409)).error, "replay");
    await agent("alpha", "claim", { task }, 409);
    await agent("beta", "start", { attempt: replacement }, 409);
    assert.equal((await fixture("cancel", { task })).phase, "cancel_requested");
    assert.equal((await agent("beta", "started", { attempt: replacement })).cancel_requested, true);
    await fixture("disconnect", { attempt: replacement });
    await agent("beta", "cancelled", { attempt: replacement, evidence: "before_start" }, 409);
    const exited = { attempt: replacement, evidence: "process_exited" };
    assert.equal((await agent("beta", "cancelled", exited)).phase, "cancelled_after_exit");
    assert.equal((await agent("beta", "cancelled", exited)).phase, "cancelled_after_exit");

    await post("telegram", update(53), webhook);
    for (const [id, authorize] of [[52, false], [53, true]]) {
        const next = `telegram:${id}`;
        const { attempt: nextAttempt } = await agent("alpha", "claim", { task: next });
        if (authorize) await agent("alpha", "start", { attempt: nextAttempt });
        assert.equal((await fixture("cancel", { task: next })).phase, authorize ? "cancel_requested" : "cancelled_before_start");
        await agent("alpha", "start", { attempt: nextAttempt }, 409);
        if (authorize) {
            assert.equal((await agent("alpha", "cancelled", { attempt: nextAttempt, evidence: "before_start" })).phase,
                "cancelled_before_start");
        }
    }

    await post("telegram", update(54), webhook);
    const expiringTask = "telegram:54";
    const expiringAttempt = (await agent("alpha", "claim", { task: expiringTask })).attempt;
    const expiry = (await agent("alpha", "start", { attempt: expiringAttempt })).start_until;
    await sleep(Math.max(0, expiry - Date.now() + 15));
    await agent("alpha", "start", { attempt: expiringAttempt }, 409);
    await agent("beta", "claim", { task: expiringTask }, 409);
});

test("outbox custody, ambiguous delivery, and one bounded explicit retry", timeout, async context => {
    const { agent, fixture, post, restart, sends } = await runFixture(context);
    for (const [id, text] of [[61, "mock success"], [62, "mock lost response"], [63, "mock lost after acceptance"], [64, "mock 429"]]) {
        const task = `telegram:${id}`;
        assert.equal((await post("telegram", update(id, id), webhook)).disposition, "task");
        const { attempt } = await agent("alpha", "claim", { task });
        await agent("alpha", "result", { attempt, text }, 409);
        await agent("alpha", "start", { attempt });
        if (id === 61) await fixture("disconnect", { attempt });
        assert.equal((await agent("alpha", "result", { attempt, text })).phase, "completed");
        assert.equal((await agent("alpha", "result", { attempt, text })).phase, "completed");
        await agent("alpha", "result", { attempt, text: "different result" }, 409);
        assert.equal((await fixture("cancel", { task })).phase, "completed");
        assert.equal(sends.length, id - 61);
        if (id === 61) {
            const outcomes = await Promise.all([
                fixture("send", { task }, [200, 409]),
                fixture("send", { task }, [200, 409]),
            ]);
            assert.deepEqual(outcomes.sort((left, right) => Number(Boolean(left.error)) - Number(Boolean(right.error))),
                [{ phase: "delivered", sends: 1 }, { error: "state_conflict" }]);
        } else if (id === 62) {
            assert.deepEqual(await fixture("send", { task }), { phase: "unknown", sends: 1 });
        } else if (id === 63) {
            await fixture("send", { task }, 503, "drop_receipt");
            await restart();
            assert.deepEqual(await fixture("recover_send", { task }), { phase: "unknown", sends: 1 });
        } else {
            assert.deepEqual(await fixture("send", { task }), { phase: "pending", sends: 1 });
            await fixture("send", { task }, 409);
            await sleep(1050);
            assert.deepEqual(await fixture("send", { task }), { phase: "failed", sends: 2 });
        }
        await fixture("send", { task }, 409);
    }
    assert.deepEqual(sends.map(send => [send.chat_id, send.message_thread_id]), [
        [321, 61], [321, 62], [321, 63], [321, 64], [321, 64],
    ]);
});

test("finite real alarm after restart, pinned placement, catch-up, duplicate wake and disable", timeout, async context => {
    const { agent, fixture, restart, sends } = await runFixture(context);
    const first = Date.now() + 1200;
    const schedule = {
        version: 1, first_at: first, count: 1, interval_ms: 1000,
        message_thread_id: 70, pin: "alpha", prompt: "one shot",
    };
    await fixture("schedule", schedule);
    await restart();
    let found;
    for (let tries = 0; tries < 24; tries++) {
        found = (await agent("alpha", "next")).task;
        if (found) break;
        await sleep(125);
    }
    assert.equal(found, `schedule:fixture:${first}`);
    assert.deepEqual(await agent("beta", "next"), { task: null });
    await agent("beta", "claim", { task: found }, 409);
    assert.equal((await agent("alpha", "claim", { task: found })).message_thread_id, 70);
    assert.equal((await fixture("cancel", { task: found })).phase, "cancelled_before_start");
    assert.deepEqual(await fixture("wake", { at_ms: first + 5000 }), { task: null });

    const later = Date.now() + 5000;
    const recurring = { ...schedule, version: 2, first_at: later, count: 3, pin: null };
    await fixture("schedule", recurring);
    await fixture("schedule", recurring);
    assert.deepEqual(await fixture("wake", { at_ms: later - 1 }), { task: null });
    const occurrence = `schedule:fixture:${later + 1000}`;
    assert.deepEqual(await fixture("wake", { at_ms: later + 1000 }), { task: occurrence });
    await fixture("schedule", recurring);
    assert.deepEqual(await fixture("wake", { at_ms: later + 1000 }), { task: null });
    assert.deepEqual(await agent("beta", "next"), { task: occurrence });
    await agent("beta", "claim", { task: occurrence });
    await fixture("cancel", { task: occurrence });
    await fixture("disable", { version: 1 }, 409);
    await fixture("disable", { version: 2 });
    await fixture("schedule", recurring);
    assert.deepEqual(await fixture("wake", { at_ms: later + 4000 }), { task: null });
    await fixture("schedule", { ...recurring, version: 3, count: 1 });
    assert.deepEqual(await fixture("wake", { at_ms: later + 65_000 }), { task: null });
    assert.deepEqual(await agent("beta", "next"), { task: null });
    assert.deepEqual(sends, []);
});
