import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash, generateKeyPairSync, sign, verify } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createServer, request } from "node:https";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { readBuildOutput } from "@cloudflare/build-output-utils";
import { Log, LogLevel, Miniflare } from "miniflare";
import { cutoverSnapshot, importCutoverFixture } from "./cutover-fixture.mjs";

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
const signingPrefix = "coconutclaw-v1\nlocal-fixture\nPOST\n/v1/agent\n";
const executorPrefix = signingPrefix.replace("local-fixture", "local-executor-fixture");

async function runFixture(context, { enabled = true, executor = false, inspect = false, mock } = {}) {
    const persistence = await mkdtemp(join(import.meta.dirname, "build", "sqlite-"));
    const sends = [];
    const sendTimes = [];
    const logs = [];
    const log = new Log(LogLevel.INFO);
    log.log = message => logs.push(message);
    const sequence = { alpha: 0, beta: 0 };
    const keys = Object.fromEntries(["alpha", "beta"].map(device => [device, generateKeyPairSync("ed25519")]));
    const publicHex = device => keys[device].publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    let service;
    let url;
    let calls = 0;
    let stopped = true;
    async function restart(edit) {
        await service?.dispose();
        stopped = true;
        if (edit) await edit();
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
                ...(enabled ? { PHASE: executor ? "phase2-local" : "phase1-local" } : {}),
                WEBHOOK_SECRET: webhook["x-telegram-bot-api-secret-token"],
                FIXTURE_SECRET: operator["x-fixture-secret"],
                ...Object.fromEntries(Object.keys(keys).map(device => [
                    `DEVICE_${device.toUpperCase()}_KEY`,
                    publicHex(device),
                ])),
                ...(executor ? { EXECUTOR_ALPHA_KEY: publicHex("alpha") } : {}),
                ...(inspect ? { LOCAL_SQL_METRICS: "1" } : {}),
            },
            durableObjects,
            durableObjectsPersist: persistence,
            outboundService: async req => {
                assert.equal(req.url, "https://telegram.invalid/sendMessage");
                assert.equal(req.method, "POST");
                const body = await req.json();
                sends.push(body);
                sendTimes.push(Date.now());
                assert.ok(sends.length <= 8);
                if (mock) return mock(body, sends.length);
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
        stopped = false;
        assert.equal(url.protocol, "https:");
        assert.equal(url.hostname, "127.0.0.1");
    }
    context.after(async () => {
        await service?.dispose();
        await rm(persistence, { recursive: true, force: true });
        assert.doesNotMatch(logs.join(""), /synthetic-webhook|synthetic-operator|sensitive synthetic task/);
    });
    await restart();

    async function rawPost(path, body, headers, chunked = false) {
        assert.ok(++calls <= (executor ? 160 : 72), "test request budget");
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
                    resolve({ status: res.statusCode, headers: res.headers, value: JSON.parse(Buffer.concat(chunks).toString()) });
                } catch (error) {
                    reject(error);
                }
            });
            req.on("error", reject);
            req.on("timeout", () => req.destroy(new Error("loopback request timed out")));
            if (chunked) req.write(bytes.slice(0, 9000));
            req.end(chunked ? bytes.slice(9000) : bytes);
        });
        return response;
    }

    async function post(path, body, headers, status = 200, chunked = false) {
        const response = await rawPost(path, body, headers, chunked);
        assert.ok([status].flat().includes(response.status), `${path} HTTP ${response.status} ${JSON.stringify(response.value)}`);
        return response.value;
    }

    function signed(device, command, { at = Date.now(), prefix = executor ? executorPrefix : signingPrefix, seq } = {}) {
        const mutating = !["hello", "next", "status"].includes(command.op);
        const body = JSON.stringify({
            device, at_ms: at, sequence: seq ?? (mutating ? ++sequence[device] : 0), command,
        });
        const signature = sign(null, Buffer.from(prefix + body), keys[device].privateKey).toString("hex");
        return { body, headers: { "x-agent-signature": signature } };
    }

    const wire = (packet, status) => post("agent", packet.body, packet.headers, status);
    const agent = (device, op, fields, status) => wire(signed(device, { op, ...fields }), status);
    const fixture = (op, fields, status, fault) => post("fixture", { op, ...fields }, {
        ...operator, ...(fault ? { "x-fixture-fault": fault } : {}),
    }, status);
    async function withFixtureDb(filePattern, use) {
        assert.ok(inspect);
        const files = (await readdir(persistence, { recursive: true })).filter(file => filePattern.test(file));
        assert.equal(files.length, 1, files.join(", "));
        const db = new DatabaseSync(join(persistence, files[0]), { readOnly: !stopped });
        try {
            db.exec("PRAGMA busy_timeout=500");
            return use(db);
        } finally {
            db.close();
        }
    }
    const ledgerFile = /[a-f0-9]{64}\.sqlite$/;
    const queryFixture = (filePattern, query, args) =>
        withFixtureDb(filePattern, db => db.prepare(query).all(...args).map(row => ({ ...row })));
    const editLedger = use => {
        assert.ok(stopped);
        return withFixtureDb(ledgerFile, use);
    };
    const sql = (query, ...args) => queryFixture(ledgerFile, query, args);
    const scheduledAlarm = () => queryFixture(/metadata\.sqlite$/, "SELECT scheduled_time/1000000 AS scheduled_time FROM _cf_ALARM", []);
    function metrics() {
        return Array.from(logs.join("\n").matchAll(/pilot_sql (\w+) (\d+) (\d+) (\d+)/g),
            ([, scope, read, written, ms]) => ({ scope, read: +read, written: +written, ms: +ms }));
    }
    return { agent, fixture, post, rawPost, restart, sends, sendTimes, signed, wire, keys, sql, editLedger, scheduledAlarm, metrics };
}

function update(id, topic = 7, text = `synthetic task ${id}`) {
    return {
        update_id: id,
        message: { chat: { id: "321", type: "private" }, from: { id: 321 },
            message_thread_id: topic, text },
    };
}

const timeout = { timeout: 20_000 };

const executable = process.env.GATEWAY_LOCAL_EXECUTOR;
const nativeTimeout = { timeout: 45_000, skip: !executable };

async function until(check, message, timeoutMs = 10_000) {
    const deadline = Date.now() + timeoutMs;
    do {
        const result = await check();
        if (result) return result;
        await sleep(50);
    } while (Date.now() < deadline);
    assert.fail(message);
}

async function attachExecutor(context, client) {
    const root = await mkdtemp(join(import.meta.dirname, "build", "executor-"));
    const dir = join(root, "journal");
    const cert = join(root, "cert.pem");
    const ca = join(root, "ca.pem");
    const caKey = join(root, "ca-key.pem");
    const tlsKey = join(root, "tls-key.pem");
    const csr = join(root, "tls.csr");
    const extensions = join(root, "tls.ext");
    const seed = join(root, "device.seed");
    await writeFile(seed, client.keys.alpha.privateKey.export({ format: "der", type: "pkcs8" }).subarray(-32), { mode: 0o600 });
    await writeFile(extensions, "basicConstraints=critical,CA:FALSE\nsubjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n");
    const openssl = (...args) => execFileSync("openssl", args, { stdio: "ignore" });
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=CoconutClaw disposable fixture CA", "-addext", "basicConstraints=critical,CA:TRUE",
        "-keyout", caKey, "-out", ca);
    openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=CoconutClaw fixture",
        "-keyout", tlsKey, "-out", csr);
    openssl("x509", "-req", "-in", csr, "-CA", ca, "-CAkey", caKey, "-CAcreateserial",
        "-days", "1", "-extfile", extensions, "-out", cert);
    const processes = [];
    const packets = [];
    const proxy = { offline: false, transform: async ({ response }) => response };
    let proxyError;
    const server = createServer({ key: await readFile(tlsKey), cert: await readFile(cert) }, async (req, res) => {
        try {
            assert.equal(req.method, "POST");
            assert.equal(req.url, "/v1/agent");
            const chunks = [];
            for await (const chunk of req) chunks.push(chunk);
            const body = Buffer.concat(chunks).toString();
            const packet = { body, headers: { "x-agent-signature": req.headers["x-agent-signature"] } };
            const envelope = JSON.parse(body);
            packets.push(envelope);
            assert.ok(packets.length <= 160, "executor request budget");
            if (proxy.offline) return res.writeHead(503).end();
            const response = await client.rawPost("agent", body, packet.headers);
            const delivered = await proxy.transform({ response, envelope, packet });
            if (!delivered) return req.socket.destroy();
            res.writeHead(delivered.status, { "content-type": "application/json" }).end(JSON.stringify(delivered.value));
        } catch (error) {
            proxyError ??= error;
            if (!res.headersSent) res.writeHead(503).end();
        }
    });
    context.after(async () => {
        for (const process of processes) await process.stop();
        server.closeAllConnections();
        if (server.listening) await new Promise(resolve => server.close(resolve));
        await rm(root, { recursive: true, force: true });
        if (proxyError) throw proxyError;
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const origin = `https://127.0.0.1:${server.address().port}`;
    function run(mode, behavior, endpoint = origin) {
        const child = spawn(executable, ["--loopback-fixture", mode, endpoint, ca, seed, dir, ...(behavior ? [behavior] : [])],
            { env: {}, stdio: ["ignore", "ignore", "pipe"] });
        let stderr = "";
        child.stderr.setEncoding("utf8").on("data", data => { stderr += data; });
        const exited = once(child, "exit");
        const process = {
            async exit(expected) {
                assert.deepEqual(await exited, [expected, null], stderr);
                return stderr;
            },
            async stop() {
                if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
                await exited;
            },
        };
        processes.push(process);
        return process;
    }
    function query(sql, ...bindings) {
        const db = new DatabaseSync(join(dir, "executor.sqlite3"), { readOnly: true });
        try {
            db.exec("PRAGMA busy_timeout=500");
            return db.prepare(sql).all(...bindings);
        } finally {
            db.close();
        }
    }
    async function launches() {
        const contents = await readFile(join(dir, "launches"), "utf8").catch(error => {
            if (error.code !== "ENOENT") throw error;
            return "";
        });
        return contents.trim().split("\n").filter(Boolean).map(Number);
    }
    function proofs(event) {
        return packets.filter(({ command }) => command.op === "report"
            && JSON.parse(command.proof.payload).event.kind === event);
    }
    function verifyProof(proof) {
        assert.ok(verify(null, Buffer.from(`coconutclaw-evidence-v1\nlocal-executor-fixture\nalpha\n${proof.payload}`),
            client.keys.alpha.publicKey, Buffer.from(proof.signature, "hex")));
        return JSON.parse(proof.payload);
    }
    return { proxy, run, launches, proofs, verifyProof, query, packets };
}

test("native executor retains result across lost receipt, reconnects, and replies in the original topic", nativeTimeout, async context => {
    const client = await runFixture(context, { executor: true });
    const { proxy, run, launches, proofs, verifyProof, query, packets } = await attachExecutor(context, client);
    const task = "telegram:101";
    const prompt = "sensitive synthetic task";
    assert.match(await run("init", null, "https://example.invalid:443").exit(1), /expected an explicit HTTPS/);
    await client.agent("beta", "hello", {}, 401);
    await client.fixture("wake", { at_ms: Date.now() }, 404);
    await client.post("fixture", { op: "cancel", task }, {}, 401);
    proxy.offline = true;
    const first = run("init");
    await until(() => packets.some(({ command }) => command.op === "hello"), "offline handshake");
    assert.match(await run("resume").exit(1), /already in use/);
    await client.post("telegram", update(101, 37, prompt), webhook);
    await client.restart();
    assert.equal((await client.post("telegram", update(101, 99, "different duplicate"), webhook)).disposition, "duplicate");
    let lostResult;
    proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op === "report" && JSON.parse(envelope.command.proof.payload).event.kind === "result" && !lostResult) {
            assert.equal(response.status, 200);
            lostResult = envelope;
            proxy.offline = true;
            return null;
        }
        if (envelope.command.op === "start") {
            assert.equal(query("SELECT phase FROM attempts WHERE attempt=?", envelope.command.attempt)[0].phase, "intent");
        }
        return response;
    };
    proxy.offline = false;
    await until(() => lostResult, "committed result reply was lost");
    const proof = lostResult.command.proof;
    const { attempt, event } = verifyProof(proof);
    assert.deepEqual(event, { kind: "result", text: `fixture output: ${prompt}` });
    assert.deepEqual(await launches(), [attempt]);
    assert.equal(query("SELECT acknowledged FROM evidence WHERE event='result'")[0].acknowledged, 0);
    assert.equal((await client.fixture("cancel", { task })).phase, "completed");
    await first.stop();
    await client.restart();
    proxy.offline = false;
    const resumed = run("resume");
    await until(() => query("SELECT acknowledged FROM evidence WHERE event='result'")[0].acknowledged, "retained result acknowledgement");
    await resumed.stop();
    assert.equal(proofs("result").length, 2);
    assert.deepEqual(proofs("result")[1].command.proof, proof);
    assert.ok(proofs("result")[1].sequence > lostResult.sequence);
    assert.deepEqual(await launches(), [attempt]);
    const floor = (await client.agent("alpha", "hello", {})).last_sequence;
    const altered = { ...proof, payload: proof.payload.replace("fixture output", "different result") };
    const unsigned = client.signed("alpha", { op: "report", proof: altered }, { seq: floor + 1 });
    await client.wire(unsigned, 401);
    altered.signature = sign(null, Buffer.from(`coconutclaw-evidence-v1\nlocal-executor-fixture\nalpha\n${altered.payload}`),
        client.keys.alpha.privateKey).toString("hex");
    await client.wire(client.signed("alpha", { op: "report", proof: altered }, { seq: floor + 1 }), 409);
    const delivered = { phase: "delivered", sends: 1, message_id: 1 };
    await until(async () => (await client.fixture("delivery_status", { task })).phase === "delivered", "automatic reply");
    await client.restart();
    assert.deepEqual(await client.fixture("delivery_status", { task }), delivered);
    await client.fixture("send", { task }, 404);
    assert.deepEqual(client.sends, [{ chat_id: 321, message_thread_id: 37, text: event.text }]);
});

test("native executor crashes on both sides of spawn and reconciles without relaunch", nativeTimeout, async context => {
    const client = await runFixture(context, { executor: true });
    const { proxy, run, launches, proofs, verifyProof, query, packets } = await attachExecutor(context, client);
    await client.post("telegram", update(111), webhook);
    let original;
    proxy.transform = async ({ response, envelope, packet }) => {
        if (envelope.command.op === "start" && !original) {
            original = packet;
            assert.equal((await client.wire(original, 409)).error, "replay");
        }
        return response;
    };
    await run("init", "crash-before-spawn").exit(72);
    const first = packets.find(({ command }) => command.op === "start").command.attempt;
    assert.deepEqual(await launches(), []);
    assert.equal(query("SELECT phase FROM attempts WHERE attempt=?", first)[0].phase, "consumed");
    await client.restart();
    await client.wire(original, 409);
    await client.post("telegram", update(112), webhook);
    await run("resume", "crash-after-spawn").exit(73);
    const second = packets.filter(({ command }) => command.op === "start").at(-1).command.attempt;
    assert.notEqual(first, second);
    await until(async () => (await launches()).includes(second), "child created before executor death");
    assert.equal(query("SELECT phase FROM attempts WHERE attempt=?", second)[0].phase, "consumed");
    await client.fixture("cancel", { task: "telegram:112" });
    await client.restart();
    const resumed = run("resume");
    await until(() => query("SELECT COUNT(*) AS total FROM evidence WHERE event='unknown' AND acknowledged=1")[0].total === 2,
        "both consumed attempts reconciled");
    for (const attempt of [first, second]) {
        assert.equal((await client.agent("alpha", "status", { attempt })).phase, "unknown");
    }
    assert.equal((await client.agent("alpha", "status", { attempt: second })).cancel_requested, true);
    assert.deepEqual(proofs("unknown").map(({ command }) => verifyProof(command.proof).attempt), [first, second]);
    assert.deepEqual(proofs("cancelled"), []);
    assert.deepEqual(await launches(), [second]);
    assert.deepEqual(await client.agent("alpha", "next"), { task: null });
    await resumed.stop();
});

test("native executor fences replayed and late grants and confirms both kinds of cancellation", nativeTimeout, async context => {
    const client = await runFixture(context, { executor: true });
    const { proxy, run, launches, proofs, verifyProof, query, packets } = await attachExecutor(context, client);
    let replayed;
    let startCalls = 0;
    let expiredAttempt;
    proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op !== "start") return response;
        if (++startCalls === 1) {
            assert.equal(response.status, 200);
            replayed = response;
            return null;
        }
        if (startCalls === 2) {
            assert.equal(response.status, 200);
            assert.equal(replayed.value.start_until, response.value.start_until);
            return replayed;
        }
        if (startCalls === 3) {
            assert.equal(response.status, 200);
            expiredAttempt = response.value.attempt;
            await sleep(Math.max(0, response.value.start_until - Date.now() + 50));
        }
        return response;
    };
    await client.post("telegram", update(121), webhook);
    const first = run("init", "hold");
    await until(() => expiredAttempt && query("SELECT acknowledged FROM evidence WHERE event='unknown'")[0]?.acknowledged,
        "late grant reconciled", 15_000);
    assert.equal(startCalls, 3);
    assert.equal((await client.agent("alpha", "status", { attempt: expiredAttempt })).phase, "unknown");
    assert.deepEqual(await launches(), []);
    let cancelledBefore;
    proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op === "start" && !cancelledBefore) {
            assert.equal(response.status, 200);
            cancelledBefore = response.value.attempt;
            assert.equal((await client.fixture("cancel", { task: "telegram:122" })).phase, "cancel_requested");
            assert.equal((await client.fixture("cancel", { task: "telegram:122" })).phase, "cancel_requested");
        }
        return response;
    };
    await client.post("telegram", update(122), webhook);
    await until(() => proofs("cancelled").length === 1
        && query("SELECT acknowledged FROM evidence WHERE attempt=? AND event='cancelled'", cancelledBefore)[0]?.acknowledged,
    "delayed grant fenced by cancel", 15_000);
    assert.deepEqual(await launches(), []);
    assert.equal((await client.agent("alpha", "status", { attempt: cancelledBefore })).phase, "cancelled_before_start");
    await client.post("telegram", update(123), webhook);
    const running = (await until(async () => {
        const ids = await launches();
        return ids.length ? ids : null;
    }, "fixture child starts", 15_000))[0];
    assert.equal((await client.fixture("cancel", { task: "telegram:123" })).phase, "cancel_requested");
    await until(() => query("SELECT acknowledged FROM evidence WHERE attempt=? AND event='cancelled'", running)[0]?.acknowledged,
        "reaped child cancellation acknowledgement");
    assert.equal((await client.agent("alpha", "status", { attempt: running })).phase, "cancelled_after_exit");
    assert.deepEqual(proofs("cancelled").map(({ command }) => verifyProof(command.proof)), [
        { attempt: cancelledBefore, event: { kind: "cancelled", evidence: "before_start" } },
        { attempt: running, event: { kind: "cancelled", evidence: "process_exited" } },
    ]);
    assert.deepEqual(await launches(), [running]);
    assert.deepEqual(packets.filter(({ command }) => command.op === "start").map(({ command }) => command.attempt),
        [expiredAttempt, expiredAttempt, expiredAttempt, cancelledBefore, running]);
    assert.deepEqual(proofs("result"), []);
    await first.stop();
});

async function completed(client, id, text, { drop = false } = {}) {
    await client.post("telegram", update(id, id), webhook);
    return reportResult(client, `telegram:${id}`, text, drop);
}

function signedReport(client, attempt, event) {
    const payload = JSON.stringify({ attempt, event });
    const signature = sign(null, Buffer.from(`coconutclaw-evidence-v1\nlocal-executor-fixture\nalpha\n${payload}`),
        client.keys.alpha.privateKey).toString("hex");
    return { op: "report", proof: { payload, signature } };
}

async function reportResult(client, task, text, drop = false) {
    const { attempt } = await client.agent("alpha", "claim", { task });
    await client.agent("alpha", "start", { attempt });
    await client.wire(client.signed("alpha", signedReport(client, attempt, { kind: "started" })));
    const result = signedReport(client, attempt, { kind: "result", text });
    const packet = client.signed("alpha", result);
    if (drop) Object.assign(packet.headers, operator, { "x-fixture-fault": "after_commit" });
    await client.wire(packet, drop ? 503 : 200);
    return { task, result };
}

test("synthetic cutover preserves task identity and quarantines ambiguous effects across restart", timeout, async context => {
    const client = await runFixture(context, { executor: true, inspect: true });
    const snapshot = cutoverSnapshot();
    assert.equal(cutoverSnapshot(), snapshot);
    const { sha256, payload } = JSON.parse(snapshot);
    function changedSnapshot(change) {
        const fixture = JSON.parse(payload);
        change(fixture);
        const changed = JSON.stringify(fixture);
        return JSON.stringify({ sha256: createHash("sha256").update(changed).digest("hex"), payload: changed });
    }
    await client.agent("alpha", "hello");
    await client.restart(() => client.editLedger(db => {
        const invalid = [
            [JSON.stringify({ sha256, payload: `${payload} ` }), /checksum mismatch/],
            [changedSnapshot(fixture => { fixture.version = "unsupported"; }), /unsupported fixture version/],
            [changedSnapshot(fixture => { fixture.tasks[1].taskId = 23; }), /duplicate fixture taskId/],
            [changedSnapshot(fixture => { fixture.tasks[0].ingress.message.message_thread_id = 99; }), /conflicting fixture route/],
        ];
        for (const [bytes, error] of invalid) assert.throws(() => importCutoverFixture(db, bytes), error);
        db.exec(`CREATE TEMP TRIGGER fail_cutover BEFORE INSERT ON outbox
                 BEGIN SELECT RAISE(ABORT, 'interrupted fixture import'); END`);
        assert.throws(() => importCutoverFixture(db, snapshot), /interrupted fixture import/);
        for (const table of ["inbox", "tasks", "attempts", "outbox"]) {
            assert.equal(db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get(), undefined);
        }
        db.exec("DROP TRIGGER fail_cutover");
        assert.equal(importCutoverFixture(db, snapshot), "imported");
    }));

    async function quarantined() {
        assert.deepEqual(await client.sql(`
            SELECT t.id, a.id AS run, a.phase, a.start_until, t.chat_id, t.thread_id, i.update_id
            FROM tasks t JOIN attempts a ON t.current_attempt=a.id JOIN inbox i ON i.sequence=t.sequence
            WHERE t.id LIKE 'legacy:%' ORDER BY t.sequence
        `), [
            { id: "legacy:23", run: 24, phase: "unknown", start_until: null, chat_id: 321, thread_id: 37, update_id: 201 },
            { id: "legacy:25", run: 25, phase: "unknown", start_until: null, chat_id: 321, thread_id: 41, update_id: 203 },
            { id: "legacy:26", run: 26, phase: "completed", start_until: null, chat_id: 321, thread_id: 53, update_id: 202 },
            { id: "legacy:27", run: 27, phase: "completed", start_until: null, chat_id: 321, thread_id: null, update_id: 204 },
        ]);
        assert.deepEqual(await client.agent("alpha", "next"), { task: null });
        for (const [task, attempt] of [["legacy:23", 24], ["legacy:25", 25]]) {
            await client.agent("alpha", "claim", { task }, 409);
            await client.agent("alpha", "start", { attempt }, 409);
            await client.wire(client.signed("alpha", signedReport(client, attempt, { kind: "started" })), 409);
        }
        assert.deepEqual(await client.fixture("delivery_status", { task: "legacy:26" }), { phase: "unknown", sends: 1 });
        assert.deepEqual(await client.fixture("delivery_status", { task: "legacy:27" }), { phase: "unknown", sends: 0 });
        assert.equal((await client.post("telegram", update(201, 99), webhook)).disposition, "duplicate");
        assert.deepEqual(await client.sql("SELECT snapshot FROM synthetic_cutover"), [{ snapshot }]);
    }
    await quarantined();
    await client.post("telegram", update(200, 37), webhook);
    const { task } = await reportResult(client, "telegram:200", "new synthetic reply");
    await until(async () => (await client.fixture("delivery_status", { task })).phase === "delivered", "new reply alarm");
    await client.restart(() => client.editLedger(db => {
        assert.equal(importCutoverFixture(db, snapshot), "already imported");
        const conflicting = changedSnapshot(fixture => { fixture.tasks[0].ingress.message.text = "conflicting fixture"; });
        assert.throws(() => importCutoverFixture(db, conflicting), /conflicting fixture snapshot/);
    }));
    await quarantined();
    assert.deepEqual(await client.fixture("delivery_status", { task }), { phase: "delivered", sends: 1, message_id: 1 });
    assert.deepEqual(client.sends, [{ chat_id: 321, message_thread_id: 37, text: "new synthetic reply" }]);
});

test("Phase 2 alarm survives a pending restart and a duplicated result with one same-topic receipt", timeout, async context => {
    const client = await runFixture(context, { executor: true, inspect: true });
    await client.agent("alpha", "hello");
    await client.restart(() => client.sql("UPDATE pilot_delivery SET not_before=?", Date.now() + 2500));
    const { task, result } = await completed(client, 130, "alarm result", { drop: true });
    assert.deepEqual(await client.fixture("delivery_status", { task }), { phase: "pending", sends: 0 });
    await client.restart();
    await Promise.all([client.wire(client.signed("alpha", result)), client.fixture("send", { task }, 404)]);
    await until(async () => (await client.fixture("delivery_status", { task })).phase === "delivered", "pending alarm after restart");
    await client.restart();
    await client.wire(client.signed("alpha", result));
    assert.deepEqual(await client.fixture("delivery_status", { task }), { phase: "delivered", sends: 1, message_id: 1 });
    assert.deepEqual(client.sends, [{ chat_id: 321, message_thread_id: 130, text: "alarm result" }]);
});

test("Phase 2 recovers an interrupted send as Unknown and never resends it", timeout, async context => {
    let release;
    const held = new Promise(resolve => { release = resolve; });
    context.after(() => release());
    const client = await runFixture(context, {
        executor: true, inspect: true,
        async mock(body, message_id) {
            if (body.text === "receipt lost") await held;
            return Response.json({ ok: true, result: { message_id } });
        },
    });
    const { task, result } = await completed(client, 131, "receipt lost");
    await until(() => client.sends.length === 1, "mock accepted the first send");
    assert.deepEqual(await client.fixture("delivery_status", { task }), { phase: "in_flight", sends: 1 });
    const queued = await completed(client, 132, "next result");
    assert.deepEqual(client.sends.map(body => body.text), ["receipt lost"]);
    await client.restart();
    release();
    await client.wire(client.signed("alpha", result));
    await until(async () => (await client.sql("SELECT phase FROM outbox WHERE task=?", queued.task))[0]?.phase === "delivered",
        "watchdog recovers and unblocks the next send");
    assert.ok(client.sendTimes[1] - client.sendTimes[0] >= 4700, "next send waited for the persisted watchdog");
    assert.deepEqual(await client.fixture("delivery_status", { task }), { phase: "unknown", sends: 1 });
    assert.deepEqual(await client.fixture("delivery_status", { task: queued.task }), { phase: "delivered", sends: 1, message_id: 2 });
    await client.fixture("recover_send", { task }, 404);
    assert.deepEqual(client.sends.map(body => body.message_thread_id), [131, 132]);
});

test("Phase 2 honors Bot-wide 429 cooldown across restart and stops after the second rejection", timeout, async context => {
    const client = await runFixture(context, {
        executor: true, inspect: true,
        mock(body, message_id) {
            return body.text === "limited" ? Response.json({
                ok: false, error_code: 429, parameters: { retry_after: 5 },
            }, { status: 429 }) : Response.json({ ok: true, result: { message_id } });
        },
    });
    const limited = await completed(client, 133, "limited");
    await until(async () => (await client.sql("SELECT sends,phase FROM outbox WHERE task=?", limited.task))[0]?.sends === 1
        && (await client.fixture("delivery_status", { task: limited.task })).phase === "pending", "first rejection settled");
    const other = await completed(client, 134, "other topic");
    await client.restart();
    await until(async () => (await client.sql("SELECT phase FROM outbox WHERE task=?", limited.task))[0]?.phase === "failed",
        "second rejection settled", 14_000);
    assert.deepEqual(client.sends.map(body => body.message_thread_id), [133, 134, 133]);
    assert.ok(client.sendTimes[1] - client.sendTimes[0] >= 4900, "429 delays all topics");
    assert.ok(client.sendTimes[2] - client.sendTimes[1] >= 3900, "single Bot send pacing");
    assert.deepEqual(await client.fixture("delivery_status", { task: other.task }), { phase: "delivered", sends: 1, message_id: 2 });
    await client.wire(client.signed("alpha", limited.result));
    assert.deepEqual(await client.fixture("delivery_status", { task: limited.task }), { phase: "failed", sends: 2 });
});

test("Phase 2 admits 20 distinct tasks per UTC day and fails closed at persistent request limits", timeout, async context => {
    const client = await runFixture(context, { executor: true, inspect: true });
    const utcDay = Math.floor(Date.now() / 86_400_000);
    const pending = await completed(client, 140, "reserved alarm budget");
    for (let id = 141; id < 159; id++) await client.post("telegram", update(id), webhook);
    await client.post("telegram", update(159), { ...webhook, ...operator, "x-fixture-fault": "rollback" }, 503);
    const last = await Promise.all([159, 160].map(id => client.rawPost("telegram", update(id), webhook)));
    assert.deepEqual(last.map(reply => reply.status).sort(), [200, 429]);
    const winner = last[0].status === 200 ? 159 : 160;
    const rejected = winner === 159 ? 160 : 159;
    assert.equal((await client.post("telegram", update(winner), webhook)).disposition, "duplicate");
    await client.restart();
    assert.equal((await client.post("telegram", update(rejected), webhook, 429)).error, "pilot_task_limit");
    assert.deepEqual(await client.sql("SELECT tasks FROM pilot_budget"), [{ tasks: 20 }]);
    assert.deepEqual(await client.sql("SELECT update_id FROM inbox WHERE update_id=?", rejected), []);
    await until(async () => (await client.sql("SELECT phase FROM outbox WHERE task=?", pending.task))[0]?.phase === "delivered",
        "initial result delivered");
    const { task: delayed } = await reportResult(client, `telegram:${winner}`, "alarm allowance");
    await client.restart(() => client.sql("UPDATE pilot_budget SET requests=9799"));
    const hits = await Promise.all([
        client.rawPost("fixture", { op: "delivery_status", task: delayed }, operator),
        client.rawPost("fixture", { op: "delivery_status", task: pending.task }, operator),
    ]);
    assert.deepEqual(hits.map(({ status }) => status).sort(), [200, 429]);
    const packet = client.signed("alpha", { op: "next" });
    const blocked = await client.rawPost("agent", packet.body, packet.headers);
    assert.equal(blocked.value.error, "pilot_request_limit");
    assert.ok(+blocked.headers["retry-after"] > 0);
    await client.restart();
    assert.equal((await client.agent("alpha", "hello", {}, 429)).error, "pilot_request_limit");
    await until(async () => (await client.sql("SELECT phase FROM outbox WHERE task=?", delayed))[0]?.phase === "delivered",
        "alarm uses its reserved allowance");
    assert.deepEqual(await client.sql("SELECT requests FROM pilot_budget"), [{ requests: 9801 }]);
    await client.restart(() => client.sql("UPDATE pilot_budget SET day=?", utcDay + 1));
    assert.equal((await client.agent("alpha", "hello", {}, 429)).error, "pilot_request_limit");
    await client.restart(() => client.sql("UPDATE pilot_budget SET day=?", utcDay - 1));
    await client.post("telegram", update(rejected), webhook);
    assert.deepEqual(await client.sql("SELECT day,requests,tasks FROM pilot_budget"), [{ day: utcDay, requests: 1, tasks: 1 }]);
});

test("Phase 2 at the hard alarm cap retains pending delivery and defers to the next UTC day", timeout, async context => {
    const client = await runFixture(context, { executor: true, inspect: true });
    const day = Math.floor(Date.now() / 86_400_000);
    await client.agent("alpha", "hello");
    await client.restart(() => client.sql("UPDATE pilot_delivery SET not_before=?", Date.now() + 1700));
    const { task } = await completed(client, 161, "deferred result");
    await client.restart(() => client.sql("UPDATE pilot_budget SET requests=10000"));
    await until(() => client.metrics().find(row => row.scope === "alarm"), "alarm handled the exhausted budget");
    assert.deepEqual(client.sends, []);
    assert.deepEqual(await client.sql("SELECT phase,sends FROM outbox WHERE task=?", task), [{ phase: "pending", sends: 0 }]);
    assert.deepEqual(await client.sql("SELECT requests FROM pilot_budget"), [{ requests: 10000 }]);
    await client.restart();
    assert.deepEqual(await client.scheduledAlarm(), [{ scheduled_time: (day + 1) * 86_400_000 }]);
});

test("Phase 2 full retained ledger stays within the measured SQL envelope", timeout, async context => {
    const client = await runFixture(context, { executor: true, inspect: true });
    await client.agent("alpha", "hello");
    await client.restart(async () => {
        const series = "WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<127)";
        await client.sql(`${series} INSERT INTO inbox(update_id,disposition) SELECT n,'task' FROM numbers`);
        await client.sql(`${series} INSERT INTO tasks(id,chat_id,thread_id,prompt)
            SELECT 'telegram:'||n,321,n,? FROM numbers`, "p".repeat(4096));
        await client.sql(`INSERT INTO attempts(task,device,lease_until,phase)
            SELECT tasks.id,'alpha',0,'expired' FROM tasks CROSS JOIN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)`);
        await client.sql(`INSERT INTO attempts(task,device,lease_until,phase)
            SELECT tasks.id,'alpha',0,'expired' FROM tasks LIMIT 3`);
        await client.sql(`INSERT INTO attempts(task,device,lease_until,start_until,phase,started_evidence)
            SELECT tasks.id,'alpha',0,0,'completed',1 FROM tasks`);
        await client.sql("UPDATE tasks SET current_attempt=384+sequence");
        await client.sql(`INSERT INTO executor_evidence(attempt,event,payload,signature)
            SELECT id,'started','{"event":{"kind":"started"}}',? FROM attempts WHERE phase='completed'
            UNION ALL SELECT id,'unknown','{"event":{"kind":"unknown"}}',? FROM attempts WHERE phase='completed'
            UNION ALL SELECT id,'result',?,? FROM attempts WHERE phase='completed'`,
        "0".repeat(128), "0".repeat(128), JSON.stringify({ event: { kind: "result", text: "r".repeat(4096) } }), "0".repeat(128));
        await client.sql("INSERT INTO outbox(task,text,phase,sends) SELECT id,?,'delivered',2 FROM tasks", "r".repeat(4096));
        await client.sql("INSERT INTO delivery_receipts(task,message_id) SELECT id,sequence FROM tasks");
    });
    assert.deepEqual(await client.agent("alpha", "next"), { task: null });
    await client.post("telegram", update(200, 200, "p".repeat(4096)), webhook);
    assert.deepEqual(await client.agent("alpha", "next"), { task: "telegram:200" });
    const { task, result } = await reportResult(client, "telegram:200", "r".repeat(4096));
    await client.post("telegram", update(201), webhook, 503);
    await client.wire(client.signed("alpha", result));
    await until(async () => (await client.fixture("delivery_status", { task })).phase === "delivered",
        "send from full ledger");
    assert.deepEqual(await client.agent("alpha", "next"), { task: null });
    await until(() => client.metrics().some(row => row.scope === "alarm"), "alarm SQL sample");
    const samples = client.metrics();
    const warmSamples = samples.slice(2);
    const peak = Object.fromEntries(["agent", "telegram", "fixture", "alarm"].map(scope => {
        const rows = samples.filter(row => row.scope === scope);
        return [scope, Object.fromEntries(["read", "written", "ms"].map(key => [key, Math.max(...rows.map(row => row[key]))]))];
    }));
    const [{ page_count }] = await client.sql("PRAGMA page_count");
    const [{ page_size }] = await client.sql("PRAGMA page_size");
    const [{ index_bytes }] = await client.sql(
        "SELECT sum(pgsize) AS index_bytes FROM dbstat WHERE name IN ('outbox_pending','outbox_in_flight')",
    );
    context.diagnostic(JSON.stringify({
        peak, warmPeakWritten: Math.max(...warmSamples.map(row => row.written)),
        indexBytes: index_bytes, sqliteBytes: page_count * page_size,
    }));
    assert.ok(samples.some(row => row.read >= 128), "captures the full candidate scan");
    for (const row of samples) {
        assert.ok(row.read <= 320 && row.written <= 40, JSON.stringify(row));
    }
    assert.ok(warmSamples.every(row => row.written <= 10), "warm writes stay below 10 rows per operation");
});

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
