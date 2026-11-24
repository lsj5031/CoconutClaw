import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import { generateKeyPairSync, randomBytes, sign } from "node:crypto";
import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { promisify } from "node:util";
import { readBuildOutput } from "@cloudflare/build-output-utils";
import { Miniflare } from "miniflare";
import { runPilot } from "./pilot-driver.mjs";

const origin = "https://pilot.example.invalid";
const instance = "hosted-synthetic-pilot";
const body = {
    update_id: 23001,
    message: { chat: { id: "321", type: "private" }, from: { id: 321 },
        message_thread_id: 37, text: "synthetic pilot" },
};
const task = "telegram:23001";
const { rootConfig, workers, containers } = await readBuildOutput(import.meta.dirname);
const { config, bundleDir } = workers.default;
const { mainModule, modules } = config.manifest;

test("pilot prebuilt config has one custom domain and one gated SQLite Worker", () => {
    assert.deepEqual(rootConfig, { buildContext: { isPreview: false, mode: "phase2-pilot" } });
    assert.deepEqual(Object.keys(workers), ["default"]);
    assert.deepEqual(containers, []);
    assert.equal(config.name, "coconutclaw-phase1-test");
    assert.equal(config.workersDev, false);
    assert.equal(config.previewUrls, false);
    assert.deepEqual(config.domains, ["bigfur.liu.nz"]);
    assert.equal(config.routes, undefined);
    assert.equal(config.triggers, undefined);
    assert.deepEqual(config.exports, { BotLedger: { type: "durable-object", storage: "sqlite" } });
    assert.deepEqual(config.env, {
        BOTS: { type: "durable-object", worker: config.name, exportName: "BotLedger" },
        PHASE: { type: "text", value: "phase2-pilot" },
        PILOT_TOKEN: { type: "secret" },
        PILOT_WINDOW: { type: "secret" },
        PILOT_ALPHA_KEY: { type: "secret" },
        PILOT_GATE: { type: "rate-limit", namespace: "23001", simple: { limit: 16, period: 10 } },
    });
});

async function fixture(context, { closed = false, rate = true, tls = {} } = {}) {
    const persistence = await mkdtemp(join(import.meta.dirname, "build", "hosted-pilot-"));
    const token = randomBytes(32).toString("hex");
    const { privateKey: key, publicKey } = generateKeyPairSync("ed25519");
    const publicHex = publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    const now = Date.now();
    const window = closed ? `${now - 5000}:${now - 2500}` : `${now - 1000}:${now + 60_000}`;
    let service;
    let outbound = 0;
    async function restart() {
        await service?.dispose();
        const { namespace, simple } = config.env.PILOT_GATE;
        service = new Miniflare({
            host: "127.0.0.1", port: 0, cf: false, liveReload: false,
            ...tls,
            modulesRoot: bundleDir,
            modules: [mainModule, ...Object.keys(modules).filter(name => name !== mainModule)].map(name => ({
                path: join(bundleDir, name),
                type: modules[name].type === "wasm" ? "CompiledWasm" : "ESModule",
            })),
            compatibilityDate: config.compatibilityDate,
            bindings: { PHASE: config.env.PHASE.value, PILOT_TOKEN: token,
                PILOT_WINDOW: window, PILOT_ALPHA_KEY: publicHex },
            ...(rate ? { ratelimits: { PILOT_GATE: { namespace_id: namespace, simple } } } : {}),
            durableObjects: { BOTS: { className: "BotLedger", useSQLite: true } },
            durableObjectsPersist: persistence,
            outboundService: () => {
                outbound++;
                throw new Error("Unexpected pilot network egress");
            },
        });
        await service.ready;
    }
    context.after(async () => {
        await service?.dispose();
        await rm(persistence, { recursive: true, force: true });
        assert.equal(outbound, 0);
    });
    await restart();

    async function transport(path, payload, headers, signal) {
        const response = await service.dispatchFetch(new URL(path, origin), {
            method: "POST", body: payload, headers, signal,
        });
        return { status: response.status, body: Buffer.from(await response.arrayBuffer()) };
    }

    async function post(path, payload, headers = {}, status = 200) {
        const response = await transport(path, typeof payload === "object" ? JSON.stringify(payload) : payload,
            { authorization: `Bearer ${token}`, ...headers });
        assert.equal(response.status, status, response.body.toString());
        return JSON.parse(response.body);
    }

    async function rows(query) {
        const files = (await readdir(persistence, { recursive: true })).filter(name => /[a-f0-9]{64}\.sqlite$/.test(name));
        assert.equal(files.length, 1);
        const connection = new DatabaseSync(join(persistence, files[0]), { readOnly: true });
        try {
            return connection.prepare(query).all().map(row => ({ ...row }));
        } finally {
            connection.close();
        }
    }

    async function empty() {
        const files = await readdir(persistence, { recursive: true });
        assert.ok(!files.some(name => /[a-f0-9]{64}\.sqlite$/.test(name)));
    }

    function signed(command, sequence, signingInstance = instance) {
        const packet = JSON.stringify({ device: "alpha", at_ms: Date.now(), sequence, command });
        return [packet, {
            "x-agent-signature": sign(null, Buffer.from(`coconutclaw-v1\n${signingInstance}\nPOST\n/v1/agent\n${packet}`), key).toString("hex"),
        }];
    }

    return { key, token, post, transport, restart, rows, empty, signed, url: () => service.ready };
}

test("bounded pilot driver preserves intake, signed result and one same-topic receipt across restarts", { timeout: 25_000 }, async context => {
    const client = await fixture(context);
    const packets = [];
    const checkpoints = [];
    const receipt = await runPilot({
        transport(path, payload, headers, signal) {
            if (path.endsWith("/agent")) packets.push(JSON.parse(payload).command);
            return client.transport(path, payload, headers, signal);
        },
        token: client.token,
        key: client.key,
        async checkpoint(point) {
            checkpoints.push(point);
            await client.restart();
        },
    });
    assert.deepEqual(checkpoints, ["intake", "result", "receipt"]);
    assert.equal(receipt.executions, 1);
    assert.ok(receipt.calls <= 16);
    assert.deepEqual(receipt.receipt, {
        phase: "delivered", sends: 1, message_id: 1,
        delivery: { chat_id: 321, message_thread_id: 37, text: "pilot: SYNTHETIC PILOT" },
    });
    assert.equal(packets.filter(packet => packet.op === "start").length, 1);
    const results = packets.filter(packet => packet.proof && JSON.parse(packet.proof.payload).event.kind === "result");
    assert.equal(results.length, 2);
    assert.deepEqual(results[0], results[1]);
    assert.deepEqual(await client.rows("SELECT update_id, disposition FROM inbox"), [
        { update_id: 23001, disposition: "task" },
    ]);
    assert.deepEqual(await client.rows("SELECT id, phase FROM attempts"), [{ id: receipt.attempt, phase: "completed" }]);
    assert.deepEqual(await client.rows("SELECT task, message_id FROM delivery_receipts"), [{ task, message_id: 1 }]);
    assert.deepEqual(await client.post("/v1/pilot/agent", ...client.signed(results[1], 5), 409), { error: "replay" });
    const forged = { ...results[1], proof: { ...results[1].proof, signature: "00".repeat(64) } };
    assert.deepEqual(await client.post("/v1/pilot/agent", ...client.signed(forged, 6), 401), { error: "unauthorized" });
    await assert.rejects(runPilot({ transport: client.transport, token: client.token, key: client.key }), /fresh named ledger/);
});

test("pilot bounds requests before the object and exposes only the fixed synthetic fixture", { timeout: 15_000 }, async context => {
    const client = await fixture(context);
    await client.post("/v1/telegram", body, {}, 404);
    await client.post("/v1/pilot/telegram?x=1", body, {}, 404);
    await client.post("/v1/pilot/telegram", body, { authorization: `Bearer ${"00".repeat(32)}` }, 401);
    await client.post("/v1/pilot/telegram", "x".repeat(16 * 1024 + 1), {}, 413);
    await client.empty();
    await client.post("/v1/pilot/telegram", { ...body, update_id: 23002 }, {}, 400);
    await client.post("/v1/pilot/fixture", { op: "send", task }, {}, 404);
    await client.post("/v1/pilot/fixture", { op: "delivery_status", task: "telegram:23002" }, {}, 404);
    await client.post("/v1/pilot/agent", ...client.signed({ op: "hello" }, 0, "local-executor-fixture"), 401);
    let admitted = 4;
    let denied = false;
    for (let index = 0; index < 48 && !denied; index++) {
        const [packet, headers] = client.signed({ op: "hello" }, 0);
        const response = await client.transport("/v1/pilot/agent", packet, {
            ...headers, authorization: `Bearer ${client.token}`,
        });
        if (response.status === 429) {
            assert.deepEqual(JSON.parse(response.body), { error: "pilot_rate_limit" });
            denied = true;
        } else {
            assert.equal(response.status, 200);
            admitted++;
        }
    }
    assert.ok(denied, "local rate limiter refused a request");
    assert.deepEqual(await client.rows("SELECT requests,tasks FROM pilot_budget"), [{ requests: admitted, tasks: 0 }]);
    assert.deepEqual(await client.rows("SELECT * FROM inbox"), []);
});

test("the pilot cannot start a second attempt after the first claim expires", { timeout: 15_000 }, async context => {
    const client = await fixture(context);
    await client.post("/v1/pilot/telegram", body);
    const claim = await client.post("/v1/pilot/agent", ...client.signed({ op: "claim", task }, 1));
    const status = () => client.post("/v1/pilot/agent", ...client.signed({
        op: "status", attempt: claim.attempt,
    }, 0));
    for (let poll = 0; poll < 10; poll++) {
        if ((await status()).phase === "expired") break;
        await new Promise(resolve => setTimeout(resolve, 550));
    }
    assert.equal((await status()).phase, "expired");
    await client.restart();
    assert.deepEqual(await client.post("/v1/pilot/agent", ...client.signed({ op: "claim", task }, 2), 409),
        { error: "state_conflict" });
    assert.deepEqual(await client.rows("SELECT id FROM attempts"), [{ id: claim.attempt }]);
});

test("a closed pilot window or unavailable rate binding never opens the ledger", { timeout: 15_000 }, async context => {
    const expired = await fixture(context, { closed: true });
    assert.deepEqual(await expired.post("/v1/pilot/telegram", body, {}, 503), { error: "pilot_closed" });
    await expired.empty();
    const missingGate = await fixture(context, { rate: false });
    assert.deepEqual(await missingGate.post("/v1/pilot/telegram", body, {}, 503), { error: "unavailable" });
    await missingGate.empty();
});

test("the pilot CLI runs once over authenticated loopback HTTPS", { timeout: 15_000 }, async context => {
    const directory = await mkdtemp(join(import.meta.dirname, "build", "pilot-tls-"));
    context.after(() => rm(directory, { recursive: true, force: true }));
    const httpsKeyPath = join(directory, "tls-key.pem");
    const httpsCertPath = join(directory, "tls-cert.pem");
    execFileSync("openssl", [
        "req", "-x509", "-newkey", "ed25519", "-noenc", "-days", "1", "-subj", "/CN=127.0.0.1",
        "-addext", "subjectAltName=IP:127.0.0.1", "-keyout", httpsKeyPath, "-out", httpsCertPath,
    ], { stdio: "pipe", timeout: 5000 });
    const client = await fixture(context, { tls: { https: true, httpsKeyPath, httpsCertPath } });
    const keyPath = join(directory, "device.pem");
    await writeFile(keyPath, client.key.export({ type: "pkcs8", format: "pem" }), { mode: 0o600 });
    const url = await client.url();
    assert.equal(url.hostname, "127.0.0.1");
    assert.equal(url.protocol, "https:");
    const { stdout } = await promisify(execFile)(process.execPath, [
        join(import.meta.dirname, "pilot-driver.mjs"), url.origin, keyPath,
    ], {
        env: { NODE_EXTRA_CA_CERTS: httpsCertPath, COCONUTCLAW_PILOT_TOKEN: client.token },
        timeout: 12_000, maxBuffer: 16 * 1024,
    });
    const result = JSON.parse(stdout);
    assert.equal(result.executions, 1);
    assert.ok(result.calls <= 16);
    assert.equal(result.receipt.message_id, 1);
    assert.deepEqual(await client.rows("SELECT task, message_id FROM delivery_receipts"), [{ task, message_id: 1 }]);
});
