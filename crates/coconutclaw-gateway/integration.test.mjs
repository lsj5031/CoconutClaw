import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash, generateKeyPairSync, sign, verify } from "node:crypto";
import { once } from "node:events";
import { cp, mkdir, mkdtemp, open, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { createServer, request } from "node:https";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { readBuildOutput } from "@cloudflare/build-output-utils";
import { Log, LogLevel, Miniflare } from "miniflare";
import { checkAuthorityHandoff } from "./authority-preflight.mjs";
import { authorityFixture, bearer, hostname, origin } from "./runtime-authority-fixture.mjs";
import {
    admitLegacyFixtureUpdate, advanceGatewayFixture, appendRunningFixtureTask, cutoverSnapshot,
    freezeGatewayFixture, importCutoverDelta,
    importCutoverFixture, returnFixtureTasks, seedCutoverSource,
} from "./cutover-fixture.mjs";

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

async function runFixture(context, {
    enabled = true, executor = false, inspect = false, mock, restore, generationPin, cutover,
    keys = Object.fromEntries(["alpha", "beta"].map(device => [device, generateKeyPairSync("ed25519")])),
} = {}) {
    const persistence = await mkdtemp(join(import.meta.dirname, "build", "sqlite-"));
    const sends = [];
    const sendTimes = [];
    const logs = [];
    const log = new Log(LogLevel.INFO);
    log.log = message => logs.push(message);
    const sequence = { alpha: 0, beta: 0 };
    const publicHex = device => keys[device].publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    let service;
    let url;
    let calls = 0;
    let stopped = true;
    let generation = 1;
    let drainProxy = async () => {};
    async function stop() {
        stopped = true;
        await drainProxy();
        await service?.dispose();
        service = undefined;
    }
    async function restart(edit, nextGeneration = generation) {
        await stop();
        if (edit) await edit();
        generation = nextGeneration;
        const pin = generationPin ? await generationPin() : String(generation);
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
                ...(executor && pin !== undefined ? { LOCAL_GENERATION: pin } : {}),
                ...(inspect ? { LOCAL_SQL_METRICS: "1" } : {}),
                ...(cutover ? {
                    PHASE: "phase2-cutover",
                    CUTOVER_TARGET: JSON.stringify(cutover.target?.pin ?? {}),
                    CF_VERSION_METADATA: { id: cutover.versionId ?? cutover.target?.versionId ?? "unset" },
                    CUTOVER_AUTHORITY_ORIGIN: origin,
                    CUTOVER_AUTHORITY_BEARER: bearer,
                    CUTOVER_CANCEL_SECRET: operator["x-fixture-secret"],
                } : {}),
            },
            durableObjects,
            durableObjectsPersist: persistence,
            outboundService: async req => {
                if (cutover) return cutover.fetch(req);
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
        await stop();
        await rm(persistence, { recursive: true, force: true });
        assert.doesNotMatch(logs.join(""), /synthetic-webhook|synthetic-operator|sensitive synthetic task/);
    });
    if (restore) await cp(restore, persistence, { recursive: true });
    await restart();
    async function snapshot(destination) {
        assert.ok(stopped);
        await cp(persistence, destination, { recursive: true, errorOnExist: true, force: false });
    }

    async function rawPost(path, body, headers, chunked = false) {
        assert.ok(++calls <= (executor ? 160 : 72), "test request budget");
        const bytes = typeof body === "string" ? body : JSON.stringify(body);
        if (cutover) {
            const res = await service.dispatchFetch(`https://${hostname}/v1/${path}`, {
                method: "POST", headers, body: bytes,
            });
            return { status: res.status, headers: res.headers, value: await res.json() };
        }
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

    async function directIngress(objectName, update) {
        const bots = await service.getDurableObjectNamespace("BOTS");
        const requestOrigin = cutover ? `https://${hostname}/` : url;
        return bots.get(bots.idFromName(objectName)).fetch(new URL("/v1/telegram", requestOrigin), {
            method: "POST", headers: webhook, body: JSON.stringify(update),
        });
    }

    async function objectId(name) {
        return (await service.getDurableObjectNamespace("BOTS")).idFromName(name).toString();
    }

    function signed(device, command, { at = Date.now(), prefix, seq } = {}) {
        prefix ??= cutover
            ? `coconutclaw-v1\nbigfur:${cutover.target.pin.generation}:${cutover.target.pin.objectId}\nPOST\n/v1/agent\n`
            : executor
            ? (generation === 1 ? executorPrefix : executorPrefix.replace("fixture\n", `fixture:${generation}\n`))
            : signingPrefix;
        const mutating = !["hello", "next", "status"].includes(command.op);
        const body = JSON.stringify({
            device, at_ms: at, sequence: seq ?? (mutating ? ++sequence[device] : 0), command,
        });
        const signature = sign(null, Buffer.from(prefix + body), keys[device].privateKey).toString("hex");
        return { body, headers: {
            "x-agent-signature": signature,
            ...(cutover ? { "x-executor-incarnation": cutover.runner } : {}),
        } };
    }

    const wire = (packet, status) => post("agent", packet.body, packet.headers, status);
    const agent = (device, op, fields, status) => wire(signed(device, { op, ...fields }), status);
    const fixture = (op, fields, status, fault) => post("fixture", { op, ...fields }, {
        ...operator, ...(fault ? { "x-fixture-fault": fault } : {}),
    }, status);
    async function fixtureDbPath(filePattern) {
        assert.ok(inspect);
        const files = (await readdir(persistence, { recursive: true })).filter(file => filePattern.test(file));
        assert.equal(files.length, 1, files.join(", "));
        return join(persistence, files[0]);
    }
    async function withFixtureDb(filePattern, use) {
        const db = new DatabaseSync(await fixtureDbPath(filePattern), { readOnly: !stopped });
        try {
            db.exec("PRAGMA busy_timeout=500");
            return use(db);
        } finally {
            db.close();
        }
    }
    const ledgerFile = () => cutover
        ? new RegExp(`/${cutover.target.pin.objectId}\\.sqlite$`)
        : /[a-f0-9]{64}\.sqlite$/;
    const queryFixture = (filePattern, query, args) =>
        withFixtureDb(filePattern, db => db.prepare(query).all(...args).map(row => ({ ...row })));
    const editLedger = use => {
        assert.ok(stopped);
        return withFixtureDb(ledgerFile(), use);
    };
    const sql = (query, ...args) => queryFixture(ledgerFile(), query, args);
    const scheduledAlarm = () => queryFixture(/metadata\.sqlite$/, "SELECT scheduled_time/1000000 AS scheduled_time FROM _cf_ALARM", []);
    function metrics() {
        return Array.from(logs.join("\n").matchAll(/pilot_sql (\w+) (\d+) (\d+) (\d+)/g),
            ([, scope, read, written, ms]) => ({ scope, read: +read, written: +written, ms: +ms }));
    }
    function registerProxyDrain(drain) {
        drainProxy = drain;
        return () => stopped;
    }
    return { agent, fixture, post, rawPost, directIngress, objectId, stop, restart, snapshot, sends, sendTimes, signed, wire, keys, sql,
        editLedger, ledgerPath: () => fixtureDbPath(ledgerFile()), scheduledAlarm, metrics, registerProxyDrain };
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
const realCodex = process.env.GATEWAY_TEST_CODEX;

async function until(check, message, timeoutMs = 10_000) {
    const deadline = Date.now() + timeoutMs;
    do {
        const result = await check();
        if (result) return result;
        await sleep(50);
    } while (Date.now() < deadline);
    assert.fail(message);
}

async function attachExecutor(context, client, production) {
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
    const forwarding = new Set();
    const workerStopped = client.registerProxyDrain(() => Promise.allSettled(forwarding));
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
            if (production) packet.headers["x-executor-incarnation"] = req.headers["x-executor-incarnation"];
            const envelope = JSON.parse(body);
            packets.push(envelope);
            assert.ok(packets.length <= 160, "executor request budget");
            if (proxy.offline || workerStopped()) return res.writeHead(503).end();
            const forwarded = client.rawPost("agent", body, packet.headers);
            forwarding.add(forwarded);
            const response = await forwarded.finally(() => forwarding.delete(forwarded));
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
    function start(args, env = {}) {
        const child = spawn(executable, args, { env, stdio: ["ignore", "ignore", "pipe"] });
        let stderr = "";
        child.stderr.setEncoding("utf8").on("data", data => { stderr += data; });
        const exited = once(child, "exit");
        const process = {
            pid: child.pid,
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
    function run(mode, behavior, endpoint = origin, generation = 1, { journal = dir, key = seed } = {}) {
        return start(["--loopback-fixture", mode, endpoint, ca, key, journal,
            ...(generation === 1 ? [] : ["--generation", String(generation)]), ...(behavior ? [behavior] : [])]);
    }
    async function runProduction(mode, config, options = {}) {
        const privateConfig = join(root, "runner.json");
        const incarnation = join(root, "incarnation");
        await writeFile(incarnation, options.runner ?? production.runner, { mode: 0o600 });
        await writeFile(privateConfig, JSON.stringify({
            origin, ca, signing_key: seed, incarnation, generation: production.target.pin.generation,
            object_id: production.target.pin.objectId, placement: "alpha", timeout_seconds: 20, ...config,
        }), { mode: 0o600 });
        return start(["--production", mode, privateConfig, options.journal ?? dir], options.env);
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
    async function launches(journal = dir) {
        const contents = await readFile(join(journal, "launches"), "utf8").catch(error => {
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
        const instance = production
            ? `bigfur:${production.target.pin.generation}:${production.target.pin.objectId}`
            : "local-executor-fixture";
        assert.ok(verify(null, Buffer.from(`coconutclaw-evidence-v1\n${instance}\nalpha\n${proof.payload}`),
            client.keys.alpha.publicKey, Buffer.from(proof.signature, "hex")));
        return JSON.parse(proof.payload);
    }
    return { proxy, run, runProduction, launches, proofs, verifyProof, query, packets, dir, seed, root };
}

async function productionRunner(context) {
    const control = await mkdtemp(join(import.meta.dirname, "build", "runner-control-"));
    const mock = await authorityFixture(join(control, "authority.sqlite"));
    context.after(async () => {
        await mock.close();
        await rm(control, { recursive: true, force: true });
    });
    const cutover = { fetch: request => mock.fetch(request), runner: "d".repeat(64) };
    const client = await runFixture(context, { executor: true, inspect: true, cutover });
    const objectId = await client.objectId("hosted-synthetic-pilot");
    const digest = "b".repeat(64);
    const setTarget = generation => {
        cutover.target = {
            pin: {
                generation, hostname, objectId, custodyDigest: digest, revision: "e".repeat(40),
                telegramCredentialVersion: "synthetic-existing-token",
                executorKey: client.keys.alpha.publicKey.export({ format: "der", type: "spki" })
                    .subarray(-32).toString("hex"),
            },
            versionId: `synthetic-runner-worker-${generation}`,
        };
    };
    const ingress = (id, text, code = 200) => client.post("telegram", update(id, 37, text), webhook, code);
    const enroll = runner => mock.publish(cutover.target, mock.checks.at(-1).objectIncarnation, runner);
    setTarget(1);
    await client.restart();
    await ingress(80, "enrollment", 503);
    enroll("c".repeat(64));
    await ingress(80, "initialize held ledger", 503);
    mock.retire();
    setTarget(2);
    await client.restart(() => client.editLedger(db => {
        db.exec("INSERT INTO local_authority VALUES(1,2,0,1)");
        db.prepare("INSERT INTO local_legacy_cutover VALUES(1,?,?,?)")
            .run(Buffer.from("baseline"), Buffer.from("delta"), Buffer.from("archived"));
        db.prepare("INSERT INTO cutover_custody VALUES(1,2,?)").run(digest);
        db.exec(`INSERT INTO tasks(id,chat_id,thread_id,prompt,pin) VALUES
            ('held-unpinned',321,37,'held',NULL), ('held-beta',321,37,'held','beta')`);
    }));
    await ingress(80, "enrollment", 503);
    enroll(cutover.runner);
    const executor = await attachExecutor(context, client, cutover);
    const workspace = join(executor.root, "work");
    const home = join(executor.root, "codex-home");
    await mkdir(workspace, { mode: 0o700 });
    await mkdir(home, { mode: 0o700 });
    return {
        ...executor, client, ingress, mock, workspace, home, cutover,
        configure: codex => ({ codex, codex_home: home, workspace }),
        cancel: id => client.post("cancel", { task: `telegram:${id}` }, {
            "x-cutover-operator": operator["x-fixture-secret"],
        }),
        async receipt(attempt, event) {
            await until(() => {
                try {
                    return executor.query(
                        "SELECT acknowledged FROM evidence WHERE attempt=? AND event=? AND acknowledged=1",
                        attempt, event,
                    ).length;
                } catch (error) {
                    if (error.errcode !== 14 && !error.message.includes("no such table: evidence")) throw error;
                    return false;
                }
            }, `no acknowledged ${event} for attempt ${attempt}`, 15_000);
            const { command } = executor.proofs(event).find(({ command }) =>
                JSON.parse(command.proof.payload).attempt === attempt);
            return executor.verifyProof(command.proof).event;
        },
    };
}

async function procChildren(pid) {
    const children = await readFile(`/proc/${pid}/task/${pid}/children`, "utf8");
    return children.trim().split(/\s+/).filter(Boolean).map(Number);
}

test("BigFur Linux runner binds placement and authority and drains owned processes without relaunch", {
    timeout: 55_000, skip: !executable || process.platform !== "linux",
}, async context => {
    const runner = await productionRunner(context);
    const codex = join(runner.root, "codex-test");
    await writeFile(codex, `#!/usr/bin/python3
import os, pathlib, subprocess, sys, time
prompt = sys.stdin.read()
with open("observed", "a") as records:
    if prompt.startswith("hold"):
        child = subprocess.Popen(["/bin/sleep", "60"], start_new_session=True)
        records.write(f"{os.getpid()} {child.pid}\\n")
    else:
        records.write(f"{os.getpid()}\\n")
if prompt.startswith("hold"):
    time.sleep(60)
pathlib.Path(sys.argv[sys.argv.index("--output-last-message") + 1]).write_text("bounded result")
`, { mode: 0o700 });
    const records = join(runner.workspace, "observed");
    const observed = async count => until(async () => {
        const text = await readFile(records, "utf8").catch(error => {
            if (error.code !== "ENOENT") throw error;
            return "";
        });
        const rows = text.trim().split("\n").filter(Boolean);
        return rows.length >= count && rows.map(row => row.split(" ").map(Number));
    }, `did not observe ${count} native starts`, 15_000);
    const config = runner.configure(codex);
    await (await runner.runProduction("init", { ...config, origin: "http://127.0.0.1" })).exit(1);
    assert.equal((await runner.client.agent("alpha", "next")).task, null);
    await runner.client.agent("alpha", "claim", { task: "held-unpinned" }, 409);
    await runner.client.agent("alpha", "claim", { task: "held-beta" }, 409);
    for (const [id, prompt] of [[81, "wrong sequence"], [82, "wrong instance"], [83, "wrong attempt"],
        [84, "wrong original deadline"], [85, "lost committed grant"], [86, "expired status"],
        [87, "reply"], [88, "hold cancel"], [89, "hold crash"]]) {
        await runner.ingress(id, prompt);
    }
    let staleCancel = true;
    let staleReplies = 0;
    runner.proxy.transform = async ({ response, envelope }) => {
        const { op, attempt } = envelope.command;
        if (response.status !== 200) return response;
        if (op === "start" && attempt === 1) response.value.request_sequence++;
        if (op === "start" && attempt === 2) response.value.instance = response.value.instance.replace("bigfur:2:", "bigfur:1:");
        if (op === "start" && attempt === 3) response.value.attempt++;
        if (op === "start" && attempt === 4) response.value.start_until--;
        if (op === "start" && attempt === 5) return undefined;
        if (op === "status" && (attempt === 6 && response.value.phase === "authorized"
            || attempt === 8 && response.value.cancel_requested && staleCancel)) {
            response.value.authority_valid_until = Date.now() - 1;
            if (attempt === 8) staleReplies++;
        }
        return response;
    };
    const started = await runner.runProduction("init", config);
    for (const attempt of [1, 2, 3, 4, 5, 6]) {
        assert.deepEqual(await runner.receipt(attempt, "unknown"), { kind: "unknown" });
    }
    assert.deepEqual(await runner.receipt(7, "result"), { kind: "result", text: "bounded result" });
    const [, holding] = await observed(2);
    assert.equal(holding.length, 2);
    await runner.cancel(88);
    await until(() => staleReplies >= 2, "no expired cancellation replies");
    for (const pid of holding) assert.doesNotThrow(() => process.kill(pid, 0));
    staleCancel = false;
    assert.deepEqual(await runner.receipt(8, "cancelled"), { kind: "cancelled", evidence: "process_exited" });
    for (const pid of holding) assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    const [, , abandoned] = await observed(3);
    await started.stop();
    await until(() => abandoned.every(pid => {
        try { process.kill(pid, 0); return false; } catch (error) { return error.code === "ESRCH"; }
    }), "supervisor did not drain descendants after loss of runner");
    const restored = join(runner.root, "restored");
    await cp(runner.dir, restored, { recursive: true });
    const old = await runner.runProduction("resume", config, { runner: "c".repeat(64), journal: restored });
    await until(() => runner.mock.checks.some(check => check.runnerIncarnation === "c".repeat(64) && !check.allowed),
        "independently restored journal did not reach the predecessor incarnation check");
    await old.stop();
    await runner.runProduction("resume", config);
    assert.deepEqual(await runner.receipt(9, "unknown"), { kind: "unknown" });
    assert.equal((await observed(3)).length, 3);
    assert.deepEqual(runner.packets.filter(({ command }) => command.op === "start")
        .map(({ command }) => command.attempt), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    context.diagnostic("3 native starts, 6 stale, lost or mismatched authorizations held, signed result and cancellation, 2 owned PIDs gone after cancel and after parent loss, no relaunch");
});

test("BigFur runs installed Codex and cancels it over the signed protocol with a local provider", {
    timeout: 50_000, skip: !executable || !realCodex || process.platform !== "linux",
}, async context => {
    const runner = await productionRunner(context);
    await writeFile(join(runner.home, "auth"), "#!/bin/sh\nprintf local-smoke-only\n", { mode: 0o700 });
    let calls = 0;
    let cancelledStreamClosed = false;
    let mockError;
    const api = createHttpServer(async (req, res) => {
        try {
            if (req.url.startsWith("/v1/models?")) {
                res.writeHead(200, { "content-type": "application/json" }).end('{"models":[]}');
                return;
            }
            assert.equal(req.url, "/v1/responses");
            assert.equal(req.headers.authorization, "Bearer local-smoke-only");
            const chunks = [];
            for await (const chunk of req) chunks.push(chunk);
            const body = JSON.parse(Buffer.concat(chunks));
            assert.equal(body.model, "gpt-6.2-sol");
            const held = JSON.stringify(body.input).includes("Hold this local smoke request");
            calls++;
            res.writeHead(200, { "content-type": "text/event-stream" });
            const item = { type: "message", id: `msg_${calls}`, role: "assistant", status: "completed",
                content: [{ type: "output_text", text: "Codex local smoke", annotations: [] }] };
            const response = { id: `resp_${calls}`, status: "completed", output: [item] };
            const event = (type, fields) => res.write(`data: ${JSON.stringify({ type, ...fields })}\n\n`);
            event("response.created", { response: { ...response, status: "in_progress", output: [] } });
            if (held) {
                res.once("close", () => { cancelledStreamClosed = true; });
                return;
            }
            event("response.output_item.added", { output_index: 0, item: { ...item, status: "in_progress", content: [] } });
            event("response.output_text.delta", { item_id: item.id, output_index: 0, content_index: 0, delta: "Codex local smoke" });
            event("response.output_item.done", { output_index: 0, item });
            event("response.completed", { response });
            res.end();
        } catch (error) {
            mockError ??= error;
            res.destroy();
        }
    });
    api.on("connect", (_req, socket) => socket.destroy());
    api.listen(0, "127.0.0.1");
    await once(api, "listening");
    context.after(async () => {
        api.closeAllConnections();
        await new Promise(resolve => api.close(resolve));
        if (mockError) throw mockError;
    });
    const endpoint = `http://127.0.0.1:${api.address().port}`;
    await writeFile(join(runner.home, "config.toml"), `
model = "gpt-6.2-sol"
model_provider = "myprovider"
check_for_update_on_startup = false
[model_providers.myprovider]
name = "myprovider"
base_url = "${endpoint}/v1"
wire_api = "responses"
[model_providers.myprovider.auth]
command = ${JSON.stringify(join(runner.home, "auth"))}
`, { mode: 0o600 });
    await runner.ingress(81, "Reply with Codex local smoke.");
    await runner.ingress(82, "Hold this local smoke request.");
    const provider_env = ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"];
    const config = { ...runner.configure(realCodex), provider_env };
    const env = { HTTP_PROXY: endpoint, HTTPS_PROXY: endpoint, ALL_PROXY: endpoint, NO_PROXY: "127.0.0.1" };
    const processHandle = await runner.runProduction("init", config, { env });
    await until(() => calls === 2 || mockError, "Codex did not open both local Responses requests");
    if (mockError) throw mockError;
    assert.deepEqual(await runner.receipt(1, "result"), { kind: "result", text: "Codex local smoke" });
    const [supervisor] = await procChildren(processHandle.pid);
    const [codexPid] = await procChildren(supervisor);
    assert.equal(await readFile(`/proc/${codexPid}/comm`, "utf8"), "codex\n");
    await runner.cancel(82);
    assert.deepEqual(await runner.receipt(2, "cancelled"), { kind: "cancelled", evidence: "process_exited" });
    assert.throws(() => process.kill(codexPid, 0), { code: "ESRCH" });
    await until(() => cancelledStreamClosed, "local Codex HTTP stream remained open");
    await processHandle.stop();
    const helloCount = runner.packets.filter(({ command }) => command.op === "hello").length;
    await runner.runProduction("resume", config, { env });
    await until(() => runner.packets.filter(({ command }) => command.op === "hello").length > helloCount
        && runner.packets.at(-1).command.op === "next", "reopened runner did not reconcile");
    assert.equal(calls, 2);
    assert.deepEqual(runner.packets.filter(({ command }) => command.op === "start")
        .map(({ command }) => command.attempt), [1, 2]);
    context.diagnostic("2 local Responses requests, signed result and process_exited, native Codex PID gone and HTTP stream closed, 0 relaunches on resume");
});

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

const cutoverCli = process.env.COCONUTCLAW_CUTOVER_CLI;

test("the gateway holds a legacy CLI import after restart", {
    ...timeout, skip: !cutoverCli,
}, async context => {
    const client = await runFixture(context, { executor: true, inspect: true });
    await client.agent("alpha", "hello");
    await client.stop();
    const root = await mkdtemp(join(import.meta.dirname, "build", "archive-"));
    context.after(() => rm(root, { recursive: true, force: true }));
    const beforeImport = join(root, "before-import");
    await client.snapshot(beforeImport);
    const data = join(root, "source");
    const baseline = join(root, "baseline.json");
    const delta = join(root, "final.json");
    const baseDb = join(root, "baseline.sqlite");
    const finalDb = join(root, "final.sqlite");
    const cli = (...args) => execFileSync(cutoverCli, ["--data-dir", data, ...args], {
        cwd: root, env: { PATH: process.env.PATH }, encoding: "utf8",
    });
    cli("nodes", "status");
    const db = new DatabaseSync(join(data, "state.db"));
    try {
        db.exec(`INSERT INTO task_runs(id,session_id,channel,source_chat_id,source_thread_id,
            update_id,prompt,status,created_at,error_summary)
            VALUES (23,'telegram:321#37','telegram','321','37','201','synthetic held task',
                'failed','t','runtime restarted; outcome unknown, do not automatically retry')`);
        db.prepare("VACUUM INTO ?").run(baseDb);
        db.exec(`INSERT INTO telegram_ingress(seq,update_id,payload,state,created_at)
            VALUES(1,'202','{"update_id":202}','pending','t');
            UPDATE kv SET value='202' WHERE key='last_update_id';
            INSERT INTO legacy_admission_freeze VALUES(1)`);
    } finally {
        db.close();
    }
    const baselineDigest = cli("legacy-cutover", "baseline", "--source", baseDb, "--source-id",
        "synthetic-local", "--output", baseline).trim();
    cli("seal-legacy-cutover");
    const sealed = new DatabaseSync(join(data, "state.db"));
    try {
        sealed.prepare("VACUUM INTO ?").run(finalDb);
    } finally {
        sealed.close();
    }
    const finalDigest = cli("legacy-cutover", "export-final", "--source", finalDb,
        "--baseline", baseline, "--baseline-digest", baselineDigest, "--output", delta).trim();
    const ledger = await client.ledgerPath();
    const importArchive = () => cli("legacy-cutover", "import-gateway-local", "--ledger", ledger,
        "--baseline", baseline, "--baseline-digest", baselineDigest, "--delta", delta, "--final-digest", finalDigest);
    const ack = importArchive();
    assert.deepEqual(JSON.parse(ack), {
        source_id: "synthetic-local", baseline_digest: baselineDigest, final_digest: finalDigest,
        projection_digest: JSON.parse(await readFile(delta, "utf8")).projection_digest, receipt_digests: {},
    });
    await client.restart();
    assert.deepEqual(await client.post("telegram", update(202), webhook, 503), { error: "legacy_cutover_held" });
    assert.deepEqual(await client.agent("alpha", "next", {}, 503), { error: "legacy_cutover_held" });
    assert.deepEqual(await client.fixture("cancel", { task: "legacy:23" }, 503), { error: "legacy_cutover_held" });
    assert.deepEqual(await client.sql("SELECT generation,attempt_floor,active FROM local_authority"),
        [{ generation: 1, attempt_floor: 0, active: 0 }]);
    const [{ projection }] = await client.sql("SELECT CAST(projection AS TEXT) AS projection FROM local_legacy_cutover");
    const archived = JSON.parse(projection);
    assert.equal(archived.task_runs[0].id, 23);
    assert.equal(archived.task_runs[0].error_summary, "runtime restarted; outcome unknown, do not automatically retry");
    assert.equal(archived.telegram_ingress[0].state, "pending");
    assert.deepEqual(await client.sql(`SELECT (SELECT count(*) FROM inbox)+(SELECT count(*) FROM tasks)
        +(SELECT count(*) FROM attempts)+(SELECT count(*) FROM outbox) AS work`), [{ work: 0 }]);
    assert.deepEqual(await client.scheduledAlarm(), []);
    assert.deepEqual(client.sends, []);
    await client.stop();
    assert.equal(importArchive(), ack);
    let pin;
    const restored = await runFixture(context, {
        executor: true, inspect: true, restore: beforeImport, generationPin: () => pin,
    });
    assert.deepEqual(await restored.sql("SELECT * FROM local_legacy_cutover"), []);
    for (const configured of [undefined, "invalid", "0", "2"]) {
        pin = configured;
        await restored.restart();
        assert.deepEqual(await restored.post("telegram", update(202), webhook, 503), { error: "inactive_generation" });
    }
    assert.deepEqual(await restored.sql("SELECT * FROM inbox"), []);
    assert.deepEqual(restored.sends, []);
});

test("synthetic cutover imports the final delta and returns settled and held work", timeout, async context => {
    const client = await runFixture(context, { executor: true, inspect: true });
    await client.agent("alpha", "hello");
    await client.stop();

    const root = await mkdtemp(join(import.meta.dirname, "build", "legacy-"));
    context.after(() => rm(root, { recursive: true, force: true }));
    const legacyPath = join(root, "legacy.sqlite");
    function legacy(use, readOnly = false) {
        const db = new DatabaseSync(legacyPath, { readOnly });
        try {
            return use(db);
        } finally {
            db.close();
        }
    }
    const baseline = legacy(db => {
        seedCutoverSource(db);
        return cutoverSnapshot(db);
    });
    function changedSnapshot(snapshot, change) {
        const { payload } = JSON.parse(snapshot);
        const fixture = JSON.parse(payload);
        change(fixture);
        const changed = JSON.stringify(fixture);
        return JSON.stringify({ sha256: createHash("sha256").update(changed).digest("hex"), payload: changed });
    }
    await client.editLedger(db => {
        const { sha256, payload } = JSON.parse(baseline);
        const invalid = [
            [JSON.stringify({ sha256, payload: `${payload} ` }), /checksum mismatch/],
            [changedSnapshot(baseline, fixture => { fixture.version = "unsupported"; }), /unsupported fixture version/],
            [changedSnapshot(baseline, fixture => { fixture.tasks[1].taskId = 23; }), /duplicate fixture taskId/],
            [changedSnapshot(baseline, fixture => { fixture.tasks[0].ingress.message.message_thread_id = 99; }), /conflicting fixture route/],
        ];
        for (const [bytes, error] of invalid) assert.throws(() => importCutoverFixture(db, bytes), error);
        db.exec(`CREATE TEMP TRIGGER fail_cutover BEFORE INSERT ON outbox
                 BEGIN SELECT RAISE(ABORT, 'interrupted fixture import'); END`);
        assert.throws(() => importCutoverFixture(db, baseline), /interrupted fixture import/);
        for (const table of ["inbox", "tasks", "attempts", "outbox"]) {
            assert.equal(db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get(), undefined);
        }
        db.exec("DROP TRIGGER fail_cutover");
        assert.equal(importCutoverFixture(db, baseline), "imported");
        assert.equal(importCutoverFixture(db, baseline), "already imported");
    });
    const lastLegacy = update(205, 59, "synthetic last legacy dispatch");
    lastLegacy.message.chat.id = 321;
    await client.restart();
    await client.post("telegram", update(205), webhook, 503);
    await client.stop();
    legacy(db => {
        appendRunningFixtureTask(db, 28, lastLegacy);
        db.exec(`
            UPDATE task_payloads SET delivered=1,
                delivery_state=json_set(delivery_state,'$.text_plan.phase','done','$.text_plan.messageId',42)
                WHERE task_run_id=26;
            UPDATE runtime_schedules SET next_due=2000 WHERE id=7;
            INSERT INTO legacy_admission_freeze VALUES(1);
        `);
        assert.throws(() => admitLegacyFixtureUpdate(db, update(206)), /legacy is frozen/);
    });
    const frozen = legacy(db => {
        for (const write of [
            "UPDATE scheduled_tasks SET done=1",
            "UPDATE task_runs SET status='running' WHERE id=23",
            "UPDATE task_payloads SET delivered=1 WHERE task_run_id=26",
            "UPDATE task_runs SET status='completed' WHERE id=28",
        ]) assert.throws(() => db.exec(write), /readonly/);
        return cutoverSnapshot(db);
    }, true);
    const frozenEnvelope = JSON.parse(frozen);
    const frozenPayload = JSON.parse(frozenEnvelope.payload);
    await client.restart(() => client.editLedger(db => {
        const wrongBaseline = changedSnapshot(baseline, fixture => { fixture.tasks.pop(); });
        const invalid = [
            [baseline, JSON.stringify({ ...frozenEnvelope, payload: `${frozenEnvelope.payload} ` }), /checksum mismatch/],
            [baseline, baseline, /missing fixture delta/],
            [baseline, changedSnapshot(frozen, fixture => { fixture.tasks.reverse(); }), /sequence must increase/],
            [baseline, changedSnapshot(frozen, fixture => { fixture.tasks[0].error += " changed"; }), /changed stable task data/],
            [baseline, changedSnapshot(frozen, fixture => { fixture.schedules[0].next_due = 999; }), /schedule regressed/],
            [baseline, changedSnapshot(frozen, fixture => { fixture.tasks.at(-1).ingress.update_id = 201; }), /duplicate fixture update/],
            [wrongBaseline, frozen, /wrong fixture baseline/],
        ];
        for (const [before, after, error] of invalid) assert.throws(() => importCutoverDelta(db, before, after), error);
        db.exec(`CREATE TEMP TRIGGER fail_delta BEFORE INSERT ON attempts
                 BEGIN SELECT RAISE(ABORT, 'interrupted fixture delta'); END`);
        assert.throws(() => importCutoverDelta(db, baseline, frozen), /interrupted fixture delta/);
        assert.equal(db.prepare("SELECT snapshot FROM synthetic_cutover").get().snapshot, baseline);
        assert.equal(db.prepare("SELECT 1 FROM inbox WHERE update_id=205").get(), undefined);
        assert.equal(db.prepare("SELECT 1 FROM tasks WHERE id='legacy:28'").get(), undefined);
        assert.equal(db.prepare("SELECT 1 FROM delivery_receipts WHERE task='legacy:26'").get(), undefined);
        assert.equal(db.prepare("SELECT active FROM local_authority").get().active, 0);
        db.exec("DROP TRIGGER fail_delta");
        assert.equal(importCutoverDelta(db, baseline, frozen), "imported");
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
            { id: "legacy:28", run: 28, phase: "unknown", start_until: null, chat_id: 321, thread_id: 59, update_id: 205 },
        ]);
        assert.deepEqual(await client.agent("alpha", "next"), { task: null });
        for (const [task, attempt] of [["legacy:23", 24], ["legacy:25", 25], ["legacy:28", 28]]) {
            await client.agent("alpha", "claim", { task }, 409);
            await client.agent("alpha", "start", { attempt }, 409);
            await client.wire(client.signed("alpha", signedReport(client, attempt, { kind: "started" })), 409);
        }
        assert.deepEqual(await client.fixture("delivery_status", { task: "legacy:26" }), { phase: "delivered", sends: 1, message_id: 42 });
        assert.deepEqual(await client.fixture("delivery_status", { task: "legacy:27" }), { phase: "unknown", sends: 0 });
        assert.equal((await client.post("telegram", update(201, 99), webhook)).disposition, "duplicate");
        assert.equal((await client.post("telegram", update(205, 99), webhook)).disposition, "duplicate");
        assert.deepEqual(await client.sql("SELECT snapshot FROM synthetic_cutover"), [{ snapshot: frozen }]);
    }
    await quarantined();
    assert.deepEqual(client.sends, []);
    const ingress = update(200, 37);
    assert.deepEqual(await client.post("telegram", ingress, webhook), { disposition: "task", task: "telegram:200" });
    const { task, result } = await reportResult(client, "telegram:200", "new synthetic reply", true);
    await client.restart(() => client.editLedger(db => {
        assert.equal(importCutoverFixture(db, baseline), "already imported");
        assert.equal(importCutoverDelta(db, baseline, frozen), "already imported");
        const conflicting = changedSnapshot(frozen, fixture => { fixture.tasks.at(-1).ingress.message.text = "conflicting fixture"; });
        assert.throws(() => importCutoverDelta(db, baseline, conflicting), /wrong fixture baseline/);
    }));
    assert.equal((await client.post("telegram", update(200, 99), webhook)).disposition, "duplicate");
    assert.deepEqual(await client.wire(client.signed("alpha", result)), { event: "result" });
    await client.agent("alpha", "claim", { task }, 409);
    await client.agent("alpha", "start", { attempt: JSON.parse(result.proof.payload).attempt }, 409);
    await quarantined();
    await until(async () => (await client.fixture("delivery_status", { task })).phase === "delivered", "new reply alarm");
    assert.deepEqual(await client.fixture("delivery_status", { task }), { phase: "delivered", sends: 1, message_id: 1 });
    const lateIngress = update(209, 41);
    const lateTask = "telegram:209";
    await client.post("telegram", lateIngress, webhook);
    const { attempt: lateAttempt } = await client.agent("alpha", "claim", { task: lateTask });
    await client.agent("alpha", "start", { attempt: lateAttempt });
    const lateStart = signedReport(client, lateAttempt, { kind: "started" });
    const lateResult = signedReport(client, lateAttempt, { kind: "result", text: "held late result" });
    await client.stop();
    await assert.rejects(() => client.post("telegram", update(207), webhook), { code: "ECONNREFUSED" });
    const publicKey = client.keys.alpha.publicKey;
    await client.restart(() => client.editLedger(db => {
        assert.equal(freezeGatewayFixture(db, 1), "frozen");
        assert.throws(() => advanceGatewayFixture(db, 2, publicKey, false), /requires signed terminal evidence/);
    }));
    await client.wire(client.signed("alpha", lateStart));
    assert.deepEqual(await client.sql("SELECT phase FROM attempts WHERE id=?", lateAttempt), [{ phase: "unknown" }]);
    const latePacket = client.signed("alpha", lateResult);
    Object.assign(latePacket.headers, operator, { "x-fixture-fault": "after_commit" });
    await client.wire(latePacket, 503);
    await client.restart();
    assert.deepEqual(await client.wire(client.signed("alpha", lateResult)), { event: "result" });
    await client.wire(client.signed("alpha", signedReport(client, lateAttempt, { kind: "unknown" })), 409);
    await client.restart(() => client.editLedger(db => {
        assert.equal(advanceGatewayFixture(db, 2, publicKey, false), "advanced");
        assert.equal(advanceGatewayFixture(db, 2, publicKey, false), "already advanced");
        assert.throws(() => advanceGatewayFixture(db, 2, publicKey, true), /conflicting fixture generation replay/);
    }));
    await client.post("telegram", update(207), webhook, 503);
    await client.stop();
    const [receipt, lateReceipt] = await client.sql(`
        SELECT t.id AS task,i.update_id,t.chat_id,t.thread_id,t.prompt,
            a.id AS attempt,a.phase AS attempt_phase,e.payload,e.signature,
            s.payload AS start_payload,s.signature AS start_signature,
            o.text,o.phase AS delivery_phase,o.sends,d.message_id
        FROM tasks t JOIN inbox i ON t.sequence=i.sequence JOIN attempts a ON t.current_attempt=a.id
        LEFT JOIN executor_evidence e ON a.id=e.attempt AND e.event='result'
        LEFT JOIN executor_evidence s ON a.id=s.attempt AND s.event='started'
        LEFT JOIN outbox o ON t.id=o.task LEFT JOIN delivery_receipts d ON o.task=d.task
        WHERE t.id LIKE 'telegram:%' ORDER BY t.sequence
    `);
    assert.deepEqual(await client.sql("SELECT id FROM tasks WHERE id LIKE 'telegram:%' ORDER BY sequence"),
        [{ id: task }, { id: lateTask }]);
    assert.equal(receipt.payload, result.proof.payload);
    assert.equal(receipt.signature, result.proof.signature);
    assert.equal(lateReceipt.payload, lateResult.proof.payload);
    assert.equal(lateReceipt.signature, lateResult.proof.signature);
    assert.deepEqual(await client.sql("SELECT id FROM attempts WHERE task=?", task), [{ id: receipt.attempt }]);
    const entries = [
        { taskId: 30, ingress: lateIngress, receipt: lateReceipt },
        { taskId: 29, ingress, receipt },
    ];
    function returnTasks(db, anchor = frozen, changed = {}) {
        return returnFixtureTasks(db, anchor, [entries[0], { ...entries[1], ...changed }], publicKey);
    }
    legacy(db => {
        assert.throws(() => returnTasks(db, frozen, { receipt: { ...receipt, delivery_phase: "unknown" } }), /requires a settled/);
        assert.throws(() => returnTasks(db, frozen, { receipt: { ...receipt, signature: "00".repeat(64) } }), /invalid return evidence signature/);
        assert.throws(() => returnTasks(db, frozen, { receipt: { ...receipt, text: "other result" } }), /result does not match delivery/);
        assert.throws(() => returnTasks(db, baseline), /changed after freeze/);
        db.exec(`CREATE TEMP TRIGGER fail_return BEFORE INSERT ON task_payloads
                 BEGIN SELECT RAISE(ABORT, 'interrupted fixture return'); END`);
        assert.throws(() => returnTasks(db), /interrupted fixture return/);
        assert.equal(cutoverSnapshot(db), frozen);
        assert.throws(() => admitLegacyFixtureUpdate(db, update(207)), /legacy is frozen/);
        db.exec("DROP TRIGGER fail_return");
        assert.equal(returnTasks(db), "imported");
    });
    legacy(db => {
        assert.equal(returnTasks(db), "already imported");
        assert.throws(() => returnTasks(db, frozen, { taskId: 31 }), /conflicting fixture return/);
        assert.throws(() => returnTasks(db, baseline), /conflicting fixture return/);
        assert.equal(admitLegacyFixtureUpdate(db, update(200, 99)), "duplicate");
        assert.equal(admitLegacyFixtureUpdate(db, update(209, 99)), "duplicate");
        assert.equal(admitLegacyFixtureUpdate(db, update(207, 37)), "accepted");
        assert.deepEqual({ ...db.prepare(`
            SELECT g.state,t.current_run_id,r.status,r.result_summary,r.source_thread_id,p.delivered
            FROM task_runs r JOIN runtime_tasks t ON t.task_id=r.id
            JOIN telegram_ingress g ON g.update_id=r.update_id JOIN task_payloads p ON p.task_run_id=r.id
            WHERE r.id=29
        `).get() }, {
            state: "done", current_run_id: 29, status: "completed", result_summary: receipt.text,
            source_thread_id: "37", delivered: 1,
        });
        const { key, ...record } = JSON.parse(db.prepare("SELECT record FROM synthetic_return WHERE task_run_id=29").get().record);
        assert.equal(key, publicKey.export({ type: "spki", format: "der" }).toString("hex"));
        assert.deepEqual(record, { frozen: frozenEnvelope.sha256, ingress, ...receipt });
        assert.deepEqual(JSON.parse(db.prepare("SELECT record FROM synthetic_return WHERE task=?").get(lateTask).record),
            { frozen: frozenEnvelope.sha256, ingress: lateIngress, ...lateReceipt, key });
        assert.deepEqual({ ...db.prepare(`SELECT r.status,r.error_summary,p.delivered FROM task_runs r
            LEFT JOIN task_payloads p ON p.task_run_id=r.id WHERE r.id=30`).get() }, {
            status: "failed", error_summary: "effect outcome unknown at handoff; do not automatically retry", delivered: null,
        });
        assert.equal(db.prepare("SELECT error_summary FROM task_runs WHERE id=24").get().error_summary,
            "runtime restarted; outcome unknown, do not automatically retry");
        assert.equal(db.prepare("SELECT current_run_id FROM runtime_tasks WHERE task_id=23").get().current_run_id, 24);
    });
    assert.deepEqual(client.sends, [{ chat_id: 321, message_thread_id: 37, text: "new synthetic reply" }]);
    context.diagnostic(JSON.stringify({
        recordedAt: new Date().toISOString(),
        version: frozenPayload.version,
        baseline: JSON.parse(baseline).sha256, frozen: frozenEnvelope.sha256,
        frozenSequence: frozenPayload.tasks.at(-1).sequence, returnedRuns: [29, 30], receipt, lateReceipt,
        publicKey: publicKey.export({ type: "spki", format: "der" }).toString("hex"),
    }));
});

test("synthetic cutover requires late signed terminal evidence before executor generation change", nativeTimeout, async context => {
    const client = await runFixture(context, { executor: true, inspect: true });
    const { proxy, run, launches, query, verifyProof, packets } = await attachExecutor(context, client);
    const publicKey = client.keys.alpha.publicKey;
    const oldTask = "telegram:211";
    let late;
    let oldGrant;
    proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op === "start") oldGrant ??= response.value;
        if (envelope.command.op === "report" && JSON.parse(envelope.command.proof.payload).event.kind === "started") {
            proxy.offline = true;
        }
        return response;
    };
    await client.post("telegram", update(211, 37), webhook);
    const original = run("init");
    await until(() => oldGrant, "old Start grant");
    await until(() => query("SELECT acknowledged FROM evidence WHERE event='started'")[0]?.acknowledged,
        "old Start custody");
    await original.stop();
    const attempt = oldGrant.attempt;
    assert.deepEqual(await launches(), [attempt]);
    await client.post("telegram", update(213, 37), webhook);
    const { last_sequence: floor } = await client.agent("alpha", "hello");
    const unissued = await client.wire(client.signed("alpha", { op: "claim", task: "telegram:213" }, { seq: floor + 1 }));
    await client.restart(() => client.editLedger(db => {
        assert.equal(freezeGatewayFixture(db, 1), "frozen");
        assert.equal(freezeGatewayFixture(db, 1), "already frozen");
        db.prepare("UPDATE attempts SET start_until=1 WHERE id=?").run(attempt);
        assert.throws(() => advanceGatewayFixture(db, 2, publicKey, true), /requires signed terminal evidence/);
        assert.deepEqual({ ...db.prepare("SELECT generation,active FROM local_authority").get() }, { generation: 1, active: 0 });
    }));
    assert.match(await run("resume", null, undefined, 2).exit(1), /unacknowledged evidence/);
    await client.post("telegram", update(212), webhook, 503);
    await client.agent("alpha", "start", { attempt }, 503);
    const bogusStart = signedReport(client, unissued.attempt, { kind: "started" });
    assert.deepEqual(await client.wire(client.signed("alpha", bogusStart, { seq: floor + 2 }), 409),
        { error: "state_conflict" });
    assert.deepEqual(await client.agent("alpha", "next"), { task: null });
    assert.deepEqual(await client.sql("SELECT event FROM executor_evidence WHERE attempt=?", attempt), [{ event: "started" }]);
    proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op === "report" && !late) {
            assert.equal(response.status, 200);
            late = envelope.command.proof;
            proxy.offline = true;
            return null;
        }
        return response;
    };
    proxy.offline = false;
    const disconnectedAgain = run("resume");
    await until(() => late, "late result committed without acknowledgement");
    await disconnectedAgain.stop();
    assert.deepEqual(verifyProof(late), {
        attempt, event: { kind: "result", text: "fixture output: synthetic task 211" },
    });
    assert.deepEqual(await client.sql("SELECT phase FROM attempts WHERE id=?", attempt), [{ phase: "unknown" }]);
    assert.deepEqual(await client.sql("SELECT * FROM outbox WHERE task=?", oldTask), []);
    await client.restart();
    proxy.offline = false;
    const resumed = run("resume");
    await until(() => query("SELECT acknowledged FROM evidence WHERE event='result'")[0]?.acknowledged,
        "exact late receipt acknowledged after restart");
    await resumed.stop();
    assert.deepEqual(await client.sql("SELECT payload,signature FROM executor_evidence WHERE attempt=? AND event='result'", attempt), [late]);
    assert.deepEqual(await launches(), [attempt]);
    const oldPacket = client.signed("alpha", { op: "start", attempt });
    await client.restart(() => client.editLedger(db => {
        db.exec(`CREATE TEMP TRIGGER interrupt_rotation BEFORE UPDATE ON local_authority
            BEGIN SELECT RAISE(ABORT,'interrupted rotation'); END`);
        assert.throws(() => advanceGatewayFixture(db, 2, publicKey, true), /interrupted rotation/);
        db.exec("DROP TRIGGER interrupt_rotation");
        assert.throws(() => advanceGatewayFixture(db, 2, client.keys.beta.publicKey, true), /invalid signed evidence/);
        assert.equal(advanceGatewayFixture(db, 2, publicKey, true), "advanced");
    }));
    await client.wire(oldPacket, 503);
    await client.restart(undefined, 2);
    await client.wire(oldPacket, 401);
    await client.agent("alpha", "start", { attempt }, 409);
    await client.wire(client.signed("alpha", { op: "report", proof: late }), 401);
    assert.equal((await client.post("telegram", update(211, 99), webhook)).disposition, "duplicate");
    await client.post("telegram", update(212, 37), webhook);
    let replayedGrant;
    let packetsAtReplay;
    let checkedReplay = false;
    proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op === "start" && response.status === 200 && !replayedGrant) {
            replayedGrant = response.value;
            packetsAtReplay = packets.length;
            return { ...response, value: { ...replayedGrant, instance: oldGrant.instance } };
        }
        if (replayedGrant && !checkedReplay && envelope.command.op === "hello") {
            return { status: 503, value: {} };
        }
        return response;
    };
    const next = run("resume", null, undefined, 2);
    await until(() => replayedGrant, "old generation response replayed against a new request");
    const afterReplay = await until(() => packets[packetsAtReplay], "executor response to old generation grant");
    assert.equal(afterReplay.command.op, "hello", "executor must reconnect without checking the old grant");
    assert.deepEqual(await launches(), [attempt]);
    checkedReplay = true;
    await until(() => query("SELECT acknowledged FROM evidence WHERE attempt=? AND event='result'", replayedGrant.attempt)[0]?.acknowledged,
        "new generation result acknowledged");
    await next.stop();
    assert.equal(replayedGrant.instance, "local-executor-fixture:2");
    assert.deepEqual(await launches(), [attempt, replayedGrant.attempt]);
    assert.match(await run("resume").exit(1), /generation must advance exactly once/);
    await until(async () => (await client.fixture("delivery_status", { task: "telegram:212" })).phase === "delivered",
        "new generation same-topic reply");
    assert.deepEqual(client.sends, [{ chat_id: 321, message_thread_id: 37, text: "fixture output: synthetic task 212" }]);
    context.diagnostic(JSON.stringify({ from: 1, to: 2, oldTask, attempt, late, nextAttempt: replayedGrant.attempt }));
});

test("stale restore fences independent ledgers and executor journals after generation change", nativeTimeout, async context => {
    const root = await mkdtemp(join(import.meta.dirname, "build", "restore-"));
    context.after(() => rm(root, { recursive: true, force: true }));
    const anchor = join(root, "generation");
    const generationPin = () => readFile(anchor, "utf8");
    async function publishGeneration(generation) {
        const pending = `${anchor}.next`;
        assert.equal(generation, Number(await generationPin().catch(error => {
            if (error.code !== "ENOENT") throw error;
            return "0";
        })) + 1);
        await writeFile(pending, String(generation), { mode: 0o600, flag: "wx", flush: true });
        await rename(pending, anchor);
        const directory = await open(root);
        try {
            await directory.sync();
        } finally {
            await directory.close();
        }
    }
    await publishGeneration(1);
    const options = { executor: true, inspect: true, generationPin };
    const source = await runFixture(context, options);
    const original = await attachExecutor(context, source);
    await source.agent("alpha", "hello");
    await source.restart(() => source.sql("UPDATE pilot_delivery SET not_before=?", Date.now() + 6000));
    await source.post("telegram", update(214, 37), webhook);
    const predecessor = original.run("init");
    await until(async () => (await original.launches()).length, "predecessor launched");
    const [oldAttempt] = await until(() => {
        const rows = original.query("SELECT attempt FROM evidence WHERE event='result' AND acknowledged=1");
        return rows.length && rows.map(row => row.attempt);
    }, "predecessor terminal evidence acknowledged");
    await predecessor.stop();
    assert.deepEqual(await original.launches(), [oldAttempt]);
    assert.deepEqual(await source.fixture("delivery_status", { task: "telegram:214" }), { phase: "pending", sends: 0 });
    await source.post("telegram", update(215, 37), webhook);
    const { last_sequence: claimFloor } = await source.agent("alpha", "hello");
    const { attempt: heldAttempt } = await source.wire(source.signed("alpha", { op: "claim", task: "telegram:215" },
        { seq: claimFloor + 1 }));
    const { last_sequence: oldSequence } = await source.agent("alpha", "hello");
    const staleStore = join(root, "stale-store");
    const restoredJournal = join(root, "restored-journal");
    await source.stop();
    await source.editLedger(db => {
        db.prepare("INSERT INTO local_authority VALUES(1,1,0,1)").run();
        db.prepare("UPDATE attempts SET lease_until=? WHERE id=?").run(Date.now() + 30_000, heldAttempt);
    });
    await source.snapshot(staleStore);
    await cp(original.dir, restoredJournal, { recursive: true });
    const journalDb = join(restoredJournal, "executor.sqlite3");
    const originalJournal = await readFile(journalDb);
    const outbox = "SELECT task,phase,sends FROM outbox ORDER BY task";
    const held = "SELECT phase,start_until FROM attempts WHERE id=?";
    await source.editLedger(db => freezeGatewayFixture(db, 1));
    const transfer = join(root, "frozen-transfer");
    await source.snapshot(transfer);
    const successor = await runFixture(context, { ...options, restore: transfer });
    await successor.stop();
    await successor.editLedger(db => {
        assert.equal(advanceGatewayFixture(db, 2, source.keys.alpha.publicKey, true), "advanced");
        db.exec("UPDATE pilot_delivery SET not_before=0");
    });
    const target = (client, generation, ledgerPath) => ({
        generation,
        revision: "0123456789abcdef0123456789abcdef01234567",
        deployment: `synthetic-deployment-${generation}`,
        hostname: `generation-${generation}.gateway.invalid`,
        objectId: ledgerPath.split("/").at(-1).split(".")[0],
        custodyDigest: createHash("sha256").update(`synthetic-custody-${generation}`).digest("hex"),
        telegramCredentialVersion: `synthetic-token-${generation}`,
        executorKey: client.keys.alpha.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex"),
    });
    const oldTarget = target(source, 1, await source.ledgerPath());
    const nextTarget = target(successor, 2, await successor.ledgerPath());
    const currentAuthority = {
        target: nextTarget,
        retirement: {
            generation: 1,
            writers: "synthetic/stopped-predecessor",
            executorGrants: "synthetic/terminal-proof",
            telegram: { disposition: "exclusive-existing-token", evidence: "synthetic/isolated-mock" },
            custody: "synthetic/frozen-transfer",
        },
    };
    const checkedGeneration = checkAuthorityHandoff(currentAuthority, nextTarget);
    await publishGeneration(checkedGeneration);
    await assert.rejects(runFixture(context, {
        ...options, restore: staleStore, keys: source.keys,
        generationPin: () => String(checkAuthorityHandoff(currentAuthority, oldTarget)),
    }), /target mismatch: generation/);
    await successor.restart(undefined, checkedGeneration);
    assert.notEqual(await source.ledgerPath(), await successor.ledgerPath());
    assert.notDeepEqual(source.keys.alpha.publicKey.export({ format: "der", type: "spki" }),
        successor.keys.alpha.publicKey.export({ format: "der", type: "spki" }));
    const stale = await runFixture(context, { ...options, restore: staleStore, keys: source.keys });
    assert.notEqual(await stale.ledgerPath(), await source.ledgerPath());
    assert.notEqual(await stale.ledgerPath(), await successor.ledgerPath());
    assert.deepEqual(await stale.sql("SELECT generation,active FROM local_authority"), [{ generation: 1, active: 1 }]);
    assert.deepEqual(await stale.sql(outbox), [{ task: "telegram:214", phase: "pending", sends: 0 }]);
    assert.deepEqual(await stale.sql(held, heldAttempt), [{ phase: "claimed", start_until: null }]);
    const budget = await stale.sql("SELECT * FROM pilot_budget");
    const inactive = { error: "inactive_generation" };
    assert.deepEqual(await stale.post("telegram", update(216, 37), webhook, 503), inactive);
    assert.deepEqual(await stale.fixture("cancel", { task: "telegram:215" }, 503), inactive);
    assert.deepEqual(await stale.wire(stale.signed("alpha", { op: "start", attempt: heldAttempt },
        { seq: oldSequence + 1 }), 503), inactive);
    const staleExecutor = await attachExecutor(context, stale);
    const replies = [];
    staleExecutor.proxy.transform = async ({ response }) => {
        replies.push(response);
        return response;
    };
    const retrying = staleExecutor.run("resume", undefined, undefined, 1, { journal: restoredJournal });
    await until(() => replies.length >= 2, "restored executor received two stale-generation replies");
    await retrying.stop();
    assert.ok(replies.every(({ status, value }) => status === 503 && value.error === inactive.error));
    assert.ok(staleExecutor.packets.every(({ command }) => command.op === "hello"));
    const alarm = await until(() => stale.metrics().find(row => row.scope === "alarm"), "restored delivery alarm ran");
    assert.deepEqual(stale.sends, [], "restored alarm made no mock Telegram request");
    assert.equal(alarm.written, 0);
    assert.deepEqual(await stale.sql(outbox), [{ task: "telegram:214", phase: "pending", sends: 0 }]);
    assert.deepEqual(await stale.sql(held, heldAttempt), [{ phase: "claimed", start_until: null }]);
    assert.deepEqual(await stale.sql("SELECT update_id FROM inbox ORDER BY update_id"), [{ update_id: 214 }, { update_id: 215 }]);
    assert.deepEqual(await stale.sql("SELECT sequence FROM agent_sequences"), [{ sequence: oldSequence }]);
    assert.deepEqual(await stale.sql("SELECT * FROM pilot_budget"), budget);
    assert.deepEqual(await stale.sql("SELECT * FROM delivery_receipts"), []);

    const current = await attachExecutor(context, successor);
    assert.deepEqual(await successor.wire(stale.signed("alpha", { op: "hello" }), 401), { error: "unauthorized" });
    const oldGeneration = current.run("resume", undefined, undefined, 1, {
        journal: restoredJournal, key: original.seed,
    });
    assert.match(await oldGeneration.exit(1), /fixture request rejected with HTTP 401/);
    assert.deepEqual(await readFile(journalDb), originalJournal);
    const oldCredential = current.run("resume", undefined, undefined, 2, {
        journal: restoredJournal, key: original.seed,
    });
    assert.match(await oldCredential.exit(1), /fixture request rejected with HTTP 401/);
    assert.deepEqual(current.packets.map(({ command }) => command.op), ["hello", "hello"]);
    assert.deepEqual(await current.launches(restoredJournal), [oldAttempt]);
    assert.deepEqual(await readFile(join(original.dir, "executor.sqlite3")), originalJournal);

    assert.equal((await successor.post("telegram", update(214, 99), webhook)).disposition, "duplicate");
    await successor.post("telegram", update(216, 37), webhook);
    const active = current.run("init", undefined, undefined, 2);
    await until(async () => (await current.launches()).length, "successor launched");
    const [result] = await until(() => {
        const rows = current.query("SELECT attempt FROM evidence WHERE event='result' AND acknowledged=1");
        return rows.length && rows;
    }, "successor terminal evidence acknowledged");
    await active.stop();
    assert.ok(result.attempt > heldAttempt);
    assert.deepEqual(await current.launches(), [result.attempt]);
    await until(async () => (await successor.fixture("delivery_status", { task: "telegram:216" })).phase === "delivered",
        "successor mock Telegram receipt committed");
    assert.deepEqual(await successor.fixture("delivery_status", { task: "telegram:216" }),
        { phase: "delivered", sends: 1, message_id: 1 });
    assert.deepEqual(successor.sends, [{ chat_id: 321, message_thread_id: 37, text: "fixture output: synthetic task 216" }]);
    assert.deepEqual(await successor.sql(outbox), [
        { task: "telegram:214", phase: "unknown", sends: 0 },
        { task: "telegram:216", phase: "delivered", sends: 1 },
    ]);
    assert.deepEqual(await successor.sql(held, heldAttempt), [{ phase: "unknown", start_until: null }]);
    assert.deepEqual(source.sends, []);
    context.diagnostic(JSON.stringify({
        generation: Number(await generationPin()), staleGeneration: 1, oldAttempt, heldAttempt,
        nextAttempt: result.attempt, staleLaunches: 0, staleSends: stale.sends.length, successorSends: successor.sends.length,
    }));
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

test("BigFur authority fences restored writers and is checked at effect boundaries", timeout, async context => {
    const external = await mkdtemp(join(import.meta.dirname, "build", "authority-"));
    const controlFile = join(external, "control.sqlite");
    let mock = await authorityFixture(controlFile);
    context.after(async () => {
        await mock.close();
        await rm(external, { recursive: true, force: true });
    });
    const objectName = "hosted-synthetic-pilot";
    const digest = "b".repeat(64);
    const oldRunner = "c".repeat(64);
    const runner = "d".repeat(64);
    const cutover = { fetch: request => mock.fetch(request), runner };
    const client = await runFixture(context, { inspect: true, cutover });
    const objectId = await client.objectId(objectName);
    const setTarget = generation => {
        cutover.target = {
            pin: {
                generation, revision: "e".repeat(40), hostname, objectId, custodyDigest: digest,
                telegramCredentialVersion: "synthetic-existing-token",
                executorKey: client.keys.alpha.publicKey.export({ format: "der", type: "spki" })
                    .subarray(-32).toString("hex"),
            },
            versionId: `synthetic-worker-${generation}`,
        };
    };
    const held = { error: "legacy_cutover_held" };
    const inactive = { error: "inactive_generation" };
    const ingress = (id, status = 200) => client.post("telegram", update(id, 37), webhook, status);
    const cancel = (task, status = 200) => client.post("cancel", { task }, {
        "x-cutover-operator": operator["x-fixture-secret"],
    }, status);
    setTarget(1);
    await client.restart();
    const oldTarget = cutover.target;
    assert.deepEqual(await ingress(41, 503), inactive);
    assert.ok(mock.checks.length, "authority received the preparation request");
    mock.publish(oldTarget, mock.checks.at(-1).objectIncarnation, oldRunner);
    assert.deepEqual(await ingress(41, 503), held);
    const originalUnknown = { id: 23, run: 24, topic: 37, outcome: "Unknown" };
    mock.retire();
    await client.stop();
    const seed = join(import.meta.dirname, "build", "authority-seed-");
    const snapshotRoot = await mkdtemp(seed);
    context.after(() => rm(snapshotRoot, { recursive: true, force: true }));
    await client.editLedger(db => {
        db.exec("INSERT INTO local_authority VALUES(1,2,0,1)");
        db.prepare("INSERT INTO local_legacy_cutover VALUES(1,?,?,?)").run(
            Buffer.from("synthetic baseline"), Buffer.from("synthetic delta"),
            Buffer.from(JSON.stringify(originalUnknown)),
        );
        db.prepare("INSERT INTO cutover_custody VALUES(1,2,?)").run(digest);
    });
    const saved = join(snapshotRoot, "before");
    await client.snapshot(saved);
    setTarget(2);
    await client.restart();
    assert.deepEqual(await ingress(42, 503), inactive);
    const activeIncarnation = mock.checks.at(-1).objectIncarnation;
    mock.publish(cutover.target, activeIncarnation, runner);
    await mock.close();
    mock = await authorityFixture(controlFile);
    assert.throws(() => mock.publish(cutover.target, activeIncarnation, runner));
    assert.throws(() => mock.publish(oldTarget, activeIncarnation, oldRunner));

    for (const [field, bad] of [
        ["highWater", 1], ["nonce", "replayed"], ["validUntil", Date.now() - 1000],
        ["objectIncarnation", "other-object-process"],
        ["target", { ...cutover.target, versionId: "older-version" }],
    ]) {
        mock.beforeReply = response => Response.json({ ...response, [field]: bad });
        assert.deepEqual(await ingress(42, 503), inactive, field);
    }
    mock.beforeReply = () => Response.error();
    assert.deepEqual(await ingress(42, 503), inactive);
    mock.beforeReply = undefined;
    assert.deepEqual(await client.sql("SELECT update_id FROM inbox"), []);
    assert.equal((await client.directIngress("other-object", update(42))).status, 404);
    await client.post("fixture", { op: "cancel", task: "telegram:42" }, operator, 404);
    assert.equal((await ingress(42)).disposition, "task");
    const hello = await client.agent("alpha", "hello");
    assert.equal(hello.instance, `bigfur:2:${objectId}`);
    const task = "telegram:42";
    const { attempt } = await client.agent("alpha", "claim", { task });

    const restored = await runFixture(context, {
        inspect: true, restore: saved, keys: client.keys, cutover: { ...cutover },
    });
    assert.deepEqual(await restored.post("telegram", update(42), webhook, 503), inactive);
    assert.notEqual(mock.checks.at(-1).objectIncarnation, activeIncarnation);
    assert.deepEqual(await restored.agent("alpha", "start", { attempt }, 503), inactive);
    const staleDeployment = { ...cutover, versionId: oldTarget.versionId };
    const staleVersion = await runFixture(context, {
        restore: saved, keys: client.keys, cutover: staleDeployment,
    });
    assert.deepEqual(await staleVersion.post("telegram", update(42), webhook, 503), inactive);
    assert.equal(mock.checks.at(-1).target.versionId, oldTarget.versionId);
    staleDeployment.target = oldTarget;
    await staleVersion.restart();
    assert.deepEqual(await staleVersion.post("telegram", update(42), webhook, 503), inactive);
    assert.equal(mock.checks.at(-1).target.pin.generation, 1);
    const start = client.signed("alpha", { op: "start", attempt });
    assert.deepEqual(await client.wire({
        ...start, headers: { ...start.headers, "x-executor-incarnation": oldRunner },
    }, 503), inactive);
    assert.deepEqual(await client.wire({ ...start, headers: {} }, 503), inactive);
    assert.deepEqual(await client.sql("SELECT phase,start_until FROM attempts"), [
        { phase: "claimed", start_until: null },
    ]);

    let checks = 0;
    const failCheck = ordinal => {
        checks = 0;
        mock.beforeReply = () => ++checks === ordinal ? new Response(null, { status: 503 }) : undefined;
    };
    failCheck(2);
    assert.deepEqual(await ingress(43, 503), inactive);
    assert.deepEqual(await client.sql("SELECT update_id FROM inbox"), [{ update_id: 42 }]);
    failCheck(2);
    assert.deepEqual(await cancel(task, 503), inactive);
    assert.deepEqual(await client.sql("SELECT phase,cancel_requested FROM attempts"), [
        { phase: "claimed", cancel_requested: 0 },
    ]);
    failCheck(2);
    assert.deepEqual(await client.wire(start, 503), inactive);
    assert.deepEqual(await client.sql("SELECT phase,start_until FROM attempts"), [
        { phase: "claimed", start_until: null },
    ]);
    mock.beforeReply = undefined;
    const grant = await client.wire(start);
    assert.equal(grant.instance, hello.instance);
    assert.ok(grant.start_until > Date.now());
    assert.ok(grant.authority_valid_until > Date.now());
    assert.deepEqual(await client.agent("alpha", "start", { attempt }, 409), { error: "state_conflict" });
    const proof = (attempt, event) => {
        const payload = JSON.stringify({ attempt, event });
        const message = `coconutclaw-evidence-v1\n${hello.instance}\nalpha\n${payload}`;
        return { payload, signature: sign(null, Buffer.from(message), client.keys.alpha.privateKey).toString("hex") };
    };
    const report = (attempt, event, status) => client.agent("alpha", "report", { proof: proof(attempt, event) }, status);
    await report(attempt, { kind: "started" });
    await report(attempt, { kind: "result", text: "same topic" });
    await until(async () => (await client.sql("SELECT message_id FROM delivery_receipts")).length,
        "authority sends and durably records the receipt");
    const delivered = [{ task, sendNumber: 1, chatId: 321, messageThreadId: 37, text: "same topic" }];
    assert.deepEqual(mock.sends, delivered);

    await ingress(43);
    const lost = await client.agent("alpha", "claim", { task: "telegram:43" });
    failCheck(3);
    assert.deepEqual(await client.agent("alpha", "start", { attempt: lost.attempt }, 503), inactive);
    mock.beforeReply = undefined;
    assert.deepEqual(await client.agent("alpha", "start", { attempt: lost.attempt }, 409), { error: "state_conflict" });
    assert.equal((await client.agent("alpha", "status", { attempt: lost.attempt })).phase, "authorized");
    await report(lost.attempt, { kind: "unknown" });

    await ingress(44);
    const interrupted = await client.agent("alpha", "claim", { task: "telegram:44" });
    await client.agent("alpha", "start", { attempt: interrupted.attempt });
    await report(interrupted.attempt, { kind: "started" });
    mock.beforeSend = () => mock.retire();
    await report(interrupted.attempt, { kind: "result", text: "reservation without egress" }, [200, 503]);
    await until(() => mock.status().retired, "revoke between Worker reservation and Telegram boundary");
    assert.deepEqual(mock.sends, delivered);
    const interruptedDelivery = () => client.sql("SELECT phase,sends FROM outbox WHERE task='telegram:44'");
    assert.deepEqual(await interruptedDelivery(), [{ phase: "in_flight", sends: 1 }]);
    assert.deepEqual(await ingress(45, 503), inactive);
    assert.deepEqual(await cancel(task, 503), inactive);
    assert.deepEqual(await client.agent("alpha", "start", { attempt }, 503), inactive);
    const [{ projection }] = await client.sql("SELECT projection FROM local_legacy_cutover");
    assert.deepEqual(JSON.parse(Buffer.from(projection)), originalUnknown);
    await client.stop();
    await client.restart();
    assert.deepEqual(await ingress(45, 503), inactive);
    const afterReopen = mock.checks.length;
    await until(() => mock.checks.slice(afterReopen).some(check => check.runnerIncarnation === null),
        "a persisted delivery alarm reaches the authority gate");
    assert.deepEqual(await interruptedDelivery(), [{ phase: "in_flight", sends: 1 }]);
    assert.deepEqual(mock.sends, delivered);
});

test("checked-in phase gate disables webhook, agent and fixture routes", timeout, async context => {
    const client = await runFixture(context, { enabled: false, executor: true });
    await client.post("telegram", update(41), webhook, 404);
    await client.agent("alpha", "hello", {}, 404);
    await client.fixture("wake", { at_ms: Date.now() }, 404);
    assert.deepEqual(client.sends, []);
});

test("missing generation stops ingress and signed requests before opening a ledger", timeout, async context => {
    let pin;
    const client = await runFixture(context, { executor: true, inspect: true, generationPin: () => pin });
    const inactive = { error: "inactive_generation" };
    assert.deepEqual(await client.post("telegram", update(41), webhook, 503), inactive);
    assert.deepEqual(await client.agent("alpha", "hello", {}, 503), inactive);
    const direct = await client.directIngress("local-executor-fixture", update(41));
    assert.equal(direct.status, 503);
    assert.deepEqual(await direct.json(), inactive);
    assert.deepEqual(client.metrics(), []);

    pin = "1";
    await client.restart();
    const wrongObject = await client.directIngress("other-ledger", update(41));
    assert.equal(wrongObject.status, 404);
    assert.deepEqual(await wrongObject.json(), { error: "not_found" });
    await client.post("telegram", update(41), { ...webhook, host: "other.gateway.invalid" }, 404);
    assert.deepEqual(await client.post("telegram", update(41), webhook), {
        disposition: "task", task: "telegram:41",
    });
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
