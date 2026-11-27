import assert from "node:assert/strict";
import { execFile, execFileSync, spawn } from "node:child_process";
import { createHash, generateKeyPairSync, sign, verify } from "node:crypto";
import { once } from "node:events";
import { cp, mkdir, mkdtemp, open, readFile, readdir, rename, rm, symlink, writeFile } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { createServer, request } from "node:https";
import { basename, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { promisify } from "node:util";
import { readBuildOutput } from "@cloudflare/build-output-utils";
import { Log, LogLevel, Miniflare } from "miniflare";
import { checkAuthorityHandoff, pairingChallenge } from "./authority-preflight.mjs";
import { runnerKey } from "./pair-runner-local.mjs";
import { authorityFixture, bearer, hostname, origin } from "./runtime-authority-fixture.mjs";
import {
    admitLegacyFixtureUpdate, advanceGatewayFixture, appendRunningFixtureTask, cutoverSnapshot,
    freezeGatewayFixture, importCutoverDelta,
    importCutoverFixture, returnFixtureTasks, seedCutoverSource,
} from "./cutover-fixture.mjs";

const { workers: { default: { bundleDir, config } } } =
    await readBuildOutput(process.env.GATEWAY_TEST_BUILD_ROOT ?? import.meta.dirname);
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
const placementPrefix = signingPrefix.replace("local-fixture", "local-placement-fixture");

async function runFixture(context, {
    enabled = true, executor = false, inspect = false, mock, restore, generationPin, cutover, placementAliases,
    keys = Object.fromEntries((placementAliases ? ["alpha", "beta", "gamma"] : ["alpha", "beta"])
        .map(device => [device, generateKeyPairSync("ed25519")])),
} = {}) {
    const persistence = await mkdtemp(join(import.meta.dirname, "build", "sqlite-"));
    const sends = [];
    const sendTimes = [];
    const logs = [];
    const log = new Log(LogLevel.INFO);
    log.log = message => logs.push(message);
    const sequence = Object.fromEntries(Object.keys(keys).map(device => [device, 0]));
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
        const aliases = placementAliases?.();
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
            modules: cutover?.holdReceipt ? [{
                path: join(bundleDir, "receipt-fixture.mjs"),
                type: "ESModule",
                contents: `
                    import worker, { BotLedger as Ledger } from ${JSON.stringify(`./${mainModule}`)};
                    const timeout = AbortSignal.timeout.bind(AbortSignal);
                    AbortSignal.timeout = ms => timeout(ms === 1500 ? 15_000 : ms);
                    const expire = "/fixture-expire";
                    const advance = "/fixture-advance-send";
                    const duplicate = "/fixture-commit-receipt";
                    export class BotLedger extends Ledger {
                        constructor(state, env) {
                            super(state, env);
                            this.storage = state.storage;
                        }
                        fetch(request) {
                            const path = new URL(request.url).pathname;
                            if (path === expire) {
                                this.storage.sql.exec("UPDATE outbox SET ready_at=0 WHERE phase='in_flight'");
                                return super.alarm();
                            }
                            if (path === advance) {
                                this.storage.sql.exec("UPDATE outbox SET sends=2 WHERE phase='unknown' AND sends=1");
                                return new Response(null, { status: 204 });
                            }
                            if (path === duplicate) {
                                this.storage.transactionSync(() => {
                                    this.storage.sql.exec("INSERT INTO delivery_receipts SELECT task,1 FROM outbox WHERE phase='unknown'");
                                    this.storage.sql.exec("UPDATE outbox SET phase='delivered' WHERE phase='unknown'");
                                });
                                return new Response(null, { status: 204 });
                            }
                            return super.fetch(request);
                        }
                        async alarm() {
                            const result = await super.alarm();
                            await fetch(${JSON.stringify(`${origin}/fixture-alarm-finished`)});
                            return result;
                        }
                    }
                    export default {
                        fetch(request, env, context) {
                            const path = new URL(request.url).pathname;
                            if (path === expire || path === advance || path === duplicate) {
                                const id = env.BOTS.idFromName("bigfur-production");
                                return env.BOTS.get(id).fetch(request);
                            }
                            return worker.fetch(request, env, context);
                        }
                    };
                `,
            }, ...workerModules] : workerModules,
            modulesRoot: bundleDir,
            compatibilityDate: config.compatibilityDate,
            compatibilityFlags: config.compatibilityFlags,
            bindings: {
                ...variables,
                ...(enabled ? { PHASE: executor ? "phase2-local" : "phase1-local" } : {}),
                ...(placementAliases ? { PHASE: "phase1-placement-local" } : {}),
                ...(aliases === undefined ? {} : { LOCAL_PLACEMENT_ALIASES: aliases }),
                WEBHOOK_SECRET: webhook["x-telegram-bot-api-secret-token"],
                FIXTURE_SECRET: operator["x-fixture-secret"],
                ...Object.fromEntries(Object.keys(cutover?.pairing ? {} : keys).map(device => [
                    `DEVICE_${device.toUpperCase()}_KEY`,
                    publicHex(device),
                ])),
                ...(executor ? { EXECUTOR_ALPHA_KEY: publicHex("alpha") } : {}),
                ...(executor && pin !== undefined ? { LOCAL_GENERATION: pin } : {}),
                ...(inspect ? { LOCAL_SQL_METRICS: "1" } : {}),
                ...(cutover ? {
                    PHASE: cutover.pairedProduction ? "phase2-production" : cutover.pairing ? "phase2-pairing-local"
                        : cutover.enrolled ? "phase2-placement-local"
                            : cutover.production ? "phase2-production" : "phase2-cutover",
                    CUTOVER_TARGET: JSON.stringify(cutover.target?.pin ?? {}),
                    CF_VERSION_METADATA: { id: cutover.versionId ?? cutover.target?.versionId ?? "unset" },
                    CUTOVER_AUTHORITY_ORIGIN: origin,
                    CUTOVER_AUTHORITY_BEARER: bearer,
                    CUTOVER_CANCEL_SECRET: operator["x-fixture-secret"],
                    ...(cutover.scope ? { PRODUCTION_TELEGRAM_SCOPE: cutover.scope } : {}),
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
            const res = await dispatch(`https://${requestHost()}/v1/${path}`, { headers, body: bytes });
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

    const requestHost = () => cutover?.target?.pin.hostname ??
        (cutover?.enrolled && !cutover.pairedProduction ? "127.0.0.1" : cutover?.production ? "bigfur.liu.nz" : hostname);
    const dispatch = (url, init) => service.dispatchFetch(url, { method: "POST", ...init });

    async function post(path, body, headers, status = 200, chunked = false) {
        const response = await rawPost(path, body, headers, chunked);
        assert.ok([status].flat().includes(response.status), `${path} HTTP ${response.status} ${JSON.stringify(response.value)}`);
        return response.value;
    }

    async function directIngress(objectName, update) {
        const bots = await service.getDurableObjectNamespace("BOTS");
        const requestOrigin = cutover ? `https://${requestHost()}/` : url;
        return bots.get(bots.idFromName(objectName)).fetch(new URL("/v1/telegram", requestOrigin), {
            method: "POST", headers: webhook, body: JSON.stringify(update),
        });
    }

    async function objectId(name) {
        return (await service.getDurableObjectNamespace("BOTS")).idFromName(name).toString();
    }

    function signed(device, command, { at = Date.now(), prefix, seq, incarnation } = {}) {
        incarnation ??= cutover?.enrolled ? cutover.enrolled[device] : cutover?.runner;
        const instance = cutover && `bigfur:${cutover.target.pin.generation}:${cutover.target.pin.objectId}`;
        prefix ??= cutover?.enrolled
            ? `coconutclaw-enrolled-request-v1\n${instance}\n${device}\n${incarnation}\nPOST\n/v1/agent\n`
            : cutover
            ? `coconutclaw-v1\n${instance}\nPOST\n/v1/agent\n`
            : executor
            ? (generation === 1 ? executorPrefix : executorPrefix.replace("fixture\n", `fixture:${generation}\n`))
            : placementAliases ? placementPrefix : signingPrefix;
        const mutating = !["hello", "next", "status"].includes(command.op);
        const body = JSON.stringify({
            device, at_ms: at, sequence: seq ?? (mutating ? ++sequence[device] : 0), command,
        });
        const signature = sign(null, Buffer.from(prefix + body), keys[device].privateKey).toString("hex");
        return { body, headers: {
            "x-agent-signature": signature,
            ...(cutover ? { "x-executor-incarnation": incarnation } : {}),
            ...(cutover?.enrolled ? { "x-executor-placement": device } : {}),
        } };
    }

    const wire = (packet, status) => post("agent", packet.body, packet.headers, status);
    const agent = (device, op, fields, status) => wire(signed(device, { op, ...fields }), status);
    const fixture = (op, fields, status, fault) => post("fixture", { op, ...fields }, {
        ...operator, ...(fault ? { "x-fixture-fault": fault } : {}),
    }, status);
    async function fixtureDbPath(filePattern) {
        assert.ok(inspect);
        const files = (await readdir(persistence, { recursive: true })).filter(file => filePattern.test(basename(file)));
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
        ? new RegExp(`^${cutover.target.pin.objectId}\\.sqlite$`)
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
    return { agent, fixture, post, rawPost, dispatch, directIngress, objectId, stop, restart, snapshot, sends, sendTimes, signed, wire, keys, sql,
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
const windowsCustody = process.platform === "win32";
const linuxCustody = process.platform === "linux";
const singletonTimeout = { ...nativeTimeout, skip: !executable || windowsCustody };
const realCodex = process.env.GATEWAY_TEST_CODEX;

async function until(check, message, timeoutMs = 10_000) {
    const deadline = Date.now() + timeoutMs;
    do {
        const result = await check();
        if (result) return result;
        await sleep(50);
    } while (Date.now() < deadline);
    assert.fail(typeof message === "function" ? message() : message);
}

function makeWindowsPrivate(root) {
    const sid = execFileSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command",
        "[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value"], { encoding: "utf8" }).trim();
    assert.match(sid, /^S-\d+(?:-\d+)+$/);
    execFileSync("icacls.exe", [root, "/inheritance:r", "/grant:r",
        `*${sid}:(OI)(CI)F`, "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F"]);
}

async function attachExecutor(context, client, production, device = "alpha", originalSeed) {
    const root = await mkdtemp(join(import.meta.dirname, "build", "executor-"));
    if (windowsCustody) makeWindowsPrivate(root);
    const dir = join(root, "journal");
    const cert = join(root, "cert.pem");
    const ca = join(root, "ca.pem");
    const caKey = join(root, "ca-key.pem");
    const tlsKey = join(root, "tls-key.pem");
    const csr = join(root, "tls.csr");
    const extensions = join(root, "tls.ext");
    const seed = originalSeed ?? join(root, "device.seed");
    if (!originalSeed) {
        await writeFile(seed, client.keys[device].privateKey.export({ format: "der", type: "pkcs8" }).subarray(-32), { mode: 0o600 });
    }
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
    const proxy = { offline: false, pairingEndpoint: undefined, transform: async ({ response }) => response };
    let proxyError;
    const server = createServer({ key: await readFile(tlsKey), cert: await readFile(cert) }, async (req, res) => {
        try {
            assert.equal(req.method, "POST");
            const chunks = [];
            for await (const chunk of req) chunks.push(chunk);
            const body = Buffer.concat(chunks).toString();
            if (req.url === "/v1/pair" && proxy.pairingEndpoint) {
                const paired = await fetch(`${proxy.pairingEndpoint}/v1/pair`, { method: "POST", body });
                res.writeHead(paired.status, { "content-type": "application/json" }).end(await paired.text());
                return;
            }
            assert.equal(req.url, "/v1/agent");
            const packet = { body, headers: { "x-agent-signature": req.headers["x-agent-signature"] } };
            if (production) packet.headers["x-executor-incarnation"] = req.headers["x-executor-incarnation"];
            if (req.headers["x-executor-placement"] !== undefined) {
                packet.headers["x-executor-placement"] = req.headers["x-executor-placement"];
            }
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
        let termination;
        child.once("close", (code, signal) => { termination = code ?? signal; });
        const process = {
            pid: child.pid,
            get exitStatus() { return { code: child.exitCode, signal: child.signalCode }; },
            get stderr() { return stderr; },
            assertRunning() {
                if (termination !== undefined) {
                    assert.fail(`executor ${child.pid} exited (${termination}): ${stderr.trim()}`);
                }
            },
            async exit(expected) {
                const outcome = await exited;
                if (termination === undefined) await once(child, "close");
                assert.deepEqual(outcome, [expected, null], stderr);
                return stderr;
            },
            async stop() {
                if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
                return exited;
            },
        };
        processes.push(process);
        return process;
    }
    function run(mode, behavior, endpoint = origin, generation = 1, { journal = dir, key = seed } = {}) {
        return start(["--loopback-fixture", mode, endpoint, ca, key, journal,
            ...(generation === 1 ? [] : ["--generation", String(generation)]), ...(behavior ? [behavior] : [])]);
    }
    function runEnrolled(mode, behavior, {
        journal = dir, key = seed, placement = device, incarnation = production.enrolled[device],
        generation = production.target.pin.generation, objectId = production.target.pin.objectId,
    } = {}) {
        return start(["--loopback-fixture", mode, origin, ca, key, journal,
            "--enrollment", String(generation), objectId, placement, incarnation,
            ...(behavior ? [behavior] : [])]);
    }
    async function runProduction(mode, config, options = {}) {
        const privateConfig = join(root, "runner.json");
        const incarnation = join(root, "incarnation");
        const enrollment = join(root, "enrollment.json");
        if (options.enrollment) {
            await writeFile(enrollment, JSON.stringify(options.enrollment), { mode: 0o600 });
        } else {
            await writeFile(incarnation, options.runner ?? production.runner, { mode: 0o600 });
        }
        await writeFile(privateConfig, JSON.stringify({
            origin, ca, signing_key: seed,
            ...(options.enrollment ? { enrollment } : {
                incarnation, generation: production.target.pin.generation,
                object_id: production.target.pin.objectId, placement: "alpha",
            }),
            timeout_seconds: 20, ...config,
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
        const domain = production?.enrolled
            ? `coconutclaw-enrolled-evidence-v1\n${instance}\n${device}\n${production.enrolled[device]}\n`
            : `coconutclaw-evidence-v1\n${instance}\nalpha\n`;
        assert.ok(verify(null, Buffer.from(domain + proof.payload),
            client.keys[device].publicKey, Buffer.from(proof.signature, "hex")));
        return JSON.parse(proof.payload);
    }
    return { proxy, run, runEnrolled, runProduction, launches, proofs, verifyProof, query, packets, dir, seed, root, origin, ca };
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
    return attachProductionRunner(context, client, cutover, mock, ingress);
}

async function attachProductionRunner(context, client, cutover, mock, ingress, device) {
    const executor = await attachExecutor(context, client, cutover, device);
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

const procStat = pid => readFile(`/proc/${pid}/stat`, "utf8").catch(error => {
    if (error.code !== "ENOENT") throw error;
    return "";
});
const statFields = stat => stat.slice(stat.lastIndexOf(")") + 2).split(" ");

async function liveLinuxTree(runner, { detached = true } = {}) {
    return until(async () => {
        try {
            const [supervisor] = await procChildren(runner.pid);
            if (!supervisor) return false;
            const [leader] = await procChildren(supervisor);
            if (!leader) return false;
            const [child] = await procChildren(leader);
            if (!child) return false;
            const stat = await procStat(child);
            const [state, parent, group, session] = statFields(stat);
            return stat.startsWith(`${child} (sleep) `) && state !== "Z"
                && Number(parent) === leader && (!detached || (Number(group) === child && Number(session) === child))
                && { supervisor, provider: [leader, child] };
        } catch (error) {
            if (error.code !== "ENOENT") throw error;
            return false;
        }
    }, "Linux supervisor owns a live provider with a sleep descendant", 3000);
}

async function linuxTreeExited(tree, reason) {
    await until(async () => (await Promise.all(tree.provider.map(procStat))).every(stat => !stat),
        `${reason} did not reap provider PIDs ${tree.provider}`, 2000);
    await until(async () => {
        const stat = await procStat(tree.supervisor);
        return !stat || statFields(stat)[0] === "Z";
    }, `${reason} did not exit supervisor ${tree.supervisor}`, 2000);
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
    const abandonedTree = await liveLinuxTree(started);
    assert.deepEqual(abandonedTree.provider, abandoned);
    assert.deepEqual(await started.stop(), [null, "SIGKILL"]);
    await linuxTreeExited(abandonedTree, "runner SIGKILL");
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
    const { client, mock, cutover, ingress, telegram } = await productionAuthorityFixture(context);
    const runner = await attachProductionRunner(context, client, cutover, mock, (id, text) => {
        const update = telegram(id);
        update.message.text = text;
        return ingress(update);
    });
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
    const provider_env = ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"];
    const config = { ...runner.configure(realCodex), provider_env };
    const env = { HTTP_PROXY: endpoint, HTTPS_PROXY: endpoint, ALL_PROXY: endpoint, NO_PROXY: "127.0.0.1" };
    await runner.ingress(80, "Initialize the ledger for the local smoke.");
    mock.publish(cutover.target, null, cutover.runner);
    cutover.controlOnline = true;
    assert.equal((await runner.cancel(80)).phase, "cancelled_before_start");
    const processHandle = await runner.runProduction("init", config, { env });
    await until(() => runner.packets.some(({ command }) => command.op === "next"), "runner did not enroll");
    runner.proxy.offline = true;
    await runner.ingress(81, "Reply with Codex local smoke.");
    await runner.ingress(82, "Hold this local smoke request.");
    const before = await client.agent("alpha", "hello");
    const enrolled = mock.checks.at(-1).objectIncarnation;
    await client.restart();
    assert.deepEqual(await ingress(telegram(81)), { disposition: "duplicate" });
    assert.equal((await client.agent("alpha", "hello")).instance, before.instance);
    assert.notEqual(mock.checks.at(-1).objectIncarnation, enrolled);
    assert.equal(calls, 0);
    runner.proxy.offline = false;
    await until(() => calls === 2 || mockError, "Codex did not open both local Responses requests", 16_000);
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
    await until(() => mock.sends.some(({ task, messageThreadId, text }) =>
        task === "telegram:81" && messageThreadId === 101550 && text === "Codex local smoke"),
    "Codex result did not reach its topic");
    context.diagnostic("2 local Responses requests, signed result and process_exited, native Codex PID gone and HTTP stream closed, 0 relaunches on resume");
});

test("native executor retains result across lost receipt, reconnects, and replies in the original topic", singletonTimeout, async context => {
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

test("native executor crashes on both sides of spawn and reconciles without relaunch", singletonTimeout, async context => {
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

test("native executor fences replayed and late grants and confirms both kinds of cancellation", singletonTimeout, async context => {
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

test("synthetic cutover requires late signed terminal evidence before executor generation change", singletonTimeout, async context => {
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

test("stale restore fences independent ledgers and executor journals after generation change", singletonTimeout, async context => {
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

async function productionAuthorityFixture(context, {
    enrolled, placementAliases, pairing = false, pairedProduction = false, control, holdReceipt,
} = {}) {
    const root = await mkdtemp(join(import.meta.dirname, "build", "direct-retirement-"));
    const mock = await authorityFixture(join(root, "control.sqlite"), { pairing, control });
    context.after(async () => {
        await mock.close();
        await rm(root, { recursive: true, force: true });
    });
    const owner = 5_123_456_789;
    const topic = 101550;
    const cutover = {
        production: true, pairedProduction, enrolled: pairedProduction ? {} : enrolled,
        pairing, holdReceipt, scope: `${owner}:${topic}`, runner: "d".repeat(64),
        controlOnline: false, authorityCalls: 0,
        fetch(request) {
            cutover.authorityCalls++;
            return cutover.controlOnline ? mock.fetch(request) : new Response(null, { status: 503 });
        },
    };
    const client = await runFixture(context, { inspect: true, cutover, placementAliases, ...(pairing ? { keys: {} } : {}) });
    const cancelHeaders = { "x-cutover-operator": operator["x-fixture-secret"] };
    let objectId;
    if (enrolled) {
        objectId = await client.objectId(pairing ? "paired-placement-fixture:1" : "enrolled-placement-fixture:1");
    } else {
        assert.equal((await client.dispatch("https://bigfur.liu.nz/v1/production/identity")).status, 401);
        assert.equal((await client.dispatch("https://other.invalid/v1/production/identity", {
            headers: cancelHeaders,
        })).status, 404);
        ({ objectId } = await client.post("production/identity", {}, cancelHeaders));
        assert.equal(objectId, await client.objectId("bigfur-production"));
    }
    const key = device => client.keys[device].publicKey.export({ format: "der", type: "spki" })
        .subarray(-32).toString("hex");
    cutover.target = {
        pin: {
            generation: 1, revision: "e".repeat(40), hostname: enrolled ? "127.0.0.1" : "bigfur.liu.nz", objectId,
            custodyDigest: "b".repeat(64),
            telegramCredentialVersion: "synthetic-existing-token",
            ...(pairedProduction ? { pairing: "production-v1" } : pairing ? { pairing: "fixture-v1" }
                : enrolled ? { executorKeys: Object.fromEntries(Object.keys(enrolled).map(device => [device, key(device)])) }
                    : { executorKey: key("alpha") }),
        },
        versionId: "direct-retirement-fixture",
    };
    await client.restart();
    const telegram = (id, thread = topic, sender = owner, chat = owner) => ({
        update_id: id,
        message: { message_id: id, date: 1795560000, chat: { id: chat, type: "private" },
            from: { id: sender, is_bot: false }, is_topic_message: true, message_thread_id: thread,
            text: `synthetic task ${id}` },
    });
    const ingress = (body, status = 200) => client.post("telegram", body, webhook, status);
    return { root, mock, owner, topic, cutover, client, cancelHeaders, telegram, ingress };
}

function productionProof(client, grant, event, incarnation) {
    const device = grant.placement ?? "alpha";
    const payload = JSON.stringify({ attempt: grant.attempt, event });
    const domain = incarnation
        ? `coconutclaw-enrolled-evidence-v1\n${grant.instance}\n${device}\n${incarnation}\n`
        : `coconutclaw-evidence-v1\n${grant.instance}\nalpha\n`;
    return { payload, signature: sign(null, Buffer.from(domain + payload), client.keys[device].privateKey).toString("hex") };
}

async function reportProductionResult(client, grant, text, incarnation) {
    for (const event of [{ kind: "started" }, { kind: "result", text }]) {
        await client.agent(grant.placement ?? "alpha", "report", { proof: productionProof(client, grant, event, incarnation) });
    }
}

async function redeemProduction(mock, keys, code) {
    const key = keys.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    const response = await fetch(`${mock.endpoint}/v1/pair`, {
        method: "POST",
        body: JSON.stringify({
            code, key, signature: sign(null, pairingChallenge(code, true), keys.privateKey).toString("hex"),
        }),
    });
    return [response.status, response.ok ? await response.json() : null];
}

async function pairProduction(mock, client, cutover, device, alias) {
    const keys = client.keys[device];
    const key = keys.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    const code = mock.issue({ key, alias });
    const [status, receipt] = await redeemProduction(mock, keys, code);
    assert.equal(status, 200);
    assert.equal(receipt.placement, device);
    assert.equal(receipt.generation, cutover.target.pin.generation);
    assert.equal(receipt.objectId, cutover.target.pin.objectId);
    cutover.enrolled[device] = receipt.incarnation;
    return receipt;
}

async function nativePlacement(context) {
    const enrolled = { alpha: "e".repeat(64), beta: "d".repeat(64) };
    const { mock, client, cutover, ingress, telegram, cancelHeaders, owner, topic } =
        await productionAuthorityFixture(context, { enrolled });
    cutover.controlOnline = true;
    await client.agent("beta", "hello", {}, 503);
    mock.publish(cutover.target, mock.checks.at(-1).objectIncarnation, enrolled);
    async function admit(id, prompt) {
        const update = telegram(id);
        update.message.text = prompt;
        return (await ingress(update)).task;
    }
    return { mock, client, cutover, admit, cancelHeaders, owner, topic };
}

test("BigFur paired duplicates acknowledge locally during outage and after retirement", timeout, async context => {
    const { mock, client, cutover, ingress, telegram, topic } =
        await productionAuthorityFixture(context, { pairedProduction: true });
    cutover.controlOnline = true;
    mock.publish(cutover.target, null);
    await pairProduction(mock, client, cutover, "alpha");
    assert.deepEqual(await ingress(telegram(45291)), { disposition: "task", task: "telegram:45291" });
    const duplicate = { disposition: "duplicate" };
    const ignored = { disposition: "ignored", task: null };
    const noAuthority = { error: "inactive_generation" };
    cutover.controlOnline = false;
    await client.restart();
    const before = cutover.authorityCalls;
    assert.deepEqual(await ingress(telegram(45291)), duplicate);
    assert.deepEqual(await ingress(telegram(45291, topic + 1)), ignored);
    assert.equal(cutover.authorityCalls, before);
    assert.deepEqual(await ingress(telegram(45292), 503), noAuthority);
    cutover.controlOnline = true;
    mock.retire();
    assert.deepEqual(await ingress(telegram(45291)), duplicate);
    assert.deepEqual(await ingress(telegram(45292), 503), noAuthority);
    assert.deepEqual(await client.sql("SELECT id,pin FROM tasks"), [{ id: "telegram:45291", pin: "alpha" }]);
    assert.deepEqual(mock.sends, []);
});

test("BigFur pending paired delivery rearms after a Control outage at cold wake", timeout, async context => {
    const { mock, client, cutover, ingress, telegram, owner, topic } =
        await productionAuthorityFixture(context, { pairedProduction: true });
    cutover.controlOnline = true;
    mock.publish(cutover.target, null);
    const alpha = await pairProduction(mock, client, cutover, "alpha");
    const { task } = await ingress(telegram(45293));
    const { attempt } = await client.agent("alpha", "claim", { task });
    const grant = await client.agent("alpha", "start", { attempt });
    await reportProductionResult(client, grant, "pending during outage", alpha.incarnation);
    const outbox = () => client.sql("SELECT phase,sends FROM outbox WHERE task=?", task);
    await client.stop();
    assert.deepEqual(await outbox(), [{ phase: "pending", sends: 0 }]);
    cutover.controlOnline = false;
    const before = cutover.authorityCalls;
    await client.restart();
    await until(() => cutover.authorityCalls > before, "pending alarm reached unavailable Control");
    assert.deepEqual(await outbox(), [{ phase: "pending", sends: 0 }]);
    assert.deepEqual(mock.sends, []);
    cutover.controlOnline = true;
    await until(async () => (await outbox())[0]?.phase === "delivered", "pending alarm resumed");
    await client.restart();
    assert.deepEqual(await outbox(), [{ phase: "delivered", sends: 1 }]);
    assert.deepEqual(mock.sends, [
        { task, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "pending during outage" },
    ]);
});

for (const scenario of ["proven unsent", "unproven unsent", "mismatched no-send proof", "received receipt", "lost receipt"]) {
    test(`BigFur delivery across Control outage with ${scenario}`, timeout, async context => {
        const { root, mock, client, cutover, ingress, telegram, owner, topic } =
            await productionAuthorityFixture(context, { pairedProduction: true });
        cutover.controlOnline = true;
        mock.publish(cutover.target, null);
        const alpha = await pairProduction(mock, client, cutover, "alpha");
        const { task } = await ingress(telegram(45298));
        const { attempt } = await client.agent("alpha", "claim", { task });
        const grant = await client.agent("alpha", "start", { attempt });
        const outbox = async () => (await client.sql(
            "SELECT phase,sends,message_id FROM outbox LEFT JOIN delivery_receipts USING(task) WHERE task=?", task,
        ))[0];
        const received = scenario === "received receipt";
        const lost = scenario === "lost receipt";
        const proven = scenario === "proven unsent";
        const mismatched = scenario === "mismatched no-send proof";
        const calls = [];
        const forward = cutover.fetch;
        cutover.fetch = async request => {
            const { pathname } = new URL(request.url);
            const { delivery, deliveryCheck } = await request.clone().json();
            if (deliveryCheck && !cutover.controlOnline) calls.push("offline alarm");
            if (pathname === "/v1/send" && delivery.closeUnsent && (proven || mismatched)) {
                const proof = await forward(request);
                assert.equal(proof.status, 200);
                const reply = await proof.json();
                if (mismatched) return Response.json({ ...reply, nonce: "unrelated" });
                cutover.controlOnline = false;
                return Response.json(reply);
            }
            if (pathname === "/v1/send" && !delivery.closeUnsent && delivery.sendNumber === 1) {
                calls.push("first dispatch");
                assert.deepEqual(await outbox(), { phase: "in_flight", sends: 1, message_id: null });
                if (!(received || lost)) {
                    if (!proven && !mismatched) cutover.controlOnline = false;
                    return new Response(null, { status: 503 });
                }
            }
            return forward(request);
        };
        mock.afterSend = () => {
            if (received || lost) cutover.controlOnline = false;
            if (lost) return Response.error();
        };
        await reportProductionResult(client, grant, scenario, alpha.incarnation);
        if (proven) {
            await until(() => calls.includes("offline alarm"), "closed attempt rearms through a second outage");
            assert.deepEqual(await outbox(), { phase: "pending", sends: 1, message_id: null });
            assert.deepEqual(mock.sends, []);
            cutover.controlOnline = true;
        }
        const delivered = proven || received;
        const expected = { phase: delivered ? "delivered" : "unknown", sends: proven ? 2 : 1, message_id: delivered ? 1 : null };
        await until(async () => (await outbox()).phase === expected.phase, `durable ${expected.phase}`);
        if (!delivered && !mismatched) assert.ok(calls.includes("offline alarm"), "expiry ran during the Control outage");
        const control = new DatabaseSync(join(root, "control.sqlite"), { readOnly: true });
        try {
            const rows = control.prepare("SELECT number,message_id FROM sends WHERE generation=1 AND task=? ORDER BY number")
                .all(task).map(row => ({ ...row }));
            const first = proven || mismatched ? { number: 1, message_id: 0 }
                : received || lost ? { number: 1, message_id: 1 } : null;
            assert.deepEqual(rows, proven ? [first, { number: 2, message_id: 1 }] : first ? [first] : []);
        } finally {
            control.close();
        }
        cutover.controlOnline = true;
        await client.restart();
        assert.deepEqual(await outbox(), expected);
        assert.equal(calls.filter(call => call === "first dispatch").length, 1);
        assert.deepEqual(mock.sends, proven || received || lost ? [
            { task, sendNumber: proven ? 2 : 1, chatId: owner, messageThreadId: topic, text: scenario },
        ] : []);
    });
}

for (const scenario of ["valid", "duplicate", "mismatched nonce", "nonpositive ID", "stale send number"]) {
    test(`BigFur late receipt after expiry with ${scenario}`, timeout, async context => {
        const { root, mock, client, cutover, ingress, telegram, owner, topic } =
            await productionAuthorityFixture(context, { pairedProduction: true, holdReceipt: true });
        cutover.controlOnline = true;
        mock.publish(cutover.target, null);
        const alpha = await pairProduction(mock, client, cutover, "alpha");
        const { task } = await ingress(telegram(45295));
        const { attempt } = await client.agent("alpha", "claim", { task });
        const grant = await client.agent("alpha", "start", { attempt });
        const outbox = async () => (await client.sql(
            "SELECT phase,sends,message_id FROM outbox LEFT JOIN delivery_receipts USING(task) WHERE task=?", task,
        ))[0];
        const gate = Promise.withResolvers();
        const forward = cutover.fetch;
        let held = false;
        let finished = false;
        let dispatches = 0;
        cutover.fetch = async request => {
            const { pathname } = new URL(request.url);
            if (pathname === "/fixture-alarm-finished") {
                finished = true;
                return new Response(null, { status: 204 });
            }
            const delivery = pathname === "/v1/send" && (await request.clone().json()).delivery;
            if (!delivery || delivery.closeUnsent) return forward(request);
            dispatches++;
            const response = await forward(request);
            assert.equal(response.status, 200);
            const receipt = await response.json();
            cutover.controlOnline = false;
            held = true;
            await gate.promise;
            if (scenario === "mismatched nonce") receipt.nonce = "unrelated";
            if (scenario === "nonpositive ID") receipt.messageId = 0;
            return Response.json(receipt);
        };
        const stale = scenario === "stale send number";
        try {
            await reportProductionResult(client, grant, scenario, alpha.incarnation);
            await until(() => held, "Control committed a receipt and the proxy is holding it");
            assert.deepEqual(await outbox(), { phase: "in_flight", sends: 1, message_id: null });
            assert.equal((await client.dispatch("https://bigfur.liu.nz/fixture-expire")).status, 200);
            assert.deepEqual(await outbox(), { phase: "unknown", sends: 1, message_id: null });
            const control = new DatabaseSync(join(root, "control.sqlite"), { readOnly: true });
            try {
                assert.deepEqual(control.prepare("SELECT generation,task,number,message_id FROM sends").all()
                    .map(row => ({ ...row })), [{ generation: 1, task, number: 1, message_id: 1 }]);
            } finally {
                control.close();
            }
            if (stale) {
                assert.equal((await client.dispatch("https://bigfur.liu.nz/fixture-advance-send")).status, 204);
            }
            if (scenario === "duplicate") {
                assert.equal((await client.dispatch("https://bigfur.liu.nz/fixture-commit-receipt")).status, 204);
            }
            mock.retire();
            cutover.controlOnline = true;
            assert.equal(finished, false);
        } finally {
            gate.resolve();
        }
        const authorityCalls = cutover.authorityCalls;
        await until(() => finished, "original send alarm finished after receipt release");
        assert.equal(cutover.authorityCalls, authorityCalls);
        const expected = scenario === "valid" || scenario === "duplicate"
            ? { phase: "delivered", sends: 1, message_id: 1 }
            : { phase: "unknown", sends: stale ? 2 : 1, message_id: null };
        assert.deepEqual(await outbox(), expected);
        assert.deepEqual(await client.scheduledAlarm(), []);
        await client.restart();
        assert.deepEqual(await outbox(), expected);
        assert.deepEqual(await ingress(telegram(45295)), { disposition: "duplicate" });
        assert.deepEqual(await ingress(telegram(45296), 503), { error: "inactive_generation" });
        assert.equal(dispatches, 1);
        assert.deepEqual(mock.sends, [{ task, sendNumber: 1, chatId: owner, messageThreadId: topic, text: scenario }]);
    });
}

const legacyDelivery = process.env.GATEWAY_TEST_LEGACY_DELIVERY;
test("BigFur deliveryCheck version interoperability", { ...timeout, skip: !legacyDelivery }, async context => {
    const oldControl = legacyDelivery === "worker-first";
    const control = oldControl ? new URL(process.env.GATEWAY_TEST_LEGACY_CONTROL) : undefined;
    const { mock, client, cutover, ingress, telegram, owner, topic } = await productionAuthorityFixture(context, { control });
    const { task } = await ingress(telegram(45299));
    mock.publish(cutover.target, null, cutover.runner);
    cutover.controlOnline = true;
    const { attempt } = await client.agent("alpha", "claim", { task });
    const grant = await client.agent("alpha", "start", { attempt });
    await reportProductionResult(client, grant, "version skew");
    const outbox = () => client.sql("SELECT phase,sends FROM outbox WHERE task=?", task);
    if (oldControl) {
        await until(() => mock.checks.filter(check => check.deliveryCheck && check.status === 400).length >= 2,
            "old Control rejects the delivery check across alarms");
        assert.deepEqual(await outbox(), [{ phase: "pending", sends: 0 }]);
        assert.deepEqual(mock.sends, []);
    } else {
        await until(async () => (await outbox())[0].phase === "delivered", "old Worker receives a durable reply");
        assert.ok(mock.checks.every(check => !check.deliveryCheck));
        await client.restart();
        assert.deepEqual(await outbox(), [{ phase: "delivered", sends: 1 }]);
        assert.deepEqual(mock.sends, [{ task, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "version skew" }]);
    }
});

test("BigFur holds a second pending reply while Control finishes a timed-out send", timeout, async context => {
    const { mock, client, cutover, ingress, telegram, owner, topic } =
        await productionAuthorityFixture(context, { pairedProduction: true });
    cutover.controlOnline = true;
    mock.publish(cutover.target, null);
    const alpha = await pairProduction(mock, client, cutover, "alpha");
    const gate = Promise.withResolvers();
    mock.holdSend = ({ task }) => task === "telegram:45296" ? gate.promise : undefined;
    try {
        for (const id of [45296, 45297]) {
            const { task } = await ingress(telegram(id));
            const { attempt } = await client.agent("alpha", "claim", { task });
            const grant = await client.agent("alpha", "start", { attempt });
            await reportProductionResult(client, grant, `reply ${id}`, alpha.incarnation);
        }
        await until(async () => mock.checks.some(check => check.deliveryCheck && !check.allowed)
            && (await client.sql("SELECT phase FROM outbox WHERE task='telegram:45296'"))[0]?.phase === "unknown",
        "Control busy at alarm and expired attempt settles");
        assert.deepEqual(await client.sql("SELECT task,phase,sends FROM outbox ORDER BY task"), [
            { task: "telegram:45296", phase: "unknown", sends: 1 },
            { task: "telegram:45297", phase: "pending", sends: 0 },
        ]);
        assert.deepEqual(mock.sends.map(send => send.task), ["telegram:45296"]);
    } finally {
        gate.resolve();
    }
    await until(async () => (await client.sql("SELECT phase FROM outbox WHERE task='telegram:45297'"))[0]?.phase === "delivered",
        "second pending reply dispatched");
    assert.deepEqual(mock.sends, [45296, 45297].map(id => ({
        task: `telegram:${id}`, sendNumber: 1, chatId: owner, messageThreadId: topic, text: `reply ${id}`,
    })));
});

test("BigFur paired successor deduplicates a completed singleton update from the frozen predecessor", timeout, async context => {
    const { mock, client, cutover, ingress, telegram, cancelHeaders, owner, topic } =
        await productionAuthorityFixture(context);
    const redelivery = telegram(45294);
    const { task } = await ingress(redelivery);
    assert.equal(task, "telegram:45294");
    cutover.controlOnline = true;
    mock.publish(cutover.target, null, cutover.runner);
    const { attempt } = await client.agent("alpha", "claim", { task });
    const grant = await client.agent("alpha", "start", { attempt });
    await reportProductionResult(client, grant, "singleton reply");
    await until(() => mock.sends.length === 1, "singleton reply sent");
    mock.retire();
    await client.post("production/ingress-handoff", {}, webhook, 401);
    const handoff = await client.post("production/ingress-handoff", {}, cancelHeaders);
    assert.deepEqual(handoff, {
        generation: 1, objectId: cutover.target.pin.objectId, custodyDigest: cutover.target.pin.custodyDigest,
        updateIds: [45294],
    });
    assert.deepEqual(await ingress(redelivery), { disposition: "duplicate" });
    assert.deepEqual(await ingress(telegram(45295), 503), { error: "inactive_generation" });
    await client.restart();
    assert.deepEqual(await client.post("production/ingress-handoff", {}, cancelHeaders), handoff);
    assert.deepEqual(await ingress(telegram(45295), 503), { error: "inactive_generation" });

    const { objectId } = await client.post("production/identity", {}, { ...cancelHeaders, "x-cutover-generation": "2" });
    const { executorKey, ...previousPin } = cutover.target.pin;
    const next = {
        pin: { ...previousPin, generation: 2, objectId, custodyDigest: "c".repeat(64), pairing: "production-v1" },
        versionId: "paired-successor",
    };
    assert.throws(() => mock.publish(next, null));
    assert.throws(() => mock.publish(next, null, undefined, [{ ...handoff, objectId }]));
    assert.throws(() => mock.publish(next, null, undefined, [{ ...handoff, updateIds: [45294, 45294] }]));
    mock.publish(next, null, undefined, [handoff]);
    Object.assign(cutover, { target: next, pairedProduction: true, enrolled: {} });
    client.keys.alpha = generateKeyPairSync("ed25519");
    await client.restart();
    await pairProduction(mock, client, cutover, "alpha");
    assert.deepEqual(await ingress(redelivery), { disposition: "duplicate" });
    assert.deepEqual(await ingress(telegram(45295)), { disposition: "task", task: "telegram:45295" });
    assert.deepEqual(await client.sql("SELECT id,pin FROM tasks"), [{ id: "telegram:45295", pin: "alpha" }]);
    assert.equal((await client.agent("alpha", "next")).task, "telegram:45295");
    assert.deepEqual(mock.sends, [
        { task, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "singleton reply" },
    ]);
});

test("BigFur production pairing keeps immutable pins and fences retired writers across cold wake", timeout, async context => {
    const { root, mock, client, cutover, ingress, telegram, cancelHeaders, owner, topic } =
        await productionAuthorityFixture(context, { pairedProduction: true });
    const noAuthority = { error: "inactive_generation" };
    const ignored = { disposition: "ignored", task: null };
    const admit = (id, text, status = 200) => {
        const update = telegram(id);
        update.message.text = text;
        return ingress(update, status);
    };
    const status = task => client.post("status", { task }, cancelHeaders);
    const outbox = task => client.sql("SELECT phase,sends FROM outbox WHERE task=?", task);
    const begin = async (device, task) => {
        const { attempt, placement, message_thread_id } = await client.agent(device, "claim", { task });
        assert.equal(placement, device);
        assert.equal(message_thread_id, topic);
        const grant = await client.agent(device, "start", { attempt });
        assert.equal(grant.placement, device);
        return grant;
    };
    const report = (grant, event, status) => client.agent(grant.placement, "report", {
        proof: productionProof(client, grant, event, cutover.enrolled[grant.placement]),
    }, status);

    assert.deepEqual(await admit(45301, "@desk blocked", 503), noAuthority);
    cutover.controlOnline = true;
    mock.publish(cutover.target, null);
    const alpha = await pairProduction(mock, client, cutover, "alpha");
    const beta = await pairProduction(mock, client, cutover, "beta", "desk");
    const { placement, incarnation } = beta;
    const betaBinding = { placement, incarnation };
    const beforeGuards = cutover.authorityCalls;
    assert.deepEqual(await ingress(telegram(45301, topic + 1)), ignored);
    assert.deepEqual(await ingress(telegram(45301, topic, owner + 1)), ignored);
    assert.equal(cutover.authorityCalls, beforeGuards);

    const admission = await Promise.all([admit(45301, "@desk hello"), admit(45301, "@desk hello")]);
    assert.deepEqual(admission.map(reply => reply.disposition).sort(), ["duplicate", "task"]);
    const pinned = "telegram:45301";
    assert.deepEqual(await status(pinned), {
        placement: "beta", runner: "Offline runner", attempt_phase: null, outcome: null,
    });
    mock.rename({ ...betaBinding, alias: "studio" });
    assert.deepEqual(await admit(45301, "@desk hello"), { disposition: "duplicate" });
    assert.deepEqual(await admit(45302, "@desk hello"), ignored);
    const first = (await admit(45302, "plain text default")).task;
    const olderEvidence = (await admit(45303, "@studio will retire")).task;
    assert.equal((await client.agent("alpha", "next")).task, first);
    assert.equal((await client.agent("beta", "next")).task, pinned);
    assert.equal((await client.agent("alpha", "hello")).device, "alpha");
    const betaPacket = client.signed("beta", { op: "hello" });
    await client.wire({ ...betaPacket, headers: {
        ...betaPacket.headers, "x-executor-incarnation": alpha.incarnation,
    } }, 503);

    const firstGrant = await begin("alpha", first);
    await reportProductionResult(client, firstGrant, "default topic", alpha.incarnation);
    await until(async () => (await outbox(first))[0]?.phase === "delivered", "first paired production send");
    const betaGrant = await begin("beta", pinned);
    await reportProductionResult(client, betaGrant, "pinned topic", incarnation);
    const retiredGrant = await begin("beta", olderEvidence);
    const oldProof = productionProof(client, retiredGrant, { kind: "started" }, incarnation);
    const savedPacket = client.signed("beta", { op: "report", proof: oldProof });
    const pending = [{ phase: "pending", sends: 0 }];
    assert.deepEqual(await outbox(pinned), pending);
    await client.stop();
    const snapshot = join(root, "paired-predecessor");
    await client.snapshot(snapshot);
    const beforeWake = mock.checks.at(-1).objectIncarnation;
    await client.restart();
    assert.equal((await client.agent("alpha", "hello")).device, "alpha");
    assert.notEqual(mock.checks.at(-1).objectIncarnation, beforeWake);

    mock.retireRunner(betaBinding);
    assert.equal((await client.agent("beta", "hello")).reconcile_only, true);
    assert.equal((await client.agent("beta", "status", { attempt: retiredGrant.attempt })).reconcile_only, true);
    assert.equal((await client.agent("alpha", "hello")).reconcile_only, undefined);
    assert.deepEqual(await client.agent("beta", "start", { attempt: retiredGrant.attempt }, 503), noAuthority);
    assert.deepEqual(await client.agent("beta", "claim", { task: olderEvidence }, 503), noAuthority);
    const wrongDomain = productionProof(client, retiredGrant, { kind: "started" }, alpha.incarnation);
    await client.agent("beta", "report", { proof: wrongDomain }, 401);
    await report(retiredGrant, { kind: "started" });
    await report(retiredGrant, { kind: "result", text: "held" });
    assert.deepEqual(await status(olderEvidence), {
        placement: "beta", runner: "Offline runner", attempt_phase: "unknown", outcome: "Unknown attempt outcome",
    });
    assert.deepEqual(await outbox(olderEvidence), []);
    assert.deepEqual(await admit(45303, "@studio will retire"), { disposition: "duplicate" });
    assert.deepEqual(await admit(45304, "@studio retired"), ignored);
    await until(async () => (await outbox(pinned))[0]?.phase === "delivered", "paired cold wake sends pending result");
    const deliveries = [
        { task: first, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "default topic" },
        { task: pinned, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "pinned topic" },
    ];
    assert.deepEqual(mock.sends, deliveries);

    const predecessor = { ...cutover };
    const { objectId } = await client.post("production/identity", {}, { ...cancelHeaders, "x-cutover-generation": "2" });
    const successor = {
        pin: { ...cutover.target.pin, generation: 2, objectId, custodyDigest: "c".repeat(64) },
        versionId: "paired-successor",
    };
    assert.throws(() => mock.publish(successor, null));
    mock.retire();
    const handoff = await client.post("production/ingress-handoff", {}, cancelHeaders);
    assert.throws(() => mock.publish({ ...successor, pin: { ...successor.pin, objectId: predecessor.target.pin.objectId } },
        null, undefined, [handoff]));
    mock.publish(successor, null, undefined, [handoff]);
    const restored = await runFixture(context, { inspect: true, restore: snapshot, cutover: predecessor, keys: client.keys });
    await restored.agent("beta", "next", {}, 503);
    await restored.agent("alpha", "start", { attempt: firstGrant.attempt }, 503);
    assert.equal((await restored.wire(savedPacket, 200)).event, "started");
    assert.deepEqual(await restored.wire(savedPacket, 409), { error: "replay" });
    await until(() => mock.checks.some(check => check.target.pin.generation === 1 &&
        check.runnerIncarnation === null && !check.allowed), "restored predecessor alarm reaches Control");
    assert.deepEqual(await restored.sql("SELECT phase,sends FROM outbox WHERE task=?", pinned), pending);
    assert.deepEqual(mock.sends, deliveries);

    cutover.target = successor;
    cutover.enrolled = {};
    await client.restart();
    const oldKey = client.keys.alpha.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    assert.equal((await redeemProduction(mock, client.keys.alpha, mock.issue({ key: oldKey })))[0], 409);
    await client.wire(betaPacket, 503);
    client.keys.alpha = generateKeyPairSync("ed25519");
    await pairProduction(mock, client, cutover, "alpha");
    assert.deepEqual(await admit(45301, "@desk hello"), { disposition: "duplicate" });
    assert.equal((await admit(45305, "fresh default")).disposition, "task");
    assert.equal((await client.agent("alpha", "next")).task, "telegram:45305");
});

test("BigFur retirement at the Start permit boundary marks recently seen alpha Offline", timeout, async context => {
    const { mock, client, cutover, ingress, telegram, cancelHeaders, owner, topic } =
        await productionAuthorityFixture(context, { pairedProduction: true });
    cutover.controlOnline = true;
    mock.publish(cutover.target, null);
    const alpha = await pairProduction(mock, client, cutover, "alpha");
    const beta = await pairProduction(mock, client, cutover, "beta", "desk");
    const { task } = await ingress(telegram(45311));
    const { attempt } = await client.agent("alpha", "claim", { task });
    const status = () => client.post("status", { task }, cancelHeaders);
    assert.equal((await status()).runner, "Online runner");

    let checks = 0;
    let originalDeadline;
    mock.beforeReply = permit => {
        if (permit.placement === "alpha" && ++checks === 2) {
            originalDeadline = permit.validUntil;
            mock.retireRunner({ placement: "alpha", incarnation: alpha.incarnation });
        }
    };
    assert.deepEqual(await client.agent("alpha", "start", { attempt }, 503), { error: "inactive_generation" });
    mock.beforeReply = undefined;
    assert.equal(checks, 3);
    const attemptRow = () => client.sql("SELECT phase,start_until FROM attempts WHERE id=?", attempt);
    assert.deepEqual(await attemptRow(), [{ phase: "authorized", start_until: originalDeadline }]);
    assert.deepEqual(await status(), {
        placement: "alpha", runner: "Offline runner", attempt_phase: "authorized", outcome: null,
    });
    assert.equal((await client.agent("alpha", "hello")).reconcile_only, true);
    await client.agent("alpha", "start", { attempt }, 503);
    assert.deepEqual(await attemptRow(), [{ phase: "authorized", start_until: originalDeadline }]);
    const alphaBinding = { placement: "alpha", instance: `bigfur:1:${alpha.objectId}`, attempt };
    for (const event of [{ kind: "started" }, { kind: "result", text: "retired alpha" }]) {
        const proof = productionProof(client, alphaBinding, event, alpha.incarnation);
        assert.equal((await client.agent("alpha", "report", { proof })).event, event.kind);
    }
    assert.deepEqual(await status(), {
        placement: "alpha", runner: "Offline runner", attempt_phase: "unknown", outcome: "Unknown attempt outcome",
    });
    assert.deepEqual(await client.sql("SELECT * FROM outbox"), []);
    assert.deepEqual(await ingress(telegram(45312)), { disposition: "ignored", task: null });
    const routed = telegram(45312);
    routed.message.text = "@desk sibling stays routable";
    const { task: sibling } = await ingress(routed);
    assert.equal((await client.agent("beta", "next")).task, sibling);
    const claim = await client.agent("beta", "claim", { task: sibling });
    const grant = await client.agent("beta", "start", { attempt: claim.attempt });
    assert.equal(grant.placement, "beta");
    await reportProductionResult(client, grant, "beta reply", beta.incarnation);
    await until(() => mock.sends.length === 1, "active sibling sends");
    assert.deepEqual(mock.sends, [
        { task: sibling, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "beta reply" },
    ]);
});

test("BigFur native production runner passes the local Codex sandbox mode", nativeTimeout, async context => {
    const { mock, client, cutover, ingress, telegram } =
        await productionAuthorityFixture(context, { pairedProduction: true });
    cutover.controlOnline = true;
    mock.publish(cutover.target, null);
    const enrollment = await pairProduction(mock, client, cutover, "alpha");
    const runner = await attachProductionRunner(context, client, cutover, mock, ingress);
    const stageFile = join(runner.workspace, "exec-stage");
    await writeFile(join(runner.workspace, "exec"), `
import { writeFileSync } from "node:fs";
const args = process.argv.slice(2);
writeFileSync("exec-stage", "script entered");
process.stdin.on("end", () => {
    writeFileSync("exec-stage", "stdin ended");
    writeFileSync(args[args.indexOf("--output-last-message") + 1], JSON.stringify(args));
    writeFileSync("exec-stage", "reply written");
});
process.stdin.resume();
`);
    const replies = new Map();
    runner.proxy.transform = async ({ response, envelope }) => {
        const { phase, event, error } = response.value ?? {};
        replies.set(envelope, { http: response.status, phase, event, error });
        return response;
    };
    const config = runner.configure(process.execPath);
    const options = { enrollment, env: windowsCustody ? { SystemRoot: process.env.SystemRoot } : {} };
    for (const codex_sandbox of ["full-access", null, true, { "danger-full-access": null }]) {
        const invalid = await runner.runProduction("init", { ...config, codex_sandbox }, options);
        const rejected = await Promise.race([
            invalid.exit(1),
            sleep(5000, undefined, { ref: false }).then(() => assert.fail("invalid runner configuration did not exit")),
        ]);
        assert.match(rejected, /invalid private runner configuration/);
    }
    assert.deepEqual(runner.packets, []);
    await assert.rejects(readdir(runner.dir), { code: "ENOENT" });

    for (const [index, codex_sandbox] of [undefined, "danger-full-access", "workspace-write"].entries()) {
        const attempt = index + 1;
        await rm(stageFile, { force: true });
        assert.equal((await ingress(telegram(45410 + index))).disposition, "task");
        const mode = index === 0 ? "init" : "resume";
        const packetStart = runner.packets.length;
        const running = await runner.runProduction(mode, { ...config, codex_sandbox }, options);
        let result;
        try {
            result = await runner.receipt(attempt, "result");
        } catch (error) {
            const packets = runner.packets.slice(packetStart);
            const operations = packets.slice(-16).map(envelope => {
                const { op, attempt: requestAttempt, proof } = envelope.command;
                const report = proof && JSON.parse(proof.payload);
                return { op, attempt: requestAttempt ?? report?.attempt, proof: report?.event?.kind, ...replies.get(envelope) };
            });
            let journal;
            try {
                journal = runner.query(`SELECT attempts.phase, evidence.event, evidence.acknowledged
                    FROM attempts LEFT JOIN evidence USING(attempt) WHERE attempt=? ORDER BY evidence.id`, attempt);
            } catch (error) {
                journal = error.message.slice(0, 256);
            }
            const stage = await readFile(stageFile, "utf8").catch(error => error.code);
            context.diagnostic(`Production launch ${JSON.stringify({
                mode, attempt, sandbox: codex_sandbox ?? "default", pid: running.pid,
                ...running.exitStatus, stderr: running.stderr.trim().slice(-1024),
                stage, journal, packetCount: packets.length, operations,
            })}`);
            throw error;
        }
        const args = JSON.parse(result.text);
        assert.deepEqual(args, [
            "--skip-git-repo-check", "--ephemeral", "--sandbox", codex_sandbox ?? "workspace-write",
            "-c", 'approval_policy="never"', "-c", 'shell_environment_policy.inherit="none"',
            "--output-last-message", args.at(-2), "-",
        ]);
        await running.stop();
    }
});

test("BigFur native production runner binds the receipt and reconciles without repeating effects", {
    ...nativeTimeout, skip: !executable || !["darwin", "linux"].includes(process.platform),
}, async context => {
    const { mock, client, cutover, ingress, telegram, owner, topic } =
        await productionAuthorityFixture(context, { pairedProduction: true });
    cutover.controlOnline = true;
    mock.publish(cutover.target, null);
    const alpha = await pairProduction(mock, client, cutover, "alpha");
    const runner = await attachProductionRunner(context, client, cutover, mock, ingress, "beta");
    runner.proxy.pairingEndpoint = mock.endpoint;
    const codeFile = join(runner.root, "code");
    const betaKey = client.keys.beta.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    const pairingCode = mock.issue({ key: betaKey, alias: "desk" });
    await writeFile(codeFile, pairingCode, { mode: 0o600 });
    const { stdout } = await promisify(execFile)(process.execPath, [
        join(import.meta.dirname, "pair-runner-local.mjs"), "redeem-production", runner.origin, runner.seed, codeFile,
    ], { env: { NODE_EXTRA_CA_CERTS: runner.ca }, timeout: 5000 });
    const beta = JSON.parse(stdout);
    assert.equal(beta.placement, "beta");
    assert.equal(beta.generation, cutover.target.pin.generation);
    assert.equal(beta.objectId, cutover.target.pin.objectId);
    cutover.enrolled.beta = beta.incarnation;
    assert.deepEqual(await redeemProduction(mock, client.keys.beta, pairingCode), [200, beta]);
    const codex = join(runner.root, "codex-test");
    await writeFile(codex, `#!/bin/sh
while [ "$#" -gt 1 ]; do
    if [ "$1" = "--output-last-message" ]; then output=$2; shift; fi
    shift
done
prompt=$(cat)
case "$prompt" in
    hold*)
        /bin/sleep 60 &
        printf '%s %s\\n' "$$" "$!" >> observed
        wait ;;
    *)
        printf '%s\\n' "$$" >> observed ;;
esac
printf '%s' "$BIGFUR_SYNTHETIC_REPLY" > "$output"
`, { mode: 0o700 });
    const observed = count => until(async () => {
        const rows = (await readFile(join(runner.workspace, "observed"), "utf8").catch(error => {
            if (error.code !== "ENOENT") throw error;
            return "";
        })).trim().split("\n").filter(Boolean);
        return rows.length >= count && rows.map(row => row.split(" ").map(Number));
    }, `observed ${count} production starts`);
    const exited = pids => until(() => pids.every(pid => {
        try { process.kill(pid, 0); return false; } catch (error) { return error.code === "ESRCH"; }
    }), "contained production PIDs exit");
    for (const [id, text] of [[45401, "wrong placement"], [45402, "lost Start"],
        [45403, "cancel before spawn"], [45404, "result"], [45405, "hold and cancel"],
        [45406, "hold and lose runner"]]) {
        const update = telegram(id);
        update.message.text = `@desk ${text}`;
        assert.equal((await ingress(update)).disposition, "task");
    }
    const config = { ...runner.configure(codex), provider_env: ["BIGFUR_SYNTHETIC_REPLY"] };
    const env = { BIGFUR_SYNTHETIC_REPLY: "synthetic production reply" };
    const options = { enrollment: beta, env };
    let lostResult;
    let attemptedBetaSignature;
    runner.proxy.transform = async ({ response, envelope, packet }) => {
        if (response.status !== 200) return response;
        const { op, attempt, proof } = envelope.command;
        if (op === "start") {
            if (attempt === 1) response.value.placement = "alpha";
            if (attempt === 2) return undefined;
            if (attempt === 3) await runner.cancel(45403);
        }
        if (op === "report" && JSON.parse(proof.payload).event.kind === "result") {
            if (!lostResult) {
                lostResult = proof;
                attemptedBetaSignature = packet;
                return undefined;
            }
            assert.deepEqual(proof, lostResult);
        }
        return response;
    };
    const running = await runner.runProduction("init", config, options);
    assert.deepEqual(await runner.receipt(1, "unknown"), { kind: "unknown" });
    assert.deepEqual(await runner.receipt(2, "unknown"), { kind: "unknown" });
    assert.deepEqual(await runner.receipt(3, "cancelled"), { kind: "cancelled", evidence: "before_start" });
    assert.deepEqual(await runner.receipt(4, "result"), { kind: "result", text: env.BIGFUR_SYNTHETIC_REPLY });
    assert.equal(runner.proofs("result").length, 2);
    await client.wire({ ...attemptedBetaSignature, headers: {
        ...attemptedBetaSignature.headers, "x-executor-placement": "alpha", "x-executor-incarnation": alpha.incarnation,
    } }, 401);
    const [, cancelled] = await observed(2);
    assert.equal(cancelled.length, 2);
    await runner.cancel(45405);
    assert.deepEqual(await runner.receipt(5, "cancelled"), { kind: "cancelled", evidence: "process_exited" });
    await exited(cancelled);
    const [, , abandoned] = await observed(3);
    assert.equal(abandoned.length, 2);
    const abandonedTree = process.platform === "linux" ? await liveLinuxTree(running, { detached: false }) : null;
    if (abandonedTree) assert.deepEqual(abandonedTree.provider, abandoned);
    assert.deepEqual(await running.stop(), [null, "SIGKILL"]);
    if (abandonedTree) await linuxTreeExited(abandonedTree, "runner SIGKILL");
    else await exited(abandoned);

    const restored = join(runner.root, "copied-journal");
    await cp(runner.dir, restored, { recursive: true });
    const packetCount = runner.packets.length;
    const copied = await runner.runProduction("resume", config, { ...options, journal: restored });
    const copyRejected = await Promise.race([
        copied.exit(1),
        sleep(5000, undefined, { ref: false }).then(() => assert.fail("copied journal continued past admission")),
    ]);
    assert.match(copyRejected, /journal identity mismatch/);
    assert.match(await (await runner.runProduction("resume", config, { ...options, enrollment: alpha })).exit(1),
        /journal identity mismatch/);
    assert.equal(runner.packets.length, packetCount);
    mock.retireRunner({ placement: beta.placement, incarnation: beta.incarnation });
    const reconciling = await runner.runProduction("resume", config, options);
    await reconciling.exit(0);
    assert.deepEqual(await runner.receipt(6, "unknown"), { kind: "unknown" });
    const reconciliation = runner.packets.slice(packetCount).map(({ command }) => command);
    assert.equal(reconciliation.shift().op, "hello");
    assert.ok(reconciliation.every(command => command.op === "report"));
    assert.deepEqual(reconciliation.map(({ proof }) => JSON.parse(proof.payload).event.kind),
        process.platform === "linux" ? ["started", "unknown"] : ["unknown"]);
    assert.deepEqual(runner.packets.filter(({ command }) => command.op === "start")
        .map(({ device, command }) => [device, command.attempt]), [1, 2, 3, 4, 5, 6].map(attempt => ["beta", attempt]));
    assert.equal((await observed(3)).length, 3);
    await until(() => mock.sends.length === 1, "owned synthetic send");
    assert.deepEqual(mock.sends, [{
        task: "telegram:45404", sendNumber: 1, chatId: owner, messageThreadId: topic, text: env.BIGFUR_SYNTHETIC_REPLY,
    }]);
    assert.deepEqual(await client.sql("SELECT id,phase FROM attempts ORDER BY id"), [
        { id: 1, phase: "unknown" }, { id: 2, phase: "unknown" }, { id: 3, phase: "cancelled_before_start" },
        { id: 4, phase: "completed" }, { id: 5, phase: "cancelled_after_exit" }, { id: 6, phase: "unknown" },
    ]);
});

test("BigFur fresh install pairs native runners and keeps the default, offline pins, and Unknown", {
    ...nativeTimeout, skip: !executable || !cutoverCli,
}, async context => {
    const { root, mock, client, cutover, ingress, telegram, cancelHeaders, owner, topic } =
        await productionAuthorityFixture(context, { pairing: true, enrolled: {} });
    const update = (id, text) => {
        const value = telegram(id);
        value.message.text = text;
        return value;
    };
    const firstUpdate = update(45200, "first runner synthetic");
    cutover.controlOnline = true;
    await ingress(firstUpdate, 503);
    const objectIncarnation = mock.checks.at(-1).objectIncarnation;
    mock.publish(cutover.target, objectIncarnation);
    assert.equal((await ingress(firstUpdate)).disposition, "ignored");
    assert.deepEqual(mock.sends, []);

    const cli = join(import.meta.dirname, "pair-runner-local.mjs");
    const command = async (program, ...args) => JSON.parse((await promisify(execFile)(program, args, {
        env: {}, timeout: 5000,
    })).stdout);
    async function pair(device, alias) {
        const data = join(root, device);
        await mkdir(data, { mode: 0o700 });
        if (windowsCustody) makeWindowsPrivate(data);
        const workspace = join(data, "workspace");
        await mkdir(workspace, { mode: 0o700 });
        const card = await command(cutoverCli, "--data-dir", data, "nodes", "enroll", "--role", "runner",
            "--bind", "127.0.0.1:49731", "--workspace", workspace);
        assert.equal(card.role, "runner");
        const seed = join(data, "runtime", "nodes", "device-key");
        const codeFile = join(data, "pairing-code");
        await writeFile(codeFile, mock.issue({ key: card.id, alias }), { mode: 0o600 });
        const receipt = await command(process.execPath, cli, "redeem", mock.endpoint, seed, codeFile);
        assert.equal(receipt.placement, device);
        assert.equal(receipt.generation, cutover.target.pin.generation);
        assert.equal(receipt.objectId, cutover.target.pin.objectId);
        cutover.enrolled[device] = receipt.incarnation;
        client.keys[device] = runnerKey(await readFile(seed));
        const runner = await attachExecutor(context, client, cutover, device, seed);
        return { ...runner, runEnrolled: (mode, behavior) => runner.runEnrolled(mode, behavior, receipt) };
    }
    const alpha = await pair("alpha");
    const first = (await ingress(firstUpdate)).task;
    assert.equal(first, "telegram:45200");
    const firstRunner = alpha.runEnrolled("init");
    async function delivered(task) {
        return until(async () => (await client.sql(
            "SELECT message_id FROM delivery_receipts WHERE task=?", task,
        ))[0]?.message_id, `settled reply for ${task}`);
    }
    assert.equal(await delivered(first), 1);
    await until(() => alpha.query("SELECT acknowledged FROM evidence WHERE event='result'")[0]?.acknowledged,
        "alpha journal has acknowledged result");
    await firstRunner.stop();
    const [alphaAttempt] = await alpha.launches();
    const settledEvidence = () => alpha.query(
        "SELECT payload,signature,acknowledged FROM evidence WHERE event='result' AND attempt=?", alphaAttempt,
    );
    const originalEvidence = settledEvidence();
    const pending = (await ingress(update(45201, "already queued for alpha"))).task;
    assert.equal((await client.agent("alpha", "next")).task, pending);

    const beta = await pair("beta", "desk");
    assert.equal((await client.agent("alpha", "status", { attempt: alphaAttempt })).phase, "completed");
    assert.equal((await client.agent("alpha", "next")).task, pending);
    assert.equal(await delivered(first), 1);
    assert.deepEqual(settledEvidence(), originalEvidence);
    assert.equal((await ingress(update(45202, "@missing synthetic"))).disposition, "ignored");
    assert.equal((await ingress(update(45200, "@desk duplicate"))).disposition, "duplicate");

    const betaHello = client.signed("beta", { op: "hello" });
    const wrongSigner = client.signed("beta", { op: "hello" });
    const prefix = `coconutclaw-enrolled-request-v1\nbigfur:1:${cutover.target.pin.objectId}\nbeta\n${cutover.enrolled.beta}\nPOST\n/v1/agent\n`;
    wrongSigner.headers["x-agent-signature"] = sign(null, Buffer.from(prefix + wrongSigner.body),
        client.keys.alpha.privateKey).toString("hex");
    await client.wire(wrongSigner, 401);
    let checks = 0;
    mock.beforeReply = permit => permit.placement === "beta" && ++checks === 2
        ? Response.json({ ...permit, key: "1".repeat(64) }) : undefined;
    await client.wire(betaHello, 503);
    mock.beforeReply = permit => permit.routes ? Response.json({
        ...permit, routes: [permit.routes[0], { placement: "beta", alias: "bad alias" }],
    }) : undefined;
    const secondUpdate = update(45203, "@desk second runner synthetic");
    await ingress(secondUpdate, 503);
    mock.beforeReply = undefined;
    const second = (await ingress(secondUpdate)).task;
    assert.equal(second, "telegram:45203");
    const status = (task, code = 200) => client.post("status", { task }, cancelHeaders, code);
    assert.deepEqual(await status(second), {
        placement: "beta", runner: "Offline runner", attempt_phase: null, outcome: null,
    });
    assert.equal((await status(pending)).runner, "Online runner");
    assert.equal((await client.agent("alpha", "next")).task, pending);
    const alphaClaim = client.signed("alpha", { op: "claim", task: second }, { seq: 1000 });
    assert.equal((await client.wire(alphaClaim, 409)).error, "state_conflict");
    assert.equal(mock.sends.length, 1);

    const betaRunner = beta.runEnrolled("init");
    assert.equal(await delivered(second), 2);
    await until(() => beta.query("SELECT acknowledged FROM evidence WHERE event='result'")[0]?.acknowledged,
        "beta journal has acknowledged result");
    await betaRunner.stop();
    assert.deepEqual(mock.sends, [
        { task: first, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "fixture output: first runner synthetic" },
        { task: second, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "fixture output: second runner synthetic" },
    ]);

    const alphaResumed = alpha.runEnrolled("resume");
    assert.equal(await delivered(pending), 3);
    await alphaResumed.stop();
    const stillDefault = (await ingress(update(45204, "still the default runner"))).task;
    assert.equal((await status(stillDefault)).placement, "alpha");
    assert.equal((await alpha.launches())[0], alphaAttempt);
    assert.deepEqual(settledEvidence(), originalEvidence);
    assert.equal(mock.sends.filter(send => send.task === first).length, 1);

    const lost = (await ingress(update(45205, "@desk synthetic interrupted child"))).task;
    const interrupted = beta.runEnrolled("resume", "hold");
    const linuxTree = linuxCustody ? await liveLinuxTree(interrupted) : undefined;
    const attempt = await until(async () => (await beta.launches())[1], "interrupted child launched");
    if (linuxTree) {
        assert.equal(beta.query("SELECT phase FROM attempts WHERE attempt=?", attempt)[0].phase, "consumed");
    }
    assert.deepEqual(await interrupted.stop(), [null, "SIGKILL"]);
    if (linuxTree) {
        await linuxTreeExited(linuxTree, "runner SIGKILL");
        context.diagnostic(`Runner ${interrupted.pid} killed and Linux tree exited before resume ${JSON.stringify(linuxTree)}`);
    }
    const recoverAt = beta.packets.length;
    const betaResumed = beta.runEnrolled("resume");
    const unknown = await until(async () => {
        if (linuxCustody) betaResumed.assertRunning();
        const state = await status(lost);
        return state.outcome && state;
    }, "operator sees Unknown after native recovery");
    assert.deepEqual(unknown, {
        placement: "beta", runner: "Online runner", attempt_phase: "unknown", outcome: "Unknown attempt outcome",
    });
    const [unknownPacket] = beta.proofs("unknown");
    const proof = unknownPacket.command.proof;
    assert.deepEqual(beta.verifyProof(proof), { attempt, event: { kind: "unknown" } });
    await until(() => beta.query("SELECT acknowledged FROM evidence WHERE attempt=? AND event='unknown'", attempt)[0]?.acknowledged
        && beta.packets.slice(recoverAt).some(({ command }) => command.op === "next"), "native recovery acknowledges and polls");
    if (linuxCustody) {
        assert.deepEqual(beta.query("SELECT payload,signature FROM evidence WHERE attempt=? AND event='unknown'", attempt)
            .map(row => ({ ...row })), [proof]);
    }
    assert.equal((await client.agent("beta", "next")).task, null);
    assert.equal((await beta.launches()).filter(id => id === attempt).length, 1);
    assert.equal(beta.packets.filter(({ command }) => command.op === "start" && command.attempt === attempt).length, 1);
    assert.equal(mock.sends.some(send => send.task === lost), false);
    await betaResumed.stop();
    assert.ok(mock.checks.every(check => check.objectIncarnation === objectIncarnation));
    context.diagnostic(`Alpha ${first} and beta ${second} settled in mock chat ${owner}, topic ${topic}.`);
    context.diagnostic(`Offline beta stayed pinned. Attempt ${attempt} became Unknown after one launch and sent no reply.`);
});

test("native enrolled runners keep separate journals and replay the original result after restart", nativeTimeout, async context => {
    const { mock, client, cutover, admit, owner, topic } = await nativePlacement(context);
    const alpha = await attachExecutor(context, client, cutover, "alpha");
    const beta = await attachExecutor(context, client, cutover, "beta");
    const alphaTask = await admit(45110, "alpha synthetic");
    const betaTask = await admit(45111, "@mars beta synthetic");
    beta.proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op === "report" && JSON.parse(envelope.command.proof.payload).event.kind === "result") {
            assert.equal(response.status, 200);
            beta.proxy.offline = true;
            return null;
        }
        return response;
    };
    const alphaRunner = alpha.runEnrolled("init");
    const betaRunner = beta.runEnrolled("init");
    await until(() => beta.proxy.offline && mock.sends.some(send => send.task === alphaTask), "both runners finish");
    await Promise.all([alphaRunner.stop(), betaRunner.stop()]);
    const [saved] = beta.proofs("result");
    const original = saved.command.proof;
    const { attempt } = beta.verifyProof(original);
    assert.equal(beta.query("SELECT acknowledged FROM evidence WHERE event='result'")[0].acknowledged, 0);
    assert.deepEqual(await beta.launches(), [attempt]);

    const before = beta.packets.length;
    for (const options of [
        { placement: "alpha" }, { incarnation: cutover.enrolled.alpha },
        { generation: 2 }, { objectId: "f".repeat(64) }, { key: alpha.seed },
    ]) {
        assert.match(await beta.runEnrolled("resume", undefined, options).exit(1), /journal identity mismatch/);
    }
    if (!windowsCustody) {
        assert.match(await beta.run("resume").exit(1), /generation must advance exactly once/);
    }
    assert.equal(beta.packets.length, before);
    assert.ok(beta.packets.every(({ device }) => device === "beta"));
    if (!windowsCustody) {
        const legacy = join(beta.root, "legacy");
        const legacyRunner = beta.run("init", undefined, undefined, 1, { journal: legacy });
        await until(() => beta.packets.length > before, "legacy journal initialized");
        await legacyRunner.stop();
        const afterLegacy = beta.packets.length;
        assert.match(await beta.runEnrolled("resume", undefined, { journal: legacy }).exit(1), /journal identity mismatch/);
        assert.equal(beta.packets.length, afterLegacy);
    }

    beta.proxy.offline = false;
    beta.proxy.transform = async ({ response }) => response;
    const resumed = beta.runEnrolled("resume");
    await until(() => beta.query("SELECT acknowledged FROM evidence WHERE event='result'")[0].acknowledged
        && mock.sends.length === 2, "original evidence acknowledged");
    await resumed.stop();
    assert.deepEqual(beta.proofs("result").map(packet => packet.command.proof), [original, original]);
    assert.deepEqual(await beta.launches(), [attempt]);
    assert.equal((await alpha.launches()).length, 1);
    assert.deepEqual(mock.sends.toSorted((left, right) => left.task.localeCompare(right.task)), [
        { task: alphaTask, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "fixture output: alpha synthetic" },
        { task: betaTask, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "fixture output: beta synthetic" },
    ]);
});

test("native enrolled runner rejects mismatched Hello, claim and Start before consumption", nativeTimeout, async context => {
    const { client, cutover, admit, cancelHeaders, mock } = await nativePlacement(context);
    const beta = await attachExecutor(context, client, cutover, "beta");
    const rejectedClaim = await admit(45112, "@mars synthetic claim");
    beta.proxy.transform = async ({ response, envelope }) => envelope.command.op === "hello"
        ? { ...response, value: { ...response.value, device: "alpha" } } : response;
    assert.match(await beta.runEnrolled("init").exit(1), /unexpected gateway identity/);
    assert.deepEqual(beta.packets.map(packet => packet.command.op), ["hello"]);

    beta.proxy.transform = async ({ response, envelope }) => envelope.command.op === "claim"
        ? { ...response, value: { ...response.value, placement: "alpha" } } : response;
    assert.match(await beta.runEnrolled("resume").exit(1), /claim lacks the enrolled runner placement/);
    assert.deepEqual(beta.query("SELECT attempt FROM attempts"), []);
    assert.equal(beta.packets.some(packet => packet.command.op === "start"), false);
    assert.equal((await client.post("cancel", { task: rejectedClaim }, cancelHeaders)).phase, "cancelled_before_start");

    for (const [index, placement] of ["alpha", undefined, "lost"].entries()) {
        const task = await admit(45113 + index, "@mars synthetic Start");
        let attempt;
        beta.proxy.transform = async ({ response, envelope }) => {
            if (envelope.command.op !== "start") return response;
            assert.equal(response.status, 200);
            assert.equal(response.value.placement, "beta");
            attempt = response.value.attempt;
            if (placement === "lost") {
                beta.proxy.offline = true;
                return null;
            }
            return { ...response, value: { ...response.value, placement } };
        };
        let runner = beta.runEnrolled("resume");
        if (placement === "lost") {
            await until(() => beta.proxy.offline, "Start acknowledgement lost after authorization");
            await runner.stop();
            assert.equal(beta.query("SELECT phase FROM attempts WHERE attempt=?", attempt)[0].phase, "intent");
            beta.proxy.offline = false;
            runner = beta.runEnrolled("resume");
        }
        await until(async () => {
            assert.deepEqual(await beta.launches(), [], "rejected grant must never spawn");
            return attempt && beta.query(
                "SELECT acknowledged FROM evidence WHERE attempt=? AND event='unknown'", attempt
            )[0]?.acknowledged;
        }, "unconsumed Start reconciled as Unknown");
        await runner.stop();
        assert.equal((await client.agent("beta", "status", { attempt })).phase, "unknown");
        assert.equal((await client.agent("beta", "next")).task, null);
        await client.agent("beta", "claim", { task }, 409);
        assert.equal(beta.packets.filter(packet => packet.command.op === "start" && packet.command.attempt === attempt).length, 1);
    }
    assert.deepEqual(await beta.launches(), []);
    assert.equal(beta.proofs("unknown").length, 3);
    assert.deepEqual(mock.sends, []);
});

test("native enrolled runner reconciles consumed grants and reports both kinds of cancellation", nativeTimeout, async context => {
    const { client, cutover, admit, cancelHeaders, mock } = await nativePlacement(context);
    const beta = await attachExecutor(context, client, cutover, "beta");
    await admit(45116, "@mars synthetic crash");
    await beta.runEnrolled("init", "crash-before-spawn").exit(72);
    const [{ attempt, phase }] = beta.query("SELECT attempt,phase FROM attempts");
    assert.equal(phase, "consumed");
    assert.deepEqual(await beta.launches(), []);

    const before = await admit(45117, "@mars synthetic cancellation before Start");
    beta.proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op === "start" && response.status === 200) {
            assert.equal((await client.post("cancel", { task: before }, cancelHeaders)).phase, "cancel_requested");
        }
        return response;
    };
    const resumed = beta.runEnrolled("resume");
    await until(() => beta.query("SELECT event FROM evidence WHERE acknowledged=1").length === 2,
        "Unknown and before-start cancellation acknowledged");
    await resumed.stop();
    assert.equal((await client.agent("beta", "status", { attempt })).phase, "unknown");
    assert.deepEqual(await beta.launches(), []);
    const [beforeProof] = beta.proofs("cancelled").map(({ command }) => beta.verifyProof(command.proof));
    assert.equal((await client.agent("beta", "status", { attempt: beforeProof.attempt })).phase, "cancelled_before_start");
    assert.equal(beforeProof.event.evidence, "before_start");

    beta.proxy.transform = async ({ response }) => response;
    const running = await admit(45118, "@mars synthetic cancellation after spawn");
    const held = beta.runEnrolled("resume", "hold");
    const started = await until(async () => (await beta.launches())[0], "synthetic child starts");
    assert.equal((await client.post("cancel", { task: running }, cancelHeaders)).phase, "cancel_requested");
    await until(() => beta.query(
        "SELECT acknowledged FROM evidence WHERE attempt=? AND event='cancelled'", started
    )[0]?.acknowledged, "exit cancellation acknowledged");
    await held.stop();
    const afterProof = beta.verifyProof(beta.proofs("cancelled").at(-1).command.proof);
    assert.deepEqual(afterProof, { attempt: started, event: { kind: "cancelled", evidence: "process_exited" } });
    assert.equal((await client.agent("beta", "status", { attempt: started })).phase, "cancelled_after_exit");
    assert.deepEqual(await beta.launches(), [started]);
    assert.deepEqual(mock.sends, []);
});

test("native enrolled Windows rejects unsafe ACLs and reparse points before opening state", {
    ...nativeTimeout, skip: !executable || !windowsCustody,
}, async context => {
    const { client, cutover, admit, mock } = await nativePlacement(context);
    const beta = await attachExecutor(context, client, cutover, "beta");
    const grant = path => execFileSync("icacls.exe", [path, "/grant", "*S-1-1-0:R"]);
    const remove = path => execFileSync("icacls.exe", [path, "/remove:g", "*S-1-1-0"]);
    assert.match(await beta.run("init").exit(1), /requires an explicit enrollment/);
    const unsafeParent = join(beta.root, "unsafe");
    await mkdir(unsafeParent);
    grant(unsafeParent);
    assert.match(await beta.runEnrolled("init", undefined, { journal: join(unsafeParent, "journal") }).exit(1),
        /unsafe node ACL/);
    assert.deepEqual(await readdir(unsafeParent), []);
    for (const [path, custodyError] of [
        [beta.seed, /check fixture seed custody[\s\S]*unsafe node ACL/],
        [join(beta.root, "ca.pem"), /check fixture CA custody[\s\S]*unsafe node ACL/],
    ]) {
        grant(path);
        assert.match(await beta.runEnrolled("init").exit(1), custodyError);
        remove(path);
    }
    assert.deepEqual(beta.packets, []);

    assert.equal(await admit(45121, "synthetic journal activation"), "telegram:45121");
    let helloStatus;
    let helloInactive;
    let next;
    beta.proxy.transform = async ({ response, envelope }) => {
        if (envelope.command.op === "hello") {
            helloStatus = response.status;
            helloInactive = response.value?.error === "inactive_generation";
        }
        if (envelope.command.op === "next") {
            next = { status: response.status, idle: response.value?.task === null };
        }
        return response;
    };
    const owner = beta.runEnrolled("init");
    try {
        await until(() => {
            owner.assertRunning();
            return next?.status === 200 && next.idle;
        }, () => `private journal is admitted (executor ${owner.pid} timed out, stderr: ${owner.stderr.trim() || "(empty)"})`);
    } catch (error) {
        const entries = await readdir(beta.dir).catch(error => [error.code ?? error.message]);
        const requests = beta.packets.map(({ command }) => command.op);
        context.diagnostic(`Private init journal entries ${JSON.stringify(entries)}, requests ${JSON.stringify(requests)}, last Hello HTTP ${helloStatus ?? "(none)"}${helloInactive ? " inactive_generation" : ""}, last Next HTTP ${next?.status ?? "(none)"}`);
        throw error;
    }
    await owner.stop();
    const packetCount = beta.packets.length;
    const db = join(beta.dir, "executor.sqlite3");
    const original = await readFile(db);
    for (const path of [beta.dir, join(beta.dir, "executor.lock"), db]) {
        grant(path);
        assert.match(await beta.runEnrolled("resume").exit(1), /unsafe node ACL/);
        remove(path);
    }
    const junction = join(beta.root, "junction");
    await symlink(beta.root, junction, "junction");
    assert.match(await beta.runEnrolled("resume", undefined, { journal: join(junction, "journal") }).exit(1),
        /directory reparse points/);
    await rm(junction);
    const wal = `${db}-wal`;
    await symlink(unsafeParent, wal, "junction");
    assert.match(await beta.runEnrolled("resume").exit(1), /state reparse points/);
    await rm(wal);
    assert.deepEqual(await readdir(unsafeParent), []);
    assert.deepEqual(await readFile(db), original);
    assert.equal(beta.packets.length, packetCount);
    assert.deepEqual(await beta.launches(), []);
    assert.deepEqual(mock.sends, []);
});

async function watchWindowsTree(context, provider) {
    const script = `
        $ErrorActionPreference = 'Stop'
        $owned = @(${provider.join(",")}) | ForEach-Object {
            $process = [System.Diagnostics.Process]::GetProcessById($_)
            if (!$process.Handle -or $process.HasExited) { throw "Fixture process $_ is not live" }
            $process
        }
        [Console]::WriteLine('ready')
        if ([Console]::ReadLine() -eq 'wait') {
            foreach ($process in $owned) {
                if (!$process.WaitForExit(3000)) { throw "Fixture process $($process.Id) is still live" }
            }
        }
    `;
    const watcher = spawn("powershell.exe", ["-NoProfile", "-NonInteractive",
        "-EncodedCommand", Buffer.from(script, "utf16le").toString("base64")]);
    let stdout = "";
    let stderr = "";
    watcher.stdout.setEncoding("utf8").on("data", data => { stdout += data; });
    watcher.stderr.setEncoding("utf8").on("data", data => { stderr += data; });
    let done = false;
    const exited = once(watcher, "close").finally(() => { done = true; });
    context.after(async () => {
        if (!watcher.stdin.destroyed) watcher.stdin.end();
        await exited;
    });
    await until(() => {
        assert.ok(!done, stderr);
        return stdout.trim() === "ready";
    }, "native process handles retained for the live provider and descendant", 5000);
    return { provider, async waitForExit() {
        watcher.stdin.end("wait\n");
        assert.deepEqual(await exited, [0, null], stderr);
    } };
}

const custodyHost = windowsCustody ? "Windows" : linuxCustody ? "Linux" : "macOS";
const ownerLoss = windowsCustody ? "abrupt runner termination" : "runner SIGKILL";
test(`native enrolled ${custodyHost} custody exits a live tree on cancellation and ${ownerLoss}`, {
    ...nativeTimeout, skip: !executable || (!windowsCustody && !linuxCustody && process.platform !== "darwin"),
}, async context => {
    const { client, cutover, admit, cancelHeaders, mock } = await nativePlacement(context);
    const beta = await attachExecutor(context, client, cutover, "beta");
    function processes() {
        return execFileSync("/bin/ps", ["-axo", "pid=,ppid=,pgid=,state=,comm="], { encoding: "utf8" })
            .trim().split("\n").map(line => {
                const [pid, parent, group, state, command] = line.trim().split(/\s+/, 5);
                return { pid: Number(pid), parent: Number(parent), group: Number(group), state, command };
            }).filter(row => !row.state.startsWith("Z"));
    }
    async function liveTree(runner, launchIndex = 0) {
        if (windowsCustody) {
            const provider = await until(async () => {
                const attempt = (await beta.launches())[launchIndex];
                if (!attempt) return false;
                const tree = await readFile(join(beta.dir, `tree-${attempt}`), "utf8").catch(error => {
                    if (error.code !== "ENOENT") throw error;
                    return "";
                });
                const ids = /^([1-9]\d*) ([1-9]\d*)\r?\n$/.exec(tree);
                return ids && ids.slice(1).map(Number);
            }, "Windows fixture provider publishes its live descendant", 5000);
            assert.equal(new Set([runner.pid, ...provider]).size, 3);
            return watchWindowsTree(context, provider);
        }
        if (linuxCustody) {
            return liveLinuxTree(runner);
        }
        return until(() => {
            const rows = processes();
            const leader = rows.find(row => row.parent === runner.pid && row.pid === row.group);
            if (!leader) return false;
            const tree = rows.filter(row => row.group === leader.pid);
            return tree.some(row => /(^|\/)sleep$/.test(row.command)
                && tree.some(parent => parent.pid === row.parent)) && tree;
        }, "runner owns a live group with a running sleep descendant", 3000);
    }
    async function treeExited(tree, reason) {
        if (windowsCustody) {
            await tree.waitForExit();
            return;
        }
        if (linuxCustody) {
            return linuxTreeExited(tree, reason);
        }
        await until(() => processes().every(row => !tree.some(member => member.pid === row.pid)),
            `${reason} left live members of group ${tree[0].group} (${tree.map(row => row.pid)})`, 2000);
    }

    const cancelled = await admit(45119, "@mars synthetic custody cancellation");
    const held = beta.runEnrolled("init", "hold");
    const cancelledTree = await liveTree(held);
    const [cancelledAttempt] = await beta.launches();
    const { generation, objectId } = cutover.target.pin;
    const identity = [{
        instance: `enrolled:bigfur:${generation}:${objectId}:beta:${cutover.enrolled.beta}`,
        public_key: client.keys.beta.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex"),
    }];
    const journalIdentity = () => beta.query("SELECT instance,public_key FROM identity").map(row => ({ ...row }));
    assert.deepEqual(journalIdentity(), identity);
    if (windowsCustody) {
        assert.match(await beta.runEnrolled("resume").exit(1), /executor journal is already in use/);
    }
    assert.equal((await client.post("cancel", { task: cancelled }, cancelHeaders)).phase, "cancel_requested");
    await until(() => beta.query(
        "SELECT acknowledged FROM evidence WHERE attempt=? AND event='cancelled'", cancelledAttempt,
    )[0]?.acknowledged, "cancellation is acknowledged");
    await treeExited(cancelledTree, "cancellation");
    const [proof] = beta.proofs("cancelled");
    assert.deepEqual(beta.verifyProof(proof.command.proof), {
        attempt: cancelledAttempt, event: { kind: "cancelled", evidence: "process_exited" },
    });
    assert.equal((await client.agent("beta", "status", { attempt: cancelledAttempt })).phase, "cancelled_after_exit");
    context.diagnostic(`cancelled_after_exit attempt ${cancelledAttempt}, runner ${held.pid}, exited tree ${JSON.stringify(cancelledTree)}`);
    await held.stop();

    const lost = await admit(45120, "@mars synthetic custody owner loss");
    const owner = beta.runEnrolled("resume", "hold");
    const lostTree = await liveTree(owner, 1);
    const [, lostAttempt] = await beta.launches();
    assert.equal(beta.query("SELECT phase FROM attempts WHERE attempt=?", lostAttempt)[0].phase, "consumed");
    context.diagnostic(`consumed attempt ${lostAttempt}, runner ${owner.pid}, live tree ${JSON.stringify(lostTree)}`);
    assert.deepEqual(await owner.stop(), [null, "SIGKILL"]);
    await treeExited(lostTree, ownerLoss);
    context.diagnostic(`${ownerLoss} and provider tree exit confirmed before recovery`);
    const recoverAt = beta.packets.length;
    assert.match(await beta.runEnrolled("resume", undefined, { incarnation: cutover.enrolled.alpha }).exit(1),
        /journal identity mismatch/);
    assert.equal(beta.packets.length, recoverAt);
    const resumed = beta.runEnrolled("resume");
    await until(() => beta.query(
        "SELECT acknowledged FROM evidence WHERE attempt=? AND event='unknown'", lostAttempt,
    )[0]?.acknowledged && beta.packets.slice(recoverAt).some(({ command }) => command.op === "next"),
    "Unknown acknowledged and recovered runner polls for work");
    const [unknown] = beta.proofs("unknown");
    assert.deepEqual(beta.verifyProof(unknown.command.proof), { attempt: lostAttempt, event: { kind: "unknown" } });
    assert.deepEqual(beta.query("SELECT payload,signature FROM evidence WHERE attempt=? AND event='unknown'", lostAttempt)
        .map(row => ({ ...row })), [unknown.command.proof]);
    assert.deepEqual(journalIdentity(), identity);
    assert.equal((await client.agent("beta", "status", { attempt: lostAttempt })).phase, "unknown");
    assert.equal((await client.agent("beta", "next")).task, null);
    await client.agent("beta", "claim", { task: lost }, 409);
    assert.deepEqual(await beta.launches(), [cancelledAttempt, lostAttempt]);
    assert.deepEqual(beta.packets.filter(({ command }) => command.op === "start")
        .map(({ command }) => command.attempt), [cancelledAttempt, lostAttempt]);
    assert.deepEqual(mock.sends, []);
    await resumed.stop();
    context.diagnostic(`recovered attempt ${lostAttempt} as Unknown with one Start, one launch and zero deliveries`);
});

test("enrolled placement binds claims, grants and journal recovery to one runner", timeout, async context => {
    const enrolled = { alpha: "e".repeat(64), beta: "d".repeat(64) };
    let aliases = "desk=mars";
    const { root, mock, client, cutover, ingress, telegram, cancelHeaders, owner, topic } =
        await productionAuthorityFixture(context, { enrolled, placementAliases: () => aliases });
    const update = (id, text) => {
        const value = telegram(id);
        value.message.text = text;
        return value;
    };
    const beta = (await ingress(update(45100, "@desk beta fixture"))).task;
    const alpha = (await ingress(update(45101, "plain fixture"))).task;
    assert.equal((await ingress(update(45102, "@mercury unregistered"))).disposition, "ignored");
    assert.equal(cutover.authorityCalls, 0);
    aliases = "desk=leo-claw";
    await client.restart();
    assert.equal((await ingress(update(45100, "@desk duplicate"))).disposition, "duplicate");

    cutover.controlOnline = true;
    await client.agent("beta", "hello", {}, 503);
    const objectIncarnation = mock.checks.at(-1).objectIncarnation;
    mock.publish(cutover.target, objectIncarnation, enrolled);
    for (const [device, task] of [["alpha", alpha], ["beta", beta]]) {
        assert.equal((await client.agent(device, "hello")).device, device);
        assert.equal((await client.agent(device, "next")).task, task);
    }
    const inactive = { error: "inactive_generation" };
    const wrongIncarnation = client.signed("beta", { op: "hello" }, { incarnation: enrolled.alpha });
    assert.deepEqual(await client.wire(wrongIncarnation, 503), inactive);
    wrongIncarnation.headers["x-executor-incarnation"] = enrolled.beta;
    await client.wire(wrongIncarnation, 401);
    const wrongPlacement = client.signed("beta", { op: "hello" });
    wrongPlacement.headers["x-executor-placement"] = "alpha";
    wrongPlacement.headers["x-executor-incarnation"] = enrolled.alpha;
    await client.wire(wrongPlacement, 401);
    const wrongKey = client.signed("beta", { op: "hello" });
    const instance = `bigfur:1:${cutover.target.pin.objectId}`;
    const betaPrefix = `coconutclaw-enrolled-request-v1\n${instance}\nbeta\n${enrolled.beta}\nPOST\n/v1/agent\n`;
    wrongKey.headers["x-agent-signature"] = sign(null, Buffer.from(betaPrefix + wrongKey.body),
        client.keys.alpha.privateKey).toString("hex");
    await client.wire(wrongKey, 401);
    const omittedPlacement = client.signed("beta", { op: "hello" });
    delete omittedPlacement.headers["x-executor-placement"];
    await client.wire(omittedPlacement, 503);
    mock.beforeReply = permit => permit.placement === "beta" ?
        Response.json({ ...permit, placement: "alpha" }) : undefined;
    assert.deepEqual(await client.agent("beta", "next", {}, 503), inactive);
    mock.beforeReply = undefined;

    await client.agent("alpha", "claim", { task: beta }, 409);
    const claimed = await client.agent("beta", "claim", { task: beta });
    assert.equal(claimed.placement, "beta");
    assert.equal(claimed.prompt, "beta fixture");
    await client.agent("alpha", "start", { attempt: claimed.attempt }, 409);
    const alphaClaim = await client.agent("alpha", "claim", { task: alpha });
    const alphaGrant = await client.agent("alpha", "start", { attempt: alphaClaim.attempt });
    assert.equal(alphaGrant.placement, "alpha");
    const started = client.signed("beta", { op: "start", attempt: claimed.attempt });
    const grant = await client.wire(started);
    assert.equal(grant.placement, "beta");
    assert.equal(grant.attempt, claimed.attempt);
    assert.equal(grant.request_sequence, JSON.parse(started.body).sequence);
    await client.agent("beta", "start", { attempt: claimed.attempt }, 409);
    await client.agent("alpha", "status", { attempt: claimed.attempt }, 409);
    assert.equal((await client.agent("beta", "status", { attempt: claimed.attempt })).start_until, grant.start_until);
    assert.equal((await client.agent("beta", "next")).task, null);

    function proof(device, attempt, event, signedPlacement = device) {
        const payload = JSON.stringify({ attempt, event });
        const domain = `coconutclaw-enrolled-evidence-v1\n${grant.instance}\n${signedPlacement}\n${enrolled[device]}\n`;
        return { payload, signature: sign(null, Buffer.from(domain + payload), client.keys[device].privateKey).toString("hex") };
    }
    const result = { kind: "result", text: "journal result" };
    await client.agent("beta", "report", { proof: proof("beta", claimed.attempt, result, "alpha") }, 401);
    await client.agent("alpha", "report", { proof: proof("alpha", claimed.attempt, result) }, 409);
    const journal = join(root, "beta-evidence.json");
    await writeFile(journal, JSON.stringify(proof("beta", claimed.attempt, result)), { mode: 0o600 });
    const recovered = JSON.parse(await readFile(journal, "utf8"));
    await client.agent("beta", "report", { proof: proof("beta", claimed.attempt, { kind: "started" }) });
    let checks = 0;
    mock.beforeReply = permit => permit.placement === "beta" && ++checks === 3 ?
        new Response(null, { status: 503 }) : undefined;
    await client.agent("beta", "report", { proof: recovered }, 503);
    mock.beforeReply = undefined;
    assert.equal((await client.agent("beta", "status", { attempt: claimed.attempt })).phase, "completed");
    await client.agent("beta", "report", { proof: recovered });
    await until(() => mock.sends.length === 1, "same-topic delivery after replay");
    assert.deepEqual(mock.sends, [{ task: beta, sendNumber: 1, chatId: owner, messageThreadId: topic, text: result.text }]);

    await client.agent("alpha", "report", { proof: proof("alpha", alphaClaim.attempt, { kind: "started" }) });
    await client.post("cancel", { task: alpha }, cancelHeaders);
    await client.agent("beta", "report", { proof: proof("beta", alphaClaim.attempt, {
        kind: "cancelled", evidence: "process_exited",
    }) }, 409);
    await client.agent("alpha", "report", { proof: proof("alpha", alphaClaim.attempt, {
        kind: "cancelled", evidence: "process_exited",
    }) });
    assert.equal((await client.agent("alpha", "status", { attempt: alphaClaim.attempt })).phase, "cancelled_after_exit");

    const lost = (await ingress(update(45103, "@mars lost Start fixture"))).task;
    const lostClaim = await client.agent("beta", "claim", { task: lost });
    let originalDeadline;
    checks = 0;
    mock.beforeReply = permit => {
        if (permit.placement !== "beta") return;
        if (++checks === 2) originalDeadline = permit.validUntil;
        if (checks === 3) return Response.json({ ...permit, placement: "alpha" });
    };
    await client.agent("beta", "start", { attempt: lostClaim.attempt }, 503);
    mock.beforeReply = undefined;
    const status = await client.agent("beta", "status", { attempt: lostClaim.attempt });
    assert.equal(status.start_until, originalDeadline);
    assert.equal(status.phase, "authorized");
    await client.agent("beta", "report", { proof: proof("beta", lostClaim.attempt, { kind: "unknown" }) });
    assert.equal((await client.agent("beta", "status", { attempt: lostClaim.attempt })).phase, "unknown");
    assert.equal((await client.agent("beta", "next")).task, null);
    await client.agent("beta", "claim", { task: lost }, 409);

    await client.restart();
    assert.deepEqual(await client.agent("beta", "status", { attempt: claimed.attempt }, 503), inactive);
    assert.notEqual(mock.checks.at(-1).objectIncarnation, objectIncarnation);
    assert.equal(mock.sends.length, 1);
});

test("BigFur production queues fresh Telegram work with the runner and control offline", timeout, async context => {
    const { root, mock, owner, topic, cutover, client, cancelHeaders, telegram, ingress } =
        await productionAuthorityFixture(context);
    const inactive = { error: "inactive_generation" };
    const ignored = { disposition: "ignored", task: null };
    const task = "telegram:41001";
    const endpoint = "https://bigfur.liu.nz/v1/telegram";
    assert.equal((await client.directIngress("hosted-synthetic-pilot", telegram(49000))).status, 404);
    for (const url of [`${endpoint}?extra=1`, endpoint.replace("https", "http"),
        endpoint.replace("bigfur.liu.nz", "other.invalid"), endpoint.replace("/telegram", "/pilot/telegram")]) {
        assert.equal((await client.dispatch(url, { headers: webhook, body: JSON.stringify(telegram(49000)) })).status, 404);
    }
    assert.deepEqual(await client.post("telegram", telegram(49000), {}, 401), { error: "unauthorized" });
    assert.deepEqual(await client.post("telegram", telegram(49000), {
        "x-telegram-bot-api-secret-token": "wrong",
    }, 401), { error: "unauthorized" });
    assert.deepEqual(await ingress(" ".repeat(16 * 1024 + 1), 413), { error: "body_limit" });
    const streamed = await client.dispatch(endpoint, {
        headers: webhook,
        duplex: "half",
        body: new ReadableStream({
            start(controller) {
                controller.enqueue(new Uint8Array(9 * 1024));
                controller.enqueue(new Uint8Array(8 * 1024));
                controller.close();
            },
        }),
    });
    assert.equal(streamed.status, 413);
    assert.deepEqual(await streamed.json(), { error: "body_limit" });
    assert.deepEqual(await ingress({ update_id: -1 }, 400), { error: "bad_input" });
    for (const message of [telegram(49000, topic + 1), telegram(49000, topic, owner + 1),
        telegram(49000, topic, owner, owner + 1), { update_id: 49000, callback_query: {} }]) {
        assert.deepEqual(await ingress(message), ignored);
    }
    const first = await Promise.all([ingress(telegram(41001)), ingress(telegram(41001))]);
    assert.deepEqual(first.map(result => result.disposition).sort(), ["duplicate", "task"]);
    assert.deepEqual(first.find(result => result.task), { disposition: "task", task });
    assert.equal(cutover.authorityCalls, 0);
    assert.deepEqual(await client.sql("SELECT name FROM sqlite_master WHERE name LIKE '%legacy%' OR name='cutover_custody'"), []);
    assert.deepEqual(await client.sql("SELECT * FROM attempts"), []);
    assert.deepEqual(await client.sql("SELECT update_id FROM inbox"), [{ update_id: 41001 }]);

    await client.stop();
    const saved = join(root, "before-offline-ack");
    await client.snapshot(saved);
    const authorityBeforeAck = await readFile(join(root, "control.sqlite"));
    await client.restart();
    assert.deepEqual(await ingress(telegram(41001)), { disposition: "duplicate" });
    assert.deepEqual(await ingress(telegram(41002)), { disposition: "task", task: "telegram:41002" });
    await client.restart();
    assert.deepEqual(await ingress(telegram(41002)), { disposition: "duplicate" });

    const restored = await runFixture(context, { inspect: true, restore: saved, cutover, keys: client.keys });
    assert.equal(await restored.objectId("bigfur-production"), cutover.target.pin.objectId);
    assert.deepEqual(await restored.sql("SELECT update_id FROM inbox"), [{ update_id: 41001 }]);
    assert.deepEqual(await restored.post("telegram", telegram(41002), webhook),
        { disposition: "task", task: "telegram:41002" });
    await restored.stop();
    assert.equal(cutover.authorityCalls, 0);
    assert.deepEqual(await readFile(join(root, "control.sqlite")), authorityBeforeAck);

    assert.deepEqual(await client.agent("alpha", "next", {}, 503), inactive);
    assert.deepEqual(await client.post("cancel", { task }, cancelHeaders, 503), inactive);
    assert.ok(cutover.authorityCalls > 0);

    const pin = cutover.target.pin;
    for (const change of [{ generation: 2 }, { custodyDigest: "c".repeat(64) },
        { objectId: "0".repeat(64) }, { scope: `${owner}:${topic + 1}` }, { scope: "" }]) {
        const { scope = `${owner}:${topic}`, ...override } = change;
        cutover.target.pin = { ...pin, ...override };
        cutover.scope = scope;
        await client.restart();
        assert.deepEqual(await ingress(telegram(41003), 503), inactive);
    }
    cutover.target.pin = pin;
    cutover.scope = `${owner}:${topic}`;
    await client.restart();
    cutover.controlOnline = true;
    assert.deepEqual(await client.post("cancel", { task }, cancelHeaders, 503), inactive);
    mock.publish(cutover.target, mock.checks.at(-1).objectIncarnation, cutover.runner);
    cutover.runner = "a".repeat(64);
    assert.deepEqual(await client.agent("alpha", "next", {}, 503), inactive);
    cutover.runner = "d".repeat(64);
    const claimed = await client.agent("alpha", "claim", { task });
    assert.equal(claimed.placement, "alpha");
    assert.equal(claimed.chat_id, owner);
    assert.equal(claimed.message_thread_id, topic);
    cutover.controlOnline = false;
    assert.deepEqual(await client.agent("alpha", "start", { attempt: claimed.attempt }, 503), inactive);
    cutover.controlOnline = true;
    const grant = await client.agent("alpha", "start", { attempt: claimed.attempt });
    assert.ok(grant.authority_valid_until > Date.now());
    assert.equal(Object.hasOwn(grant, "placement"), false);
    assert.deepEqual(await client.sql("SELECT update_id FROM inbox ORDER BY sequence"), [
        { update_id: 41001 }, { update_id: 41002 },
    ]);
    await reportProductionResult(client, grant, "same topic");
    await until(async () => (await client.sql("SELECT message_id FROM delivery_receipts")).length,
        "production alarm durably records the authority's send receipt");
    assert.deepEqual(mock.sends, [{
        task, sendNumber: 1, chatId: owner, messageThreadId: topic, text: "same topic",
    }]);
    mock.retire();
    assert.deepEqual(await client.post("cancel", { task: "telegram:41002" }, cancelHeaders, 503), inactive);
    assert.equal((await ingress(telegram(41003))).disposition, "task");
});

test("BigFur production preserves queued work and pending delivery but fences both cold starts", timeout, async context => {
    const { root, mock, cutover, client, cancelHeaders, telegram, ingress } =
        await productionAuthorityFixture(context);
    const inactive = { error: "inactive_generation" };
    const first = "telegram:42001";
    const pending = "telegram:42002";
    const queued = "telegram:42003";
    const outbox = "SELECT phase,sends FROM outbox WHERE task=?";
    cutover.controlOnline = true;
    await ingress(telegram(42001));
    assert.deepEqual(await client.post("cancel", { task: first }, cancelHeaders, 503), inactive);
    const enrolled = mock.checks.at(-1).objectIncarnation;
    mock.publish(cutover.target, enrolled, cutover.runner);

    const complete = async task => {
        const { attempt } = await client.agent("alpha", "claim", { task });
        const grant = await client.agent("alpha", "start", { attempt });
        await reportProductionResult(client, grant, task);
    };
    await ingress(telegram(42002));
    await ingress(telegram(42003));
    await complete(first);
    await until(async () => (await client.sql(outbox, first))[0]?.phase === "delivered",
        "the enrolled writer sends the first reply");
    await complete(pending);
    assert.equal((await client.agent("alpha", "next", {})).task, queued);
    assert.deepEqual(await client.sql(outbox, pending), [{ phase: "pending", sends: 0 }]);
    assert.ok((await client.scheduledAlarm())[0].scheduled_time > Date.now());

    await client.stop();
    const saved = join(root, "saved");
    await client.snapshot(saved);
    const deliveries = mock.sends.slice();
    assert.equal(deliveries.length, 1);
    const expectedOutbox = [{ phase: "pending", sends: 0 }];
    const verifyFenced = async (candidate, firstCheck) => {
        assert.equal(await candidate.objectId("bigfur-production"), cutover.target.pin.objectId);
        const claim = candidate.signed("alpha", { op: "claim", task: queued }, { seq: 100 });
        assert.deepEqual(await candidate.wire(claim, 503), inactive);
        await until(() => mock.checks.slice(firstCheck).some(check => check.runnerIncarnation === null),
            "the persisted alarm reaches control after a cold start");
        const checks = mock.checks.slice(firstCheck);
        assert.ok(checks.some(check => check.runnerIncarnation === cutover.runner));
        for (const check of checks) {
            assert.deepEqual(check.target, cutover.target);
            assert.notEqual(check.objectIncarnation, enrolled);
            assert.equal(check.allowed, false);
        }
        assert.deepEqual(await candidate.sql("SELECT current_attempt FROM tasks WHERE id=?", queued),
            [{ current_attempt: null }]);
        assert.deepEqual(await candidate.sql(outbox, pending), expectedOutbox);
        assert.deepEqual(mock.sends, deliveries);
    };

    const restoreChecks = mock.checks.length;
    const restored = await runFixture(context, { inspect: true, restore: saved, cutover, keys: client.keys });
    await verifyFenced(restored, restoreChecks);
    await restored.stop();

    const restartChecks = mock.checks.length;
    await client.restart();
    assert.deepEqual(await ingress(telegram(42003)), { disposition: "duplicate" });
    assert.deepEqual(await ingress(telegram(42004)), { disposition: "task", task: "telegram:42004" });
    await verifyFenced(client, restartChecks);
    assert.deepEqual(await restored.sql("SELECT id FROM tasks WHERE id='telegram:42004'"), []);
    assert.notEqual(mock.checks[restartChecks].objectIncarnation, mock.checks[restoreChecks].objectIncarnation);
});

test("BigFur fresh-object policy resumes pending replies and signed claims after cold wake", timeout, async context => {
    const { mock, cutover, client, ingress, telegram, owner, topic } = await productionAuthorityFixture(context);
    const first = "telegram:42101";
    const pending = "telegram:42102";
    const queued = "telegram:42103";
    const outbox = task => client.sql("SELECT phase,sends FROM outbox WHERE task=?", task);
    for (const id of [42101, 42102, 42103]) await ingress(telegram(id));
    mock.publish(cutover.target, null, cutover.runner);
    cutover.controlOnline = true;

    const complete = async task => {
        const { attempt } = await client.agent("alpha", "claim", { task });
        const grant = await client.agent("alpha", "start", { attempt });
        await reportProductionResult(client, grant, task);
    };
    await complete(first);
    await until(async () => (await outbox(first))[0]?.phase === "delivered", "first send completes");
    await complete(pending);
    assert.deepEqual(await outbox(pending), [{ phase: "pending", sends: 0 }]);
    const preWake = mock.checks.at(-1).objectIncarnation;

    await client.restart();
    assert.deepEqual(await ingress(telegram(42103)), { disposition: "duplicate" });
    assert.equal((await client.agent("alpha", "next")).task, queued);
    assert.notEqual(mock.checks.at(-1).objectIncarnation, preWake);
    await complete(queued);
    await until(async () => (await outbox(pending))[0]?.phase === "delivered", "persisted alarm dispatches after wake");
    mock.afterSend = () => Response.error();
    await until(async () => (await outbox(queued))[0]?.phase === "unknown", "a lost acknowledgment becomes Unknown");
    assert.deepEqual(await outbox(queued), [{ phase: "unknown", sends: 1 }]);
    await client.restart();
    assert.deepEqual(await ingress(telegram(42103)), { disposition: "duplicate" });
    assert.equal((await client.agent("alpha", "next")).task, null);
    assert.deepEqual(await outbox(queued), [{ phase: "unknown", sends: 1 }]);
    assert.deepEqual(mock.sends, [first, pending, queued].map(task => ({
        task, sendNumber: 1, chatId: owner, messageThreadId: topic, text: task,
    })));
});

test("BigFur recovery routes a new generation only after retiring predecessor effects", timeout, async context => {
    const { mock, cutover, client, cancelHeaders, ingress, telegram } = await productionAuthorityFixture(context);
    const stale = { ...cutover };
    const inactive = { error: "inactive_generation" };
    await ingress(telegram(42201));
    mock.publish(stale.target, null, stale.runner);
    cutover.controlOnline = true;
    stale.controlOnline = true;
    const oldPacket = client.signed("alpha", { op: "claim", task: "telegram:42201" });
    const retired = await runFixture(context, { inspect: true, cutover: stale, keys: { ...client.keys } });
    await retired.post("telegram", telegram(42201), webhook);
    const legacyClaim = await retired.agent("alpha", "claim", { task: "telegram:42201" });
    const legacyGrant = await retired.agent("alpha", "start", { attempt: legacyClaim.attempt });
    const started = productionProof(retired, legacyGrant, { kind: "started" });
    const result = productionProof(retired, legacyGrant, { kind: "result", text: "held legacy result" });

    const generationHeader = { ...cancelHeaders, "x-cutover-generation": "2" };
    await client.post("production/identity", {}, { "x-cutover-generation": "2" }, 401);
    await client.post("production/identity", {}, { ...generationHeader, "x-cutover-generation": "0" }, 400);
    const { objectId } = await client.post("production/identity", {}, generationHeader);
    assert.notEqual(objectId, stale.target.pin.objectId);
    cutover.target = {
        pin: { ...stale.target.pin, generation: 2, objectId, custodyDigest: "c".repeat(64) },
        versionId: "replacement-worker",
    };
    cutover.runner = "e".repeat(64);
    await client.restart();
    assert.equal((await client.directIngress("bigfur-production", telegram(42202))).status, 404);
    await ingress(telegram(42202));
    assert.deepEqual(await client.agent("alpha", "next", {}, 503), inactive);
    assert.throws(() => mock.publish(cutover.target, null, cutover.runner));
    mock.retire();
    const firstHandoff = await retired.post("production/ingress-handoff", {}, cancelHeaders);
    assert.deepEqual(await retired.wire(oldPacket, 503), inactive);
    assert.throws(() => mock.publish({
        ...cutover.target, pin: { ...cutover.target.pin, objectId: stale.target.pin.objectId },
    }, null, cutover.runner));
    assert.throws(() => mock.publish(cutover.target, null, stale.runner));
    mock.publish(cutover.target, null, cutover.runner);
    assert.deepEqual(await retired.wire(oldPacket, 503), inactive);
    assert.deepEqual(await client.wire(oldPacket, 503), inactive);
    const oldSignature = { ...oldPacket, headers: { ...oldPacket.headers, "x-executor-incarnation": cutover.runner } };
    await client.wire(oldSignature, 401);
    assert.equal((await client.agent("alpha", "claim", { task: "telegram:42202" })).task, "telegram:42202");
    assert.deepEqual(await client.sql("SELECT update_id FROM inbox"), [{ update_id: 42202 }]);

    const { objectId: placementObject } = await client.post("production/identity", {}, {
        ...cancelHeaders, "x-cutover-generation": "3",
    });
    const secondHandoff = await client.post("production/ingress-handoff", {}, cancelHeaders);
    const { executorKey: legacyKey, ...previousPin } = cutover.target.pin;
    cutover.target = {
        pin: { ...previousPin, generation: 3, objectId: placementObject, pairing: "production-v1" },
        versionId: "production-placements",
    };
    cutover.pairedProduction = true;
    cutover.enrolled = {};
    assert.throws(() => mock.publish(cutover.target, null));
    mock.retire();
    assert.throws(() => mock.publish(cutover.target, null, undefined, [secondHandoff]));
    mock.publish(cutover.target, null, undefined, [firstHandoff, secondHandoff]);
    await client.restart();
    assert.equal((await redeemProduction(mock, client.keys.alpha, mock.issue({ key: legacyKey })))[0], 409);
    client.keys.alpha = generateKeyPairSync("ed25519");
    await pairProduction(mock, client, cutover, "alpha");
    assert.deepEqual(await ingress(telegram(42201)), { disposition: "duplicate" });
    assert.deepEqual(await ingress(telegram(42202)), { disposition: "duplicate" });
    await ingress(telegram(42203));
    assert.equal((await client.agent("alpha", "next")).task, "telegram:42203");

    assert.equal((await retired.agent("alpha", "hello")).reconcile_only, true);
    await retired.agent("alpha", "start", { attempt: legacyGrant.attempt }, 503);
    assert.equal((await retired.agent("alpha", "report", { proof: started })).event, "started");
    assert.equal((await retired.agent("alpha", "report", { proof: result })).event, "result");
    assert.equal((await retired.agent("alpha", "status", { attempt: legacyGrant.attempt })).phase, "unknown");
    assert.deepEqual(await retired.sql("SELECT * FROM outbox"), []);
    assert.deepEqual(mock.sends, []);
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
    await until(async () => (await interruptedDelivery())[0]?.phase === "unknown", "retired flight expires without egress");
    assert.deepEqual(await interruptedDelivery(), [{ phase: "unknown", sends: 1 }]);
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
    const initial = update(41, 7, "@mars remains literal in the legacy fixture");
    const concurrently = await Promise.all([post("telegram", initial, webhook), post("telegram", initial, webhook)]);
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
        assert.equal(claim.prompt, id === 41 ? initial.message.text
            : id === 38 ? "sensitive synthetic task" : `synthetic task ${id}`);
    }
    assert.deepEqual(await agent("alpha", "next"), { task: null });
});

function placementUpdate(id, text, thread = 37) {
    const input = update(id, thread, text);
    input.message.from.is_bot = false;
    return input;
}

test("manual placement pins signed claims across alias changes, offline time and lease expiry", timeout, async context => {
    let aliases = "desk=mars";
    const { agent, fixture, post, restart, sends, signed, wire } = await runFixture(context, {
        placementAliases: () => aliases,
    });
    const mars = "telegram:81";
    const mercury = "telegram:83";
    await post("telegram", placementUpdate(81, "@desk inspect synthetic tests"), {}, 401);
    assert.deepEqual(await post("telegram", placementUpdate(81, " @desk\tinspect synthetic tests "), webhook),
        { disposition: "task", task: mars });
    assert.deepEqual(await agent("alpha", "next"), { task: null });
    assert.deepEqual(await agent("gamma", "next"), { task: null });
    await agent("alpha", "claim", { task: mars }, 409);

    aliases = "desk=mercury";
    await restart();
    assert.deepEqual(await post("telegram", placementUpdate(81, "@desk changed duplicate"), webhook),
        { disposition: "duplicate" });
    await post("telegram", placementUpdate(82, "plain synthetic task @mars"), webhook);
    await post("telegram", placementUpdate(83, "@desk inspect fixture"), webhook);
    await post("telegram", placementUpdate(84, "@leo-claw cancel queued fixture"), webhook);
    await post("telegram", placementUpdate(85, "@mars cancel started fixture"), webhook);
    assert.deepEqual(await fixture("cancel", { task: "telegram:84" }), { phase: "cancelled_before_start" });

    assert.deepEqual(await agent("alpha", "next"), { task: "telegram:82" });
    const plain = await agent("alpha", "claim", { task: "telegram:82" });
    assert.equal(plain.placement, "alpha");
    assert.equal(plain.prompt, "plain synthetic task @mars");
    assert.deepEqual(await agent("alpha", "next"), { task: null });
    assert.deepEqual(await agent("beta", "next"), { task: mars });
    assert.deepEqual(await agent("gamma", "next"), { task: mercury });

    const spoofed = signed("alpha", { op: "claim", task: mars });
    await wire({ ...spoofed, body: spoofed.body.replace('"alpha"', '"beta"') }, 401);
    await wire(signed("beta", { op: "next" }, { prefix: signingPrefix }), 401);
    const claim = await agent("beta", "claim", { task: mars });
    assert.equal(claim.placement, "beta");
    assert.equal(claim.prompt, "inspect synthetic tests");
    assert.equal(claim.chat_id, 321);
    assert.equal(claim.message_thread_id, 37);
    assert.deepEqual(await agent("beta", "claim", { task: mars }), claim);
    await sleep(Math.max(0, claim.lease_until - Date.now() + 15));
    await agent("beta", "start", { attempt: claim.attempt }, 409);
    await agent("gamma", "claim", { task: mars }, 409);
    const { attempt } = await agent("beta", "claim", { task: mars });
    assert.notEqual(attempt, claim.attempt);
    await agent("gamma", "start", { attempt }, 409);
    await agent("beta", "start", { attempt });
    await agent("beta", "started", { attempt });
    await agent("gamma", "result", { attempt, text: "wrong runner" }, 409);
    await agent("beta", "result", { attempt, text: "synthetic result" });
    await fixture("send", { task: mars });
    assert.deepEqual(sends, [{ chat_id: 321, message_thread_id: 37, text: "synthetic result" }]);

    const unknown = (await agent("gamma", "claim", { task: mercury })).attempt;
    await agent("gamma", "start", { attempt: unknown });
    assert.equal((await fixture("disconnect", { attempt: unknown })).phase, "unknown");
    await restart();
    assert.deepEqual(await agent("gamma", "next"), { task: null });
    await agent("gamma", "start", { attempt: unknown }, 409);
    await agent("beta", "claim", { task: mercury }, 409);
    await agent("gamma", "claim", { task: mercury }, 409);

    const cancelTask = "telegram:85";
    const cancelled = (await agent("beta", "claim", { task: cancelTask })).attempt;
    await agent("beta", "start", { attempt: cancelled });
    await agent("beta", "started", { attempt: cancelled });
    assert.equal((await fixture("cancel", { task: cancelTask })).phase, "cancel_requested");
    const exit = { attempt: cancelled, evidence: "process_exited" };
    await agent("gamma", "cancelled", exit, 409);
    assert.equal((await agent("beta", "cancelled", exit)).phase, "cancelled_after_exit");
    assert.deepEqual(await agent("beta", "next"), { task: null });
});

test("manual placement fails closed on scope, selectors, alias collisions and shared keys", timeout, async context => {
    const shared = generateKeyPairSync("ed25519");
    let aliases;
    const { agent, dispatch, fixture, post, restart } = await runFixture(context, {
        placementAliases: () => aliases,
        keys: { alpha: generateKeyPairSync("ed25519"), beta: shared, gamma: shared },
    });
    const wrongOwner = placementUpdate(90, "@mars synthetic");
    wrongOwner.message.from.id = 322;
    const bot = placementUpdate(91, "@mars synthetic");
    bot.message.from.is_bot = true;
    const wrongChat = placementUpdate(99, "@mars synthetic");
    wrongChat.message.chat.id = 322;
    const missingPrivate = placementUpdate(100, "@mars synthetic");
    delete missingPrivate.message.chat.type;
    for (const input of [
        wrongOwner, bot, wrongChat, missingPrivate, update(92, 37, "@mars synthetic"),
        placementUpdate(93, "@mars synthetic", 38), placementUpdate(94, "@unknown synthetic"),
        placementUpdate(95, "@mercury "), placementUpdate(96, "@mars /cancel"),
        placementUpdate(97, "@mars: synthetic"),
    ]) {
        assert.deepEqual(await post("telegram", input, webhook), { disposition: "ignored", task: null });
    }
    assert.deepEqual(await post("telegram", placementUpdate(94, "@leo-claw duplicate"), webhook),
        { disposition: "duplicate" });
    const nonLoopback = await dispatch("https://placement.invalid/v1/telegram", {
        headers: webhook, body: JSON.stringify(placementUpdate(98, "@mars fixture")),
    });
    assert.equal(nonLoopback.status, 404);
    await fixture("wake", { at_ms: Date.now() }, 404);
    assert.deepEqual(await agent("alpha", "next"), { task: null });
    await agent("beta", "next", {}, 401);
    await agent("gamma", "next", {}, 401);
    for (const config of ["mars=mercury", "mac=mars,mac=mercury", "mac=wrong", "Mac=mars", "mac!=mars"]) {
        aliases = config;
        await restart();
        assert.deepEqual(await post("telegram", placementUpdate(98, "plain"), webhook, 503),
            { error: "invalid_placement_aliases" });
        assert.deepEqual(await agent("alpha", "next"), { task: null });
    }
    aliases = "mac=mars";
    await restart();
    assert.equal((await post("telegram", placementUpdate(98, "@mac synthetic"), webhook)).disposition, "task");
    assert.equal((await post("telegram", placementUpdate(101, "@mercury synthetic"), webhook)).disposition, "task");
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
