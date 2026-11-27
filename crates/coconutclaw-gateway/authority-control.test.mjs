import assert from "node:assert/strict";
import { execFile, fork } from "node:child_process";
import { generateKeyPairSync, randomUUID, sign } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import * as control from "./authority-control.mjs";
import { pairingChallenge } from "./authority-preflight.mjs";

const { observeAuthority, serveAuthority, telegramSender } = control;

const exec = promisify(execFile);
const cli = join(import.meta.dirname, "authority-control.mjs");
const bearer = "a".repeat(64);
const token = "123:synthetic_bot_token";
const credentialVersion = "synthetic-credential";
const options = { timeout: 15_000 };
const publication = generation => ({
    target: {
        generation, revision: "1".repeat(40), deployment: `fixture/worker/${generation}`,
        hostname: "worker.invalid", objectId: "2".repeat(64), custodyDigest: "3".repeat(64),
        telegramCredentialVersion: credentialVersion, executorKey: "4".repeat(64),
    },
    retirement: {
        generation: generation - 1, writers: "synthetic/writers", executorGrants: "synthetic/grants",
        telegram: { disposition: "exclusive-existing-token", evidence: "synthetic/token" },
        custody: "synthetic/custody",
    },
    versionId: `version-${generation}`,
    objectIncarnation: randomUUID(),
    runnerIncarnation: `${generation}`.repeat(64),
});
const sendDelivery = (number, extra = {}) => ({
    task: "telegram:42", sendNumber: number, chatId: -321, messageThreadId: 37, text: "synthetic reply", ...extra,
});

test("local pairing redeems bound cards atomically and appends to the active generation", options, async context => {
    const root = await mkdtemp(join(tmpdir(), "authority-pairing-"));
    const path = join(root, "authority.sqlite");
    const sends = [];
    assert.equal(typeof control.createPairingAuthority, "function");
    const authority = await control.createPairingAuthority(path, { bearer, credentialVersion }, delivery => {
        sends.push(delivery);
        return 42;
    });
    context.after(async () => {
        await authority.close();
        await rm(root, { recursive: true, force: true });
    });
    assert.throws(() => serveAuthority(path, { bearer, credentialVersion }, () => 99), /invalid authority store/);
    const first = publication(1);
    delete first.runnerIncarnation;
    delete first.target.executorKey;
    Object.assign(first.target, { hostname: "127.0.0.1", pairing: "fixture-v1" });
    authority.publish(first);
    const { deployment, ...pin } = first.target;
    const target = { pin, versionId: first.versionId };
    const post = async (route, body) => {
        const response = await fetch(`${authority.endpoint}/v1/${route}`, {
            method: "POST", headers: { authorization: `Bearer ${bearer}` },
            body: JSON.stringify(body), signal: AbortSignal.timeout(3000),
        });
        return { status: response.status, reply: response.ok ? await response.json() : null };
    };
    const permit = (extra, route = "check") => post(route, {
        nonce: randomUUID(), target, objectIncarnation: first.objectIncarnation,
        runnerIncarnation: null, delivery: null, ...extra,
    });
    const card = () => {
        const { publicKey, privateKey } = generateKeyPairSync("ed25519");
        return { key: publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex"), privateKey };
    };
    const alpha = card();
    const beta = card();
    const redeem = (code, runner, signingCard = runner, extra = {}) => post("pair", {
        code, key: runner.key,
        signature: sign(null, Buffer.from(`coconutclaw-fixture-pairing-v1\n${code}`),
            signingCard.privateKey).toString("hex"),
        ...extra,
    });
    assert.deepEqual((await permit()).reply.routes, []);
    const alphaCode = authority.issue({ key: alpha.key });
    const expired = authority.issue({ key: alpha.key, ttlMs: 1 });
    await sleep(20);
    assert.equal((await redeem(expired, alpha)).status, 410);
    assert.equal((await redeem(alphaCode, beta)).status, 401);
    assert.equal((await redeem(alphaCode, alpha, beta)).status, 401);
    assert.equal((await redeem(alphaCode, alpha, alpha, { placement: "beta" })).status, 400);
    assert.deepEqual((await permit()).reply.routes, []);
    const race = await Promise.all([redeem(alphaCode, alpha), redeem(alphaCode, alpha)]);
    assert.deepEqual(race.map(result => result.status).sort(), [200, 409]);
    const enrolledAlpha = race.find(result => result.status === 200).reply;
    assert.equal(enrolledAlpha.placement, "alpha");
    const alphaBinding = { placement: "alpha", runnerIncarnation: enrolledAlpha.incarnation };
    assert.equal((await permit(alphaBinding)).reply.key, alpha.key);
    assert.equal((await permit({ runnerIncarnation: enrolledAlpha.incarnation })).status, 503);
    const delivery = sendDelivery(1);
    assert.equal((await permit({ delivery }, "send")).reply.messageId, 42);

    const betaCode = authority.issue({ key: beta.key, alias: "desk" });
    const enrolledBeta = (await redeem(betaCode, beta)).reply;
    assert.equal(enrolledBeta.placement, "beta");
    assert.notEqual(enrolledBeta.incarnation, enrolledAlpha.incarnation);
    assert.deepEqual((await permit()).reply.routes, [
        { placement: "alpha", alias: null }, { placement: "beta", alias: "desk" },
    ]);
    const betaBinding = { placement: "beta", runnerIncarnation: enrolledBeta.incarnation };
    assert.equal((await permit(betaBinding)).reply.key, beta.key);
    assert.equal((await permit(alphaBinding)).reply.key, alpha.key);
    assert.equal((await permit({ ...betaBinding, runnerIncarnation: enrolledAlpha.incarnation })).status, 503);
    assert.equal((await permit({ ...betaBinding, objectIncarnation: randomUUID() })).status, 503);
    assert.equal((await permit({ ...betaBinding, target: { ...target, versionId: "wrong" } })).status, 503);
    const duplicate = card();
    const duplicateAlias = authority.issue({ key: duplicate.key, alias: "desk" });
    const duplicateKey = authority.issue({ key: beta.key, alias: "spare" });
    const missingAlias = authority.issue({ key: duplicate.key });
    for (const [code, runner] of [[duplicateAlias, duplicate], [duplicateKey, beta], [missingAlias, duplicate]]) {
        assert.equal((await redeem(code, runner)).status, 409);
    }
    assert.equal((await redeem(betaCode, beta)).status, 409);
    assert.equal((await permit()).reply.routes.length, 2);
    assert.equal((await permit({ delivery }, "send")).status, 409);
    assert.deepEqual(sends, [delivery]);
    authority.retire(1);
    assert.equal((await permit(alphaBinding)).status, 503);
    assert.equal((await redeem(duplicateAlias, duplicate)).status, 503);
});

async function fixture(context) {
    const root = await mkdtemp(join(tmpdir(), "authority-control-"));
    const path = join(root, "authority.sqlite");
    const received = [];
    const children = new Set();
    let handle = (_, response) => response.end(JSON.stringify({ ok: true, result: { message_id: 42 } }));
    const fake = createServer(async (request, response) => {
        assert.equal(request.method, "POST");
        assert.equal(request.url, `/bot${token}/sendMessage`);
        let body = "";
        for await (const chunk of request) body += chunk;
        received.push(JSON.parse(body));
        handle(request, response);
    });
    fake.listen(0, "127.0.0.1");
    await once(fake, "listening");
    const endpoint = `http://127.0.0.1:${fake.address().port}`;
    context.after(async () => {
        await Promise.all([...children].map(kill));
        fake.closeAllConnections();
        await new Promise(resolve => fake.close(resolve));
        await rm(root, { recursive: true, force: true });
    });
    const admin = async (command, ...args) => {
        const { stdout } = await exec(process.execPath, [cli, command, path, ...args], { env: {} });
        return JSON.parse(stdout);
    };
    const recordCommand = async (command, record) => {
        const input = join(root, `${randomUUID()}.json`);
        await writeFile(input, JSON.stringify(record), { mode: 0o600 });
        return admin(command, input);
    };
    async function start(version = credentialVersion) {
        const child = fork(fileURLToPath(import.meta.url), ["serve-test", path, endpoint, version], {
            env: {}, execArgv: [], stdio: ["ignore", "ignore", "inherit", "ipc"],
        });
        children.add(child);
        child.once("exit", () => children.delete(child));
        const port = await Promise.race([
            once(child, "message").then(([port]) => port),
            once(child, "exit").then(() => { throw new Error("authority exited before listening"); }),
        ]);
        return {
            stop: () => kill(child),
            pair: async body => {
                const response = await fetch(`http://127.0.0.1:${port}/v1/pair`, {
                    method: "POST", body: JSON.stringify(body), signal: AbortSignal.timeout(3000),
                });
                return { status: response.status, reply: response.ok ? await response.json() : null };
            },
            post: async (record, route = "check", extra = {}, key = bearer) => {
                const { deployment, ...pin } = record.target;
                const body = {
                    nonce: randomUUID(), target: { pin, versionId: record.versionId },
                    objectIncarnation: record.objectIncarnation, runnerIncarnation: null, delivery: null,
                    ...extra,
                };
                const response = await fetch(`http://127.0.0.1:${port}/v1/${route}`, {
                    method: "POST", headers: { authorization: `Bearer ${key}` },
                    body: JSON.stringify(body), signal: AbortSignal.timeout(3000),
                });
                const text = await response.text();
                assert.equal(response.headers.get("cache-control"), "no-store");
                return { status: response.status, reply: text ? JSON.parse(text) : null, body };
            },
        };
    }
    return {
        root, path, admin, publish: record => recordCommand("publish", record),
        issue: record => recordCommand("issue", record), start, received, respond: callback => { handle = callback; },
    };
}

async function kill(child) {
    const exited = once(child, "exit");
    child.kill("SIGKILL");
    assert.deepEqual(await exited, [null, "SIGKILL"]);
}

if (process.argv[2] === "serve-test") {
    const [, , , path, endpoint, credentialVersion] = process.argv;
    assert.equal(new URL(endpoint).hostname, "127.0.0.1");
    const server = serveAuthority(path, { bearer, credentialVersion }, telegramSender(endpoint, token));
    server.listen(0, "127.0.0.1", () => process.send(server.address().port));
} else {
    test("populated Control upgrades and recovers the same authenticated production pairing receipt", options, async context => {
        const { path, admin, publish, issue, start, received, respond } = await fixture(context);
        const legacy = publication(1);
        const paired = publication(2);
        delete paired.target.executorKey;
        delete paired.objectIncarnation;
        delete paired.runnerIncarnation;
        Object.assign(paired.target, { hostname: "bigfur.liu.nz", objectId: "5".repeat(64), pairing: "production-v1" });
        paired.continuity = "fresh-object-only";
        const objectIncarnation = randomUUID();
        await admin("init");
        await publish(legacy);
        let authority = await start();
        assert.equal((await authority.post(legacy, "send", { delivery: sendDelivery(1) })).status, 200);
        respond(request => request.socket.destroy());
        assert.equal((await authority.post(legacy, "send", { delivery: sendDelivery(2) })).status, 502);
        await admin("retire", "1");
        const stored = () => {
            const db = new DatabaseSync(path, { readOnly: true });
            try {
                return {
                    publication: db.prepare("SELECT publication FROM publications WHERE generation=1").get(),
                    retirement: db.prepare("SELECT generation FROM retirements").all(),
                    sends: db.prepare("SELECT * FROM sends ORDER BY generation,task,number").all(),
                };
            } finally {
                db.close();
            }
        };
        const legacyRows = stored();
        assert.deepEqual(legacyRows.sends.map(row => row.message_id), [42, null]);
        await publish(paired);
        assert.deepEqual(stored(), legacyRows);
        assert.equal((await authority.post(legacy, "send", { delivery: sendDelivery(3) })).status, 503);
        assert.equal(received.length, 2);
        const { publicKey, privateKey } = generateKeyPairSync("ed25519");
        const key = publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
        const { code: expired } = await issue({ key, ttlMs: 1 });
        await sleep(20);
        assert.equal((await authority.pair({
            code: expired, key, signature: sign(null, pairingChallenge(expired, true), privateKey).toString("hex"),
        })).status, 410);
        const { code } = await issue({ key });
        const request = { code, key, signature: sign(null, pairingChallenge(code, true), privateKey).toString("hex") };
        assert.equal((await authority.pair({
            ...request, signature: sign(null, pairingChallenge(code, false), privateKey).toString("hex"),
        })).status, 401);
        const attempts = await Promise.all([authority.pair(request), authority.pair(request)]);
        assert.equal(attempts[0].status, 200);
        assert.deepEqual(attempts[1], attempts[0]);
        const receipt = attempts[0].reply;
        assert.equal(receipt.placement, "alpha");
        assert.equal(receipt.generation, 2);
        assert.equal(receipt.objectId, paired.target.objectId);

        await authority.stop();
        authority = await start();
        assert.deepEqual(await authority.pair(request), attempts[0]);
        assert.equal((await authority.pair({ ...request, key: "6".repeat(64) })).status, 401);
        assert.equal((await authority.pair({ ...request, signature: "0".repeat(128) })).status, 401);
        assert.equal((await authority.pair({ ...request, alias: "other" })).status, 400);
        assert.deepEqual((await authority.post(paired, "check", { objectIncarnation })).reply.routes, [
            { placement: "alpha", alias: null },
        ]);
        assert.deepEqual(stored(), legacyRows);
        const unused = (await issue({ key })).code;
        await admin("retire", "2");
        assert.deepEqual(await authority.pair(request), attempts[0]);
        assert.equal((await authority.pair({
            code: unused, key, signature: sign(null, pairingChallenge(unused, true), privateKey).toString("hex"),
        })).status, 503);
        assert.equal((await authority.post(paired, "check", {
            objectIncarnation, placement: "alpha", runnerIncarnation: receipt.incarnation,
        })).reply.reconcileOnly, true);
        const successor = {
            ...paired, versionId: "version-3",
            target: { ...paired.target, generation: 3, objectId: "7".repeat(64), custodyDigest: "8".repeat(64) },
            retirement: { ...paired.retirement, generation: 2 },
            ingress: [{
                generation: 2, objectId: paired.target.objectId, custodyDigest: paired.target.custodyDigest, updateIds: [42],
            }],
        };
        await publish(successor);
        await authority.stop();
        authority = await start();
        assert.deepEqual(await authority.pair(request), attempts[0]);
        assert.equal((await authority.post(successor, "check", { objectIncarnation, updateId: 42 })).reply.duplicate, true);
        assert.equal((await authority.post(successor, "check", { objectIncarnation, updateId: 43 })).reply.duplicate, false);
    });

    test("observation records one authenticated exact non-agent candidate while denying every request", options, async context => {
        const record = publication(1);
        const observed = [];
        const server = observeAuthority({ target: record.target, versionId: record.versionId }, bearer,
            result => observed.push(result));
        server.listen(0, "127.0.0.1");
        await once(server, "listening");
        context.after(() => new Promise(resolve => server.close(resolve)));
        const { deployment, ...pin } = record.target;
        const body = {
            nonce: randomUUID(), target: { pin, versionId: record.versionId },
            objectIncarnation: record.objectIncarnation, runnerIncarnation: null, delivery: null,
        };
        const post = (body, key = bearer, route = "check") => fetch(`http://127.0.0.1:${server.address().port}/v1/${route}`, {
            method: "POST", headers: { authorization: `Bearer ${key}` }, body: JSON.stringify(body),
        }).then(response => response.status);
        assert.equal(await post(body, "b".repeat(64)), 401);
        assert.equal(await post({ ...body, target: { ...body.target, versionId: "previous-version" } }), 503);
        assert.equal(await post({ ...body, runnerIncarnation: record.runnerIncarnation }), 503);
        assert.equal(await post({ ...body, delivery: sendDelivery(1) }, bearer, "send"), 503);
        assert.deepEqual(observed, []);
        assert.equal(await post(body), 503);
        assert.equal(await post({ ...body, objectIncarnation: randomUUID() }), 503);
        assert.deepEqual(observed, [{ objectIncarnation: record.objectIncarnation }]);
    });

    test("publication, exact permits and retirement survive service restart and stale client restore", options, async context => {
        const { root, admin, publish, start, received } = await fixture(context);
        const first = publication(1);
        const next = publication(2);
        await assert.rejects(admin("status"));
        await assert.rejects(publish(first));
        assert.deepEqual(await admin("init"), { highWater: 0, retired: false });
        await assert.rejects(admin("init"));
        let authority = await start();
        assert.equal((await authority.post(first)).status, 503);
        await assert.rejects(publish(next));
        await assert.rejects(publish({ ...first, retirement: { generation: 0 } }));
        const concurrent = await Promise.allSettled([publish(first), publish(first)]);
        assert.equal(concurrent.filter(result => result.status === "fulfilled").length, 1);
        await assert.rejects(publish({ ...first, objectIncarnation: randomUUID() }));
        await assert.rejects(publish(next));
        const saved = join(root, "old-client.json");
        await writeFile(saved, JSON.stringify(first));
        const before = Date.now();
        const permit = await authority.post(first, "check", { runnerIncarnation: first.runnerIncarnation });
        assert.equal(permit.status, 200);
        const { delivery, ...echo } = permit.body;
        assert.deepEqual(permit.reply, { ...echo, highWater: 1, validUntil: permit.reply.validUntil });
        assert.ok(permit.reply.validUntil > Date.now() && permit.reply.validUntil <= before + 2000);
        assert.equal((await authority.post(first, "check", {}, "b".repeat(64))).status, 401);
        for (const [field, value] of Object.entries(first.target)) {
            if (field === "deployment") continue;
            const altered = { ...first, target: { ...first.target, [field]: String(value) + "0" } };
            assert.equal((await authority.post(altered)).status, 503, field);
        }
        assert.equal((await authority.post({ ...first, versionId: "other-version" })).status, 503);
        assert.equal((await authority.post(first, "check", { objectIncarnation: randomUUID() })).status, 503);
        assert.equal((await authority.post(first, "check", { runnerIncarnation: next.runnerIncarnation })).status, 503);
        assert.equal((await authority.post(first, "check", { nonce: "bad" })).status, 400);
        assert.equal((await authority.post(first, "send", {
            runnerIncarnation: first.runnerIncarnation, delivery: sendDelivery(1),
        })).status, 400);
        assert.deepEqual(await admin("retire", "1"), { highWater: 1, retired: true });
        await authority.stop();
        authority = await start();
        assert.deepEqual(await admin("retire", "1"), { highWater: 1, retired: true });
        assert.equal((await authority.post(first)).status, 503);
        assert.equal((await authority.post(first, "send", { delivery: { task: "bad" } })).status, 400);
        await assert.rejects(publish(first));
        assert.deepEqual(await publish(next), { highWater: 2, retired: false });
        await authority.stop();
        authority = await start();
        const restored = JSON.parse(await readFile(saved, "utf8"));
        assert.equal((await authority.post(restored)).status, 503);
        assert.equal((await authority.post(restored, "send", {
            delivery: sendDelivery(1),
        })).status, 503);
        assert.equal((await authority.post(next, "check", { objectIncarnation: randomUUID() })).status, 503);
        assert.equal((await authority.post(next)).reply.highWater, 2);
        await assert.rejects(admin("retire", "1"));
        assert.deepEqual(await admin("status"), { highWater: 2, retired: false });
        assert.deepEqual(received, []);
        await authority.stop();
        authority = await start("wrong-credential-version");
        assert.equal((await authority.post(next)).status, 503);
        await authority.stop();
    });

    test("placement publications bind exact keys and incarnations and fence predecessor reuse", options, async context => {
        const { admin, publish, start } = await fixture(context);
        const { executorKey, ...pin } = publication(1).target;
        const { runnerIncarnation, ...rest } = publication(1);
        const target = { ...pin, hostname: "127.0.0.1", executorKeys: { alpha: executorKey, beta: "5".repeat(64) } };
        const runners = { alpha: runnerIncarnation, beta: "6".repeat(64) };
        const first = { ...rest, target, runnerIncarnations: runners };
        await admin("init");
        await assert.rejects(publish({ ...first, runnerIncarnations: { alpha: runners.alpha } }));
        await assert.rejects(publish({ ...first, runnerIncarnations: { alpha: runners.alpha, beta: runners.alpha } }));
        await assert.rejects(publish({ ...first, target: { ...target, executorKeys: { alpha: executorKey, beta: executorKey } } }));
        await assert.rejects(publish({ ...first, target: { ...target, hostname: "bigfur.liu.nz" } }));
        await publish(first);
        let authority = await start();
        for (const [name, incarnation] of Object.entries(runners)) {
            const permit = await authority.post(first, "check", { placement: name, runnerIncarnation: incarnation });
            assert.equal(permit.status, 200);
            const { delivery, ...echo } = permit.body;
            assert.deepEqual(permit.reply, { ...echo, highWater: 1, validUntil: permit.reply.validUntil });
        }
        const beta = { placement: "beta", runnerIncarnation: runners.beta };
        assert.equal((await authority.post(first, "check", { runnerIncarnation: runners.beta })).status, 503);
        assert.equal((await authority.post(first, "check", { ...beta, placement: "alpha" })).status, 503);
        assert.equal((await authority.post(first, "check", { ...beta, placement: "gamma" })).status, 503);
        assert.equal((await authority.post(first, "check", { ...beta, objectIncarnation: randomUUID() })).status, 503);
        const changed = { ...first, target: { ...target, executorKeys: { ...target.executorKeys, beta: "7".repeat(64) } } };
        assert.equal((await authority.post(changed, "check", beta)).status, 503);
        assert.equal((await authority.post(first, "send", { ...beta, delivery: sendDelivery(1) })).status, 400);
        assert.equal((await authority.post(first, "send", { delivery: sendDelivery(1) })).status, 200);
        await admin("retire", "1");
        const second = {
            ...first,
            target: { ...target, generation: 2, objectId: "8".repeat(64) },
            retirement: { ...first.retirement, generation: 1 },
            objectIncarnation: randomUUID(),
            runnerIncarnations: { alpha: "7".repeat(64), beta: "8".repeat(64) },
        };
        await assert.rejects(publish({ ...second, target: { ...second.target, objectId: target.objectId } }));
        await assert.rejects(publish({ ...second, runnerIncarnations: { ...second.runnerIncarnations, alpha: runners.beta } }));
        await publish(second);
        await authority.stop();
        authority = await start();
        assert.equal((await authority.post(first, "check", beta)).status, 503);
        assert.equal((await authority.post(second, "check", beta)).status, 503);
        assert.equal((await authority.post(second, "check", {
            placement: "beta", runnerIncarnation: second.runnerIncarnations.beta,
        })).status, 200);
    });

    test("a lost reply or process kill after dispatch never resends a reservation", options, async context => {
        const { admin, publish, start, received, respond } = await fixture(context);
        const record = publication(1);
        await admin("init");
        await publish(record);
        let authority = await start();
        const send = (number, extra) => authority.post(record, "send", { delivery: sendDelivery(number, extra) });
        const acknowledged = await send(1);
        assert.deepEqual(acknowledged.reply, { nonce: acknowledged.body.nonce, messageId: 42 });
        respond(request => request.socket.destroy());
        assert.equal((await send(2)).status, 502);
        assert.equal((await send(2, { text: "changed payload" })).status, 409);
        const accepted = Promise.withResolvers();
        respond(() => accepted.resolve());
        const lost = assert.rejects(send(3));
        await accepted.promise;
        await authority.stop();
        await lost;

        authority = await start();
        for (const number of [1, 2, 3]) assert.equal((await send(number)).status, 409);
        assert.deepEqual(received, Array(3).fill({ chat_id: -321, message_thread_id: 37, text: "synthetic reply" }));
        assert.equal((await authority.post(record)).reply.highWater, 1);
        respond((_, response) => response.end(JSON.stringify({ ok: true, result: { message_id: 0 } })));
        assert.equal((await send(4, { messageThreadId: null })).status, 502);
        assert.equal((await send(4)).status, 409);
        assert.deepEqual(received.at(-1), { chat_id: -321, text: "synthetic reply" });
        assert.equal(received.length, 4);
    });

    test("the dispatch writer orders retirement across processes and denies egress after retirement", options, async context => {
        const { admin, publish, start, received, respond } = await fixture(context);
        const record = publication(1);
        await admin("init");
        await publish(record);
        const authority = await start();
        const peer = await start();
        const accepted = Promise.withResolvers();
        respond((_, response) => accepted.resolve(response));
        const sent = authority.post(record, "send", { delivery: sendDelivery(1) });
        const fakeReply = await accepted.promise;
        assert.equal((await authority.post(record, "check", { deliveryCheck: true })).status, 503);
        assert.equal((await peer.post(record, "check", { deliveryCheck: true })).status, 503);
        assert.equal((await authority.post(record)).status, 200);
        assert.equal((await peer.post(record, "send", { delivery: sendDelivery(2) })).status, 503);
        let retired = false;
        const retirement = admin("retire", "1").then(status => {
            retired = true;
            return status;
        });
        await sleep(100);
        assert.equal(retired, false);
        fakeReply.end(JSON.stringify({ ok: true, result: { message_id: 43 } }));
        assert.equal((await sent).reply.messageId, 43);
        assert.deepEqual(await retirement, { highWater: 1, retired: true });
        assert.equal((await peer.post(record, "send", { delivery: sendDelivery(2) })).status, 503);
        assert.equal((await authority.post(record, "check", { deliveryCheck: true })).status, 410);
        assert.equal((await authority.post(record)).status, 503);
        assert.equal(received.length, 1);
    });
}
