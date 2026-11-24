import assert from "node:assert/strict";
import { execFile, fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { observeAuthority, serveAuthority, telegramSender } from "./authority-control.mjs";

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
    const publish = async record => {
        const input = join(root, `${randomUUID()}.json`);
        await writeFile(input, JSON.stringify(record), { mode: 0o600 });
        return admin("publish", input);
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
    return { root, admin, publish, start, received, respond: callback => { handle = callback; } };
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
        assert.equal((await authority.post(record)).status, 503);
        assert.equal(received.length, 1);
    });
}
