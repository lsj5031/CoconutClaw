import assert from "node:assert/strict";
import { generateKeyPairSync, randomUUID, sign } from "node:crypto";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { controlWorkerFixture, credentialVersion, operatorBearer } from "./control-worker-fixture.mjs";
import { snapshotDigest } from "./control-migration.mjs";
import { pairingChallenge } from "./authority-preflight.mjs";
import { authorityFixture, bearer, origin } from "./runtime-authority-fixture.mjs";

const versionId = "service-binding-version";
const target = {
    pin: {
        generation: 1, revision: "e".repeat(40), hostname: "bigfur.liu.nz", objectId: "b".repeat(64),
        custodyDigest: "c".repeat(64), telegramCredentialVersion: credentialVersion, pairing: "production-v1",
    },
    versionId: "legacy-gateway-version",
};
const rebound = { ...target, versionId };
const objectIncarnation = randomUUID();
const request = (options = {}) => ({
    nonce: randomUUID(), target: rebound, objectIncarnation, runnerIncarnation: null, delivery: null, ...options,
});
const delivery = (task, extras = {}) => ({
    task, sendNumber: 1, chatId: 321, messageThreadId: 37, text: "synthetic result", ...extras,
});
const runner = () => {
    const keys = generateKeyPairSync("ed25519");
    const key = keys.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
    return { key, redeem: code => ({
        code, key, signature: sign(null, pairingChallenge(code, true), keys.privateKey).toString("hex"),
    }) };
};
async function expect(response, status) {
    const result = await response;
    assert.equal(result.status, status, await result.clone().text());
    if (result.ok) return result.json();
    await result.text();
}
async function fixture(context) {
    const root = await mkdtemp(join(tmpdir(), "bigfur-control-worker-"));
    const path = join(root, "legacy.sqlite");
    const persistence = join(root, "control-do");
    const control = await controlWorkerFixture(context, persistence);
    const legacy = await authorityFixture(path);
    legacy.publish(target, null);
    let legacyClosed = false;
    async function closeLegacy() {
        if (!legacyClosed) await legacy.close();
        legacyClosed = true;
    }
    context.after(async () => {
        await closeLegacy();
        await rm(root, { recursive: true, force: true });
    });
    const migrate = async () => {
        await closeLegacy();
        const source = legacy.snapshot();
        return { source, sourceSha256: snapshotDigest(source), gatewayVersion: versionId };
    };
    return { control, legacy, migrate, persistence };
}

test("private Control atomically imports the active generation, registry, and every kind of send evidence", async context => {
    const { control, legacy, migrate } = await fixture(context);
    const alpha = runner();
    const beta = runner();
    const gamma = runner();
    const alphaSigned = alpha.redeem(legacy.issue({ key: alpha.key }));
    const betaSigned = beta.redeem(legacy.issue({ key: beta.key, alias: "desk" }));
    const gammaSigned = gamma.redeem(legacy.issue({ key: gamma.key, alias: "travel" }));
    const pair = signed => fetch(`${legacy.endpoint}/v1/pair`, { method: "POST", body: JSON.stringify(signed) });
    const alphaReceipt = await expect(pair(alphaSigned), 200);
    const betaReceipt = await expect(pair(betaSigned), 200);
    legacy.retireRunner({ placement: betaReceipt.placement, incarnation: betaReceipt.incarnation });
    const legacySend = value => legacy.fetch(new Request(`${origin}/v1/send`, {
        method: "POST", headers: { authorization: `Bearer ${bearer}` },
        body: JSON.stringify(request({ target, delivery: value })),
    }));
    await expect(legacySend(delivery("delivered")), 200);
    await expect(legacySend(delivery("closed", { closeUnsent: true })), 200);
    legacy.holdSend = () => { throw new Error("synthetic lost response"); };
    await expect(legacySend(delivery("ambiguous")), 503);
    const migration = await migrate();
    await expect(control.runtime("operator/import", migration, operatorBearer), 404);
    await expect(control.operator("import", migration, "0".repeat(64)), 401);
    await expect(control.runtime("check", request()), 503);
    const corrupt = structuredClone(migration);
    corrupt.source.sends[0].generation = 2;
    corrupt.sourceSha256 = snapshotDigest(corrupt.source);
    await expect(control.operator("import", corrupt), 400);
    assert.equal((await expect(control.operator("status"), 200)).highWater, 0);
    const imported = await expect(control.operator("import", migration), 200);
    assert.deepEqual(await expect(control.operator("import", migration), 200), imported);
    await expect(control.operator("import", { ...migration, gatewayVersion: "other-version" }), 409);
    const readback = await expect(control.operator("snapshot"), 200);
    assert.deepEqual(readback.source, migration.source);
    assert.equal(readback.sourceSha256, migration.sourceSha256);
    assert.deepEqual(readback.source.sends.map(row => [row.task, row.message_id]),
        [["ambiguous", null], ["closed", 0], ["delivered", 1]]);
    assert.equal((await expect(control.operator("status"), 200)).retired, false);
    assert.equal(imported.generation, 1);
    await expect(control.runtime("check", request({ target })), 503);
    await expect(control.runtime("check", request(), "0".repeat(64)), 401);
    await expect(control.runtime("check", request(), undefined, { method: "GET", body: undefined }), 405);
    await expect(control.runtime("check", request(), undefined, { body: " ".repeat(32 * 1024 + 1) }), 413);
    await expect(control.runtime("check", request({ deliveryCheck: true })), 503);

    await control.restart(true);
    assert.deepEqual((await expect(control.runtime("check", request()), 200)).routes,
        [{ placement: "alpha", alias: null }]);
    assert.deepEqual(await expect(control.runtime("pair", alphaSigned), 200), alphaReceipt);
    assert.deepEqual(await expect(control.runtime("pair", betaSigned), 200), betaReceipt);
    await expect(control.runtime("pair", { ...betaSigned, signature: alphaSigned.signature }), 401);
    assert.equal((await expect(control.runtime("pair", gammaSigned), 200)).placement, "gamma");
    const betaCheck = request({ placement: "beta", runnerIncarnation: betaReceipt.incarnation });
    assert.equal((await expect(control.runtime("check", betaCheck), 200)).reconcileOnly, true);
    await expect(control.runtime("check", { ...betaCheck, runnerIncarnation: alphaReceipt.incarnation }), 503);
    for (const task of ["delivered", "closed", "ambiguous"]) {
        await expect(control.runtime("send", request({ delivery: delivery(task) })), 409);
    }
    await expect(control.runtime("send", request({ delivery: delivery("ambiguous", { closeUnsent: true }) })), 409);
    assert.deepEqual(control.sends, []);
    await expect(control.operator("retire", { generation: 1 }), 200);
    const publication = JSON.parse(readback.source.publications[0].publication);
    const successor = {
        ...publication,
        target: { ...target.pin, deployment: "synthetic/successor", generation: 2, objectId: "9".repeat(64) },
        versionId: "successor-version",
        retirement: { ...publication.retirement, generation: 1 },
    };
    await expect(control.operator("publish", successor), 503);
    const { generation, objectId, custodyDigest } = target.pin;
    successor.ingress = [{ generation, objectId, custodyDigest, updateIds: [46501] }];
    assert.deepEqual(await expect(control.operator("publish", successor), 200), { highWater: 2, retired: false });
    await control.restart(true);
    const next = { ...rebound, pin: { ...target.pin, generation: 2, objectId: "9".repeat(64) }, versionId: successor.versionId };
    assert.equal((await expect(control.runtime("check", request({ target: next, updateId: 46501 })), 200)).duplicate, true);
    assert.equal((await expect(control.runtime("check", request({ target: next, updateId: 46502 })), 200)).duplicate, false);
    assert.equal((await expect(control.runtime("check", betaCheck), 200)).highWater, 2);
    await expect(control.runtime("check", { ...betaCheck, target }), 503);
    await expect(control.runtime("send", request({ delivery: delivery("new") })), 503);
});

test("Control commits before Telegram, holds retirement during dispatch, and never replays a spent slot after reopen", async context => {
    const { control, migrate, persistence } = await fixture(context);
    await expect(control.operator("import", await migrate()), 200);
    await control.restart(true);
    const { promise: entered, resolve: observed, reject: rejectObserved } = Promise.withResolvers();
    const { promise: held, resolve: release } = Promise.withResolvers();
    control.onSend = async payload => {
        try {
            assert.deepEqual(payload, { chat_id: 321, message_thread_id: 37, text: "synthetic result" });
            const paths = (await readdir(persistence, { recursive: true }))
                .filter(name => name.includes("ControlLedger") && /^[a-f0-9]{64}\.sqlite$/.test(basename(name)));
            assert.equal(paths.length, 1);
            const db = new DatabaseSync(join(persistence, paths[0]), { readOnly: true });
            try {
                const reservation = db.prepare("SELECT message_id FROM sends WHERE generation=1 AND task='lost' AND number=1").get();
                assert.equal(reservation.message_id, null);
            } finally {
                db.close();
            }
            observed();
        } catch (error) {
            rejectObserved(error);
        }
        await held;
        return new Response(null, { status: 502 });
    };
    const lost = request({ delivery: delivery("lost") });
    const sending = control.runtime("send", lost);
    try {
        await Promise.race([entered, sending.then(response => assert.fail(`Telegram mock never entered (${response.status})`)),
            sleep(4000, undefined, { ref: false }).then(() => assert.fail("Telegram mock deadline"))]);
        assert.equal((await expect(control.operator("status"), 200)).dispatching, true);
        await expect(control.operator("retire", { generation: 1 }), 503);
        await expect(control.runtime("check", request({ deliveryCheck: true })), 503);
        await expect(control.runtime("send", lost), 503);
        assert.equal((await expect(control.operator("status"), 200)).retired, false);
    } finally {
        release();
    }
    await expect(sending, 502);
    await control.restart(true);
    await expect(control.runtime("send", lost), 409);
    await expect(control.runtime("send", request({ delivery: delivery("lost", { closeUnsent: true }) })), 409);
    delete control.onSend;
    const safe = request({ delivery: delivery("unissued", { closeUnsent: true }) });
    assert.deepEqual(await expect(control.runtime("send", safe), 200), { nonce: safe.nonce, noSend: true });
    await expect(control.runtime("send", request({ delivery: delivery("unissued") })), 409);
    const next = request({ delivery: delivery("unissued", { sendNumber: 2 }) });
    assert.deepEqual(await expect(control.runtime("send", next), 200), { nonce: next.nonce, messageId: 2 });
    assert.deepEqual(await expect(control.operator("retire", { generation: 1 }), 200), { highWater: 1, retired: true });
    await control.restart(true);
    await expect(control.runtime("check", request({ deliveryCheck: true })), 410);
    await expect(control.runtime("send", request({ delivery: delivery("fresh") })), 503);
    const { source } = await expect(control.operator("snapshot"), 200);
    assert.deepEqual(source.sends.map(row => [row.task, row.number, row.message_id]),
        [["lost", 1, null], ["unissued", 1, 0], ["unissued", 2, 2]]);
    assert.equal(control.sends.length, 2);
});
