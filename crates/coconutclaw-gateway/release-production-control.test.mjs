import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

function fixture(context) {
    const root = mkdtempSync(join(tmpdir(), "bigfur-release-test-"));
    context.after(() => rmSync(root, { recursive: true, force: true }));
    for (const file of ["package.json", "release-production-control.mjs", "authority-core.mjs", "authority-preflight.mjs",
        "stage.mjs", "cloudflare.config.ts", "worker.mjs"]) {
        copyFileSync(join(import.meta.dirname, file), join(root, file));
    }
    const output = join(root, ".cloudflare/output/v0");
    const bin = join(root, "node_modules/.bin");
    mkdirSync(bin, { recursive: true });
    symlinkSync(join(import.meta.dirname, "node_modules/@cloudflare"), join(root, "node_modules/@cloudflare"), "junction");
    mkdirSync(join(root, "build/worker"), { recursive: true });
    writeFileSync(join(root, "build/worker/fixture.js"), "export default {};");
    execFileSync(process.execPath, ["stage.mjs", "phase2-production-control"], { cwd: root });
    const setBuildContext = (mode, isPreview = false) => writeFileSync(join(output, "config.json"),
        JSON.stringify({ buildContext: { mode, isPreview } }));
    const called = join(root, "deploy-called");
    const credential = "synthetic-private-credential";
    writeFileSync(join(bin, "cf"), `#!/usr/bin/env node
        const assert = require("node:assert/strict");
        const fs = require("node:fs");
        const args = process.argv.slice(2);
        fs.writeFileSync("deploy-called", args[5] ?? "");
        assert.deepEqual(args.slice(0, 5), ["deploy", "--prebuilt", "--mode", "phase2-production-control", "--secrets-file"]);
        assert.equal(args.length, 6);
        assert.equal(fs.statSync(args[5]).mode & 0o777, 0o600);
        const staged = JSON.parse(fs.readFileSync("secrets.json"));
        staged.CUTOVER_TARGET = JSON.stringify(JSON.parse(staged.CUTOVER_TARGET));
        assert.deepEqual(JSON.parse(fs.readFileSync(args[5])), staged);
        console.log(${JSON.stringify(credential)});
    `, { mode: 0o700 });

    const pin = {
        generation: 1, revision: "a".repeat(40), hostname: "bigfur.liu.nz",
        objectId: "b".repeat(64), custodyDigest: "c".repeat(64),
        telegramCredentialVersion: "synthetic-credential-version", pairing: "production-v1",
    };
    const publication = {
        target: { ...pin, deployment: "synthetic-deployment" },
        versionId: "synthetic-version",
        continuity: "fresh-object-only",
        retirement: {
            generation: 0, writers: "synthetic-writers", executorGrants: "synthetic-grants", custody: "synthetic-custody",
            telegram: { disposition: "issuer-invalidated", evidence: "synthetic-telegram" },
        },
    };
    const authority = (...args) => execFileSync(process.execPath,
        [join(import.meta.dirname, "authority-control.mjs"), ...args], { cwd: root });
    const publish = record => {
        writeFileSync(join(root, "publication.json"), JSON.stringify(record));
        authority("publish", "authority.db", "publication.json");
    };
    authority("init", "authority.db");
    publish(publication);
    const current = { ...pin, generation: 2, objectId: "e".repeat(64), custodyDigest: "f".repeat(64) };
    authority("retire", "authority.db", "1");
    publish({
        ...publication,
        target: { ...current, deployment: "successor-deployment" },
        retirement: { ...publication.retirement, generation: 1 },
        ingress: [{ generation: 1, objectId: pin.objectId, custodyDigest: pin.custodyDigest, updateIds: [] }],
    });
    const exportSource = () => {
        const bytes = authority("export", "authority.db");
        writeFileSync(join(root, "source.json"), bytes);
        return createHash("sha256").update(bytes).digest("hex");
    };
    const digest = exportSource();
    const stageSecrets = (target = current) => writeFileSync(join(root, "secrets.json"),
        JSON.stringify({ CUTOVER_TARGET: JSON.stringify(target, null, 2), WEBHOOK_SECRET: credential }));
    const reorderedPin = JSON.stringify(Object.fromEntries(Object.entries(current).reverse()), null, 2);
    writeFileSync(join(root, "secrets.json"), JSON.stringify({
        CUTOVER_TARGET: reorderedPin.replace("{", '{"generation":1,').replace('"generation": 2', '"generation": 2.0'),
        WEBHOOK_SECRET: credential,
    }));
    const run = (name = "release", args = ["source.json", digest, "secrets.json"]) => {
        const result = spawnSync("npm", ["run", "--silent", `${name}:production-control`, "--", ...args], {
            cwd: root, encoding: "utf8",
        });
        assert.ifError(result.error);
        assert.equal((result.stdout + result.stderr).includes(credential), false);
        assert.equal((result.stdout + result.stderr).includes(pin.telegramCredentialVersion), false);
        return result;
    };
    return { called, digest, current, pin, root, output, run, stageSecrets, setBuildContext, authority, exportSource };
}

test("matching verified pin reaches the real release entry point's mocked cf with checked secrets", context => {
    const { called, run } = fixture(context);
    const check = run("preflight");
    assert.equal(check.status, 0, check.stderr);
    assert.equal(existsSync(called), false);
    const release = run();
    assert.equal(release.status, 0, release.stderr);
    assert.equal(existsSync(called), true);
    const privateCopy = readFileSync(called, "utf8");
    assert.ok(privateCopy.endsWith("/secrets.json"));
    assert.equal(existsSync(privateCopy), false);
});

test("stale staged production secrets stop the real release before cf", context => {
    const { called, current, pin, run, stageSecrets } = fixture(context);
    for (const change of [
        pin, { generation: 1 }, { revision: "d".repeat(40) }, { hostname: "other.invalid" },
        { objectId: "d".repeat(64) }, { custodyDigest: "d".repeat(64) },
        { pairing: undefined }, { telegramCredentialVersion: "older-version" },
        { deployment: "synthetic-deployment" }, { extra: "unexpected" },
    ]) {
        stageSecrets({ ...current, ...change });
        const release = run();
        assert.equal(release.status, 1);
        assert.match(release.stderr, /release preflight failed/);
        assert.equal(existsSync(called), false);
    }
});

test("release fails closed for missing, unverifiable, retired or incorrectly staged input", context => {
    const { called, digest, root, output, run, setBuildContext, authority, exportSource } = fixture(context);
    for (const args of [[], ["missing", digest, "secrets.json"], ["source.json", "0".repeat(64), "secrets.json"],
        ["source.json", digest, "missing"]]) {
        assert.equal(run("release", args).status, 1);
    }
    setBuildContext("phase2-production");
    assert.equal(run().status, 1);
    setBuildContext("phase2-production-control", true);
    assert.equal(run().status, 1);
    setBuildContext("phase2-production-control");
    const source = join(root, "source.json");
    const verifiedSource = readFileSync(source);
    writeFileSync(source, Buffer.concat([verifiedSource, Buffer.from("\n")]));
    assert.equal(run().status, 1);
    const malformed = Buffer.from("{ invalid json");
    writeFileSync(source, malformed);
    assert.equal(run("release", ["source.json", createHash("sha256").update(malformed).digest("hex"), "secrets.json"]).status, 1);
    authority("retire", "authority.db", "2");
    assert.equal(run("release", ["source.json", exportSource(), "secrets.json"]).status, 1);
    writeFileSync(source, verifiedSource);
    rmSync(join(output, "workers"), { recursive: true });
    assert.equal(run().status, 1);
    assert.equal(existsSync(called), false);
});
