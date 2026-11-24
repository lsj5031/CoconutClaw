import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { readBuildOutput } from "@cloudflare/build-output-utils";

test("prebuilt output stages one disabled Worker with one SQLite class", async () => {
    const { rootConfig, workers, containers } = await readBuildOutput(import.meta.dirname);
    assert.deepEqual(rootConfig, { buildContext: { isPreview: false } });
    assert.deepEqual(Object.keys(workers), ["default"]);
    assert.deepEqual(containers, []);

    const { assetsDir, bundleDir, config } = workers.default;
    assert.equal(assetsDir, undefined);
    assert.equal(config.name, "coconutclaw-phase1-test");
    assert.equal(config.compatibilityDate, "2026-07-30");
    assert.equal(config.workersDev, false);
    assert.equal(config.previewUrls, false);
    assert.equal(config.triggers, undefined);
    assert.deepEqual(config.env, {
        BOTS: { type: "durable-object", worker: config.name, exportName: "BotLedger" },
        PHASE: { type: "text", value: "disabled" },
    });
    assert.deepEqual(config.exports, {
        BotLedger: { type: "durable-object", storage: "sqlite" },
    });
    const { type: manifestType, mainModule, modules } = config.manifest;
    assert.equal(manifestType, "complete");
    assert.equal(mainModule, "worker.mjs");
    assert.equal(modules[mainModule].type, "esm");
    assert.equal(modules["build/worker/coconutclaw_gateway.js"].type, "esm");
    assert.deepEqual(Object.keys(modules).filter(name => modules[name].type === "wasm"), [
        "build/worker/coconutclaw_gateway_bg.wasm",
    ]);
    for (const [modulePath, { type }] of Object.entries(modules)) {
        const bytes = await readFile(join(bundleDir, modulePath));
        assert.ok(bytes.length > 0, modulePath);
        if (type === "wasm") assert.ok(WebAssembly.validate(bytes), modulePath);
    }
});
