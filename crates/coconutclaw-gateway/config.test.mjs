import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { readBuildOutput } from "@cloudflare/build-output-utils";
import { loadAndParseConfig } from "@cloudflare/config";

test("only non-preview phase2-pilot selects the custom domain", async () => {
    for (const isPreview of [false, true]) {
        for (const mode of [undefined, "production", "phase2-local", "phase2-pilot"]) {
            const { result } = await loadAndParseConfig(join(import.meta.dirname, "cloudflare.config.ts"), {
                isPreview, mode,
            });
            if (!result.success) throw result.error;
            const { worker } = result.data;
            const pilot = !isPreview && mode === "phase2-pilot";
            assert.deepEqual(worker.domains, pilot ? ["bigfur.liu.nz"] : undefined, JSON.stringify({ isPreview, mode }));
            assert.equal(worker.env.PHASE.value, pilot ? "phase2-pilot" : "disabled");
            assert.equal(worker.workersDev, false);
            assert.equal(worker.previewUrls, false);
            assert.equal(worker.routes, undefined);
            assert.equal(worker.triggers, undefined);
            assert.equal(worker.env.LOCAL_GENERATION, undefined);
        }
    }
});

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
    assert.equal(config.domains, undefined);
    assert.equal(config.routes, undefined);
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
