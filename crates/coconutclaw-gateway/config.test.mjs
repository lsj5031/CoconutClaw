import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { readBuildOutput } from "@cloudflare/build-output-utils";
import { loadAndParseConfig } from "@cloudflare/config";

test("gateway config separates public production, external legacy authority, and private staging", async () => {
    for (const isPreview of [false, true]) {
        for (const mode of [undefined, "production", "phase2-cutover", "phase2-local", "phase2-pilot",
            "phase2-production", "phase2-production-control", "phase2-control-staging"]) {
            const { result } = await loadAndParseConfig(join(import.meta.dirname, "cloudflare.config.ts"), {
                isPreview, mode,
            });
            if (!result.success) throw result.error;
            const { worker } = result.data;
            const pilot = !isPreview && mode === "phase2-pilot";
            const staging = !isPreview && mode === "phase2-control-staging";
            const bound = staging || (!isPreview && mode === "phase2-production-control");
            const production = bound || (!isPreview && mode === "phase2-production");
            assert.deepEqual(worker.domains, pilot || (production && !staging) ? ["bigfur.liu.nz"] : undefined,
                JSON.stringify({ isPreview, mode }));
            assert.equal(worker.env.PHASE.value, pilot ? mode : production ? "phase2-production" : "disabled");
            assert.equal(worker.workersDev, false);
            assert.equal(worker.previewUrls, false);
            assert.equal(worker.routes, undefined);
            assert.equal(worker.triggers, undefined);
            assert.equal(worker.env.LOCAL_GENERATION, undefined);
            assert.equal(worker.env.PILOT_GATE?.type, pilot ? "rate-limit" : undefined);
            assert.equal(worker.env.CF_VERSION_METADATA?.type, production ? "version-metadata" : undefined);
            assert.equal(worker.env.CUTOVER_AUTHORITY_ORIGIN?.type, production && !bound ? "secret" : undefined);
            assert.deepEqual(worker.env.CONTROL, bound ? {
                type: "worker", worker: staging ? "coconutclaw-bigfur-control-staging" : "coconutclaw-bigfur-control",
            } : undefined);
            assert.equal(worker.env.CONTROL_REQUIRED?.value, bound ? "1" : undefined);
            for (const name of ["CUTOVER_TARGET", "CUTOVER_AUTHORITY_BEARER",
                "CUTOVER_CANCEL_SECRET", "PRODUCTION_TELEGRAM_SCOPE", "WEBHOOK_SECRET"]) {
                assert.equal(worker.env[name]?.type, production ? "secret" : undefined, name);
            }
            assert.equal(worker.env.CONTROL_OPERATOR_BEARER, undefined);
            assert.equal(worker.env.CONTROL_TELEGRAM_BOT_TOKEN, undefined);
        }
    }
});

test("Control owns a private SQLite namespace and exposes the operator on a named entrypoint", async () => {
    for (const [isPreview, mode] of [[false, undefined], [false, "staging"], [true, undefined]]) {
        const { result } = await loadAndParseConfig(join(import.meta.dirname, "control/cloudflare.config.ts"), {
            isPreview, mode,
        });
        if (!result.success) throw result.error;
        const { worker } = result.data;
        const suffix = isPreview || mode === "staging" ? "-staging" : "";
        assert.equal(worker.name, `coconutclaw-bigfur-control${suffix}`);
        assert.equal(worker.workersDev, false);
        assert.equal(worker.previewUrls, false);
        assert.equal(worker.routes, undefined);
        assert.equal(worker.domains, undefined);
        assert.equal(worker.triggers, undefined);
        assert.deepEqual(worker.exports, {
            ControlLedger: { type: "durable-object", storage: "sqlite" },
            ControlOperator: { type: "worker" },
        });
        assert.deepEqual(worker.env.CONTROL_LEDGER, {
            type: "durable-object", worker: worker.name, exportName: "ControlLedger",
        });
        for (const name of ["AUTOMATION_MODE", "AUTOMATION_REPOSITORY", "AUTOMATION_METADATA", "AUTOMATION_TELEGRAM"]) {
            assert.equal(worker.env[name], undefined, name);
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
