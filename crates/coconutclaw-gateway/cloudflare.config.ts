import { bindings, defineConfig, exports } from "@cloudflare/config";

const name = "coconutclaw-phase1-test";
const ledger = "BotLedger";

export default defineConfig({
    worker: {
        name,
        entrypoint: "worker.mjs",
        compatibilityDate: "2026-07-30",
        workersDev: false,
        previewUrls: false,
        exports: {
            [ledger]: exports.durableObject({ storage: "sqlite" }),
        },
        env: {
            BOTS: bindings.durableObject({ worker: name, exportName: ledger }),
            PHASE: bindings.text("disabled"),
        },
    },
});
