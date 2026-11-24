import { bindings, defineConfig, exports } from "@cloudflare/config";

const name = "coconutclaw-phase1-test";
const ledger = "BotLedger";

export default defineConfig(({ isPreview, mode }) => ({
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
            PHASE: bindings.text(!isPreview && mode === "phase2-pilot" ? mode : "disabled"),
            ...(!isPreview && mode === "phase2-pilot" ? {
                PILOT_TOKEN: bindings.secret(),
                PILOT_WINDOW: bindings.secret(),
                PILOT_ALPHA_KEY: bindings.secret(),
                PILOT_GATE: bindings.rateLimit({
                    namespace: "23001",
                    simple: { limit: 16, period: 10 },
                }),
            } : {}),
        },
    },
}));
