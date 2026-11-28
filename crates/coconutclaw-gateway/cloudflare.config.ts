import { bindings, defineConfig, exports } from "@cloudflare/config";

const ledger = "BotLedger";

export default defineConfig(({ isPreview, mode }) => {
    const pilot = !isPreview && mode === "phase2-pilot";
    const staging = !isPreview && mode === "phase2-control-staging";
    const bound = staging || (!isPreview && mode === "phase2-production-control");
    const production = bound || (!isPreview && mode === "phase2-production");
    const name = staging ? "coconutclaw-bigfur-gateway-staging" : "coconutclaw-phase1-test";
    return {
        worker: {
            name,
            entrypoint: "worker.mjs",
            compatibilityDate: "2026-07-30",
            workersDev: false,
            previewUrls: false,
            ...(pilot || (production && !staging) ? { domains: ["bigfur.liu.nz"] } : {}),
            exports: {
                [ledger]: exports.durableObject({ storage: "sqlite" }),
            },
            env: {
                BOTS: bindings.durableObject({ worker: name, exportName: ledger }),
                PHASE: bindings.text(production ? "phase2-production" : pilot ? "phase2-pilot" : "disabled"),
                ...(production ? {
                    CF_VERSION_METADATA: bindings.versionMetadata(),
                    CUTOVER_TARGET: bindings.secret(),
                    ...(bound ? {
                        CONTROL: bindings.worker({ worker: staging ? "coconutclaw-bigfur-control-staging" : "coconutclaw-bigfur-control" }),
                        CONTROL_REQUIRED: bindings.text("1"),
                    } : { CUTOVER_AUTHORITY_ORIGIN: bindings.secret() }),
                    CUTOVER_AUTHORITY_BEARER: bindings.secret(),
                    CUTOVER_CANCEL_SECRET: bindings.secret(),
                    PRODUCTION_TELEGRAM_SCOPE: bindings.secret(),
                    WEBHOOK_SECRET: bindings.secret(),
                } : {}),
                ...(pilot ? {
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
    };
});
