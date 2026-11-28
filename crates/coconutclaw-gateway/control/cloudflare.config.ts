import { bindings, defineConfig, exports } from "@cloudflare/config";

const ledger = "ControlLedger";

export default defineConfig(({ isPreview, mode }) => {
    const name = isPreview || mode === "staging" ? "coconutclaw-bigfur-control-staging" : "coconutclaw-bigfur-control";
    return {
        worker: {
            name,
            entrypoint: "control-worker.mjs",
            compatibilityDate: "2026-07-30",
            compatibilityFlags: ["nodejs_compat"],
            workersDev: false,
            previewUrls: false,
            exports: {
                [ledger]: exports.durableObject({ storage: "sqlite" }),
                ControlOperator: exports.worker(),
            },
            env: {
                CONTROL_LEDGER: bindings.durableObject({ worker: name, exportName: ledger }),
                CUTOVER_AUTHORITY_BEARER: bindings.secret(),
                CONTROL_OPERATOR_BEARER: bindings.secret(),
                CONTROL_TELEGRAM_CREDENTIAL_VERSION: bindings.secret(),
                CONTROL_TELEGRAM_BOT_TOKEN: bindings.secret(),
            },
        },
    };
});
