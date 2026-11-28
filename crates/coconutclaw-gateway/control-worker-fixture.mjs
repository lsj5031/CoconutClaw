import assert from "node:assert/strict";
import { join } from "node:path";
import { Miniflare } from "miniflare";
import { bearer } from "./runtime-authority-fixture.mjs";

export const operatorBearer = "f".repeat(64);
export const credentialVersion = "synthetic-existing-token";
const token = "123:synthetic-control-worker";

export async function controlWorkerFixture(context, persistence, {
    bundleDir = process.env.GATEWAY_CONTROL_TEST_BUNDLE_DIR ?? import.meta.dirname,
    automation = false,
} = {}) {
    const sends = [];
    const reads = [];
    let service;
    async function restart(sender = false) {
        await service?.dispose();
        service = new Miniflare({
            host: "127.0.0.1",
            durableObjectsPersist: persistence,
            workers: [{
                name: "fixture-ingress",
                compatibilityDate: "2026-07-30",
                modules: true,
                script: `
                    export default {
                        async fetch(request, env) {
                            const url = new URL(request.url);
                            if (url.hostname === "fault.invalid" && env.FIXTURE_LEDGER) {
                                try {
                                    const stub = env.FIXTURE_LEDGER.getByName("bigfur-control-v1");
                                    const result = await stub.fixtureCommand(await request.json());
                                    return Response.json(result ?? null);
                                } catch {
                                    return new Response(null, { status: 503 });
                                }
                            }
                            return (url.hostname === "operator.invalid" ? env.OPERATOR : env.CONTROL).fetch(request);
                        }
                    };
                `,
                serviceBindings: {
                    CONTROL: "fixture-control",
                    OPERATOR: { name: "fixture-control", entrypoint: "ControlOperator" },
                },
                ...(automation ? { durableObjects: {
                    FIXTURE_LEDGER: { className: "ControlLedger", scriptName: "fixture-control" },
                } } : {}),
                outboundService: () => { throw new Error("unexpected ingress egress"); },
            }, {
                name: "fixture-control",
                compatibilityDate: "2026-07-30",
                compatibilityFlags: ["nodejs_compat"],
                modulesRoot: bundleDir,
                modules: [
                    ...(automation ? [{ type: "ESModule", path: join(bundleDir, "automation-test-worker.mjs") }] : []),
                    ...["control-worker.mjs", "authority-core.mjs", "authority-preflight.mjs", "control-migration.mjs",
                        "automation-digest.mjs", "automation-ledger.mjs"]
                        .map(name => ({ type: "ESModule", path: join(bundleDir, name) })),
                ],
                bindings: {
                    CUTOVER_AUTHORITY_BEARER: bearer,
                    CONTROL_OPERATOR_BEARER: operatorBearer,
                    CONTROL_TELEGRAM_CREDENTIAL_VERSION: credentialVersion,
                    ...(sender ? { CONTROL_TELEGRAM_BOT_TOKEN: token } : {}),
                    ...(automation ? { AUTOMATION_MODE: "fixture", AUTOMATION_REPOSITORY: "fixture/project" } : {}),
                },
                ...(automation ? { serviceBindings: {
                    AUTOMATION_METADATA: async request => {
                        assert.equal(request.url, "https://ci-metadata.invalid/v1/repos/fixture/project/runs");
                        assert.equal(request.method, "GET");
                        assert.equal(request.headers.has("authorization"), false);
                        reads.push(request.url);
                        return await fixture.onRead?.() ?? Response.json({ runs: [] });
                    },
                    AUTOMATION_TELEGRAM: async request => {
                        assert.equal(request.url, "https://telegram-fixture.invalid/sendMessage");
                        assert.equal(request.method, "POST");
                        const payload = await request.json();
                        const count = sends.push(payload);
                        return await fixture.onSend?.(payload) ??
                            Response.json({ ok: true, result: { message_id: count } });
                    },
                } } : {}),
                durableObjects: { CONTROL_LEDGER: { className: "ControlLedger", useSQLite: true } },
                outboundService: async request => {
                    assert.equal(request.url, `https://api.telegram.org/bot${token}/sendMessage`);
                    assert.equal(request.method, "POST");
                    const payload = await request.json();
                    const count = sends.push(payload);
                    return await fixture.onSend?.(payload) ??
                        Response.json({ ok: true, result: { message_id: count } });
                },
            }],
        });
        await service.ready;
    }
    const fixture = {
        sends, reads, restart,
        command: body => service.dispatchFetch("https://fault.invalid/", {
            method: "POST", body: JSON.stringify(body),
        }),
        runtime: (path, body, secret = bearer, init = {}) =>
            service.dispatchFetch(`https://control.invalid/v1/${path}`, {
                method: "POST",
                body: JSON.stringify(body),
                headers: { authorization: `Bearer ${secret}` },
                ...init,
            }),
        operator: (path, body, secret = operatorBearer) =>
            service.dispatchFetch(`https://operator.invalid/v1/operator/${path}`, {
                method: "POST", body: JSON.stringify(body), headers: { authorization: `Bearer ${secret}` },
            }),
        forward: async request => service.dispatchFetch(request.url, {
            method: request.method, headers: Object.fromEntries(request.headers),
            body: await request.arrayBuffer(),
        }),
    };
    context.after(() => service?.dispose());
    await restart();
    return fixture;
}
