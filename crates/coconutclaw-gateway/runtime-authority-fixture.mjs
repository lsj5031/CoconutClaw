import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createPairingAuthority, serveAuthority } from "./authority-control.mjs";

export const origin = "https://control.invalid";
export const hostname = "bigfur.invalid";
export const bearer = "a".repeat(64);

export async function authorityFixture(path, { pairing = false } = {}) {
    const cli = join(import.meta.dirname, "authority-control.mjs");
    const admin = (command, ...args) => JSON.parse(execFileSync(process.execPath, [cli, command, path, ...args], {
        env: {}, stdio: ["ignore", "pipe", "pipe"],
    }));
    const record = (command, value) => {
        const input = `${path}.json`;
        writeFileSync(input, JSON.stringify(value), { mode: 0o600 });
        return admin(command, input);
    };
    if (!pairing && !existsSync(path)) admin("init");
    const checks = [];
    const sends = [];
    const credentialVersion = "synthetic-existing-token";
    const credentials = { bearer, credentialVersion };
    const send = delivery => {
        sends.push(delivery);
        return sends.length;
    };
    const local = pairing ? await createPairingAuthority(path, credentials, send) : undefined;
    const server = local ? undefined : serveAuthority(path, credentials, send);
    if (server) {
        server.listen(0, "127.0.0.1");
        await once(server, "listening");
    }
    const endpoint = local?.endpoint ?? `http://127.0.0.1:${server.address().port}`;
    const fixture = {
        checks,
        sends,
        endpoint,
        issue: local?.issue ?? (card => record("issue", card).code),
        rename: runner => record("rename", runner),
        retireRunner: runner => record("retire-runner", runner),
        beforeReply: undefined,
        beforeSend: undefined,
        afterSend: undefined,
        close: local?.close ?? (() => new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()))),
        status: () => admin("status"),
        publish(target, objectIncarnation, runner) {
            const publication = {
                target: { ...target.pin, deployment: "synthetic/deployment" },
                versionId: target.versionId,
                ...(target.pin.pairing ? {} : typeof runner === "string"
                    ? { runnerIncarnation: runner } : { runnerIncarnations: runner }),
                ...(objectIncarnation === null ? { continuity: "fresh-object-only" } : { objectIncarnation }),
                retirement: {
                    generation: target.pin.generation - 1, writers: "synthetic/writers",
                    executorGrants: "synthetic/grants", custody: "synthetic/custody",
                    telegram: { disposition: "exclusive-existing-token", evidence: "synthetic/telegram" },
                },
            };
            if (local) return local.publish(publication);
            return record("publish", publication);
        },
        retire() {
            if (local) return local.retire(1);
            return admin("retire", String(fixture.status().highWater));
        },
        async fetch(request) {
            const url = new URL(request.url);
            assert.equal(url.origin, origin);
            assert.equal(request.method, "POST");
            assert.equal(request.headers.get("authorization"), `Bearer ${bearer}`);
            const body = await request.text();
            const { target, objectIncarnation, runnerIncarnation, placement } = JSON.parse(body);
            if (url.pathname === "/v1/send") await fixture.beforeSend?.();
            const response = await fetch(`${endpoint}${url.pathname}`, {
                method: "POST", headers: { authorization: `Bearer ${bearer}` }, body,
            });
            const text = await response.text();
            if (url.pathname === "/v1/check") {
                checks.push({ target, objectIncarnation, runnerIncarnation,
                    ...(placement === undefined ? {} : { placement }), allowed: response.ok });
                if (response.ok) return await fixture.beforeReply?.(JSON.parse(text)) ?? Response.json(JSON.parse(text));
            } else if (response.ok) {
                const reply = await fixture.afterSend?.();
                if (reply) return reply;
            }
            return new Response(text, { status: response.status });
        },
    };
    return fixture;
}
