import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { serveAuthority } from "./authority-control.mjs";

export const origin = "https://control.invalid";
export const hostname = "bigfur.invalid";
export const bearer = "a".repeat(64);

export async function authorityFixture(path) {
    const cli = join(import.meta.dirname, "authority-control.mjs");
    const admin = (command, ...args) => JSON.parse(execFileSync(process.execPath, [cli, command, path, ...args], {
        env: {}, stdio: ["ignore", "pipe", "pipe"],
    }));
    if (!existsSync(path)) admin("init");
    const checks = [];
    const sends = [];
    const credentialVersion = "synthetic-existing-token";
    const server = serveAuthority(path, { bearer, credentialVersion }, delivery => {
        sends.push(delivery);
        return sends.length;
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const endpoint = `http://127.0.0.1:${server.address().port}`;
    const fixture = {
        checks,
        sends,
        beforeReply: undefined,
        beforeSend: undefined,
        afterSend: undefined,
        close: () => new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())),
        status: () => admin("status"),
        publish(target, objectIncarnation, runnerIncarnation) {
            const input = `${path}.json`;
            writeFileSync(input, JSON.stringify({
                target: { ...target.pin, deployment: "synthetic/deployment" },
                versionId: target.versionId, runnerIncarnation,
                ...(objectIncarnation === null ? { continuity: "fresh-object-only" } : { objectIncarnation }),
                retirement: {
                    generation: target.pin.generation - 1, writers: "synthetic/writers",
                    executorGrants: "synthetic/grants", custody: "synthetic/custody",
                    telegram: { disposition: "exclusive-existing-token", evidence: "synthetic/telegram" },
                },
            }), { mode: 0o600 });
            admin("publish", input);
        },
        retire() {
            return admin("retire", String(fixture.status().highWater));
        },
        async fetch(request) {
            const url = new URL(request.url);
            assert.equal(url.origin, origin);
            assert.equal(request.method, "POST");
            assert.equal(request.headers.get("authorization"), `Bearer ${bearer}`);
            const body = await request.text();
            const { target, objectIncarnation, runnerIncarnation } = JSON.parse(body);
            if (url.pathname === "/v1/send") await fixture.beforeSend?.();
            const response = await fetch(`${endpoint}${url.pathname}`, {
                method: "POST", headers: { authorization: `Bearer ${bearer}` }, body,
            });
            const text = await response.text();
            if (url.pathname === "/v1/check") {
                checks.push({ target, objectIncarnation, runnerIncarnation, allowed: response.ok });
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
