import { BotLedger, fetch, initSync } from "./build/worker/coconutclaw_gateway.js";
import module from "./build/worker/coconutclaw_gateway_bg.wasm";

initSync({ module });

export { BotLedger };

const pilotPaths = new Set(["/v1/pilot/telegram", "/v1/pilot/agent", "/v1/pilot/fixture"]);
const bodyLimit = 16 * 1024;

async function pilot(request, env, context) {
    const url = new URL(request.url);
    if (request.method !== "POST" || url.protocol !== "https:" || url.search ||
        !pilotPaths.has(url.pathname)) {
        return Response.json({ error: "not_found" }, { status: 404 });
    }

    const window = /^\d{13}:\d{13}$/.test(env.PILOT_WINDOW) && env.PILOT_WINDOW.split(":").map(Number);
    const now = Date.now();
    if (!window || window[1] - window[0] > 600_000 || now < window[0] || now >= window[1]) {
        return Response.json({ error: "pilot_closed" }, { status: 503 });
    }
    const token = request.headers.get("authorization");
    const encoder = new TextEncoder();
    if (!/^[a-f0-9]{64}$/.test(env.PILOT_TOKEN) || !/^Bearer [a-f0-9]{64}$/.test(token) ||
        !crypto.subtle.timingSafeEqual(encoder.encode(token.slice(7)), encoder.encode(env.PILOT_TOKEN))) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
    }
    if (!(await env.PILOT_GATE.limit({ key: "phase2-pilot" })).success) {
        return Response.json({ error: "pilot_rate_limit" }, {
            status: 429, headers: { "retry-after": "10" },
        });
    }

    const bytes = new Uint8Array(bodyLimit);
    let length = 0;
    const reader = request.body?.getReader();
    if (!reader || Number(request.headers.get("content-length")) > bodyLimit) {
        return Response.json({ error: "body_limit" }, { status: 413 });
    }
    const deadline = AbortSignal.timeout(2000);
    try {
        const expired = new Promise((_, reject) => {
            deadline.addEventListener("abort", () => reject(deadline.reason), { once: true });
        });
        while (true) {
            const { value, done } = await Promise.race([reader.read(), expired]);
            if (done) break;
            if (value.length > bodyLimit - length) {
                return Response.json({ error: "body_limit" }, { status: 413 });
            }
            bytes.set(value, length);
            length += value.length;
        }
    } catch {
        return Response.json({ error: "invalid_body" }, { status: 400 });
    } finally {
        void reader.cancel().catch(() => {});
    }
    if (Date.now() >= window[1]) {
        return Response.json({ error: "pilot_closed" }, { status: 503 });
    }
    const headers = new Headers({ "content-type": "application/json" });
    const signature = request.headers.get("x-agent-signature");
    if (signature && signature.length === 128) headers.set("x-agent-signature", signature);
    return fetch(new Request(url, { method: "POST", body: bytes.subarray(0, length), headers }),
        env, context);
}

export default {
    async fetch(request, env, context) {
        if (env.PHASE === "phase2-production" && env.CONTROL && new URL(request.url).pathname === "/v1/pair") {
            const url = new URL(request.url);
            if (url.origin !== "https://bigfur.liu.nz" || url.search || request.method !== "POST") {
                return Response.json({ error: "not_found" }, { status: 404 });
            }
            try {
                if (JSON.parse(env.CUTOVER_TARGET).pairing !== "production-v1") {
                    return Response.json({ error: "not_found" }, { status: 404 });
                }
                return await env.CONTROL.fetch("https://control.internal/v1/pair", {
                    method: "POST",
                    headers: { "content-type": "application/json" },
                    body: request.body,
                });
            } catch {
                return Response.json({ error: "unavailable" }, { status: 503 });
            }
        }
        if (env.PHASE !== "phase2-pilot") return fetch(request, env, context);
        try {
            return await pilot(request, env, context);
        } catch {
            return Response.json({ error: "unavailable" }, { status: 503 });
        }
    },
};
