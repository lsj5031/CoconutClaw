import assert from "node:assert/strict";
import { createPrivateKey, sign } from "node:crypto";
import { readFile } from "node:fs/promises";
import { request } from "node:https";
import { setTimeout as sleep } from "node:timers/promises";
import { pathToFileURL } from "node:url";

const instance = "hosted-synthetic-pilot";
const task = "telegram:23001";
const topic = 37;
const prompt = "synthetic pilot";
const resultText = "pilot: SYNTHETIC PILOT";
const bodyLimit = 16 * 1024;
const requestLimit = 16;
const requestDomain = `coconutclaw-v1\n${instance}\nPOST\n/v1/agent\n`;
const evidenceDomain = `coconutclaw-evidence-v1\n${instance}\nalpha\n`;

export function httpsPilotTransport(origin) {
    const base = new URL(origin);
    if (base.protocol !== "https:" || base.href !== `${base.origin}/`) {
        throw new Error("Expected an HTTPS origin without credentials, path, query or fragment");
    }
    return (path, body, headers, signal) => new Promise((resolve, reject) => {
        const req = request(new URL(path, base), {
            method: "POST", headers, signal, agent: false, rejectUnauthorized: true,
        }, res => {
            const chunks = [];
            let length = 0;
            res.on("error", reject);
            res.on("data", chunk => {
                length += chunk.length;
                if (length > bodyLimit) {
                    req.destroy(new Error("Pilot response exceeds body limit"));
                    return;
                }
                chunks.push(chunk);
            });
            res.on("end", () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
        });
        req.on("error", reject);
        req.end(body);
    });
}

export async function runPilot({ transport, token, key, checkpoint = async () => {} }) {
    if (!/^[a-f0-9]{64}$/.test(token) || key?.type !== "private" || key.asymmetricKeyType !== "ed25519") {
        throw new Error("Expected a disposable 32-byte hex token and Ed25519 private key");
    }
    let calls = 0;
    let sequence = 0;
    let executions = 0;
    const deadline = AbortSignal.timeout(20_000);
    const signature = (domain, payload) => sign(null, Buffer.from(domain + payload), key).toString("hex");

    async function post(route, payload, proof) {
        deadline.throwIfAborted();
        assert.ok(++calls <= requestLimit, "Pilot request budget");
        const body = Buffer.from(payload);
        assert.ok(body.length <= bodyLimit);
        const response = await transport(`/v1/pilot/${route}`, body, {
            authorization: `Bearer ${token}`,
            "content-type": "application/json",
            "content-length": body.length,
            ...(proof ? { "x-agent-signature": proof } : {}),
        }, AbortSignal.any([deadline, AbortSignal.timeout(2000)]));
        if (response.status !== 200) throw new Error(`Pilot ${route} HTTP ${response.status}`);
        assert.ok(response.body.length <= bodyLimit);
        return JSON.parse(response.body);
    }

    function agent(command, mutates = false) {
        const payload = JSON.stringify({
            device: "alpha", at_ms: Date.now(), sequence: mutates ? ++sequence : 0, command,
        });
        return post("agent", payload, signature(requestDomain, payload));
    }

    const hello = await agent({ op: "hello" });
    assert.deepEqual(hello, {
        protocol: 1, instance, device: "alpha", capabilities: ["text"], last_sequence: 0,
    }, "The driver requires a fresh named ledger");
    const update = JSON.stringify({
        update_id: 23001,
        message: { chat: { id: "321", type: "private" }, from: { id: 321 }, message_thread_id: topic, text: prompt },
    });
    assert.deepEqual(await post("telegram", update), { disposition: "task", task });
    await checkpoint("intake");
    assert.deepEqual(await post("telegram", update), { disposition: "duplicate" });
    assert.deepEqual(await agent({ op: "next" }), { task });

    const claim = await agent({ op: "claim", task }, true);
    const { attempt } = claim;
    assert.ok(Number.isSafeInteger(attempt) && attempt > 0);
    assert.equal(claim.phase, "claimed");
    assert.equal(claim.task, task);
    assert.equal(claim.prompt, prompt);
    const sentAt = Date.now();
    const sentMonotonic = performance.now();
    const grant = await agent({ op: "start", attempt }, true);
    assert.equal(grant.attempt, attempt);
    assert.equal(grant.request_sequence, sequence);
    assert.ok(Number.isSafeInteger(grant.start_until));
    const status = await agent({ op: "status", attempt });
    assert.deepEqual(status, {
        attempt, phase: "authorized", start_until: grant.start_until, cancel_requested: false,
    });
    assert.ok(Date.now() < grant.start_until && grant.start_until <= sentAt + 7000);
    assert.ok(performance.now() < sentMonotonic + 5000);
    const text = `pilot: ${claim.prompt.toUpperCase()}`;
    executions++;

    function evidence(event) {
        const payload = JSON.stringify({ attempt, event });
        return { op: "report", proof: { payload, signature: signature(evidenceDomain, payload) } };
    }
    assert.deepEqual(await agent(evidence({ kind: "started" }), true), { event: "started" });
    const result = evidence({ kind: "result", text });
    assert.deepEqual(await agent(result, true), { event: "result" });
    await checkpoint("result");
    assert.deepEqual(await agent(result, true), { event: "result" });
    assert.deepEqual(await agent({ op: "next" }), { task: null });
    const expected = {
        phase: "delivered", sends: 1, message_id: 1,
        delivery: { chat_id: 321, message_thread_id: topic, text: resultText },
    };
    const lookup = JSON.stringify({ op: "delivery_status", task });
    for (let poll = 0; poll < 4; poll++) {
        const receipt = await post("fixture", lookup);
        if (receipt.phase === "delivered") {
            assert.deepEqual(receipt, expected);
            await checkpoint("receipt");
            assert.deepEqual(await post("fixture", lookup), expected);
            return { task, attempt, executions, calls, receipt };
        }
        assert.deepEqual(receipt, { phase: "pending", sends: 0 });
        if (poll < 3) await sleep(350, undefined, { signal: deadline });
    }
    throw new Error("Pilot exhausted its receipt poll budget");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    const [origin, keyPath, ...extra] = process.argv.slice(2);
    try {
        if (!origin || !keyPath || extra.length) throw new Error("Expected HTTPS origin and disposable key path");
        const key = createPrivateKey(await readFile(keyPath));
        const result = await runPilot({
            transport: httpsPilotTransport(origin), token: process.env.COCONUTCLAW_PILOT_TOKEN, key,
        });
        console.log(JSON.stringify(result));
    } catch (error) {
        console.error(error.message);
        process.exitCode = 1;
    }
}
