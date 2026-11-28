import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import {
    AuthorityStore, Denied, authoritySchema, bodyLimit, checkPermit, equalSecret, parseJson, positiveInteger,
    productionEnrollmentSchema, validateRequest,
} from "./authority-core.mjs";
import { fields, reference, targetFields } from "./authority-preflight.mjs";
import { renderDigest } from "./automation-digest.mjs";
import { AutomationLedger, automationSchema } from "./automation-ledger.mjs";
import {
    authoritySnapshot, importAuthority, migrationBodyLimit, migrationRecord, migrationSchema, snapshotDigest,
} from "./control-migration.mjs";

const runtimePaths = new Set(["/v1/check", "/v1/send", "/v1/pair"]);
const operatorPaths = new Set([
    "/v1/operator/status", "/v1/operator/snapshot", "/v1/operator/import", "/v1/operator/publish",
    "/v1/operator/retire", "/v1/operator/issue", "/v1/operator/rename", "/v1/operator/retire-runner",
]);
const automationRoot = "/v1/operator/automation/";
const automationActions = ["save", "preview", "dry-run", "run-once", "pause", "resume", "history", "freeze"];
for (const action of automationActions) operatorPaths.add(`${automationRoot}${action}`);
const automationReads = new Set(["preview", "dry-run", "history"]);
const wakeWatchdog = 30_000;
const headers = { "cache-control": "no-store" };

async function respond(operation) {
    try {
        return Response.json(await operation(), { headers });
    } catch (error) {
        return new Response(null, { status: error instanceof Denied ? error.status : 503, headers });
    }
}

function route(request, paths) {
    const url = new URL(request.url);
    if (url.protocol !== "https:" || url.search || !paths.has(url.pathname)) throw new Denied(404);
    if (request.method !== "POST") throw new Denied(405);
    return url.pathname;
}

function requireBearer(request, secret) {
    const authorization = request.headers.get("authorization");
    if (!authorization?.startsWith("Bearer ") || !equalSecret(authorization.slice(7), secret)) {
        throw new Denied(401);
    }
}

async function readJson(message, limit = bodyLimit, timeout = 2000) {
    if (Number(message.headers.get("content-length")) > limit) throw new Denied(413);
    const reader = message.body?.getReader();
    if (!reader) throw new Denied(400);
    const chunks = [];
    let length = 0;
    let timer;
    try {
        const expired = new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Denied(408)), timeout);
        });
        while (true) {
            const { value, done } = await Promise.race([reader.read(), expired]);
            if (done) break;
            length += value.byteLength;
            if (length > limit) throw new Denied(413);
            chunks.push(value);
        }
        return parseJson(Buffer.concat(chunks, length));
    } finally {
        clearTimeout(timer);
        void reader.cancel().catch(() => {});
    }
}

const ledger = env => env.CONTROL_LEDGER.getByName("bigfur-control-v1");

export default {
    async fetch(request, env) {
        try {
            route(request, runtimePaths);
            return await ledger(env).fetch(request);
        } catch (error) {
            return respond(() => { throw error; });
        }
    },
};

export class ControlOperator extends WorkerEntrypoint {
    async fetch(request) {
        try {
            route(request, operatorPaths);
            return await ledger(this.env).admin(request);
        } catch (error) {
            return respond(() => { throw error; });
        }
    }
}

export class ControlLedger extends DurableObject {
    constructor(ctx, env) {
        super(ctx, env);
        const { sql } = ctx.storage;
        const write = operation => ctx.storage.transactionSync(operation);
        write(() => {
            if (!sql.exec("SELECT 1 FROM sqlite_master WHERE type='table' AND name='migration'").toArray().length) {
                sql.exec(`${authoritySchema} ${productionEnrollmentSchema} ${migrationSchema}`);
            }
        });
        const db = {
            prepare(query) {
                return {
                    all: (...args) => sql.exec(query, ...args).toArray(),
                    get: (...args) => sql.exec(query, ...args).toArray()[0],
                    run: (...args) => {
                        sql.exec(query, ...args);
                        return sql.exec("SELECT changes() AS changes").one();
                    },
                };
            },
        };
        this.store = new AuthorityStore(db, write, {
            gatewayVersion: generation => db.prepare("SELECT gateway_version FROM migration WHERE generation=?")
                .get(generation)?.gateway_version,
        });
        this.dispatching = false;
        this.waking = false;
        if (env.AUTOMATION_MODE === "fixture" && env.AUTOMATION_REPOSITORY &&
            env.AUTOMATION_METADATA && env.AUTOMATION_TELEGRAM) {
            write(() => sql.exec(automationSchema));
            this.automation = new AutomationLedger(this.store);
        }
    }

    get token() {
        const token = this.env.CONTROL_TELEGRAM_BOT_TOKEN;
        return /^[0-9]{1,24}:[A-Za-z0-9_-]{1,128}$/.test(token ?? "") ? token : undefined;
    }

    async fetch(request) {
        return respond(async () => {
            const path = route(request, runtimePaths);
            if (path !== "/v1/pair") requireBearer(request, this.env.CUTOVER_AUTHORITY_BEARER);
            const body = await readJson(request);
            const credentialVersion = this.env.CONTROL_TELEGRAM_CREDENTIAL_VERSION;
            if (path === "/v1/pair") {
                if (this.dispatching) throw new Denied(503);
                return this.store.redeem(body);
            }
            if (!reference(credentialVersion)) throw new Denied(503);
            const check = path === "/v1/check";
            validateRequest(body, check);
            if (check) {
                if (body.deliveryCheck && this.dispatching) throw new Denied(503);
                const permit = checkPermit(this.store, body, credentialVersion);
                if (body.deliveryCheck && !this.token) throw new Denied(503);
                return permit;
            }
            if (this.dispatching) throw new Denied(503);
            if (body.delivery.closeUnsent) {
                this.store.closeUnsent(body, credentialVersion);
                return { nonce: body.nonce, noSend: true };
            }
            const token = this.token;
            if (!token) throw new Denied(503);
            this.dispatching = true;
            try {
                const generation = this.store.reserve(body, credentialVersion);
                await this.ctx.storage.sync();
                this.store.authorize(body, credentialVersion);
                const messageId = await this.sendTelegram(token, body.delivery);
                this.store.write(() => this.store.recordSend(generation, body.delivery, messageId));
                await this.ctx.storage.sync();
                return { nonce: body.nonce, messageId };
            } finally {
                this.dispatching = false;
            }
        });
    }

    async sendTelegram(token, { chatId, messageThreadId, text }, transport = fetch) {
        try {
            const response = await transport(`https://api.telegram.org/bot${token}/sendMessage`, {
                method: "POST",
                redirect: "manual",
                signal: AbortSignal.timeout(10_000),
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ chat_id: chatId, message_thread_id: messageThreadId ?? undefined, text }),
            });
            if (response.status === 200) {
                const body = await readJson(response);
                if (body?.ok === true) return body.result?.message_id;
            } else {
                void response.body?.cancel().catch(() => {});
            }
        } catch {
            throw new Denied(502);
        }
        throw new Denied(502);
    }

    automationNow() {
        return Date.now();
    }

    automationContext() {
        return {
            repository: this.env.AUTOMATION_REPOSITORY,
            credentialVersion: this.env.CONTROL_TELEGRAM_CREDENTIAL_VERSION,
            sender: !!this.token,
        };
    }

    stageAutomationAlarm(at) {
        return at === null ? this.ctx.storage.deleteAlarm() : this.ctx.storage.setAlarm(at);
    }

    automationWrite(operation) {
        const { result, alarm } = this.store.write(() => {
            const now = this.automationNow();
            const result = operation(now);
            const next = this.waking ? now + wakeWatchdog : this.automation.nextWake(now, this.dispatching);
            return { result, alarm: this.stageAutomationAlarm(next) };
        });
        return alarm.then(() => result);
    }

    async alarm() {
        if (!this.automation || this.waking) return;
        this.waking = true;
        try {
            await this.automationWrite(now => this.automation.advance(this.automationContext(), now));
            if (!this.dispatching) {
                const run = this.automation.waiting();
                if (run) await this.processAutomation(run);
            }
        } finally {
            this.waking = false;
            await this.automationWrite(() => {});
        }
    }

    async processAutomation(run) {
        const { repository, chatId, messageThreadId } = JSON.parse(run.config);
        const context = this.automationContext();
        if (run.execution === "pending") {
            let digest;
            let failure = "deadline";
            if (run.deadline > this.automationNow()) {
                try {
                    this.automation.enforceGate(run.generation, context);
                    if (repository !== context.repository) throw new Denied(503);
                    failure = "metadata_unavailable";
                    const response = await this.env.AUTOMATION_METADATA.fetch(
                        `https://ci-metadata.invalid/v1/repos/${repository}/runs`,
                        { method: "GET", redirect: "manual", signal: AbortSignal.timeout(5_000) },
                    );
                    if (!response.ok) {
                        void response.body?.cancel().catch(() => {});
                        throw new Denied(503);
                    }
                    digest = renderDigest(repository, await readJson(response, 8192));
                } catch {
                    await this.automationWrite(() => this.automation.finish(run, "failed", failure));
                    return;
                }
            }
            await this.automationWrite(() => this.automation.finish(run, digest ? "ready" : "failed", digest ?? failure));
            if (!digest) return;
            run = this.automation.run(run.schedule_id, run.run_key);
        }
        if (this.dispatching) return;
        this.dispatching = true;
        try {
            const reserved = await this.automationWrite(now => {
                let reason = now >= run.deadline ? "deadline" : null;
                try {
                    this.automation.enforceGate(run.generation, this.automationContext());
                    if (repository !== this.env.AUTOMATION_REPOSITORY) throw new Denied(503);
                } catch (error) {
                    if (!(error instanceof Denied)) throw error;
                    reason ??= "control_unavailable";
                }
                if (reason) {
                    this.automation.finish(run, "blocked", reason);
                    return false;
                }
                this.automation.finish(run, "reserve", "send_receipt_missing");
                return true;
            });
            if (!reserved) return;
            await this.ctx.storage.sync();
            let messageId;
            try {
                messageId = await this.sendTelegram(this.token, { chatId, messageThreadId, text: run.digest },
                    (_url, init) => this.env.AUTOMATION_TELEGRAM.fetch("https://telegram-fixture.invalid/sendMessage", init));
            } catch {
                return;
            }
            if (positiveInteger(messageId)) {
                await this.automationWrite(() => this.automation.finish(run, "sent", messageId));
            }
        } finally {
            this.dispatching = false;
        }
    }

    async admin(request) {
        return respond(async () => {
            const path = route(request, operatorPaths);
            requireBearer(request, this.env.CONTROL_OPERATOR_BEARER);
            if (path.startsWith(automationRoot)) {
                if (!this.automation) throw new Denied(404);
                const action = path.slice(automationRoot.length);
                const body = await readJson(request);
                const operate = now => this.automation.operate(action, body, this.automationContext(), now);
                if (automationReads.has(action)) return operate(this.automationNow());
                if (action === "freeze" && this.dispatching) throw new Denied(503);
                return this.automationWrite(operate);
            }
            const migration = migrationRecord(this.store.db) ?? null;
            if (path === "/v1/operator/status") {
                return { ...this.store.status(), migration, senderConfigured: !!this.token, dispatching: this.dispatching };
            }
            if (path === "/v1/operator/snapshot") {
                const source = authoritySnapshot(this.store.db);
                return { source, sourceSha256: snapshotDigest(source), migration };
            }
            const importing = path === "/v1/operator/import";
            const body = await readJson(request, importing ? migrationBodyLimit : bodyLimit);
            if (this.dispatching) throw new Denied(503);
            if (importing) {
                if (!fields(body, ["source", "sourceSha256", "gatewayVersion"])) throw new Denied(400);
                if (!migrationRecord(this.store.db) && this.token) throw new Denied(409);
                return importAuthority(this.store, body);
            }
            if (!migrationRecord(this.store.db)) throw new Denied(503);
            if (path === "/v1/operator/publish") {
                if (body?.target?.telegramCredentialVersion !== this.env.CONTROL_TELEGRAM_CREDENTIAL_VERSION) {
                    throw new Denied(400);
                }
                return this.store.publish(body);
            }
            if (path === "/v1/operator/retire") {
                if (!fields(body, ["generation"]) || !targetFields.generation(body.generation)) throw new Denied(400);
                return this.store.retire(body.generation);
            }
            if (path === "/v1/operator/issue") return { code: this.store.issue(body) };
            return this.store.changeRunner(body, path === "/v1/operator/retire-runner");
        });
    }
}
