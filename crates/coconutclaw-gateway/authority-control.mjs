import { timingSafeEqual } from "node:crypto";
import { closeSync, openSync, readFileSync } from "node:fs";
import { createServer, request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { DatabaseSync } from "node:sqlite";
import { isDeepStrictEqual } from "node:util";
import {
    checkAuthorityHandoff, fields, placement, reference, sha256, targetFields, validEnrollments, validTarget,
} from "./authority-preflight.mjs";

const permitMs = 900;
const bodyLimit = 32 * 1024;
const freshObjectOnly = "fresh-object-only";
const positiveInteger = value => Number.isSafeInteger(value) && value > 0;
const uuid = value => typeof value === "string" &&
    /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(value);
const wireTarget = ({ target: { deployment, ...pin }, versionId }) => ({ pin, versionId });

class Denied extends Error {
    constructor(status) {
        super("authority request denied");
        this.status = status;
    }
}

class AuthorityStore {
    constructor(path, { initialize = false, timeout = 0 } = {}) {
        closeSync(openSync(path, initialize ? "wx" : "r", 0o600));
        this.db = new DatabaseSync(path, { timeout });
        try {
            this.db.exec("PRAGMA synchronous=EXTRA");
            if (initialize) {
                this.db.exec(`
                    BEGIN IMMEDIATE;
                    CREATE TABLE publications (
                        generation INTEGER PRIMARY KEY,
                        publication TEXT NOT NULL
                    ) STRICT;
                    CREATE TABLE retirements (
                        generation INTEGER PRIMARY KEY REFERENCES publications
                    ) STRICT;
                    CREATE TABLE sends (
                        generation INTEGER NOT NULL REFERENCES publications,
                        task TEXT NOT NULL,
                        number INTEGER NOT NULL,
                        message_id INTEGER,
                        PRIMARY KEY (generation, task, number)
                    ) STRICT;
                    PRAGMA user_version=1;
                    COMMIT;
                `);
            }
            if (this.db.prepare("PRAGMA user_version").get().user_version !== 1 ||
                this.db.prepare("PRAGMA journal_mode").get().journal_mode !== "delete") {
                throw new Error("invalid authority store");
            }
        } catch (error) {
            this.db.close();
            throw error;
        }
    }

    write(operation) {
        this.db.exec("BEGIN IMMEDIATE");
        try {
            const result = operation();
            this.db.exec("COMMIT");
            return result;
        } catch (error) {
            this.db.exec("ROLLBACK");
            throw error;
        }
    }

    current() {
        return this.db.prepare(`
            SELECT generation, publication, retirements.generation IS NOT NULL AS retired
            FROM publications LEFT JOIN retirements USING (generation)
            ORDER BY generation DESC LIMIT 1
        `).get();
    }

    status() {
        const current = this.current();
        return { highWater: current?.generation ?? 0, retired: !!current?.retired };
    }

    publish(publication) {
        const fresh = publication?.continuity === freshObjectOnly;
        const keys = publication?.target?.executorKeys;
        const multi = keys !== undefined;
        const incarnations = publication?.runnerIncarnations;
        if (!fields(publication, ["target", "retirement", "versionId",
            multi ? "runnerIncarnations" : "runnerIncarnation",
            fresh ? "continuity" : "objectIncarnation"]) ||
            !validTarget(publication.target) || !reference(publication.versionId) ||
            (multi ? fresh || !validEnrollments(incarnations) || !fields(incarnations, Object.keys(keys))
                : !sha256(publication.runnerIncarnation)) ||
            (fresh ? publication.target.hostname !== "bigfur.liu.nz" : !uuid(publication.objectIncarnation))) {
            throw new Error("invalid publication");
        }
        const generation = checkAuthorityHandoff(publication, publication.target);
        return this.write(() => {
            const current = this.current();
            if (generation !== (current?.generation ?? 0) + 1 || (current && !current.retired)) {
                throw new Error("publication requires the next generation and a retired predecessor");
            }
            const runners = multi ? Object.values(incarnations) : [publication.runnerIncarnation];
            const reused = this.db.prepare("SELECT publication FROM publications").iterate()
                .some(row => {
                    const old = JSON.parse(row.publication);
                    if (!(fresh || multi || old.runnerIncarnations || old.continuity === freshObjectOnly)) return false;
                    const previous = old.runnerIncarnations ? Object.values(old.runnerIncarnations) : [old.runnerIncarnation];
                    return old.target.objectId === publication.target.objectId ||
                        previous.some(runner => runners.includes(runner));
                });
            if (reused) throw new Error("publication requires a fresh object and runner incarnation");
            this.db.prepare("INSERT INTO publications VALUES (?, ?)").run(generation, JSON.stringify(publication));
            return this.status();
        });
    }

    retire(generation) {
        return this.write(() => {
            if (this.current()?.generation !== generation) {
                throw new Error("retirement requires the current generation");
            }
            this.db.prepare("INSERT OR IGNORE INTO retirements VALUES (?)").run(generation);
            return this.status();
        });
    }

    authorize({ target, objectIncarnation, runnerIncarnation, placement: name }, credentialVersion) {
        const current = this.current();
        if (!current || current.retired) throw new Denied(503);
        const enrolled = JSON.parse(current.publication);
        if (!isDeepStrictEqual(target, wireTarget(enrolled))) {
            throw new Denied(503);
        }
        const runner = enrolled.runnerIncarnations ? enrolled.runnerIncarnations[name] : enrolled.runnerIncarnation;
        if (enrolled.target.telegramCredentialVersion !== credentialVersion ||
            (enrolled.continuity !== freshObjectOnly && objectIncarnation !== enrolled.objectIncarnation) ||
            (name !== undefined && !enrolled.runnerIncarnations) ||
            (runnerIncarnation !== null && (runner === undefined || !equalSecret(runnerIncarnation, runner)))) {
            throw new Denied(503);
        }
        return current.generation;
    }

    async dispatch(request, credentialVersion, send) {
        const { task, sendNumber } = request.delivery;
        const generation = this.write(() => {
            const generation = this.authorize(request, credentialVersion);
            const result = this.db.prepare("INSERT OR IGNORE INTO sends VALUES (?, ?, ?, NULL)")
                .run(generation, task, sendNumber);
            if (!result.changes) throw new Denied(409);
            return generation;
        });
        this.db.exec("BEGIN IMMEDIATE");
        try {
            this.authorize(request, credentialVersion);
            const messageId = await send(request.delivery);
            if (!positiveInteger(messageId)) throw new Denied(502);
            this.db.prepare("UPDATE sends SET message_id=? WHERE generation=? AND task=? AND number=?")
                .run(messageId, generation, task, sendNumber);
            this.db.exec("COMMIT");
            return messageId;
        } catch (error) {
            this.db.exec("ROLLBACK");
            throw error;
        }
    }
}

function equalSecret(candidate, secret) {
    return sha256(candidate) && timingSafeEqual(Buffer.from(candidate, "hex"), Buffer.from(secret, "hex"));
}

async function readJson(stream) {
    const chunks = [];
    let length = 0;
    for await (const chunk of stream.iterator({ destroyOnReturn: false })) {
        length += chunk.length;
        if (length > bodyLimit) throw new Denied(413);
        chunks.push(chunk);
    }
    try {
        return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks)));
    } catch {
        throw new Denied(400);
    }
}

function validDelivery(delivery) {
    return fields(delivery, ["task", "sendNumber", "chatId", "messageThreadId", "text"]) &&
        typeof delivery.task === "string" && /^[A-Za-z0-9:_-]{1,128}$/.test(delivery.task) &&
        positiveInteger(delivery.sendNumber) &&
        Number.isSafeInteger(delivery.chatId) && delivery.chatId !== 0 &&
        (delivery.messageThreadId === null ||
            (positiveInteger(delivery.messageThreadId) && delivery.messageThreadId <= 0x7fff_ffff)) &&
        typeof delivery.text === "string" && delivery.text.trim().length > 0 &&
        Buffer.byteLength(delivery.text) <= 4096;
}

export function telegramSender(origin, token) {
    const url = new URL(`/bot${token}/sendMessage`, origin);
    const request = url.protocol === "https:" ? httpsRequest : httpRequest;
    return async ({ chatId, messageThreadId, text }) => {
        const payload = JSON.stringify({
            chat_id: chatId, message_thread_id: messageThreadId ?? undefined, text,
        });
        return new Promise((resolve, reject) => {
            const fail = () => reject(new Denied(502));
            const outgoing = request(url, {
                method: "POST", agent: false, signal: AbortSignal.timeout(10000),
                headers: { "content-type": "application/json", "content-length": Buffer.byteLength(payload) },
            }, async incoming => {
                try {
                    if (incoming.statusCode !== 200) return fail();
                    const reply = await readJson(incoming);
                    resolve(reply?.ok === true ? reply.result?.message_id : null);
                } catch {
                    fail();
                } finally {
                    incoming.destroy();
                }
            });
            outgoing.on("error", fail);
            outgoing.end(payload);
        });
    };
}

function authorityServer(bearer, decide) {
    if (!sha256(bearer)) throw new Error("invalid service credentials");
    return createServer({ requestTimeout: 2000, headersTimeout: 2000 }, async (incoming, response) => {
        response.setHeader("cache-control", "no-store");
        try {
            const check = incoming.url === "/v1/check";
            if (!check && incoming.url !== "/v1/send") throw new Denied(404);
            if (incoming.method !== "POST") throw new Denied(405);
            const authorization = incoming.headers.authorization;
            if (!authorization?.startsWith("Bearer ") || !equalSecret(authorization.slice(7), bearer)) {
                throw new Denied(401);
            }
            const body = await readJson(incoming);
            const members = ["nonce", "target", "objectIncarnation", "runnerIncarnation", "delivery"];
            const scoped = body?.placement !== undefined;
            if (scoped) members.push("placement");
            if (!fields(body, members) ||
                !uuid(body.nonce) || !uuid(body.objectIncarnation) ||
                (scoped && (!check || !placement(body.placement) || !sha256(body.runnerIncarnation))) ||
                (check ? body.delivery !== null || (body.runnerIncarnation !== null && !sha256(body.runnerIncarnation))
                    : body.runnerIncarnation !== null || !validDelivery(body.delivery))) {
                throw new Denied(400);
            }
            const reply = await decide(body, check);
            response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(reply));
        } catch (error) {
            response.writeHead(error instanceof Denied ? error.status : 503, { connection: "close" }).end();
        }
    });
}

export function serveAuthority(path, { bearer, credentialVersion }, send) {
    if (!sha256(bearer) || !reference(credentialVersion)) throw new Error("invalid service credentials");
    const store = new AuthorityStore(path);
    let dispatching = false;
    const server = authorityServer(bearer, async (body, check) => {
        if (check) {
            const validUntil = Date.now() + permitMs;
            const highWater = store.authorize(body, credentialVersion);
            if (validUntil <= Date.now()) throw new Denied(503);
            const { delivery, ...binding } = body;
            return { ...binding, highWater, validUntil };
        }
        if (dispatching) throw new Denied(503);
        dispatching = true;
        try {
            return { nonce: body.nonce, messageId: await store.dispatch(body, credentialVersion, send) };
        } finally {
            dispatching = false;
        }
    });
    server.once("close", () => store.db.close());
    return server;
}

function listen(server, port) {
    server.on("error", () => {
        console.error("authority listener failed");
        process.exitCode = 1;
    });
    server.listen(port, "127.0.0.1");
}

export function observeAuthority(candidate, bearer, record) {
    if (!fields(candidate, ["target", "versionId"]) || !validTarget(candidate.target) || !reference(candidate.versionId)) {
        throw new Error("invalid observation target");
    }
    const target = wireTarget(candidate);
    let observed = false;
    return authorityServer(bearer, (body, check) => {
        if (!observed && check && body.runnerIncarnation === null && isDeepStrictEqual(body.target, target)) {
            observed = true;
            record({ objectIncarnation: body.objectIncarnation });
        }
        throw new Denied(503);
    });
}

function main([command, path, argument, ...extra]) {
    const usage = "usage: authority-control.mjs init|status DB | publish DB RECORD | retire DB GENERATION | observe CANDIDATE PORT | serve DB PORT";
    if (!path || extra.length) throw new Error(usage);
    if (["observe", "serve"].includes(command)) {
        const port = Number(argument);
        if (!/^[0-9]+$/.test(argument) || !positiveInteger(port) || port > 65535) throw new Error(usage);
        const bearer = process.env.CUTOVER_AUTHORITY_BEARER;
        if (command === "observe") {
            const candidate = JSON.parse(readFileSync(path, "utf8"));
            listen(observeAuthority(candidate, bearer, result => console.log(JSON.stringify(result))), port);
            return;
        }
        const { CONTROL_TELEGRAM_CREDENTIAL_VERSION: credentialVersion, CONTROL_TELEGRAM_BOT_TOKEN: token } = process.env;
        if (!/^[0-9]{1,24}:[A-Za-z0-9_-]{1,128}$/.test(token ?? "")) throw new Error("invalid service credentials");
        listen(serveAuthority(path, { bearer, credentialVersion }, telegramSender("https://api.telegram.org", token)), port);
        return;
    }
    if ((["init", "status"].includes(command) && argument !== undefined) ||
        (command === "publish" && !argument) ||
        (command === "retire" && !targetFields.generation(Number(argument))) ||
        !["init", "status", "publish", "retire"].includes(command)) throw new Error(usage);
    const store = new AuthorityStore(path, { initialize: command === "init", timeout: 5000 });
    try {
        let status;
        if (command === "publish") status = store.publish(JSON.parse(readFileSync(argument, "utf8")));
        else if (command === "retire") status = store.retire(Number(argument));
        else status = store.status();
        console.log(JSON.stringify(status));
    } finally {
        store.db.close();
    }
}

if (import.meta.main) {
    process.umask(0o077);
    try {
        main(process.argv.slice(2));
    } catch {
        console.error("authority command failed");
        process.exitCode = 1;
    }
}
