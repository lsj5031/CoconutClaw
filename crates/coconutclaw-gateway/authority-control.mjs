import { once } from "node:events";
import { closeSync, openSync, readFileSync } from "node:fs";
import { createServer, request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { DatabaseSync } from "node:sqlite";
import { isDeepStrictEqual } from "node:util";
import { fields, reference, sha256, targetFields, validTarget } from "./authority-preflight.mjs";
import {
    AuthorityStore, Denied, authoritySchema, bodyLimit, checkPermit, enrollmentSchema, equalSecret,
    parseJson, positiveInteger, productionEnrollmentSchema, validateRequest, wireTarget,
} from "./authority-core.mjs";
import { authoritySnapshot } from "./control-migration.mjs";

function openStore(path, { initialize = false, timeout = 0, pairing = false, readOnly = false } = {}) {
    closeSync(openSync(path, initialize ? "wx" : "r", 0o600));
    const db = new DatabaseSync(path, { timeout, readOnly });
    const write = operation => {
        db.exec("BEGIN IMMEDIATE");
        try {
            const result = operation();
            db.exec("COMMIT");
            return result;
        } catch (error) {
            db.exec("ROLLBACK");
            throw error;
        }
    };
    const version = () => db.prepare("PRAGMA user_version").get().user_version;
    try {
        if (!readOnly) db.exec("PRAGMA synchronous=EXTRA");
        if (initialize) write(() => db.exec(`
            ${authoritySchema}
            ${pairing ? enrollmentSchema : ""}
            PRAGMA user_version=${pairing ? 2 : 1};
        `));
        if (!(pairing ? [2] : [1, 3]).includes(version()) ||
            db.prepare("PRAGMA journal_mode").get().journal_mode !== "delete") {
            throw new Error("invalid authority store");
        }
        return new AuthorityStore(db, write, {
            pairing,
            initializeRegistry() {
                if (version() === 1) db.exec(`${productionEnrollmentSchema} PRAGMA user_version=3;`);
            },
        });
    } catch (error) {
        db.close();
        throw error;
    }
}

async function readJson(stream) {
    const chunks = [];
    let length = 0;
    for await (const chunk of stream.iterator({ destroyOnReturn: false })) {
        length += chunk.length;
        if (length > bodyLimit) throw new Denied(413);
        chunks.push(chunk);
    }
    return parseJson(Buffer.concat(chunks));
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

function authorityServer(bearer, decide, redeem) {
    if (!sha256(bearer)) throw new Error("invalid service credentials");
    return createServer({ requestTimeout: 2000, headersTimeout: 2000 }, async (incoming, response) => {
        response.setHeader("cache-control", "no-store");
        try {
            const check = incoming.url === "/v1/check";
            const pairing = redeem && incoming.url === "/v1/pair";
            if (!check && !pairing && incoming.url !== "/v1/send") throw new Denied(404);
            if (incoming.method !== "POST") throw new Denied(405);
            let reply;
            if (pairing) {
                reply = redeem(await readJson(incoming));
            } else {
                const authorization = incoming.headers.authorization;
                if (!authorization?.startsWith("Bearer ") || !equalSecret(authorization.slice(7), bearer)) {
                    throw new Denied(401);
                }
                const body = await readJson(incoming);
                validateRequest(body, check);
                reply = await decide(body, check);
            }
            response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(reply));
        } catch (error) {
            response.writeHead(error instanceof Denied ? error.status : 503, { connection: "close" }).end();
        }
    });
}

export function serveAuthority(path, { bearer, credentialVersion }, send) {
    if (!sha256(bearer) || !reference(credentialVersion)) throw new Error("invalid service credentials");
    return serve(openStore(path), { bearer, credentialVersion }, send);
}

function serve(store, { bearer, credentialVersion }, send) {
    let dispatching = false;
    const server = authorityServer(bearer, async (body, check) => {
        if (check) {
            if (body.deliveryCheck && dispatching) throw new Denied(503);
            return checkPermit(store, body, credentialVersion);
        }
        if (dispatching) throw new Denied(503);
        if (body.delivery.closeUnsent) {
            store.closeUnsent(body, credentialVersion);
            return { nonce: body.nonce, noSend: true };
        }
        dispatching = true;
        try {
            const generation = store.reserve(body, credentialVersion);
            store.db.exec("BEGIN IMMEDIATE");
            try {
                store.authorize(body, credentialVersion);
                const messageId = store.recordSend(generation, body.delivery, await send(body.delivery));
                store.db.exec("COMMIT");
                return { nonce: body.nonce, messageId };
            } catch (error) {
                store.db.exec("ROLLBACK");
                throw error;
            }
        } finally {
            dispatching = false;
        }
    }, body => {
        if (dispatching) throw new Denied(503);
        return store.redeem(body);
    });
    server.once("close", () => store.db.close());
    return server;
}

export async function createPairingAuthority(path, credentials, send) {
    if (!sha256(credentials.bearer) || !reference(credentials.credentialVersion)) throw new Error("invalid service credentials");
    const store = openStore(path, { initialize: true, pairing: true });
    const server = serve(store, credentials, send);
    server.listen(0, "127.0.0.1");
    try {
        await once(server, "listening");
    } catch (error) {
        store.db.close();
        throw error;
    }
    return {
        endpoint: `http://127.0.0.1:${server.address().port}`,
        publish(publication) {
            if (publication?.target?.telegramCredentialVersion !== credentials.credentialVersion) {
                throw new Error("invalid publication credential version");
            }
            return store.publish(publication);
        },
        retire: generation => store.retire(generation),
        issue: card => store.issue(card),
        close: () => new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())),
    };
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
    const usage = "usage: authority-control.mjs init|status|export DB | publish|issue|rename|retire-runner DB RECORD | retire DB GENERATION | observe CANDIDATE PORT | serve DB PORT";
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
    const noArgument = ["init", "status", "export"];
    const fileArgument = ["publish", "issue", "rename", "retire-runner"];
    if ((noArgument.includes(command) && argument !== undefined) ||
        (fileArgument.includes(command) && !argument) ||
        (command === "retire" && !targetFields.generation(Number(argument))) ||
        ![...noArgument, ...fileArgument, "retire"].includes(command)) throw new Error(usage);
    const store = openStore(path, { initialize: command === "init", timeout: 5000, readOnly: command === "export" });
    try {
        let status;
        if (command === "export") {
            store.db.exec("BEGIN");
            const integrity = store.db.prepare("PRAGMA integrity_check").get();
            if (integrity.integrity_check !== "ok" || store.db.prepare("PRAGMA foreign_key_check").get()) {
                throw new Error("invalid source database");
            }
            const registry = store.db.prepare("PRAGMA user_version").get().user_version === 3;
            status = authoritySnapshot(store.db, registry);
            store.db.exec("COMMIT");
        } else if (command === "publish") status = store.publish(JSON.parse(readFileSync(argument, "utf8")));
        else if (command === "retire") status = store.retire(Number(argument));
        else if (command === "issue") status = { code: store.issue(JSON.parse(readFileSync(argument, "utf8"))) };
        else if (command === "rename" || command === "retire-runner") {
            status = store.changeRunner(JSON.parse(readFileSync(argument, "utf8")), command === "retire-runner");
        } else status = store.status();
        if (command === "export") process.stdout.write(JSON.stringify(status));
        else console.log(JSON.stringify(status));
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
