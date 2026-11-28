import { createHash, createPublicKey, randomBytes, timingSafeEqual, verify } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
    checkAuthorityHandoff, fields, pairingChallenge, placement, productionPairing, reference, sha256,
    targetFields, updateId, validEnrollments, validTarget,
} from "./authority-preflight.mjs";

export const permitMs = 900;
export const bodyLimit = 32 * 1024;
const closedUnsent = 0;
const freshObjectOnly = "fresh-object-only";
export const positiveInteger = value => Number.isSafeInteger(value) && value > 0;
const uuid = value => typeof value === "string" &&
    /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(value);
export const wireTarget = ({ target: { deployment, ...pin }, versionId }) => ({ pin, versionId });
const placements = ["alpha", "beta", "gamma"];
const invitationHash = code => createHash("sha256").update(code).digest("hex");
const aliasName = value => typeof value === "string" && /^[a-z0-9-]{1,32}$/.test(value);
export const enrollmentSchema = `
    CREATE TABLE invitations (
        hash TEXT PRIMARY KEY,
        generation INTEGER NOT NULL REFERENCES publications,
        key TEXT NOT NULL,
        alias TEXT,
        expires INTEGER NOT NULL,
        placement TEXT,
        incarnation TEXT UNIQUE,
        CHECK ((placement IS NULL) = (incarnation IS NULL)),
        UNIQUE (generation, placement)
    ) STRICT;
    CREATE UNIQUE INDEX enrolled_keys ON invitations(generation, key) WHERE placement IS NOT NULL;
    CREATE UNIQUE INDEX enrolled_aliases ON invitations(generation, alias) WHERE placement IS NOT NULL;`;
export const productionEnrollmentSchema = `
    ${enrollmentSchema}
    CREATE TABLE retired_runners (
        incarnation TEXT PRIMARY KEY REFERENCES invitations(incarnation)
    ) STRICT;`;

export const authoritySchema = `
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
    ) STRICT;`;

export class Denied extends Error {
    constructor(status) {
        super("authority request denied");
        this.status = status;
    }
}

export class AuthorityStore {
    constructor(db, write, { pairing = false, initializeRegistry, gatewayVersion } = {}) {
        this.db = db;
        this.write = write;
        this.pairing = pairing;
        this.initializeRegistry = initializeRegistry;
        this.gatewayVersion = gatewayVersion;
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

    publish(publication, encoded = JSON.stringify(publication)) {
        const fresh = publication?.continuity === freshObjectOnly;
        const production = publication?.target?.pairing === productionPairing;
        const keys = publication?.target?.executorKeys;
        const multi = keys !== undefined;
        const incarnations = publication?.runnerIncarnations;
        const members = ["target", "retirement", "versionId", fresh ? "continuity" : "objectIncarnation"];
        if (!this.pairing && !production) members.push(multi ? "runnerIncarnations" : "runnerIncarnation");
        if (production && publication.ingress !== undefined) members.push("ingress");
        if (!fields(publication, members) ||
            !validTarget(publication.target, this.pairing) || !reference(publication.versionId) ||
            (this.pairing ? production || publication.target.generation !== 1
                : production ? !fresh
                : multi ? fresh || !validEnrollments(incarnations) || !fields(incarnations, Object.keys(keys))
                    : !sha256(publication.runnerIncarnation)) ||
            (fresh ? publication.target.hostname !== "bigfur.liu.nz" : !uuid(publication.objectIncarnation))) {
            throw new Error("invalid publication");
        }
        const generation = checkAuthorityHandoff(publication, publication.target, this.pairing);
        return this.write(() => {
            const current = this.current();
            if (generation !== (current?.generation ?? 0) + 1 || (current && !current.retired)) {
                throw new Error("publication requires the next generation and a retired predecessor");
            }
            const runners = multi ? Object.values(incarnations) : [publication.runnerIncarnation];
            const history = this.db.prepare("SELECT publication FROM publications ORDER BY generation").all()
                .map(row => JSON.parse(row.publication));
            const reused = history.some(old => {
                if (!(fresh || multi || old.runnerIncarnations || old.continuity === freshObjectOnly)) return false;
                const previous = old.runnerIncarnations ? Object.values(old.runnerIncarnations) : [old.runnerIncarnation];
                return old.target.objectId === publication.target.objectId ||
                    previous.some(runner => runner !== undefined && runners.includes(runner)) ||
                    (old.target.pairing === productionPairing && !production);
            });
            if (reused) throw new Error("publication requires a fresh object and runner incarnation");
            if (production) {
                const covered = new Set(history.flatMap(old => (old.ingress ?? []).map(record => record.generation)));
                const expected = history.map(old => old.target)
                    .filter(target => target.hostname === "bigfur.liu.nz" && !covered.has(target.generation))
                    .map(({ generation, objectId, custodyDigest }) => ({ generation, objectId, custodyDigest }));
                const submitted = (publication.ingress ?? []).map(({ updateIds, ...source }) => source);
                if (!isDeepStrictEqual(expected, submitted)) throw new Error("missing or mismatched frozen ingress handoff");
            }
            if (production) this.initializeRegistry?.();
            this.db.prepare("INSERT INTO publications VALUES (?, ?)").run(generation, encoded);
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

    active() {
        const current = this.current();
        if (!current || current.retired) throw new Denied(503);
        return { generation: current.generation, publication: JSON.parse(current.publication) };
    }

    issue({ key, alias = null, ttlMs = 5 * 60_000 }) {
        if (!sha256(key) || (alias !== null && !aliasName(alias)) ||
            !positiveInteger(ttlMs) || ttlMs > 5 * 60_000) throw new Denied(400);
        return this.write(() => {
            const { generation, publication } = this.active();
            if (!this.pairing && publication.target.pairing !== productionPairing) throw new Denied(503);
            const code = randomBytes(32).toString("hex");
            this.db.prepare("INSERT INTO invitations (hash,generation,key,alias,expires) VALUES (?,?,?,?,?)")
                .run(invitationHash(code), generation, key, alias, Date.now() + ttlMs);
            return code;
        });
    }

    redeem(body) {
        if (!fields(body, ["code", "key", "signature"]) || !sha256(body.code) || !sha256(body.key) ||
            typeof body.signature !== "string" || !/^[a-f0-9]{128}$/.test(body.signature)) throw new Denied(400);
        return this.write(() => {
            const current = this.current();
            if (!current || (this.pairing && current.retired)) throw new Denied(503);
            const publication = JSON.parse(current.publication);
            const production = publication.target.pairing === productionPairing;
            if (!this.pairing && !production) throw new Denied(503);
            const hash = invitationHash(body.code);
            const invite = this.db.prepare("SELECT * FROM invitations WHERE hash=?").get(hash);
            if (!invite || !equalSecret(body.key, invite.key)) throw new Denied(401);
            const der = Buffer.from(`302a300506032b6570032100${body.key}`, "hex");
            const key = createPublicKey({ key: der, format: "der", type: "spki" });
            if (!verify(null, pairingChallenge(body.code, production), key, Buffer.from(body.signature, "hex"))) throw new Denied(401);
            const { generation } = invite;
            const currentGeneration = current.generation === generation;
            const receipt = (placement, incarnation) => {
                const issued = currentGeneration ? publication : JSON.parse(this.db
                    .prepare("SELECT publication FROM publications WHERE generation=?").get(generation).publication);
                return { generation, objectId: issued.target.objectId, placement, incarnation };
            };
            if (invite.placement !== null) {
                if (!production) throw new Denied(409);
                return receipt(invite.placement, invite.incarnation);
            }
            if (current.retired || !currentGeneration) throw new Denied(503);
            if (invite.expires <= Date.now()) throw new Denied(410);
            const enrolled = this.db.prepare("SELECT placement,key,alias FROM invitations WHERE generation=? AND placement IS NOT NULL")
                .all(generation);
            const name = placements.find(name => !enrolled.some(runner => runner.placement === name));
            if (!name || (name !== "alpha" && invite.alias === null) ||
                enrolled.some(runner => runner.key === invite.key || runner.alias === invite.alias)) throw new Denied(409);
            if (production && (this.db.prepare("SELECT 1 FROM invitations WHERE key=? AND placement IS NOT NULL")
                .get(invite.key) || this.db.prepare("SELECT publication FROM publications").all()
                .some(row => JSON.parse(row.publication).target.executorKey === invite.key))) throw new Denied(409);
            const incarnation = randomBytes(32).toString("hex");
            this.db.prepare("UPDATE invitations SET placement=?,incarnation=? WHERE hash=?").run(name, incarnation, hash);
            return receipt(name, incarnation);
        });
    }

    changeRunner(record, retiring) {
        const expected = retiring ? ["placement", "incarnation"] : ["placement", "incarnation", "alias"];
        if (!fields(record, expected)) throw new Denied(400);
        const { placement: name, alias, incarnation } = record;
        if (!placement(name) || !sha256(incarnation) ||
            (!retiring && !aliasName(alias))) throw new Denied(400);
        return this.write(() => {
            const { generation, publication } = this.active();
            if (publication.target.pairing !== productionPairing) throw new Denied(503);
            const runner = this.db.prepare("SELECT incarnation FROM invitations WHERE generation=? AND placement=?")
                .get(generation, name);
            if (!runner || !equalSecret(incarnation, runner.incarnation)) throw new Denied(409);
            if (retiring) {
                this.db.prepare("INSERT OR IGNORE INTO retired_runners VALUES (?)").run(incarnation);
            } else {
                const changed = this.db.prepare(`UPDATE invitations SET alias=? WHERE incarnation=?
                    AND NOT EXISTS (SELECT 1 FROM retired_runners WHERE incarnation=?)`)
                    .run(alias, incarnation, incarnation);
                if (!changed.changes) throw new Denied(409);
            }
            return { generation, placement: name };
        });
    }

    authorize({ target, objectIncarnation, runnerIncarnation, placement: name, deliveryCheck, updateId }, credentialVersion) {
        const current = this.current();
        const generation = target?.pin?.generation;
        if (!current || !targetFields.generation(generation)) throw new Denied(503);
        const row = this.db.prepare(`
            SELECT publication, retirements.generation IS NOT NULL AS retired
            FROM publications LEFT JOIN retirements USING (generation) WHERE generation=?
        `).get(generation);
        if (!row) throw new Denied(503);
        const enrolled = JSON.parse(row.publication);
        const production = enrolled.target.pairing === productionPairing;
        const historical = row.retired || generation !== current.generation;
        const expected = wireTarget(enrolled);
        expected.versionId = this.gatewayVersion?.(generation) ?? expected.versionId;
        if (!isDeepStrictEqual(target, expected) ||
            (!historical && enrolled.target.telegramCredentialVersion !== credentialVersion)) {
            throw new Denied(503);
        }
        if ((enrolled.continuity !== freshObjectOnly && objectIncarnation !== enrolled.objectIncarnation) ||
            (historical && (runnerIncarnation === null || enrolled.continuity !== freshObjectOnly))) {
            throw new Denied(deliveryCheck ? 410 : 503);
        }
        if (updateId !== undefined && !production) throw new Denied(503);
        if (this.pairing || production) {
            if (name === undefined && runnerIncarnation === null) {
                const routes = this.db.prepare(`SELECT placement,alias FROM invitations
                    WHERE generation=? AND placement IS NOT NULL
                    ${production ? "AND incarnation NOT IN (SELECT incarnation FROM retired_runners)" : ""}
                    ORDER BY placement`)
                    .all(generation);
                const duplicate = updateId === undefined ? {} : {
                    duplicate: !!this.db.prepare(`
                        SELECT 1 FROM publications,
                            json_each(publication, '$.ingress') AS handoff,
                            json_each(handoff.value, '$.updateIds') AS receipt
                        WHERE receipt.value=? LIMIT 1
                    `).get(updateId),
                };
                return { generation, routes, ...duplicate };
            }
            const runner = this.db.prepare("SELECT key,incarnation FROM invitations WHERE generation=? AND placement=?")
                .get(generation, name ?? "");
            if (!runner || !equalSecret(runnerIncarnation, runner.incarnation)) throw new Denied(503);
            const retired = production && this.db.prepare("SELECT 1 FROM retired_runners WHERE incarnation=?")
                .get(runner.incarnation);
            return { generation: current.generation, key: runner.key, ...(historical || retired ? { reconcileOnly: true } : {}) };
        }
        const runner = enrolled.runnerIncarnations ? enrolled.runnerIncarnations[name] : enrolled.runnerIncarnation;
        if ((name !== undefined && !enrolled.runnerIncarnations) ||
            (runnerIncarnation !== null && (runner === undefined || !equalSecret(runnerIncarnation, runner)))) {
            throw new Denied(503);
        }
        return { generation: current.generation, ...(historical ? { reconcileOnly: true } : {}) };
    }

    closeUnsent(request, credentialVersion) {
        return this.write(() => {
            const { generation } = this.authorize(request, credentialVersion);
            const { task, sendNumber } = request.delivery;
            this.db.prepare("INSERT OR IGNORE INTO sends VALUES (?, ?, ?, ?)")
                .run(generation, task, sendNumber, closedUnsent);
            const { message_id: messageId } = this.db.prepare(
                "SELECT message_id FROM sends WHERE generation=? AND task=? AND number=?",
            ).get(generation, task, sendNumber);
            if (messageId !== closedUnsent) throw new Denied(409);
        });
    }

    reserve(request, credentialVersion) {
        const { task, sendNumber } = request.delivery;
        return this.write(() => {
            const { generation } = this.authorize(request, credentialVersion);
            const result = this.db.prepare("INSERT OR IGNORE INTO sends VALUES (?, ?, ?, NULL)")
                .run(generation, task, sendNumber);
            if (!result.changes) throw new Denied(409);
            return generation;
        });
    }

    recordSend(generation, { task, sendNumber }, messageId) {
        if (!positiveInteger(messageId)) throw new Denied(502);
        const { changes } = this.db.prepare("UPDATE sends SET message_id=? WHERE generation=? AND task=? AND number=? AND message_id IS NULL")
            .run(messageId, generation, task, sendNumber);
        if (!changes) throw new Denied(503);
        return messageId;
    }
}

export function equalSecret(candidate, secret) {
    return sha256(candidate) && sha256(secret) &&
        timingSafeEqual(Buffer.from(candidate, "hex"), Buffer.from(secret, "hex"));
}

export function parseJson(bytes) {
    try {
        return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    } catch {
        throw new Denied(400);
    }
}

function validDelivery(delivery) {
    const members = ["task", "sendNumber", "chatId", "messageThreadId", "text"];
    if (delivery?.closeUnsent !== undefined) {
        if (delivery.closeUnsent !== true) return false;
        members.push("closeUnsent");
    }
    return fields(delivery, members) &&
        typeof delivery.task === "string" && /^[A-Za-z0-9:_-]{1,128}$/.test(delivery.task) &&
        positiveInteger(delivery.sendNumber) &&
        Number.isSafeInteger(delivery.chatId) && delivery.chatId !== 0 &&
        (delivery.messageThreadId === null ||
            (positiveInteger(delivery.messageThreadId) && delivery.messageThreadId <= 0x7fff_ffff)) &&
        typeof delivery.text === "string" && delivery.text.trim().length > 0 &&
        Buffer.byteLength(delivery.text) <= 4096;
}

export function validateRequest(body, check) {
    const members = ["nonce", "target", "objectIncarnation", "runnerIncarnation", "delivery"];
    const scoped = body?.placement !== undefined;
    if (scoped) members.push("placement");
    if (body?.deliveryCheck !== undefined) members.push("deliveryCheck");
    if (body?.updateId !== undefined) members.push("updateId");
    if (!fields(body, members) ||
        !uuid(body.nonce) || !uuid(body.objectIncarnation) ||
        (scoped && (!check || !placement(body.placement) || !sha256(body.runnerIncarnation))) ||
        (body.deliveryCheck !== undefined && (!check || body.deliveryCheck !== true || body.runnerIncarnation !== null)) ||
        (body.updateId !== undefined && (!check || body.deliveryCheck !== undefined ||
            body.runnerIncarnation !== null || !updateId(body.updateId))) ||
        (check ? body.delivery !== null || (body.runnerIncarnation !== null && !sha256(body.runnerIncarnation))
            : body.runnerIncarnation !== null || !validDelivery(body.delivery))) throw new Denied(400);
}

export function checkPermit(store, body, credentialVersion) {
    const validUntil = Date.now() + permitMs;
    const authorize = () => store.authorize(body, credentialVersion);
    const { generation: highWater, ...enrollment } = body.deliveryCheck ? store.write(authorize) : authorize();
    if (validUntil <= Date.now()) throw new Denied(503);
    const { delivery, deliveryCheck, updateId, ...binding } = body;
    return { ...binding, highWater, validUntil, ...enrollment };
}
