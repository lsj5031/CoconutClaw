import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { Denied, positiveInteger } from "./authority-core.mjs";
import { fields, placement, productionPairing, reference, sha256, targetFields } from "./authority-preflight.mjs";

export const migrationBodyLimit = 1024 * 1024;
export const migrationSchema = `
    CREATE TABLE migration (
        singleton INTEGER PRIMARY KEY CHECK (singleton=1),
        source_sha256 TEXT NOT NULL,
        generation INTEGER NOT NULL REFERENCES publications,
        gateway_version TEXT
    ) STRICT;`;

const tables = {
    publications: { columns: ["generation", "publication"], order: "generation" },
    retirements: { columns: ["generation"], order: "generation" },
    invitations: { columns: ["hash", "generation", "key", "alias", "expires", "placement", "incarnation"], order: "hash" },
    retired_runners: { columns: ["incarnation"], order: "incarnation" },
    sends: { columns: ["generation", "task", "number", "message_id"], order: "generation,task,number" },
};

export function authoritySnapshot(db, registry = true) {
    const snapshot = { format: "bigfur-control-v1" };
    for (const [table, { columns, order }] of Object.entries(tables)) {
        snapshot[table] = !registry && ["invitations", "retired_runners"].includes(table) ? []
            : db.prepare(`SELECT ${columns.join(",")} FROM ${table} ORDER BY ${order}`).all()
                .map(row => Object.fromEntries(columns.map(column => [column, row[column]])));
    }
    return snapshot;
}

export const snapshotDigest = snapshot => createHash("sha256").update(JSON.stringify(snapshot)).digest("hex");
export const migrationRecord = db => db.prepare(
    "SELECT source_sha256 AS sourceSha256, generation, gateway_version AS gatewayVersion FROM migration WHERE singleton=1",
).get();

export function importAuthority(store, { source, sourceSha256, gatewayVersion }) {
    if (!fields(source, ["format", ...Object.keys(tables)]) || source.format !== "bigfur-control-v1" ||
        !sha256(sourceSha256) || snapshotDigest(source) !== sourceSha256 ||
        (gatewayVersion !== null && !reference(gatewayVersion))) throw new Denied(400);
    for (const [table, { columns }] of Object.entries(tables)) {
        if (!Array.isArray(source[table]) || !source[table].every(row => fields(row, columns))) throw new Denied(400);
    }

    return store.write(() => {
        const { db } = store;
        const imported = migrationRecord(db);
        if (imported) {
            if (imported.sourceSha256 !== sourceSha256 || imported.gatewayVersion !== gatewayVersion) throw new Denied(409);
            return imported;
        }
        if (store.current() || !source.publications.length) throw new Denied(409);
        try {
            const production = new Set();
            const retiredGenerations = new Set(source.retirements.map(row => row.generation));
            for (const row of source.publications) {
                const publication = JSON.parse(row.publication);
                if (row.generation !== publication?.target?.generation) throw new Denied(400);
                store.publish(publication, row.publication);
                if (retiredGenerations.has(row.generation)) store.retire(row.generation);
                if (publication.target.pairing === productionPairing) production.add(row.generation);
            }
            const { highWater: generation, retired } = store.status();
            const current = JSON.parse(store.current().publication);
            if (retired ? gatewayVersion !== null : !gatewayVersion || gatewayVersion === current.versionId ||
                current.continuity !== "fresh-object-only") throw new Denied(400);

            for (const row of source.invitations) {
                if (!production.has(row.generation) || !sha256(row.hash) || !sha256(row.key) ||
                    !positiveInteger(row.expires) || (row.alias !== null &&
                        (typeof row.alias !== "string" || !/^[a-z0-9-]{1,32}$/.test(row.alias))) ||
                    (row.placement === null ? row.incarnation !== null :
                        !placement(row.placement) || !sha256(row.incarnation) ||
                        (row.placement !== "alpha" && row.alias === null))) throw new Denied(400);
            }
            for (const row of source.sends) {
                if (!targetFields.generation(row.generation) || !positiveInteger(row.number) ||
                    typeof row.task !== "string" || !/^[A-Za-z0-9:_-]{1,128}$/.test(row.task) ||
                    (row.message_id !== null && row.message_id !== 0 && !positiveInteger(row.message_id))) throw new Denied(400);
            }
            for (const table of ["invitations", "retired_runners", "sends"]) {
                const { columns } = tables[table];
                const insert = db.prepare(`INSERT INTO ${table} (${columns.join(",")}) VALUES (${columns.map(() => "?").join(",")})`);
                for (const row of source[table]) insert.run(...columns.map(column => row[column]));
            }
            if (!isDeepStrictEqual(authoritySnapshot(db), source)) throw new Denied(400);
            db.prepare("INSERT INTO migration VALUES (1,?,?,?)").run(sourceSha256, generation, gatewayVersion);
            return { sourceSha256, generation, gatewayVersion };
        } catch {
            throw new Denied(400);
        }
    });
}
