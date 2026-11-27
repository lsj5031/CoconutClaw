import { readFile } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";

const generation = value => Number.isInteger(value) && value > 0 && value <= 0xffff_ffff;
export const reference = value => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$/.test(value);
export const sha256 = value => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
export const productionPairing = "production-v1";
export const updateId = value => Number.isInteger(value) && value >= 0 && value <= 0x7fff_ffff;
export const pairingChallenge = (code, production = false) =>
    Buffer.from(`coconutclaw-${production ? "production" : "fixture"}-pairing-v1\n${code}`);
const hostname = value => typeof value === "string" && value.length <= 253 &&
    /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(value);

export const targetFields = {
    generation,
    revision: value => typeof value === "string" && /^[a-f0-9]{40}$/.test(value),
    deployment: reference,
    hostname,
    objectId: sha256,
    custodyDigest: sha256,
    telegramCredentialVersion: reference,
    executorKey: sha256,
};

export function fields(value, keys) {
    return value !== null && typeof value === "object" &&
        Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
}

export const placement = value => ["alpha", "beta", "gamma"].includes(value);

export function validEnrollments(value) {
    if (value === null || typeof value !== "object" || !Object.hasOwn(value, "alpha")) return false;
    const entries = Object.entries(value);
    return entries.every(([name, hex]) => placement(name) && sha256(hex)) &&
        new Set(entries.map(([, hex]) => hex)).size === entries.length;
}

function targetValidators(target, pairing) {
    const { executorKey, ...common } = targetFields;
    if (target?.pairing === productionPairing) {
        return { ...common, hostname: value => value === "bigfur.liu.nz", pairing: value => value === productionPairing };
    }
    if (pairing) {
        return { ...common, hostname: value => value === "127.0.0.1", pairing: value => value === "fixture-v1" };
    }
    return target?.hostname === "127.0.0.1"
        ? { ...common, hostname: value => value === "127.0.0.1", executorKeys: validEnrollments }
        : targetFields;
}

export function validTarget(target, pairing = false) {
    const validators = targetValidators(target, pairing);
    return fields(target, Object.keys(validators)) &&
        Object.entries(validators).every(([key, valid]) => valid(target[key]));
}

function validFrozenIngress(records, successor) {
    if (!Array.isArray(records)) return false;
    return records.every((record, index) => {
        if (!fields(record, ["generation", "objectId", "custodyDigest", "updateIds"]) ||
            !generation(record.generation) || record.generation >= successor ||
            (index > 0 && record.generation <= records[index - 1].generation) ||
            !sha256(record.objectId) || !sha256(record.custodyDigest) ||
            !Array.isArray(record.updateIds) || record.updateIds.length > 128) return false;
        return record.updateIds.every((id, position, ids) => updateId(id) && (position === 0 || id > ids[position - 1]));
    });
}

export function checkAuthorityHandoff(current, candidate, pairing = false) {
    const validators = targetValidators(candidate, pairing);
    for (const [field, valid] of Object.entries(validators)) {
        if (!valid(candidate?.[field]) || !isDeepStrictEqual(candidate[field], current?.target?.[field])) {
            throw new Error(`authority target mismatch: ${field}`);
        }
    }
    if (!fields(candidate, Object.keys(validators)) || !isDeepStrictEqual(candidate, current.target)) {
        throw new Error("invalid authority target");
    }
    const retirement = current.retirement;
    if (retirement?.generation !== candidate.generation - 1) {
        throw new Error("missing or mismatched predecessor generation");
    }
    for (const field of ["writers", "executorGrants", "custody"]) {
        if (!reference(retirement[field])) {
            throw new Error(`missing independently verified evidence: ${field}`);
        }
    }
    if (!["exclusive-existing-token", "issuer-invalidated"].includes(retirement.telegram?.disposition)
        || !reference(retirement.telegram?.evidence)) {
        throw new Error("missing independently verified evidence: telegram");
    }
    if (current.ingress !== undefined &&
        (candidate.pairing !== productionPairing || !validFrozenIngress(current.ingress, candidate.generation))) {
        throw new Error("invalid frozen ingress handoff");
    }
    return candidate.generation;
}

if (import.meta.main) {
    const [currentPath, candidatePath, ...extra] = process.argv.slice(2);
    if (!currentPath || !candidatePath || extra.length) {
        throw new Error("Expected CURRENT_AUTHORITY.json CANDIDATE.json");
    }
    const current = JSON.parse(await readFile(currentPath, "utf8"));
    const candidate = JSON.parse(await readFile(candidatePath, "utf8"));
    const checked = checkAuthorityHandoff(current, candidate);
    console.log(JSON.stringify({ checkedGeneration: checked }));
}
