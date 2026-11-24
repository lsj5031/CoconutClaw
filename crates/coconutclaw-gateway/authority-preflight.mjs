import { readFile } from "node:fs/promises";

const generation = value => Number.isInteger(value) && value > 0 && value <= 0xffff_ffff;
const reference = value => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$/.test(value);
const sha256 = value => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const hostname = value => typeof value === "string" && value.length <= 253 &&
    /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(value);

const targetFields = {
    generation,
    revision: value => typeof value === "string" && /^[a-f0-9]{40}$/.test(value),
    deployment: reference,
    hostname,
    objectId: sha256,
    custodyDigest: sha256,
    telegramCredentialVersion: reference,
    executorKey: sha256,
};

export function checkAuthorityHandoff(current, candidate) {
    for (const [field, valid] of Object.entries(targetFields)) {
        if (!valid(candidate?.[field]) || candidate[field] !== current?.target?.[field]) {
            throw new Error(`authority target mismatch: ${field}`);
        }
    }
    const retirement = current.retirement;
    if (retirement?.generation !== candidate.generation - 1) {
        throw new Error("missing or mismatched predecessor generation");
    }
    for (const field of ["writers", "executorGrants", "telegram", "custody"]) {
        if (!reference(retirement[field])) {
            throw new Error(`missing independently verified evidence: ${field}`);
        }
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
