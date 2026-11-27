import { createPrivateKey, createPublicKey, sign } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fields, pairingChallenge, placement, sha256, targetFields } from "./authority-preflight.mjs";

export function runnerKey(seed) {
    if (seed.length !== 32) throw new Error("invalid fixture seed");
    const der = Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed]);
    const privateKey = createPrivateKey({ key: der, format: "der", type: "pkcs8" });
    return { privateKey, publicKey: createPublicKey(privateKey) };
}

const keyHex = key => key.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");

async function main([command, ...args]) {
    const production = command === "redeem-production";
    if ((command === "redeem" || production) && args.length === 3) {
        const [endpoint, seedFile, codeFile] = args;
        const origin = new URL(endpoint);
        if (production ? origin.protocol !== "https:" || origin.origin !== endpoint
            : !/^http:\/\/127\.0\.0\.1:[1-9][0-9]{0,4}$/.test(endpoint)) throw new Error("invalid Control endpoint");
        const code = (await readFile(codeFile, "utf8")).trim();
        if (!sha256(code)) throw new Error("invalid pairing code");
        const { privateKey, publicKey } = runnerKey(await readFile(seedFile));
        const response = await fetch(`${endpoint}/v1/pair`, {
            method: "POST", redirect: "error", signal: AbortSignal.timeout(3000),
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                code, key: keyHex(publicKey), signature: sign(null, pairingChallenge(code, production), privateKey).toString("hex"),
            }),
        });
        if (!response.ok) throw new Error(`pairing denied (HTTP ${response.status})`);
        const receipt = await response.json();
        if (!fields(receipt, ["generation", "objectId", "placement", "incarnation"]) ||
            !(production ? targetFields.generation(receipt.generation) : receipt.generation === 1) || !sha256(receipt.objectId) ||
            !placement(receipt.placement) || !sha256(receipt.incarnation)) throw new Error("invalid enrollment receipt");
        return receipt;
    }
    throw new Error("usage: pair-runner-local.mjs redeem LOOPBACK_CONTROL SEED CODE_FILE | redeem-production HTTPS_CONTROL SEED CODE_FILE");
}

if (import.meta.main) {
    process.umask(0o077);
    try {
        console.log(JSON.stringify(await main(process.argv.slice(2))));
    } catch (error) {
        console.error(error.code ?? error.message);
        process.exitCode = 1;
    }
}
