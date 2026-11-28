import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { readBuildOutput } from "@cloudflare/build-output-utils";
import { wireTarget } from "./authority-core.mjs";
import { checkAuthorityHandoff, fields, reference, sha256, targetFields, validTarget } from "./authority-preflight.mjs";

const mode = "phase2-production-control";
const root = import.meta.dirname;

function activePin(sourcePath, verifiedDigest) {
    const bytes = readFileSync(sourcePath);
    if (!sha256(verifiedDigest) || createHash("sha256").update(bytes).digest("hex") !== verifiedDigest) {
        throw new Error();
    }
    const source = JSON.parse(bytes);
    if (!fields(source, ["format", "publications", "retirements", "invitations", "retired_runners", "sends"]) ||
        source.format !== "bigfur-control-v1" ||
        ![source.publications, source.retirements, source.invitations, source.retired_runners, source.sends]
            .every(Array.isArray)) {
        throw new Error();
    }
    const active = source.publications.at(-1);
    if (!active || !targetFields.generation(active.generation) ||
        !source.publications.every((row, index) =>
            fields(row, ["generation", "publication"]) && row.generation === index + 1) ||
        source.retirements.length !== active.generation - 1 ||
        !source.retirements.every((row, index) => fields(row, ["generation"]) && row.generation === index + 1)) {
        throw new Error();
    }
    const publication = JSON.parse(active.publication);
    const { target, versionId } = publication;
    if (!validTarget(target) || target.generation !== active.generation ||
        target.hostname !== "bigfur.liu.nz" || !reference(versionId)) {
        throw new Error();
    }
    checkAuthorityHandoff(publication, target);
    return wireTarget(publication).pin;
}

const args = process.argv.slice(2);
const checkOnly = args[0] === "--check";
if (checkOnly) args.shift();
let secrets;
try {
    const [sourcePath, verifiedDigest, secretsPath] = args;
    if (args.length !== 3) throw new Error();
    const { rootConfig: { buildContext }, workers, containers } = await readBuildOutput(root);
    const { bundleDir, config } = workers.default;
    if (buildContext.mode !== mode || buildContext.isPreview !== false ||
        !bundleDir || !config.manifest || config.env?.CUTOVER_TARGET?.type !== "secret" ||
        Object.keys(workers).length !== 1 || containers.length !== 0) {
        throw new Error();
    }
    const expected = activePin(sourcePath, verifiedDigest);
    secrets = JSON.parse(readFileSync(secretsPath));
    const pin = JSON.parse(secrets.CUTOVER_TARGET);
    if (Object.getPrototypeOf(secrets) !== Object.prototype ||
        Object.values(secrets).some(value => typeof value !== "string") ||
        !isDeepStrictEqual(pin, expected)) {
        throw new Error();
    }
    secrets.CUTOVER_TARGET = JSON.stringify(pin);
} catch {
    console.error("BigFur release preflight failed. Check the staged mode, verified active export and staged secrets.");
    process.exit(1);
}

if (checkOnly) {
    console.log("BigFur release preflight passed.");
} else {
    let directory;
    try {
        directory = mkdtempSync(join(tmpdir(), "bigfur-release-"));
        const path = join(directory, "secrets.json");
        writeFileSync(path, JSON.stringify(secrets), { flag: "wx", mode: 0o600 });
        const result = spawnSync("cf", ["workers", "versions", "create", "--prebuilt", "--mode", mode, "--secrets-file", path], {
            cwd: root,
            env: { ...process.env, CF_SEND_TELEMETRY: "false", FORCE_COLOR: "0", CF_QUIET: "false" },
            encoding: "utf8",
            stdio: ["ignore", "pipe", "pipe"],
        });
        if (result.error || result.status !== 0) throw new Error();
        const versionLines = result.stdout.split(/\r?\n/).filter(line => /^\s*(?:│\s*)?Worker Version ID:/.test(line));
        const version = versionLines.length === 1 && /^\s*(?:│\s*)?Worker Version ID: ([a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12})\s*$/
            .exec(versionLines[0]);
        if (!version) throw new Error();
        console.log(version[1]);
    } catch {
        console.error("BigFur upload did not return a verified candidate version ID. Check upload state before retrying.");
        process.exitCode = 1;
    } finally {
        if (directory) rmSync(directory, { recursive: true, force: true });
    }
}
