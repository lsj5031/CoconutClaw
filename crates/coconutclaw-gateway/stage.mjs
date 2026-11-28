import { copyFile, mkdir, readdir } from "node:fs/promises";
import { dirname, extname, join, relative, sep } from "node:path";
import {
    cleanBuildOutputDir, getWorkerBundleDir, writeRootConfig, writeWorkerConfig,
} from "@cloudflare/build-output-utils";
import { loadAndParseConfig } from "@cloudflare/config";

const source = import.meta.dirname;
const [mode, ...extra] = process.argv.slice(2);
const control = mode === "control" || mode === "control-staging";
const root = control ? join(source, "control") : source;
const modes = ["phase2-pilot", "phase2-production", "phase2-production-control", "phase2-control-staging",
    "control", "control-staging"];
if (extra.length || (mode !== undefined && !modes.includes(mode))) {
    throw new Error(`Expected no mode or one of ${modes.join(", ")}`);
}
const buildContext = { isPreview: false, mode: control ? (mode === "control-staging" ? "staging" : undefined) : mode };
const { result } = await loadAndParseConfig(join(root, "cloudflare.config.ts"), buildContext);
if (!result.success) throw result.error;
const { worker, containers, ...settings } = result.data;
if (typeof worker?.entrypoint !== "string") throw new Error("Expected a Worker entrypoint path");
if (containers.length !== 0) throw new Error("Expected a Worker without Containers");

const generatedFiles = control ? [
    "authority-core.mjs", "authority-preflight.mjs", "control-migration.mjs",
    "automation-digest.mjs", "automation-ledger.mjs",
]
    : (await readdir(join(source, "build/worker"), { recursive: true, withFileTypes: true }))
        .filter(entry => entry.isFile())
        .map(entry => relative(source, join(entry.parentPath, entry.name)).split(sep).join("/")).sort();
const moduleTypes = { ".js": "esm", ".mjs": "esm", ".wasm": "wasm" };
const modules = Object.fromEntries([worker.entrypoint, ...generatedFiles].map(modulePath => {
    const type = moduleTypes[extname(modulePath)];
    if (!type) throw new Error(`Unsupported Worker module ${modulePath}`);
    return [modulePath, { type }];
}));

await cleanBuildOutputDir(root);
const bundle = getWorkerBundleDir(root);
for (const modulePath of Object.keys(modules)) {
    const destination = join(bundle, modulePath);
    await mkdir(dirname(destination), { recursive: true });
    await copyFile(join(source, modulePath), destination);
}
await writeWorkerConfig({
    root,
    config: worker,
    manifest: { type: "complete", mainModule: worker.entrypoint, modules },
});
await writeRootConfig(root, settings, buildContext);
