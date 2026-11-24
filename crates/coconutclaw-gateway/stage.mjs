import { copyFile, mkdir, readdir } from "node:fs/promises";
import { dirname, extname, join, relative, sep } from "node:path";
import {
    cleanBuildOutputDir, getWorkerBundleDir, writeRootConfig, writeWorkerConfig,
} from "@cloudflare/build-output-utils";
import { loadAndParseConfig } from "@cloudflare/config";

const root = import.meta.dirname;
const buildContext = { isPreview: false, mode: undefined };
const { result } = await loadAndParseConfig(join(root, "cloudflare.config.ts"), buildContext);
if (!result.success) throw result.error;
const { worker, containers, ...settings } = result.data;
if (typeof worker?.entrypoint !== "string") throw new Error("Expected a Worker entrypoint path");
if (containers.length !== 0) throw new Error("Expected a Worker without Containers");

const generated = await readdir(join(root, "build/worker"), { recursive: true, withFileTypes: true });
const generatedFiles = generated.filter(entry => entry.isFile())
    .map(entry => relative(root, join(entry.parentPath, entry.name)).split(sep).join("/"))
    .sort();
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
    await copyFile(join(root, modulePath), destination);
}
await writeWorkerConfig({
    root,
    config: worker,
    manifest: { type: "complete", mainModule: worker.entrypoint, modules },
});
await writeRootConfig(root, settings, buildContext);
