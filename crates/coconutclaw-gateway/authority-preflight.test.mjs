import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { checkAuthorityHandoff } from "./authority-preflight.mjs";

const target = {
    generation: 2,
    revision: "a".repeat(40),
    deployment: "synthetic-account/gateway/deployment-2/worker-version-2",
    hostname: "synthetic.gateway.invalid",
    objectId: "b".repeat(64),
    custodyDigest: "c".repeat(64),
    telegramCredentialVersion: "synthetic-token-2",
    executorKey: "d".repeat(64),
};
const retirement = {
    generation: 1,
    writers: "evidence/writers",
    executorGrants: "evidence/executor",
    telegram: "evidence/telegram",
    custody: "evidence/custody",
};
const current = { target, retirement };

test("preflight binds the candidate to the current target and complete predecessor evidence", () => {
    assert.equal(checkAuthorityHandoff(current, { ...target }), 2);
    for (const [field, different] of Object.entries({
        generation: 1,
        revision: "e".repeat(40),
        deployment: "synthetic-other-deployment",
        hostname: "other.gateway.invalid",
        objectId: "e".repeat(64),
        custodyDigest: "e".repeat(64),
        telegramCredentialVersion: "synthetic-token-1",
        executorKey: "e".repeat(64),
    })) {
        assert.throws(() => checkAuthorityHandoff(current, { ...target, [field]: different }),
            new RegExp(`target mismatch: ${field}`));
    }
    for (const field of Object.keys(retirement).filter(field => field !== "generation")) {
        const { [field]: omitted, ...incomplete } = retirement;
        assert.ok(omitted);
        assert.throws(() => checkAuthorityHandoff({ target, retirement: incomplete }, target),
            new RegExp(`missing independently verified evidence: ${field}`));
    }
    assert.throws(() => checkAuthorityHandoff({ target, retirement: { ...retirement, generation: 0 } }, target),
        /mismatched predecessor generation/);
    const malformed = { ...target, generation: "2" };
    assert.throws(() => checkAuthorityHandoff({ target: malformed, retirement }, malformed),
        /target mismatch: generation/);
    assert.throws(() => checkAuthorityHandoff(undefined, target), /target mismatch: generation/);
});

test("preflight command emits a generation only on a checked handoff", async context => {
    const dir = await mkdtemp(join(tmpdir(), "gateway-authority-"));
    context.after(() => rm(dir, { recursive: true, force: true }));
    const currentPath = join(dir, "current.json");
    const candidatePath = join(dir, "candidate.json");
    const command = fileURLToPath(new URL("./authority-preflight.mjs", import.meta.url));
    const run = () => spawnSync(process.execPath, [command, currentPath, candidatePath], { encoding: "utf8" });

    await writeFile(currentPath, JSON.stringify(current));
    await writeFile(candidatePath, JSON.stringify(target));
    const checked = run();
    assert.equal(checked.status, 0, checked.stderr);
    assert.deepEqual(JSON.parse(checked.stdout), { checkedGeneration: 2 });

    await writeFile(currentPath, "{");
    const malformed = run();
    assert.equal(malformed.status, 1);
    assert.equal(malformed.stdout, "");
});
