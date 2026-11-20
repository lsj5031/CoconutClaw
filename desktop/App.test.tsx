import { expect, open, test } from "nsh/test";
import { demo } from "./demo";
import { maxSnapshotBytes } from "./snapshot";

test("reachability, assigned computer, evidence, and delivery stay independent", async () => {
  const app = await open("./App.tsx");
  expect(app.texts()).toContain("Synthetic demo");
  expect(app.texts()).toContain("Reachable at last check");
  expect(app.texts()).toContain("Runner · 51c2a740");
  expect(app.texts().join("\n")).toContain(
    "Unreachable at last check\nChecked\n2026-11-20 · 01:50:00 UTC\nLast reached\n2026-11-20 · 01:14:00 UTC",
  );

  await app.press("Jobs");
  const jobs = app.texts().join("\n");
  expect(jobs).toContain("Run #1042\nUnknown\nAssigned computer\nRunner · 51c2a740");
  expect(jobs).toContain("Signed start\nNo verified start\nSigned result\nNo verified result\nMessage delivery\nDelivered");
  expect(jobs).toContain("Verified start\nSigned result\nVerified result\nMessage delivery\nPending");

  await app.press("Nodes");
  expect(app.texts()).toContain("Reachable at last check");
});

test("reviews only a selected public card and clears it on cancel or rejection", async () => {
  const app = await open("./App.tsx");
  const card = {
    version: 1, role: "coordinator", id: "ab".repeat(32),
    address: "[2001:db8::10]:49731", revision: "cd".repeat(20),
  };
  const pick = (value: unknown) => [{
    name: "public.json", data: Uint8Array.from(JSON.stringify(value), (char) => char.charCodeAt(0)),
  }];
  app.onPick(() => pick(card));
  await app.press("Review public card");
  for (const value of ["Coordinator card for runner", card.id, card.address, card.revision]) {
    expect(app.texts()).toContain(value);
  }
  await app.press("Close card");
  expect(app.texts()).not.toContain(card.id);
  await app.press("Review public card");
  app.onPick(() => null);
  await app.press("Review public card");
  expect(app.texts()).not.toContain(card.id);

  app.onPick(() => pick({ ...card, workspace: "private path" }));
  await app.press("Review public card");
  expect(app.texts()).toContain("Couldn’t review the card. Choose a UTF-8 public node card from nodes card.");
  expect(app.texts()).not.toContain("private path");
  await app.press("Jobs");
  expect(app.texts()).toContain("Run #1042");
});

test("opens an explicit export, keeps its snapshot after cancel or invalid selection", async () => {
  const app = await open("./App.tsx");
  const pick = (contents: string) => [{
    name: "snapshot.json", data: Uint8Array.from(contents, (char) => char.charCodeAt(0)),
  }];
  const exportData = {
    ...demo,
    source: "coordinator",
    nodes: [],
    jobs: [{ ...demo.jobs[0], signed_evidence: "verified" }],
  };
  app.onPick((request) => {
    expect(request.maxSize).toBe(maxSnapshotBytes);
    return pick(JSON.stringify(exportData));
  });

  await app.press("Open snapshot");
  expect(app.texts()).toContain("Snapshot · 2026-11-20 · 01:52:00 UTC");
  expect(app.texts()).toContain("No paired runners in this snapshot.");
  await app.press("Jobs");
  expect(app.texts()).toContain("Unknown");
  expect(app.texts()).toContain("Verified result");

  app.onPick(() => null);
  await app.press("Open snapshot");
  expect(app.texts()).toContain("Run #1042");

  app.onPick(() => pick(JSON.stringify({ ...exportData, jobs: [{ ...exportData.jobs[0], status: "constructor" }] })));
  await app.press("Open snapshot");
  expect(app.texts()).toContain("Couldn’t open the snapshot. Choose a coordinator JSON export under 64 KiB.");
  expect(app.texts()).toContain("Run #1042");

  app.onPick(() => null);
  await app.press("Open snapshot");
  expect(app.texts()).toContain("Couldn’t open the snapshot. Choose a coordinator JSON export under 64 KiB.");
  expect(app.texts()).toContain("Run #1042");

  app.onPick(() => pick(JSON.stringify({ ...exportData, jobs: [] })));
  await app.press("Open snapshot");
  expect(app.texts()).toContain("No jobs in this snapshot.");
  expect(app.texts()).not.toContain("Couldn’t open the snapshot. Choose a coordinator JSON export under 64 KiB.");
});
