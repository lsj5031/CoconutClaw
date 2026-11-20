import { expect, open, test } from "nsh/test";
import { demo } from "./demo";
import { maxSnapshotBytes } from "./snapshot";

test("reachability, assigned computer, evidence, and delivery stay independent", async () => {
  const app = await open("./App.tsx");
  expect(app.texts()).toContain("Synthetic demo");
  expect(app.texts()).toContain("Reachable at last check");
  expect(app.texts()).toContain("Runner · 51c2a740");

  await app.press("Jobs");
  const jobs = app.texts().join("\n");
  expect(jobs).toContain("Run #1042\nUnknown\nAssigned computer\nRunner · 51c2a740");
  expect(jobs).toContain("Signed start\nNo verified start\nSigned result\nNo verified result\nDelivery\nDelivered");
  expect(jobs).toContain("Verified start\nSigned result\nVerified result\nDelivery\nPending");

  await app.press("Nodes");
  expect(app.texts()).toContain("Reachable at last check");
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
});
