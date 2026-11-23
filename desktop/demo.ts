import type { Snapshot } from "./snapshot";

const runner = "0".repeat(56) + "51c2a740";

export const demo = {
  version: 2,
  source: "demo",
  observed_at: "2026-11-20T01:52:00Z",
  nodes: [
    { id: runner, observation: "reachable", last_reached: "2026-11-20T01:51:00Z",
      check: { reachable: true, checked_at: "2026-11-20T01:51:00Z" } },
    { id: "0".repeat(56) + "c917df26", observation: "unreachable", last_reached: "2026-11-20T01:14:00Z",
      check: { reachable: false, checked_at: "2026-11-20T01:50:00Z" } },
  ],
  jobs: [
    { run_id: "1042", assigned_node: runner, status: "unknown", signed_evidence: "none", delivery: "delivered" },
    { run_id: "1041", assigned_node: runner, status: "completed", signed_evidence: "verified", delivery: "pending" },
  ],
  routing: { runner: "0".repeat(56) + "c917df26" },
  schedules: [
    { id: "21", revision: "2", state: "active", next_due: "2026-11-22T19:30:00Z",
      timing: { cadence: "weekly mon,thu 08:30", timezone: "Pacific/Auckland",
        fold: "second", gap: "next-valid", missed: "coalesce" }, blocked: true, latest_run_id: "1042" },
    { id: "22", revision: "1", state: "done", next_due: null,
      timing: { cadence: "once 2026-11-19T22:00:00+00:00", timezone: "UTC",
        fold: "first", gap: "skip", missed: "skip" }, blocked: false, latest_run_id: "1041" },
  ],
  schedule_count: 2,
} satisfies Snapshot;
