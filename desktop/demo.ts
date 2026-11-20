import type { Snapshot } from "./snapshot";

const runner = "0".repeat(56) + "51c2a740";

export const demo: Snapshot = {
  version: 1,
  source: "demo",
  observed_at: "2026-11-20T01:52:00Z",
  nodes: [
    { id: runner, observation: "reachable", last_reached: "2026-11-20T01:51:00Z" },
    { id: "0".repeat(56) + "c917df26", observation: "unreachable", last_reached: "2026-11-20T01:14:00Z" },
  ],
  jobs: [
    { run_id: "1042", assigned_node: runner, status: "unknown", signed_evidence: "none", delivery: "delivered" },
    { run_id: "1041", assigned_node: runner, status: "completed", signed_evidence: "verified", delivery: "pending" },
  ],
};
