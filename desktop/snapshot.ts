const observations = {
  reachable: "Reachable at last check",
  unreachable: "Unreachable at last check",
  exchange: "Reached in last exchange",
  unconfirmed: "Last exchange unconfirmed",
  unchecked: "No saved check",
} as const;

const statuses = {
  queued: "Queued",
  running: "Running",
  awaiting_approval: "Awaiting approval",
  waiting_remote: "Waiting for runner",
  waiting_event: "Waiting for event",
  cancel_requested: "Cancel requested",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
  unknown: "Unknown",
} as const;

const delivery = {
  unknown: "Unknown",
  delivered: "Delivered",
  inflight: "Sending",
  pending: "Pending",
  unavailable: "No delivery record",
} as const;

export { observations, statuses, delivery };

type Node = {
  id: string;
  observation: keyof typeof observations;
  last_reached: string | null;
  check?: { checked_at: string; reachable: boolean };
};

type Job = {
  run_id: string;
  assigned_node: string | null;
  status: keyof typeof statuses;
  signed_evidence: "none" | "verified";
  delivery: keyof typeof delivery;
};

export type Snapshot = {
  version: 1;
  source: "demo" | "coordinator";
  observed_at: string;
  nodes: Node[];
  jobs: Job[];
};

export const maxSnapshotBytes = 64 * 1024;

const timestamp = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|\+00:00)$/;
const nodeId = /^[0-9a-f]{64}$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isTimestamp(value: unknown): value is string {
  return typeof value === "string" && timestamp.test(value) && Number.isFinite(Date.parse(value));
}

function isNodeId(value: unknown): value is string {
  return typeof value === "string" && nodeId.test(value);
}

function hasKey(value: unknown, labels: object): boolean {
  return typeof value === "string" && Object.hasOwn(labels, value);
}

function isNode(value: unknown): value is Node {
  return isRecord(value)
    && isNodeId(value.id)
    && hasKey(value.observation, observations)
    && (value.last_reached === null || isTimestamp(value.last_reached))
    && (value.check === undefined || (isRecord(value.check)
      && isTimestamp(value.check.checked_at) && typeof value.check.reachable === "boolean"));
}

export type PublicCard = {
  role: "coordinator" | "runner";
  id: string;
  address: string;
  revision: string;
};

export function readPublicCard(contents: string): PublicCard {
  const value: unknown = JSON.parse(contents);
  const address = isRecord(value) && typeof value.address === "string"
    ? /^(?:\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-fA-F:.]+(?:%\d{1,10})?\]):(\d{1,5})$/.exec(value.address)
    : null;
  if (!isRecord(value) || value.version !== 1 || Object.keys(value).length !== 5
      || (value.role !== "coordinator" && value.role !== "runner")
      || !isNodeId(value.id)
      || typeof value.address !== "string" || !address || address[0] !== value.address
      || Number(address[1]) < 1 || Number(address[1]) > 65535
      || typeof value.revision !== "string" || !/^[0-9a-fA-F]{40}$/.test(value.revision)) {
    throw new Error("Invalid public node card");
  }
  return { role: value.role, id: value.id, address: value.address, revision: value.revision };
}

function isJob(value: unknown): value is Job {
  return isRecord(value)
    && typeof value.run_id === "string" && /^[1-9]\d{0,18}$/.test(value.run_id)
    && (value.assigned_node === null || isNodeId(value.assigned_node))
    && hasKey(value.status, statuses)
    && (value.signed_evidence === "none" || value.signed_evidence === "verified")
    && hasKey(value.delivery, delivery);
}

export function readSnapshot(contents: string): Snapshot {
  const value: unknown = JSON.parse(contents);
  if (!isRecord(value) || value.version !== 1 || value.source !== "coordinator"
      || !isTimestamp(value.observed_at)
      || !Array.isArray(value.nodes) || value.nodes.length > 32 || !value.nodes.every(isNode)
      || !Array.isArray(value.jobs) || value.jobs.length > 50 || !value.jobs.every(isJob)) {
    throw new Error("Invalid coordinator snapshot");
  }
  return {
    version: 1,
    source: "coordinator",
    observed_at: value.observed_at,
    nodes: value.nodes,
    jobs: value.jobs,
  };
}
