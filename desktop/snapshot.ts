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

const scheduleStates = {
  active: "Active",
  paused: "Paused",
  cancelled: "Cancelled",
  done: "Finished",
  unknown: "Unknown",
} as const;

export { observations, statuses, delivery, scheduleStates };

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

export type Routing = { runner: string | null };

type ScheduleTiming = {
  cadence: string;
  timezone: string;
  fold: "first" | "second";
  gap: "skip" | "next-valid";
  missed: "skip" | "coalesce";
};

export type Schedule = {
  id: string;
  revision: string;
  state: keyof typeof scheduleStates;
  next_due: string | null;
  timing: ScheduleTiming | null;
  blocked: boolean;
  latest_run_id: string | null;
};

type SnapshotBase = {
  source: "demo" | "coordinator";
  observed_at: string;
  nodes: Node[];
  jobs: Job[];
};

export type Snapshot = SnapshotBase & ({ version: 1 } | {
  version: 2;
  routing: Routing | null;
  schedules: Schedule[];
  schedule_count: number;
});

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
    && isId(value.run_id)
    && (value.assigned_node === null || isNodeId(value.assigned_node))
    && hasKey(value.status, statuses)
    && (value.signed_evidence === "none" || value.signed_evidence === "verified")
    && hasKey(value.delivery, delivery);
}

function isId(value: unknown): value is string {
  return typeof value === "string" && /^[1-9]\d{0,18}$/.test(value);
}

function isRouting(value: unknown): value is Routing {
  return isRecord(value) && (value.runner === null || isNodeId(value.runner));
}

function isScheduleTiming(value: unknown): value is ScheduleTiming {
  return isRecord(value)
    && typeof value.cadence === "string"
    && (value.cadence.startsWith("once ") ? isTimestamp(value.cadence.slice(5))
      : /^(?:daily|weekly (?:mon|tue|wed|thu|fri|sat|sun)(?:,(?:mon|tue|wed|thu|fri|sat|sun)){0,6}) (?:[01]\d|2[0-3]):[0-5]\d$/.test(value.cadence))
    && typeof value.timezone === "string" && /^[A-Za-z0-9_+/-]{1,64}$/.test(value.timezone)
    && (value.fold === "first" || value.fold === "second")
    && (value.gap === "skip" || value.gap === "next-valid")
    && (value.missed === "skip" || value.missed === "coalesce");
}

function isSchedule(value: unknown): value is Schedule {
  return isRecord(value)
    && isId(value.id) && isId(value.revision)
    && hasKey(value.state, scheduleStates)
    && (value.next_due === null || isTimestamp(value.next_due))
    && (value.timing === null || isScheduleTiming(value.timing))
    && typeof value.blocked === "boolean"
    && (value.latest_run_id === null || isId(value.latest_run_id));
}

export function readSnapshot(contents: string): Snapshot {
  const value: unknown = JSON.parse(contents);
  if (!isRecord(value) || value.source !== "coordinator"
      || !isTimestamp(value.observed_at)
      || !Array.isArray(value.nodes) || value.nodes.length > 32 || !value.nodes.every(isNode)
      || !Array.isArray(value.jobs) || value.jobs.length > 50 || !value.jobs.every(isJob)) {
    throw new Error("Invalid coordinator snapshot");
  }
  const base = {
    source: value.source,
    observed_at: value.observed_at,
    nodes: value.nodes,
    jobs: value.jobs,
  } as const;
  if (value.version === 1) return { ...base, version: 1 };
  if (value.version !== 2
      || (value.routing !== null && !isRouting(value.routing))
      || !Array.isArray(value.schedules) || value.schedules.length > 50 || !value.schedules.every(isSchedule)
      || typeof value.schedule_count !== "number" || !Number.isSafeInteger(value.schedule_count)
      || value.schedules.length !== Math.min(50, value.schedule_count)) {
    throw new Error("Invalid coordinator snapshot");
  }
  return { ...base, version: 2, routing: value.routing, schedules: value.schedules, schedule_count: value.schedule_count };
}
