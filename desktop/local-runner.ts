import { readFile, rename, writeFile } from "fs/promises";
import process from "process";
import type { PublicCard } from "./snapshot";

export const runnerLabels = {
  idle: "Runner is not started by this window",
  starting: "Waiting for runner readiness",
  running: "Local runner ready",
  stopping: "Waiting for runner exit",
  startup_timeout: "Runner readiness was not verified. Waiting for exit.",
  stop_timeout: "Runner exit has not been observed. The window still holds the child handle.",
  startup_failed: "Runner did not start. Check its configuration and whether another launcher owns it.",
  exited: "Local runner exited",
  stopped: "Local runner stopped. Instance lock released.",
  exit_unverified: "Runner exited without confirming graceful shutdown.",
  lock_unverified: "Runner exited. Instance lock release could not be verified.",
} as const;

type Action =
  | { type: "review"; bytes: number[] }
  | { type: "confirm"; digest: string }
  | { type: "start" }
  | { type: "stop" };

type Reply = {
  sequence: number;
  runner: keyof typeof runnerLabels;
  preview?: { card: PublicCard; digest: string };
  message?: string;
};

function channelPath(name: string) {
  const dir = process.dataDir;
  if (!dir) throw new Error("Desktop host directory is unavailable");
  return process.platform === "windows" ? dir + "\\" + name.replaceAll("/", "\\") : dir + "/" + name;
}

export async function readRunner(): Promise<Reply> {
  const reply = JSON.parse(await readFile(channelPath("outgoing/reply.json"), "utf8")) as Reply;
  if (!Number.isSafeInteger(reply.sequence) || !Object.hasOwn(runnerLabels, reply.runner)) {
    throw new Error("Invalid desktop host reply");
  }
  return reply;
}

export async function requestRunner(sequence: number, action: Action): Promise<Reply> {
  const draft = channelPath("incoming/request.tmp");
  await writeFile(draft, JSON.stringify({ sequence, action }));
  await rename(draft, channelPath("incoming/request.json"));
  const deadline = Date.now() + 15_000;
  do {
    const reply = await readRunner();
    if (reply.sequence === sequence) return reply;
    if (reply.sequence > sequence) break;
    await new Promise<void>((resolve) => setTimeout(resolve, 100));
  } while (Date.now() < deadline);
  throw new Error("Desktop host response could not be verified");
}
