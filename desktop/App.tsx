import { cleanup, mount, Picker, Scroll, signal, Text, View } from "nsh";
import type { ScrollRef, ViewRef } from "nsh";
import { demo } from "./demo";
import { readRunner, requestRunner, runnerLabels } from "./local-runner";
import { delivery, maxSnapshotBytes, observations, readPublicCard, readSnapshot, scheduleStates, statuses } from "./snapshot";
import type { PublicCard, Snapshot } from "./snapshot";

const pages = ["Nodes", "Jobs", "Routing", "Schedules"] as const;
type Page = typeof pages[number];
type Operation = "snapshot" | "card" | "confirm" | "start" | "stop";

const focusRing = { outlineWidth: 2, outlineOffset: 2, outlineColor: "#1756ad" } as const;
const navFocusRing = { ...focusRing, outlineColor: "#9ebffb" } as const;
const unacknowledged = "The desktop host did not acknowledge the action. Reopen the companion to verify the outcome.";

function utc(iso: string) {
  return `${iso.slice(0, 10)} · ${iso.slice(11, 19)} UTC`;
}

function runnerLabel(id: string) {
  return `Runner · ${id.slice(-8)}`;
}

function NavItem(props: { label: Page; selected: boolean; onPress: () => void }) {
  return (
    <View role="button" accessibilityLabel={props.label} accessibilityState={{ selected: props.selected }}
      onPress={props.onPress} focusVisibleStyle={navFocusRing}
      class={props.selected
        ? "rounded-md bg-[#dce7fb] px-3 py-3 active:bg-[#c6d8f7]"
        : "rounded-md px-3 py-3 hover:bg-[#33394a] active:bg-[#444c60]"}>
      <Text class={props.selected ? "text-sm font-semibold text-[#192843]" : "text-sm font-semibold text-[#c5ccdd]"}>
        {props.label}
      </Text>
    </View>
  );
}

function Fact(props: { label: string; value: string; publicValue?: boolean }) {
  return (
    <View selectable={props.publicValue} class={props.publicValue ? "min-w-0" : "gap-1"}>
      <Text class="text-xs text-[#526174]">{props.label}</Text>
      <Text class={props.publicValue
        ? "font-mono text-sm text-[#1d2938]" : "text-sm text-[#1d2938]"}>{props.value}</Text>
    </View>
  );
}

function Nodes(props: { nodes: Snapshot["nodes"] }) {
  return (
    <View class="gap-3">
      <Text class="text-sm text-[#526174]">Saved reachability observations</Text>
      {props.nodes.length === 0 && <Text class="text-sm text-[#526174]">No paired runners in this snapshot.</Text>}
      {props.nodes.map((node) => {
        const observation = node.check ? (node.check.reachable ? "reachable" : "unreachable") : node.observation;
        return (
          <View class="gap-2 rounded-lg border border-[#d8dfe8] bg-white px-4 py-3">
            <View class="flex-row flex-wrap items-center justify-between gap-2">
              <Text class="text-base font-semibold text-[#1d2938]" accessibilityLabel={`Runner ${node.id}`}>
                {runnerLabel(node.id)}
              </Text>
              <Text class={["reachable", "exchange"].includes(observation)
                ? "text-sm font-semibold text-[#17614c]"
                : "text-sm font-semibold text-[#526174]"}>
                {observations[observation]}
              </Text>
            </View>
            <View class="flex-row flex-wrap gap-x-6 gap-y-2">
              {(node.check || observation === "reachable" || observation === "unreachable") && (
                <Fact label="Checked" value={node.check ? utc(node.check.checked_at) : "Time not recorded"} />
              )}
              <Fact label="Last reached" value={node.last_reached ? utc(node.last_reached) : "No recorded contact"} />
            </View>
          </View>
        );
      })}
    </View>
  );
}

function Jobs(props: { jobs: Snapshot["jobs"] }) {
  return (
    <View class="gap-3">
      {props.jobs.length === 0 && <Text class="text-sm text-[#526174]">No jobs in this snapshot.</Text>}
      {props.jobs.map((job) => {
        const signed = job.signed_evidence === "verified";
        return (
          <View class="gap-3 rounded-lg border border-[#d8dfe8] bg-white px-4 py-3">
            <View class="flex-row flex-wrap items-center justify-between gap-2">
              <Text class="text-base font-semibold text-[#1d2938]">{`Run #${job.run_id}`}</Text>
              <Text class={job.status === "unknown"
                ? "rounded bg-[#fff0d2] px-2 py-1 text-sm font-semibold text-[#764608]"
                : "rounded bg-[#eef2f6] px-2 py-1 text-sm font-semibold text-[#354459]"}>
                {statuses[job.status]}
              </Text>
            </View>
            <View class="flex-row flex-wrap gap-x-6 gap-y-3">
              <Fact label="Assigned computer" value={job.assigned_node ? runnerLabel(job.assigned_node) : "No recorded runner"} />
              <Fact label="Signed start" value={signed ? "Verified start" : "No verified start"} />
              <Fact label="Signed result" value={signed ? "Verified result" : "No verified result"} />
              <Fact label="Message delivery" value={delivery[job.delivery]} />
            </View>
          </View>
        );
      })}
    </View>
  );
}

function Routing(props: { snapshot: Snapshot }) {
  return (
    <>
      {props.snapshot.version === 1 ? (
        <Text class="text-sm text-[#526174]">Routing was not recorded in this snapshot.</Text>
      ) : props.snapshot.routing ? (
        <View class="gap-2 rounded-lg border border-[#d8dfe8] bg-white px-4 py-3">
          <Fact label="New Telegram text tasks"
            value={props.snapshot.routing.runner ? runnerLabel(props.snapshot.routing.runner) : "Local coordinator"} />
          <Text class="text-sm text-[#526174]">Existing jobs keep their assignment.</Text>
        </View>
      ) : (
        <Text class="text-sm text-[#526174]">This snapshot covers a local conversation.</Text>
      )}
    </>
  );
}

function Schedules(props: { snapshot: Snapshot }) {
  return (
    <View class="gap-3">
      {props.snapshot.version === 1 && (
        <Text class="text-sm text-[#526174]">Schedules were not recorded in this snapshot.</Text>
      )}
      {props.snapshot.version === 2 && props.snapshot.schedules.length === 0 && (
        <Text class="text-sm text-[#526174]">No schedules in this snapshot.</Text>
      )}
      {props.snapshot.version === 2 && props.snapshot.schedule_count > props.snapshot.schedules.length && (
        <Text class="text-sm text-[#526174]">
          {`Showing ${props.snapshot.schedules.length} of ${props.snapshot.schedule_count} schedules.`}
        </Text>
      )}
      {props.snapshot.version === 2 && props.snapshot.schedules.map((schedule) => {
        const timing = schedule.timing;
        return (
          <View class="gap-3 rounded-lg border border-[#d8dfe8] bg-white px-4 py-3">
            <View class="flex-row flex-wrap items-center justify-between gap-2">
              <Text class="text-base font-semibold text-[#1d2938]">{`Schedule #${schedule.id}`}</Text>
              <Text class="text-sm font-semibold text-[#354459]">{scheduleStates[schedule.state]}</Text>
            </View>
            {schedule.blocked && <Text class="text-sm font-semibold text-[#764608]">Blocked by a prior run</Text>}
            <View class="flex-row flex-wrap gap-x-6 gap-y-3">
              <Fact label="Planned next run" value={schedule.next_due ? utc(schedule.next_due) : "No time recorded"} />
              <Fact label="Latest run" value={schedule.latest_run_id ? `Run #${schedule.latest_run_id}` : "No run recorded"} />
              <Fact label="Revision" value={schedule.revision} />
            </View>
            {timing ? (
              <View class="flex-row flex-wrap gap-x-6 gap-y-3">
                <Fact label="Cadence" value={timing.cadence.startsWith("once ")
                  ? `Once · ${utc(timing.cadence.slice(5))}` : timing.cadence} />
                <Fact label="Timezone" value={timing.timezone} />
                {!timing.cadence.startsWith("once ") && (
                  <>
                    <Fact label="Repeated time" value={timing.fold === "second" ? "Second occurrence" : "First occurrence"} />
                    <Fact label="Skipped time" value={timing.gap === "next-valid" ? "Use next valid time" : "Skip occurrence"} />
                  </>
                )}
                <Fact label="Missed runs" value={timing.missed === "coalesce" ? "Combine into one run" : "Skip"} />
              </View>
            ) : <Text class="text-sm text-[#526174]">Timing unavailable.</Text>}
          </View>
        );
      })}
    </View>
  );
}

function App() {
  const page = signal<Page>("Nodes");
  const current = signal<Snapshot>(demo);
  const operation = signal<Operation | null>(null);
  const snapshotError = signal("");
  const pairNotice = signal("");
  const runnerNotice = signal("");
  const card = signal<PublicCard | null>(null);
  const digest = signal<string | null>(null);
  const runner = signal<Awaited<ReturnType<typeof readRunner>>["runner"] | null>(null);
  let scroll: ScrollRef | null = null;
  let openButton: ViewRef | null = null;
  let reviewButton: ViewRef | null = null;
  let sequence = 0;
  let connected = false;
  let polling = false;
  let closed = false;

  async function refreshRunner() {
    if (operation() || polling) return;
    polling = true;
    try {
      const reply = await readRunner();
      if (closed || operation() || (connected && reply.sequence !== sequence)) return;
      connected = true;
      sequence = reply.sequence;
      runner.set(reply.runner);
    } catch {
      if (connected && !closed && !operation()) {
        runner.set(null);
      }
    } finally {
      polling = false;
    }
  }

  void refreshRunner();
  const timer = setInterval(() => { if (connected) void refreshRunner(); }, 350);
  cleanup(() => { closed = true; clearInterval(timer); });

  function clearCard() {
    card.set(null);
    digest.set(null);
  }

  function closeCard() {
    clearCard();
    reviewButton?.focus();
  }

  function showPage(target: Page) {
    if (page() === target) return;
    page.set(target);
    scroll?.scrollTo({ y: 0, animated: false });
  }

  async function localAction(action: Parameters<typeof requestRunner>[1]) {
    try {
      const reply = await requestRunner(++sequence, action);
      runner.set(reply.runner);
      return reply;
    } catch {
      runner.set(null);
    }
  }

  async function pressLocal(type: "confirm" | "start" | "stop") {
    if (operation() || !runner()) return;
    const reviewed = digest();
    if (type === "confirm" && !reviewed) return;
    operation.set(type);
    try {
      if (type === "confirm" && reviewed) {
        pairNotice.set("");
        clearCard();
        const reply = await localAction({ type, digest: reviewed });
        pairNotice.set(reply ? reply.message ?? "" : unacknowledged);
      } else if (type !== "confirm") {
        runnerNotice.set("");
        const reply = await localAction({ type });
        runnerNotice.set(reply ? reply.message ?? "" : unacknowledged);
      }
    } finally {
      operation.set(null);
      if (type === "confirm" && page() === "Nodes") reviewButton?.focus();
    }
  }

  async function openFile(kind: "snapshot" | "card") {
    if (operation()) return;
    operation.set(kind);
    if (kind === "card") {
      clearCard();
      pairNotice.set("");
    }
    try {
      const [picked] = await Picker.pickFiles({
        filters: [{ name: kind === "snapshot" ? "Coordinator snapshots" : "Public node cards", extensions: ["json"] }],
        maxSize: maxSnapshotBytes,
      });
      if (!picked) return;
      if (!picked.file) throw new Error("File is too large");
      if (kind === "snapshot") {
        current.set(readSnapshot(await picked.file.text()));
        snapshotError.set("");
        scroll?.scrollTo({ y: 0, animated: false });
      } else if (runner()) {
        const bytes = new Uint8Array(await picked.file.arrayBuffer());
        const reply = await localAction({ type: "review", bytes: Array.from(bytes) });
        pairNotice.set(reply ? reply.message ?? "" : unacknowledged);
        if (reply?.preview) {
          card.set(reply.preview.card);
          digest.set(reply.preview.digest);
        }
      } else {
        card.set(readPublicCard(await picked.file.text()));
      }
    } catch {
      if (kind === "snapshot") {
        snapshotError.set("Couldn’t open the snapshot. Choose a coordinator JSON export under 64 KiB.");
      } else {
        pairNotice.set("Couldn’t review the card. Choose a UTF-8 public node card from nodes card.");
      }
    } finally {
      operation.set(null);
      if (kind === "snapshot") openButton?.focus();
      else if (page() === "Nodes") reviewButton?.focus();
    }
  }

  return (
    <View class="flex-1 flex-row bg-[#f3f5f9]">
      <View class="w-40 shrink-0 gap-1 bg-[#212635] px-3 py-5">
        <Text class="mb-5 px-2 text-base font-semibold text-white">CoconutClaw</Text>
        {pages.map((label) => <NavItem label={label} selected={page() === label} onPress={() => showPage(label)} />)}
      </View>
      <View class="min-w-0 flex-1">
        <View class="flex-row flex-wrap items-center justify-between gap-2 border-b border-[#d8dfe8] bg-white px-5 py-3">
          <Text class="text-sm font-semibold text-[#526174]">
            {current().source === "demo" ? "Synthetic demo" : `Snapshot · ${utc(current().observed_at)}`}
          </Text>
          <ActionButton label="Open snapshot" disabled={operation() !== null}
            buttonRef={(ref) => { openButton = ref; }} onPress={() => openFile("snapshot")} />
        </View>
        {snapshotError() ? (
          <Text class="border-b border-[#efd2c8] bg-[#fff3ee] px-5 py-2 text-sm text-[#913c27]"
            role="alert">{snapshotError()}</Text>
        ) : null}
        <Scroll class="flex-1" ref={(ref) => { scroll = ref; }}>
          <View class="gap-4 px-5 py-5">
            <View class="flex-row flex-wrap items-center justify-between gap-2">
              <Text class="text-2xl font-semibold text-[#1d2938]" role="header" accessibilityLevel={1}>{page()}</Text>
              {page() === "Nodes" && (
                <ActionButton label="Review public card" disabled={operation() !== null}
                  buttonRef={(ref) => { reviewButton = ref; }} onPress={() => openFile("card")} />
              )}
            </View>
            {page() === "Nodes" && (
              <>
                {card() && <CardReview card={card()!} digest={digest()} busy={operation() !== null}
                  clear={closeCard} confirm={() => pressLocal("confirm")} />}
                {(pairNotice() || operation() === "confirm") && (
                  <Text role="alert" class="text-sm text-[#354459]">
                    {operation() === "confirm" ? "Confirming pairing…" : pairNotice()}
                  </Text>
                )}
                {(runner() || runnerNotice() || (connected && !runner())) && (
                  <View class="gap-2 rounded-lg border border-[#d8dfe8] bg-white px-4 py-3">
                    <Text class="text-base font-semibold text-[#1d2938]">Local runner</Text>
                    <Text class="text-sm text-[#354459]">
                      {operation() === "start" ? "Requesting runner start…"
                        : operation() === "stop" ? "Requesting runner stop…"
                        : runner() ? runnerLabels[runner()!]
                        : "Local runner state is unavailable. Reopen the companion to reconnect."}
                    </Text>
                    {runnerNotice() ? <Text role="alert" class="text-sm text-[#354459]">{runnerNotice()}</Text> : null}
                    {runner() && (
                      <View class="flex-row flex-wrap gap-2">
                        <ActionButton label="Start local runner"
                          disabled={operation() !== null || ["starting", "running", "stopping", "startup_timeout", "stop_timeout"].includes(runner()!)}
                          onPress={() => pressLocal("start")} />
                        <ActionButton label="Stop local runner" disabled={operation() !== null || runner() !== "running"}
                          onPress={() => pressLocal("stop")} />
                      </View>
                    )}
                  </View>
                )}
                <Nodes nodes={current().nodes} />
              </>
            )}
            {page() === "Jobs" && <Jobs jobs={current().jobs} />}
            {page() === "Routing" && <Routing snapshot={current()} />}
            {page() === "Schedules" && <Schedules snapshot={current()} />}
          </View>
        </Scroll>
      </View>
    </View>
  );
}

function ActionButton(props: {
  label: string;
  disabled?: boolean;
  buttonRef?: (ref: ViewRef | null) => void;
  onPress: () => void;
}) {
  return (
    <View role="button" accessibilityLabel={props.label} accessibilityState={{ disabled: props.disabled }}
      ref={props.buttonRef} focusVisibleStyle={focusRing}
      onPress={() => { if (!props.disabled) props.onPress(); }}
      class={props.disabled
        ? "self-start rounded-md border border-[#d8dfe8] bg-[#eef2f6] px-3 py-2"
        : "self-start rounded-md border border-[#78889d] bg-white px-3 py-2 hover:bg-[#eef2f6] active:bg-[#dce7fb]"}>
      <Text class={props.disabled ? "text-sm font-semibold text-[#526174]" : "text-sm font-semibold text-[#1d2938]"}>
        {props.label}
      </Text>
    </View>
  );
}

function CardReview(props: { card: PublicCard; digest: string | null; busy: boolean; clear: () => void; confirm: () => void }) {
  return (
    <View class="gap-1 rounded-lg border border-[#d8dfe8] bg-white px-4 py-3">
      <View class="flex-row flex-wrap items-center justify-between gap-2">
        <Text class="text-base font-semibold text-[#1d2938]">
          {props.card.role === "runner" ? "Runner card for coordinator" : "Coordinator card for runner"}
        </Text>
        <ActionButton label="Close card" disabled={props.busy} onPress={props.clear} />
      </View>
      <Fact label="Public ID" value={props.card.id} publicValue />
      <Fact label="Fixed UDP address" value={props.card.address} publicValue />
      <Fact label="Source revision" value={props.card.revision} publicValue />
      {props.digest ? (
        <>
          <Fact label="Confirmation digest" value={props.digest} publicValue />
          <Text class="text-sm text-[#526174]">Compare these values independently before confirming.</Text>
          <ActionButton label="Confirm pairing" disabled={props.busy} onPress={props.confirm} />
        </>
      ) : (
        <Text class="text-sm text-[#526174]">Compare these values independently, then use nodes pair --preview on the intended host.</Text>
      )}
    </View>
  );
}

mount(App);
