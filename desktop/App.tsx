import { cleanup, LayoutAnimation, mount, Picker, Scroll, signal, Text, View } from "nsh";
import type { ScrollRef, ViewRef } from "nsh";
import { demo } from "./demo";
import { readRunner, requestRunner, runnerLabels } from "./local-runner";
import {
  age, delivery, deliveryTone, isStale, maxSnapshotBytes, observations, observationTone,
  readPublicCard, readSnapshot, scheduleStates, scheduleTone, statuses, statusTone,
} from "./snapshot";
import type { PublicCard, Schedule, Snapshot, Tone } from "./snapshot";
import { theme } from "./theme";

const pages = ["Nodes", "Jobs", "Routing", "Schedules"] as const;
type Page = typeof pages[number];
type Operation = "snapshot" | "card" | "confirm" | "start" | "stop";
type Runners = Snapshot["nodes"];

const unacknowledgedPairing =
  "The desktop host did not acknowledge the pairing review. Check admission with nodes status before confirming again.";
const unacknowledgedRunner =
  "The desktop host did not acknowledge the request. Keep this window open: a reopened companion cannot control a runner this one started.";
const hostLost =
  "Local runner state is unavailable. The host channel was lost; keep this window open and check the runner from its own console.";

function utc(iso: string) {
  return `${iso.slice(0, 10)} · ${iso.slice(11, 19)} UTC`;
}

function runnerLabel(id: string) {
  return `Runner · ${id.slice(-8)}`;
}

function plural(noun: string, total: number) {
  return `${total} ${noun}${total === 1 ? "" : "s"}`;
}

function observationOf(node: Runners[number]) {
  return node.check ? (node.check.reachable ? "reachable" : "unreachable") : node.observation;
}

// A runner named outside Nodes carries its saved observation, or says it is not
// in the export at all. This annotates identity; it never becomes evidence.
function runnerNote(id: string | null, nodes: Runners): { text: string; tone: Tone } | null {
  if (!id) return null;
  const node = nodes.find((candidate) => candidate.id === id);
  if (!node) return { text: "not in this snapshot", tone: "neutral" };
  const observation = observationOf(node);
  const tone = observationTone[observation];
  return tone === "ok" ? null : { text: observations[observation].toLowerCase(), tone };
}

function scheduleNeedsAttention(schedule: Schedule) {
  return schedule.blocked || scheduleTone[schedule.state] === "attention";
}

function NavItem(props: { label: Page; selected: boolean; onPress: () => void }) {
  return (
    <View role="button" accessibilityLabel={props.label} accessibilityState={{ selected: props.selected }}
      onPress={props.onPress} focusVisibleStyle={theme.navFocus}
      class={`rounded-md px-3 py-3 ${theme.press} ${props.selected ? theme.railItemSelected : theme.railItemHover}`}>
      <Text class={`text-sm font-semibold ${props.selected ? theme.railItemSelectedText : theme.railItem}`}>
        {props.label}
      </Text>
    </View>
  );
}

function Fact(props: {
  label: string;
  value: string;
  publicValue?: boolean;
  valueTone?: Tone;
  note?: string;
  noteTone?: Tone;
}) {
  return (
    <View selectable={props.publicValue} class={props.publicValue ? "min-w-0" : "gap-1"}>
      <Text class={`text-xs ${theme.label}`}>{props.label}</Text>
      <Text class={`${props.publicValue ? "font-mono " : ""}text-sm ${props.valueTone ? theme.tone[props.valueTone] : theme.value}`}>
        {props.value}
      </Text>
      {props.note ? <Text class={`text-xs ${theme.tone[props.noteTone ?? "neutral"]}`}>{props.note}</Text> : null}
    </View>
  );
}

function Pill(props: { label: string; tone: Tone }) {
  return (
    <Text class={`rounded px-2 py-1 text-sm font-semibold ${theme.pill[props.tone]}`}>{props.label}</Text>
  );
}

function Chip(props: { label: string; selected: boolean; disabled?: boolean; onPress: () => void }) {
  const marked = () => props.selected && props.disabled !== true;
  return (
    <View role="button" accessibilityLabel={props.label}
      accessibilityState={{ selected: marked(), disabled: props.disabled === true }}
      focusVisibleStyle={theme.focus}
      onPress={() => { if (props.disabled !== true) props.onPress(); }}
      class={`rounded-md border px-3 py-1 ${theme.press} ${props.disabled === true
        ? theme.buttonDisabled : marked() ? theme.chipSelected : theme.chip}`}>
      <Text class={`text-xs font-semibold ${props.disabled === true
        ? theme.buttonDisabledText : marked() ? theme.chipSelectedText : theme.chipText}`}>
        {props.label}
      </Text>
    </View>
  );
}

// Rows keep the coordinator's order; the filter only hides what is not asking
// for an operator.
function Triage(props: {
  noun: string;
  total: number;
  attention: number;
  attentionOnly: boolean;
  onChange: (only: boolean) => void;
}) {
  return (
    <View class="flex-row flex-wrap items-center gap-2">
      <Chip label={`All · ${plural(props.noun, props.total)}`} selected={!props.attentionOnly}
        onPress={() => props.onChange(false)} />
      <Chip label={`Needs attention · ${plural(props.noun, props.attention)}`}
        selected={props.attentionOnly} disabled={props.attention === 0}
        onPress={() => props.onChange(true)} />
    </View>
  );
}

function Busy(props: { label: string }) {
  return (
    <View class="flex-row items-center gap-2">
      <View class={`h-3 w-3 shrink-0 rounded-full border-2 ${theme.spinner} ${theme.spin}`} />
      <Text class={`text-sm ${theme.secondary}`}>{props.label}</Text>
    </View>
  );
}

function Nodes(props: { nodes: Runners; attentionOnly: boolean; onChange: (only: boolean) => void }) {
  const attention = () => props.nodes.filter((node) => observationTone[observationOf(node)] === "attention").length;
  const shown = () => props.attentionOnly
    ? props.nodes.filter((node) => observationTone[observationOf(node)] === "attention")
    : props.nodes;
  return (
    <View class="gap-3">
      <Triage noun="runner" total={props.nodes.length} attention={attention()}
        attentionOnly={props.attentionOnly} onChange={props.onChange} />
      <Text class={`text-sm ${theme.muted}`}>Saved reachability observations</Text>
      {shown().length === 0 && (
        <Text class={`text-sm ${theme.muted}`}>
          {props.attentionOnly ? "No runner needs attention in this snapshot." : "No paired runners in this snapshot."}
        </Text>
      )}
      {shown().map((node) => {
        const observation = observationOf(node);
        return (
          <View class={`gap-2 rounded-lg border px-4 py-3 ${theme.border} ${theme.surface}`}>
            <View class="flex-row flex-wrap items-center justify-between gap-2">
              <Text class={`text-base font-semibold ${theme.heading}`} accessibilityLabel={`Runner ${node.id}`}>
                {runnerLabel(node.id)}
              </Text>
              <Text class={`text-sm font-semibold ${theme.tone[observationTone[observation]]}`}>
                {observations[observation]}
              </Text>
            </View>
            <View class="flex-row flex-wrap gap-x-6 gap-y-2">
              {(node.check || observation === "reachable" || observation === "unreachable") && (
                <Fact label="Checked" value={node.check ? utc(node.check.checked_at) : "Time not recorded"} />
              )}
              <Fact label="Last reached" value={node.last_reached ? utc(node.last_reached) : "No recorded contact"} />
            </View>
            <Fact label="Full public ID" value={node.id} publicValue />
          </View>
        );
      })}
    </View>
  );
}

function Jobs(props: {
  jobs: Snapshot["jobs"];
  nodes: Runners;
  attentionOnly: boolean;
  onChange: (only: boolean) => void;
}) {
  const attention = () => props.jobs.filter((job) => statusTone[job.status] === "attention").length;
  const shown = () => props.attentionOnly
    ? props.jobs.filter((job) => statusTone[job.status] === "attention")
    : props.jobs;
  return (
    <View class="gap-3">
      <Triage noun="job" total={props.jobs.length} attention={attention()}
        attentionOnly={props.attentionOnly} onChange={props.onChange} />
      {shown().length === 0 && (
        <Text class={`text-sm ${theme.muted}`}>
          {props.attentionOnly ? "No job needs attention in this snapshot." : "No jobs in this snapshot."}
        </Text>
      )}
      {shown().map((job) => {
        const signed = job.signed_evidence === "verified";
        const note = runnerNote(job.assigned_node, props.nodes);
        const assigned = job.assigned_node ? runnerLabel(job.assigned_node) : "No recorded runner";
        return (
          <View class={`gap-3 rounded-lg border px-4 py-3 ${theme.border} ${theme.surface}`}>
            <View class="flex-row flex-wrap items-center justify-between gap-2">
              <Text class={`text-base font-semibold ${theme.heading}`}>{`Run #${job.run_id}`}</Text>
              <Pill label={statuses[job.status]} tone={statusTone[job.status]} />
            </View>
            <View class="flex-row flex-wrap gap-x-6 gap-y-3">
              {note ? (
                <Fact label="Assigned computer" value={assigned} note={note.text} noteTone={note.tone} />
              ) : (
                <Fact label="Assigned computer" value={assigned} />
              )}
              <Fact label="Signed start" value={signed ? "Verified" : "No verified record"} />
              <Fact label="Signed result" value={signed ? "Verified" : "No verified record"} />
              <Fact label="Message delivery" value={delivery[job.delivery]} valueTone={deliveryTone[job.delivery]} />
            </View>
          </View>
        );
      })}
    </View>
  );
}

function Routing(props: { snapshot: Snapshot }) {
  const note = () => props.snapshot.version === 1 ? null : runnerNote(props.snapshot.routing?.runner ?? null, props.snapshot.nodes);
  const target = () => {
    if (props.snapshot.version === 1) return "Local coordinator";
    const runner = props.snapshot.routing?.runner;
    return runner ? runnerLabel(runner) : "Local coordinator";
  };
  return (
    <View class="gap-3">
      {props.snapshot.version === 1 ? (
        <Text class={`text-sm ${theme.muted}`}>Routing was not recorded in this snapshot.</Text>
      ) : props.snapshot.routing === null ? (
        <Text class={`text-sm ${theme.muted}`}>This snapshot covers a local conversation.</Text>
      ) : (
        <View class={`gap-2 rounded-lg border px-4 py-3 ${theme.border} ${theme.surface}`}>
          {note() ? (
            <Fact label="New Telegram text tasks" value={target()} note={note()!.text} noteTone={note()!.tone} />
          ) : (
            <Fact label="New Telegram text tasks" value={target()} />
          )}
          <Text class={`text-sm ${theme.muted}`}>Existing jobs keep their assignment.</Text>
        </View>
      )}
    </View>
  );
}

function Schedules(props: { snapshot: Snapshot; attentionOnly: boolean; onChange: (only: boolean) => void }) {
  const schedules = () => props.snapshot.version === 1 ? [] : props.snapshot.schedules;
  const scheduleCount = () => props.snapshot.version === 1 ? 0 : props.snapshot.schedule_count;
  const attention = () => schedules().filter((schedule) => scheduleNeedsAttention(schedule)).length;
  const shown = () => props.attentionOnly
    ? schedules().filter((schedule) => scheduleNeedsAttention(schedule))
    : schedules();
  return (
    <View class="gap-3">
      {props.snapshot.version === 1 ? (
        <Text class={`text-sm ${theme.muted}`}>Schedules were not recorded in this snapshot.</Text>
      ) : (
        <>
          <Triage noun="schedule" total={schedules().length} attention={attention()}
            attentionOnly={props.attentionOnly} onChange={props.onChange} />
          {schedules().length === 0 && <Text class={`text-sm ${theme.muted}`}>No schedules in this snapshot.</Text>}
          {scheduleCount() > schedules().length && (
            <Text class={`text-sm ${theme.muted}`}>
              {`Showing ${schedules().length} of ${scheduleCount()} schedules.`}
            </Text>
          )}
          {shown().length === 0 && schedules().length > 0 && (
            <Text class={`text-sm ${theme.muted}`}>No schedule needs attention in this snapshot.</Text>
          )}
          {shown().map((schedule) => {
            const timing = schedule.timing;
            return (
              <View class={`gap-3 rounded-lg border px-4 py-3 ${theme.border} ${theme.surface}`}>
                <View class="flex-row flex-wrap items-center justify-between gap-2">
                  <Text class={`text-base font-semibold ${theme.heading}`}>{`Schedule #${schedule.id}`}</Text>
                  <Pill label={scheduleStates[schedule.state]}
                    tone={schedule.blocked ? "attention" : scheduleTone[schedule.state]} />
                </View>
                {schedule.blocked && (
                  <Text class={`text-sm font-semibold ${theme.tone.attention}`}>Blocked by a prior run</Text>
                )}
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
                ) : <Text class={`text-sm ${theme.muted}`}>Timing unavailable.</Text>}
              </View>
            );
          })}
        </>
      )}
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
  const clock = signal(Date.now());
  const attentionOnly = signal(false);
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
  const timer = setInterval(() => {
    if (connected) void refreshRunner();
  }, 1000);
  const ageTimer = setInterval(() => { clock.set(Date.now()); }, 30_000);
  cleanup(() => { closed = true; clearInterval(timer); clearInterval(ageTimer); });

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
    LayoutAnimation.easeInEaseOut();
    attentionOnly.set(false);
    page.set(target);
    scroll?.scrollTo({ y: 0, animated: false });
  }

  function onlyAttention(only: boolean) {
    attentionOnly.set(only);
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
        pairNotice.set(reply ? reply.message ?? "" : unacknowledgedPairing);
      } else if (type !== "confirm") {
        runnerNotice.set("");
        const reply = await localAction({ type });
        runnerNotice.set(reply ? reply.message ?? "" : unacknowledgedRunner);
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
        attentionOnly.set(false);
        clock.set(Date.now());
        scroll?.scrollTo({ y: 0, animated: false });
      } else if (runner()) {
        const bytes = new Uint8Array(await picked.file.arrayBuffer());
        const reply = await localAction({ type: "review", bytes: Array.from(bytes) });
        pairNotice.set(reply ? reply.message ?? "" : unacknowledgedPairing);
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
    <View class={`flex-1 flex-row ${theme.canvas}`}>
      <View class={`w-40 shrink-0 gap-1 px-3 py-5 ${theme.rail}`}>
        <Text class={`mb-5 px-2 text-base font-semibold ${theme.railTitle}`}>CoconutClaw</Text>
        {pages.map((label) => <NavItem label={label} selected={page() === label} onPress={() => showPage(label)} />)}
      </View>
      <View class="min-w-0 flex-1">
        <View class={`flex-row flex-wrap items-center justify-between gap-2 border-b px-5 py-3 ${theme.surface}`}>
          <View class="min-w-0 gap-1">
            <Text class={`text-sm font-semibold ${theme.secondary}`}>
              {current().source === "demo" ? "Synthetic demo" : `Snapshot · ${utc(current().observed_at)}`}
            </Text>
            {current().source === "demo" ? null : (
              <Text class={`text-xs ${isStale(current().observed_at, clock()) ? theme.tone.attention : theme.muted}`}>
                {`${age(current().observed_at, clock())} · refresh by opening a new export`}
              </Text>
            )}
          </View>
          <ActionButton label="Open snapshot" disabled={operation() !== null}
            buttonRef={(ref) => { openButton = ref; }} onPress={() => openFile("snapshot")} />
        </View>
        {current().source === "demo" ? (
          <Text class={`border-b px-5 py-2 text-sm font-semibold ${theme.demoSurface} ${theme.demoText}`}>
            Fabricated sample data. Not a coordinator export, and no live fleet is shown.
          </Text>
        ) : null}
        {snapshotError() ? (
          <Text class={`border-b px-5 py-2 text-sm ${theme.errorSurface} ${theme.errorText}`}
            role="alert">{snapshotError()}</Text>
        ) : null}
        <Scroll class="flex-1" ref={(ref) => { scroll = ref; }}>
          <View class="gap-4 px-5 py-5">
            <View class="flex-row flex-wrap items-center justify-between gap-2">
              <Text class={`text-2xl font-semibold ${theme.heading}`} role="header" accessibilityLevel={1}>{page()}</Text>
              {page() === "Nodes" && (
                <ActionButton label="Review public card" disabled={operation() !== null}
                  buttonRef={(ref) => { reviewButton = ref; }} onPress={() => openFile("card")} />
              )}
            </View>
            {page() === "Nodes" && (
              <>
                {card() && <CardReview card={card()!} digest={digest()} busy={operation() !== null}
                  clear={closeCard} confirm={() => pressLocal("confirm")} />}
                {(operation() === "confirm" || (operation() === "card" && runner())) ? (
                  <Busy label={operation() === "confirm"
                    ? "Confirming pairing — waiting for the host" : "Reviewing card — waiting for the host"} />
                ) : pairNotice() ? (
                  <Text role="alert" class={`text-sm ${theme.secondary}`}>{pairNotice()}</Text>
                ) : null}
                {(runner() || runnerNotice() || (connected && !runner())) && (
                  <View class={`gap-2 rounded-lg border px-4 py-3 ${theme.border} ${theme.surface}`}>
                    <Text class={`text-base font-semibold ${theme.heading}`}>Local runner</Text>
                    {(operation() === "start" || operation() === "stop") ? (
                      <Busy label={operation() === "start"
                        ? "Requesting runner start — waiting for the host"
                        : "Requesting runner stop — waiting for the host"} />
                    ) : (
                      <Text class={`text-sm ${theme.secondary}`}>
                        {runner() ? runnerLabels[runner()!] : hostLost}
                      </Text>
                    )}
                    {runnerNotice() ? <Text role="alert" class={`text-sm ${theme.secondary}`}>{runnerNotice()}</Text> : null}
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
                <Nodes nodes={current().nodes} attentionOnly={attentionOnly()} onChange={onlyAttention} />
              </>
            )}
            {page() === "Jobs" && (
              <Jobs jobs={current().jobs} nodes={current().nodes}
                attentionOnly={attentionOnly()} onChange={onlyAttention} />
            )}
            {page() === "Routing" && <Routing snapshot={current()} />}
            {page() === "Schedules" && (
              <Schedules snapshot={current()} attentionOnly={attentionOnly()} onChange={onlyAttention} />
            )}
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
      ref={props.buttonRef} focusVisibleStyle={theme.focus}
      onPress={() => { if (!props.disabled) props.onPress(); }}
      class={`self-start rounded-md border px-3 py-2 ${theme.press} ${props.disabled
        ? theme.buttonDisabled : theme.button}`}>
      <Text class={`text-sm font-semibold ${props.disabled ? theme.buttonDisabledText : theme.heading}`}>
        {props.label}
      </Text>
    </View>
  );
}

function CardReview(props: { card: PublicCard; digest: string | null; busy: boolean; clear: () => void; confirm: () => void }) {
  return (
    <View class={`gap-1 rounded-lg border px-4 py-3 ${theme.border} ${theme.surface}`}>
      <View class="flex-row flex-wrap items-center justify-between gap-2">
        <Text class={`text-base font-semibold ${theme.heading}`}>
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
          <Text class={`text-sm ${theme.muted}`}>Compare these values independently before confirming.</Text>
          <ActionButton label="Confirm pairing" disabled={props.busy} onPress={props.confirm} />
        </>
      ) : (
        <Text class={`text-sm ${theme.muted}`}>Compare these values independently, then use nodes pair --preview on the intended host.</Text>
      )}
    </View>
  );
}

mount(App);
