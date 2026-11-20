import { mount, Picker, Scroll, signal, Text, View } from "nsh";
import { demo } from "./demo";
import { delivery, maxSnapshotBytes, observations, readPublicCard, readSnapshot, statuses } from "./snapshot";
import type { PublicCard, Snapshot } from "./snapshot";

type Page = "Nodes" | "Jobs";

function utc(iso: string) {
  return `${iso.slice(0, 10)} · ${iso.slice(11, 19)} UTC`;
}

function runnerLabel(id: string) {
  return `Runner · ${id.slice(-8)}`;
}

function NavItem(props: { label: Page; selected: boolean; onPress: () => void }) {
  return (
    <View role="button" accessibilityLabel={props.label} accessibilityState={{ selected: props.selected }}
      onPress={props.onPress}
      class={props.selected
        ? "rounded-md bg-[#33394a] px-3 py-3"
        : "rounded-md px-3 py-3 hover:bg-[#2a3040]"}>
      <Text class={props.selected ? "text-sm font-semibold text-white" : "text-sm font-semibold text-[#c5ccdd]"}>
        {props.label}
      </Text>
    </View>
  );
}

function Fact(props: { label: string; value: string }) {
  return (
    <View class="gap-1">
      <Text class="text-xs text-[#526174]">{props.label}</Text>
      <Text class="text-sm text-[#1d2938]">{props.value}</Text>
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
              <Fact label="Delivery" value={delivery[job.delivery]} />
            </View>
          </View>
        );
      })}
    </View>
  );
}

function App() {
  const page = signal<Page>("Nodes");
  const current = signal<Snapshot>(demo);
  const busy = signal(false);
  const error = signal("");
  const card = signal<PublicCard | null>(null);

  async function openFile(kind: "snapshot" | "card") {
    if (busy()) return;
    busy.set(true);
    error.set("");
    if (kind === "card") card.set(null);
    try {
      const [picked] = await Picker.pickFiles({
        filters: [{ name: kind === "snapshot" ? "Coordinator snapshots" : "Public node cards", extensions: ["json"] }],
        maxSize: maxSnapshotBytes,
      });
      if (picked) {
        if (!picked.file) throw new Error("File is too large");
        const contents = await picked.file.text();
        if (kind === "snapshot") current.set(readSnapshot(contents));
        else card.set(readPublicCard(contents));
      }
    } catch {
      error.set(kind === "snapshot"
        ? "Couldn’t open the snapshot. Choose a coordinator JSON export under 64 KiB."
        : "Couldn’t review the card. Choose a UTF-8 public node card from nodes card.");
    } finally {
      busy.set(false);
    }
  }

  return (
    <View class="flex-1 flex-row bg-[#f3f5f9]">
      <View class="w-40 shrink-0 gap-1 bg-[#212635] px-3 py-5">
        <Text class="mb-5 px-2 text-base font-semibold text-white">CoconutClaw</Text>
        <NavItem label="Nodes" selected={page() === "Nodes"} onPress={() => page.set("Nodes")} />
        <NavItem label="Jobs" selected={page() === "Jobs"} onPress={() => page.set("Jobs")} />
      </View>
      <View class="min-w-0 flex-1">
        <View class="flex-row flex-wrap items-center justify-between gap-2 border-b border-[#d8dfe8] bg-white px-5 py-3">
          <Text class="text-sm font-semibold text-[#526174]">
            {current().source === "demo" ? "Synthetic demo" : `Snapshot · ${utc(current().observed_at)}`}
          </Text>
          <View role="button" accessibilityLabel="Open snapshot" onPress={() => openFile("snapshot")}
            accessibilityState={{ disabled: busy() }}
            class="rounded-md border border-[#d8dfe8] px-3 py-2 hover:bg-[#eef2f6] active:bg-[#e4e9f1]">
            <Text class="text-sm font-semibold text-[#1d2938]">Open snapshot</Text>
          </View>
        </View>
        {error() ? <Text class="px-5 pt-3 text-sm text-[#913c27]" role="alert">{error()}</Text> : null}
        <Scroll class="flex-1">
          <View class="gap-4 px-5 py-5">
            <Text class="text-2xl font-semibold text-[#1d2938]" role="header" accessibilityLevel={1}>{page()}</Text>
            {page() === "Nodes" ? (
              <>
                <View role="button" accessibilityLabel="Review public card" onPress={() => openFile("card")}
                  accessibilityState={{ disabled: busy() }}
                  class="self-start rounded-md border border-[#d8dfe8] bg-white px-3 py-2 hover:bg-[#eef2f6]">
                  <Text class="text-sm font-semibold text-[#1d2938]">Review public card</Text>
                </View>
                {card() && <CardReview card={card()!} clear={() => card.set(null)} />}
                <Nodes nodes={current().nodes} />
              </>
            ) : <Jobs jobs={current().jobs} />}
          </View>
        </Scroll>
      </View>
    </View>
  );
}

function CardReview(props: { card: PublicCard; clear: () => void }) {
  return (
    <View class="gap-2 rounded-lg border border-[#d8dfe8] bg-white px-4 py-3">
      <View class="flex-row flex-wrap items-center justify-between gap-2">
        <Text class="text-base font-semibold text-[#1d2938]">
          {props.card.role === "runner" ? "Runner card for coordinator" : "Coordinator card for runner"}
        </Text>
        <View role="button" accessibilityLabel="Close card" onPress={props.clear} class="px-2 py-1">
          <Text class="text-sm text-[#354459]">Close card</Text>
        </View>
      </View>
      <Fact label="Public ID" value={props.card.id} />
      <Fact label="Fixed UDP address" value={props.card.address} />
      <Fact label="Source revision" value={props.card.revision} />
      <Text class="text-sm text-[#526174]">Compare these values independently, then use nodes pair --preview on the intended host.</Text>
    </View>
  );
}

mount(App);
