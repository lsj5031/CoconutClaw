import { mount, Picker, Scroll, signal, Text, View } from "nsh";
import { demo } from "./demo";
import { delivery, maxSnapshotBytes, observations, readSnapshot, statuses } from "./snapshot";
import type { Snapshot } from "./snapshot";

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
      {props.nodes.map((node) => (
        <View class="gap-2 rounded-lg border border-[#d8dfe8] bg-white px-4 py-3">
          <View class="flex-row flex-wrap items-center justify-between gap-2">
            <Text class="text-base font-semibold text-[#1d2938]" accessibilityLabel={`Runner ${node.id}`}>
              {runnerLabel(node.id)}
            </Text>
            <Text class={["reachable", "exchange"].includes(node.observation)
              ? "text-sm font-semibold text-[#17614c]"
              : "text-sm font-semibold text-[#526174]"}>
              {observations[node.observation]}
            </Text>
          </View>
          <Fact label="Last reached" value={node.last_reached ? utc(node.last_reached) : "No recorded contact"} />
        </View>
      ))}
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

  async function openSnapshot() {
    if (busy()) return;
    busy.set(true);
    error.set("");
    try {
      const [picked] = await Picker.pickFiles({
        filters: [{ name: "Coordinator snapshots", extensions: ["json"] }],
        maxSize: maxSnapshotBytes,
      });
      if (picked) {
        if (!picked.file) throw new Error("Snapshot is too large");
        current.set(readSnapshot(await picked.file.text()));
      }
    } catch {
      error.set("Couldn’t open the snapshot. Choose a coordinator JSON export under 64 KiB.");
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
          <View role="button" accessibilityLabel="Open snapshot" onPress={openSnapshot}
            accessibilityState={{ disabled: busy() }}
            class="rounded-md border border-[#d8dfe8] px-3 py-2 hover:bg-[#eef2f6] active:bg-[#e4e9f1]">
            <Text class="text-sm font-semibold text-[#1d2938]">{busy() ? "Opening…" : "Open snapshot"}</Text>
          </View>
        </View>
        {error() ? <Text class="px-5 pt-3 text-sm text-[#913c27]" role="alert">{error()}</Text> : null}
        <Scroll class="flex-1">
          <View class="gap-4 px-5 py-5">
            <Text class="text-2xl font-semibold text-[#1d2938]" role="header" accessibilityLevel={1}>{page()}</Text>
            {page() === "Nodes" ? <Nodes nodes={current().nodes} /> : <Jobs jobs={current().jobs} />}
          </View>
        </Scroll>
      </View>
    </View>
  );
}

mount(App);
