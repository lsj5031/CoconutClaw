# CoconutClaw

A local Telegram agent using Codex for conversation and delegated tasks.

## Start here (Linux and macOS)

CoconutClaw runs on your computer: Telegram supplies messages, your locally
installed Codex executes them, and SQLite keeps history and task results.
Start with one computer. Adding an experimental runner is a separate procedure
below, not a setting that silently enables networking in the normal application.

Native Windows work is being handled separately by Leo. This guide does **not**
claim native Windows runner support or provide an unverified PowerShell setup.
Use the Windows guide only after that work's native validation gate is published.

### 1. Prerequisites and build

You need a source checkout, Rust stable/Cargo (Rust 2024 edition), and a C compiler
for bundled SQLite. On macOS use the Xcode Command Line Tools; on Linux use your
distribution's C development toolchain. SQLite itself needs no separate install.
Install/provision these through your approved package source or offline bundle.

Codex must already be installed and authenticated **on the computer that runs
it**. Check `codex --version`, then verify Codex works independently in your
intended workspace using your organization's supported authentication procedure.
CoconutClaw does not install Codex, log in for you, or copy credentials to runners.
Normal messaging needs reachable Telegram and Codex endpoints. In an isolated
network, use approved reachable endpoints; local build/tests/doctor do not prove
external connectivity.

From the repository directory:

```sh
rustc --version
cargo --version
cargo build --release -p coconutclaw --locked
BIN="$PWD/target/release/coconutclaw"
"$BIN" --help
```

If dependencies are already cached, append `--offline` to the build command.
A cache miss requires an approved dependency mirror/bundle, not a login or
security workaround. If you set `CARGO_TARGET_DIR`, adjust `BIN` to that directory.
Keep using the absolute `BIN` path below, or install the binary in your own PATH.

### 2. Create a private data directory

For **new data only** (read [migration](docs/migration-phase-0.md) before reusing
an older installation):

```sh
DATA="$HOME/.local/state/coconutclaw/default"
mkdir -p "$DATA"
chmod 700 "$DATA"
# Run this copy only if config.toml does not already exist.
test -e "$DATA/config.toml" || cp config.toml.example "$DATA/config.toml"
chmod 600 "$DATA/config.toml"
```

Open `$DATA/config.toml` in your editor. Set these existing keys, leaving the
other example settings unchanged initially:

```toml
TELEGRAM_BOT_TOKEN = "your-own-bot-token"
TELEGRAM_CHAT_ID = "your-numeric-chat-id"
TELEGRAM_CHAT_IDS = ""
CODEX_BIN = "codex"
EXEC_POLICY = "safe"
TIMEZONE = "UTC"
```

Use an absolute Codex executable path if it is not on PATH. Paths containing
spaces need quotes *inside* the TOML string, for example
`CODEX_BIN = "'/absolute/path with spaces/codex'"`. Doctor currently checks the
whole command string as a path rather than parsing such quoted command specs,
so it can report `codex_bin: false` for them. Prefer a simple executable on PATH
for your first setup. Do not put tokens in Git,
screenshots, command arguments or shared logs. Environment variables override
file settings; stale `TELEGRAM_*`, `CODEX_BIN` or `COCONUTCLAW_DATA_DIR` exports can
therefore change what runs. `TELEGRAM_CHAT_IDS` accepts comma-separated IDs for
additional explicitly allowed chats, not a wildcard.

### 3. Connect your Telegram bot

Create your own bot with Telegram's **@BotFather** and save its token only in
your private configuration. Send that bot a private message. You also need the
numeric ID of **your chat**, not the bot's username or ID. Obtain it through your
own bot administration tooling. If needed, this optional Python 3 snippet reads
`getUpdates` and prints only chat IDs/types, not tokens or message contents:

```sh
python3 - <<'PYTHON'
import getpass, json, urllib.request
key = getpass.getpass("Bot token (hidden): ")
try:
    with urllib.request.urlopen("https://api.telegram.org/bot" + key + "/getUpdates", timeout=30) as response:
        updates = json.load(response)
except Exception:
    raise SystemExit("Could not read updates; check your approved network and bot setup.") from None
for update in updates.get("result", []):
    message = update.get("message", {})
    chat = message.get("chat", {})
    if "id" in chat:
        print(chat["id"], chat.get("type", ""))
PYTHON
```

Run this yourself only where Telegram is reachable. Stop other pollers first;
if no ID appears, send a fresh message to your bot and try again. Do not disable
chat restrictions or send your token to a third-party ID lookup bot. Start with
one private chat before considering group access.

### 4. Check, run, and send your first message

```sh
"$BIN" --data-dir "$DATA" doctor --json
"$BIN" --data-dir "$DATA" run
```

Doctor should show `codex_bin`, `telegram_token` and `telegram_chat_id` as `true`.
These are local presence/format checks, **not** authentication or network tests;
inspect the JSON even when doctor exits successfully.

Keep the terminal running and send your bot `Hello, reply with a short greeting`.
That is a real Codex request and may incur provider charges. Try `/tasks` to
inspect tasks; `/cancel <task-id>` cancels a task in the same chat/topic. `/fresh`
starts a new conversation context without deleting history. Stop with Ctrl+C,
then restart the same `run` command with the **same data directory**. Do not
start two runtimes against the same directory or reset state to bypass an
outcome-unknown task.

`once --inject-text "hello"`, `once --inject-file /absolute/path/to/file`, and
`run --inject-text` also invoke Codex. They print markers without Telegram delivery
and are **not dry runs**. Development tests use fake providers instead.

### Where your data lives

`--data-dir` selects the directory **containing** `config.toml` and `state.db`,
not their parent. `COCONUTCLAW_DATA_DIR` has the same meaning. Without either,
Unix uses `$XDG_STATE_HOME/coconutclaw/default` or
`~/.local/state/coconutclaw/default`; the configured Windows default path is
`%LOCALAPPDATA%/CoconutClaw/default` (a path definition, not a runner-support claim).
History, memory, task receipts and projections stay local. Codex authentication
remains in its separately provisioned local home. Keep state private and back it
up consistently while stopped; copying only `state.db` during a running WAL
transaction is not a safe backup procedure.

## Connect an experimental runner (Linux/macOS)

There is **no production `runner add` command** yet. The implemented connection
path is the disposable native **test-harness** launcher below. It runs one owned
file fixture through the same signed Start/Result/cancellation and durable Store
path; it is not general remote Telegram task dispatch. Independent review is
**deferred, not approved**. Shared-original-Store multi-runner concurrency remains
**constrained**. Read [the full acceptance contract](docs/multihost-acceptance.md)
before using credentials. Historical accepted Mac A/B/C2 evidence is retained
there; this setup guide does not claim a new live acceptance run.

Requirements: two explicitly reachable Linux/macOS hosts, matching source and
locked dependencies, Python **3.11+**, and an already provisioned native Codex on
the runner. This finite harness selects `gpt-6.1-terra`/`max` and the dangerous
approval/sandbox bypass for its narrowly authorized fixture. It requires an
existing named `responses` provider with the supported API-key auth file or
host-local auth callback. Ordinary account/OAuth-only provisioning is not a
supported fallback for this harness. Never point its roots at real user work.

On **each** host, from the matching checkout, build a fresh native role binary
(the build is offline and fails on a missing dependency):

```sh
SCRIPT="$PWD/scripts/multihost-acceptance.py"
python3 "$SCRIPT" build --output /tmp/cc-role
```

Choose a shared alphanumeric/hyphen `RUN_ID` of 16–128 characters, and set
`CORE_IP`/`RUNNER_IP` to the actual reachable local interface addresses. The
`/tmp/cc-core`, `/tmp/cc-runner`, profile and binary paths below must be fresh;
use short paths because Unix sockets have length limits.

On the **coordinator**:

```sh
RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
# Set CORE_IP to this host's reachable address, not 0.0.0.0 or 127.0.0.1.
python3 "$SCRIPT" prepare --root /tmp/cc-core --role coordinator \
  --run-id "$RUN_ID" --bind "$CORE_IP:0"
python3 "$SCRIPT" launch --root /tmp/cc-core --executable /tmp/cc-role
```

Inspect the returned log and wait for `/tmp/cc-core/ready.json`. Transfer **only**
that public ready file to `/tmp/cc-core-ready.json` on the runner through your
approved file-copy channel. Set the **same** `RUN_ID` on the runner. On that host,
set `CODEX_EXE`, `AUTH_JSON`, and `SHARED_CONFIG` to its own existing absolute
Codex/auth/config file paths, then:

```sh
PYTHON_EXE="$(python3 -c 'import sys; print(sys.executable)')"
python3 "$SCRIPT" profile --output /tmp/cc-provider.json \
  --executable "$CODEX_EXE" --python "$PYTHON_EXE" \
  --auth-json "$AUTH_JSON" --shared-config "$SHARED_CONFIG"
python3 "$SCRIPT" prepare --root /tmp/cc-runner --role runner \
  --run-id "$RUN_ID" --bind "$RUNNER_IP:0" \
  --core-ready /tmp/cc-core-ready.json --provider-profile /tmp/cc-provider.json
python3 "$SCRIPT" launch --root /tmp/cc-runner --executable /tmp/cc-role
```

Transfer only the runner's public `/tmp/cc-runner/ready.json` back to
`/tmp/cc-runner-ready.json` on the coordinator. Never transfer role directories,
device keys, credentials or native session state. On the **coordinator**:

```sh
# Transport-only preflight, no provider invocation or task allocation.
python3 "$SCRIPT" control --root /tmp/cc-core check \
  --runner-ready /tmp/cc-runner-ready.json
python3 "$SCRIPT" control --root /tmp/cc-core stop
```

After both roles report stopped, relaunch the **same roots** on their respective
hosts with the `launch` commands above. Exchange their **fresh** ready files.
Then, only when you authorize the real harmless fixture request:

```sh
python3 "$SCRIPT" control --root /tmp/cc-core run \
  --runner-ready /tmp/cc-runner-ready.json
# From another terminal while it runs:
python3 "$SCRIPT" control --root /tmp/cc-core status
# If you need cancellation instead of completion:
# python3 "$SCRIPT" control --root /tmp/cc-core cancel
```

After collecting the receipt, `control --root /tmp/cc-core stop` stops both roles.
For a successful completed fixture, relaunch the same stopped roots, exchange new
ready files, then use `control --root /tmp/cc-core replay --runner-ready
/tmp/cc-runner-ready.json` (on one shell line). Use `replay-cancel` only for a
settled cancellation. Replay must preserve exact event hashes and one invocation.
On any failed/ambiguous run, preserve roots/logs and reconcile explicitly; do not
`prepare` over them or blindly re-run the model. Direct networking needs reachable
UDP in both directions; the full guide also documents an explicitly supplied
relay, with no default relay, discovery or firewall changes.

## Troubleshooting

- **Executable not found:** inspect `doctor` and `command -v codex`. Use an
  absolute `CODEX_BIN` if your service/terminal PATH differs.
- **No Telegram reply:** verify the token and numeric allowlisted chat ID locally,
  check that only one bot poller is running, then inspect local logs for network
  or provider errors. Never paste token-bearing URLs or raw credential files.
- **Doctor passes, Codex fails:** doctor does not check login, model availability
  or routing. Test your separately provisioned Codex directly; do not enable
  `yolo` merely to fix authentication.
- **Already running/locked:** stop the owned runtime and confirm its exit. Do not
  delete locks or launch another writer against an active state directory.
- **Unknown outcome after a crash:** inspect the saved task/result and reconcile;
  a retry may repeat external effects. Restart does not authorize a new attempt.
- **Runner does not become ready:** inspect its returned role log, exact source
  version, host-local dependency paths, IP/relay reachability and short fresh
  root path. Preserve failed roots; do not copy credentials to make it connect.

## Behavior

- Telegram polling only, restricted to configured chat IDs.
- One ordered conversation lane and explicit Codex session per private topic,
  with a separate root session. Five task workers globally, with durable queues.
- Native Rich Markdown final replies, including tables, headings and math.
  See [topics, rich output and delivery safety](docs/topics-rich.md) for mode
  transitions, supported markup, API limits and mock acceptance evidence.
- `/fresh` starts a new context/session without deleting history.
- Images are attached through Codex's image input; documents and videos are
  available as local file paths. No local vision engine or audio transcription.
- `SOUL.md`, session-scoped SQLite pending tasks and history provide context.
  `TASKS/pending.md` is an operator-only aggregate: neither its manual bytes nor
  its mixed-session managed block is injected into provider prompts.
  Session-scoped SQLite memory has compact curated context and bounded FTS5
  search/source reads. `MEMORY_APPEND` proposes a candidate, not authority.
  Use `/memory remember <text>`, `list [before-id]`, `search <text>`, `get <id>`,
  `promote <id>`, `correct <active-id> <text>` or `undo`.
  See [local memory](docs/memory.md) for budgets, scope and limitations.
  Explicit local `import-openclaw` previews selected Markdown files by default;
  `--apply` imports scoped candidates, never a live OpenClaw backend. See the
  [import contract](docs/openclaw-import.md) before selecting personal data.
- A `DELEGATE_TASK` request creates a durable task ID with a separate Codex
  session and no parent conversation history. Its labeled result is delivered
  to, and retained in the history of, the originating topic/root session.
- `/tasks`, `/cancel [task-id]`, `/approve <approval-id>` and
  `/reject <approval-id>` are scoped to the originating topic/root session.
- `SEND_APPROVAL` pauses the request until an explicit approval. Approval is
  advisory model coordination, not a sandbox or substitute for Codex's own
  execution policy. The default `EXEC_POLICY = "safe"` does not bypass Codex
  approvals/sandbox; `"yolo"` explicitly enables the dangerous bypass.

Final results and per-operation delivery acknowledgments live outside Codex
context. Failed delivery retries the saved output, never re-runs Codex.
After a crash, a saved final turn can be finalized without another agent call;
started attempts without a final result are marked outcome-unknown and are
not automatically retried. Pending approvals survive restart.
Provider progress is persisted as a coalesced, bounded latest status even when
the Telegram placeholder fails. Managed Markdown projections use a single writer,
synced staging files and atomic replacement, preserving surrounding manual bytes.

Telegram has no transactional send API. Durable task sends with ambiguous
outcomes, including a crash before acknowledgment, are held for inspection
rather than automatically resent or switched to another representation.
Cancellation stops the local process group; it cannot undo actions already
performed. Codex subprocess reconciliation across a host crash is not claimed.
Handled SIGINT/SIGTERM stops intake and queue advancement, cancels active groups,
and joins execution workers with a ten-second shutdown deadline. HTTP work is
interruptible without transferring SQLite or projection ownership to HTTP threads.
If the deadline is exhausted, the process reports failure and unfinished started
attempts are reconciled as outcome-unknown on restart. Queued jobs remain saved.

## Service

After the foreground run works, stop it before starting a service. Keep the
binary at a stable absolute path, then explicitly install/start the user service:

```sh
"$BIN" --data-dir "$DATA" service install
"$BIN" --data-dir "$DATA" service start
"$BIN" --data-dir "$DATA" service status
# To stop it later:
"$BIN" --data-dir "$DATA" service stop
```

`service install|start|stop|status|uninstall` manages only the single runtime
service, using systemd user services on Linux or launchd on macOS. These commands
change local service state only when explicitly invoked. They do not manage old
timers or named-instance services.

## Configuration and migration

See [Phase 0 migration](docs/migration-phase-0.md) **before using existing data**.
Removed settings, including empty/off settings, produce actionable errors.
Environment values override retained config keys. Unknown TOML keys are errors.
`config.toml.example` lists the complete supported configuration.
Supplied types, numeric domains, timezone/URL values and state path conflicts are
validated before layout creation. Diagnostics redact values from their full error
chains. Raw and `telegram:`-prefixed historical chat IDs share history and `/fresh`
boundaries without rewriting historical rows.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build -p coconutclaw --bin coconutclaw --locked
cargo test --workspace --all-targets --locked
```

Build the normal executable before the native lifecycle tests. Tests use local
fake Telegram endpoints and fake Codex executables. No live
messaging or agent calls are required. See [architecture](docs/architecture.md).
Provider executions have no default total deadline; see
[activity-aware timeouts](docs/provider-timeouts.md) for idle limits and explicit caps.
The [Phase 0 validation record](docs/validation-phase-0.md) lists checks and limitations.
Historical release notes and `docs/superpowers/` describe earlier versions,
not the supported runtime.
