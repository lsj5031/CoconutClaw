# Plan: Absorb service/run scripts into Rust CLI

## Goal

Eliminate 16 shell/PowerShell scripts by absorbing service management into a
`coconutclaw service` subcommand. Keep only `asr.sh` and `tts.sh` (which
genuinely need shell evaluation for `ASR_CMD_TEMPLATE` / `TTS_CMD_TEMPLATE`).

## Scripts to delete

| Script | Reason |
|--------|--------|
| `scripts/run.sh` | Redundant — users run `coconutclaw run` directly |
| `scripts/run.ps1` | Same |
| `scripts/heartbeat.ps1` | Wrapper for `run.ps1 --heartbeat`, redundant |
| `scripts/nightly_reflection.ps1` | Wrapper for `run.ps1 --nightly-reflection`, redundant |
| `scripts/install.sh` | Shortcut for `service.sh install` |
| `scripts/start.sh` | Shortcut for `service.sh start` |
| `scripts/stop.sh` | Shortcut for `service.sh stop` |
| `scripts/status.sh` | Shortcut for `service.sh status` |
| `scripts/uninstall.sh` | Shortcut for `service.sh uninstall` |
| `scripts/service.sh` | Absorbed into Rust `coconutclaw service` |
| `scripts/install.ps1` | Absorbed into Rust |
| `scripts/start.ps1` | Absorbed into Rust |
| `scripts/stop.ps1` | Absorbed into Rust |
| `scripts/status.ps1` | Absorbed into Rust |
| `scripts/uninstall.ps1` | Absorbed into Rust |
| `scripts/service.ps1` | Absorbed into Rust |

## Scripts to keep

| Script | Reason |
|--------|--------|
| `scripts/asr.sh` | `ASR_CMD_TEMPLATE` needs `bash -lc` shell evaluation |
| `scripts/tts.sh` | `TTS_CMD_TEMPLATE` needs `bash -lc` shell evaluation |

## New CLI surface

```
coconutclaw service install [--heartbeat HH:MM] [--reflection HH:MM]
coconutclaw service start
coconutclaw service stop
coconutclaw service status
coconutclaw service uninstall
```

Global flags `--instance` / `--instance-dir` apply as usual.

## Implementation steps

### 1. Add `Service` subcommand to CLI (crates/coconutclaw-cli/src/main.rs)

Add a new `Commands::Service(ServiceArgs)` variant:

```rust
#[derive(Subcommand, Debug)]
enum Commands {
    Once(TurnArgs),
    Run(TurnArgs),
    Heartbeat,
    NightlyReflection,
    Doctor,
    Service(ServiceArgs),
}

#[derive(Args, Debug)]
struct ServiceArgs {
    #[command(subcommand)]
    action: ServiceAction,
}

#[derive(Subcommand, Debug)]
enum ServiceAction {
    Install {
        #[arg(long, default_value = "09:00")]
        heartbeat: String,
        #[arg(long, default_value = "22:30")]
        reflection: String,
    },
    Start,
    Stop,
    Status,
    Uninstall,
}
```

### 2. Create service module (crates/coconutclaw-cli/src/service.rs)

New module with the following structure:

```
run_service(cfg, action) -> Result<()>
├── detect_platform() -> Platform { Linux, MacOS }
├── service_names(cfg) -> ServiceNames
│
├── Linux (systemd)
│   ├── install: write .service + .timer files to ~/.config/systemd/user/
│   │   - main run service (Type=simple, Restart=always)
│   │   - heartbeat oneshot + timer (OnCalendar)
│   │   - nightly-reflection oneshot + timer (OnCalendar)
│   │   - systemctl --user daemon-reload + enable
│   ├── start: systemctl --user start
│   ├── stop: systemctl --user stop
│   ├── status: systemctl --user status
│   └── uninstall: stop + disable + rm files + daemon-reload
│
├── MacOS (launchd)
│   ├── install: write .plist files to ~/Library/LaunchAgents/
│   │   - run plist (KeepAlive + RunAtLoad)
│   │   - heartbeat plist (StartCalendarInterval)
│   │   - reflection plist (StartCalendarInterval)
│   │   - launchctl bootstrap
│   ├── start: launchctl kickstart
│   ├── stop: launchctl bootout
│   ├── status: launchctl print
│   └── uninstall: stop + rm plists
│
└── Windows (unsupported on Unix build, print hint to user)
```

Key differences from the bash version:
- Service units point directly to the `coconutclaw` binary (no `run.sh` wrapper)
- The binary path is resolved via `std::env::current_exe()` for installed binary,
  or the user can override with a config value
- Instance args (`--instance` / `--instance-dir`) are baked into the unit files
- `ExecStart` / `ProgramArguments` call `coconutclaw run`, `coconutclaw heartbeat`,
  `coconutclaw nightly-reflection` directly

### 3. Service naming convention

Preserve the existing naming from `service.sh`:

- Base names: `coconutclaw.service`, `coconutclaw-heartbeat.{service,timer}`, etc.
- Non-default instances: `coconutclaw-{key}.service` where key is sanitized instance name
- Instance-dir mode: `coconutclaw-dir-{basename}-{cksum}.service`
- LaunchAgent labels: `io.coconutclaw.run`, `io.coconutclaw.heartbeat`, etc.

### 4. Binary resolution for unit files

The generated systemd/launchd units need the absolute path to the `coconutclaw`
binary. Resolution order:

1. `std::env::current_exe()` — works when installed via `cargo install`
2. Fall back to `which coconutclaw` equivalent (search PATH)
3. Bail with a clear error if not found

### 5. Wire up in main.rs

```rust
Commands::Service(args) => service::run_service(&cfg, args),
```

### 6. Delete scripts

Remove all 16 scripts listed above. Update:
- `AGENTS.md` — remove service scripts from architecture section
- `Makefile` — remove any references to deleted scripts
- `README.md` — update usage instructions
- `.github/` — update any CI references

### 7. Update shellcheck validation

Update `AGENTS.md` validation command to only lint the remaining scripts:

```
shellcheck scripts/asr.sh scripts/tts.sh
```

## Validation

- `cargo test` — all existing tests pass
- `cargo run -p coconutclaw -- service install` — generates correct systemd units on Linux
- `cargo run -p coconutclaw -- service status` — shows service status
- `cargo run -p coconutclaw -- service uninstall` — cleans up
- `shellcheck scripts/asr.sh scripts/tts.sh` — remaining scripts still pass
