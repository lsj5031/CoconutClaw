# Phase 2 hosted synthetic pilot candidate

The default `cloudflare.config.ts` stages `coconutclaw-phase1-test` with
`PHASE=disabled`. The explicit `phase2-pilot` mode selects one named
SQLite `BotLedger`, `hosted-synthetic-pilot`, with its own agent and evidence
signing domain. Both builds disable `workers.dev` and version previews and
declare no routes or triggers. The base for this candidate is source commit
`c3da96d39950c95575b3cf3ad9f1484875c30586`. The parent independently read back
isolated Worker version `9bb8cd05-c44d-4b39-90be-bece54f83ee6` on Mars.

## Invocation

Cloudflare documents private invocation through Service Bindings with
another Worker as the caller. A trusted caller for this isolated service
and a direct authenticated invocation from a local process both remain
unverified. The `cf` and Workers Scripts API references distinguish
deployment commands from version and preview URLs.
[Service Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/)
· [cf project commands](https://developers.cloudflare.com/cf/projects/)
· [Workers Scripts API](https://developers.cloudflare.com/api/resources/workers/subresources/scripts/)

The documented no-paid fallback for a separately authorized hosted test is
one Worker route on a dedicated, already-proxied hostname in an eligible
Cloudflare Free zone. Use an exact-host custom rule to allow only the
approved driver source IP and the exact POST paths. Use the Free WAF
rate-limiting rule on those paths with a target of 16 requests per source IP
per 10 seconds and a 10-second block. Keep `workers.dev` and version previews
disabled. Free WAF rate limiting counts per IP and per data center, and
enforcement can lag. An allowlisted network can share egress IPs. Route
isolation, WAF rule attachment, and raw Worker traffic remain unverified on
the hosted target.
[Workers routes](https://developers.cloudflare.com/workers/configuration/routing/routes/)
· [WAF custom rules](https://developers.cloudflare.com/waf/custom-rules/)
· [WAF rate-limiting rules](https://developers.cloudflare.com/waf/rate-limiting-rules/)

The Worker requires HTTPS POST, an allowed path, a disposable 256-bit
`PILOT_TOKEN` bearer, and a current `PILOT_WINDOW` (`start_ms:end_ms`, absolute
UTC epoch milliseconds, at most ten minutes). It then checks the
`PILOT_GATE` binding at 16 requests per ten seconds using a single fixed
key. It forwards at most 16 KiB after a two-second body deadline and a second
window check. Missing or failing rate binding fails closed. This gate runs
inside the Worker before the DO call. The platform limiter is approximate
per Cloudflare location. Raw Worker hits reach the Worker before these
guards, and authenticated requests with malformed protocol payloads can
still reach the DO. The DO's existing persistent admission counters provide
a separate budget after dispatch.
[Workers Rate Limiting binding](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/)

## One run

The driver submits the fixed update `23001` for synthetic chat `321` and
topic `37`. The ledger persists one inbox row and admits one claim for its
lifetime, including across expiration and restart. The driver uses a
disposable Ed25519 key corresponding to `PILOT_ALPHA_KEY` to sign requests
and result evidence. It checks the grant and status, performs one fixed
in-process text transform, then replays the exact inner result proof with
a new signed outer sequence. The driver stops after at most 16 requests,
four receipt polls, a two-second request deadline, or a 20-second overall
deadline. Redirects and failed runs are terminal.

The alarm reserves and records the synthetic delivery in SQLite with the
same chat, topic and result text. The reservation, singleton payload and
receipt commit in one transaction with zero outbound calls.
The only pilot fixture query reads that receipt. The test restarts workerd
after intake, result and receipt against one retained SQLite directory.
It checks the repeated update, repeated result proof, stale sequence,
forged proof, a single completed attempt, and a single same-topic receipt.
It observes local rate-limiter denial with no DO admission counter increase,
and checks that wrong credentials, oversize input, a closed window or a
missing limiter do not create an object.

## Local acceptance

Use the compiler and Wasm tooling from the
[Phase 2 loopback validation](cloud-gateway-phase-2-local.md#local-validation).
Stage only one mode at a time in the local build output directory.

```sh
mise x -- npm --prefix crates/coconutclaw-gateway run stage:pilot
mise x -- node --test crates/coconutclaw-gateway/pilot.test.mjs
mise x -- npm --prefix crates/coconutclaw-gateway run check:cf:pilot
mise x -- npm --prefix crates/coconutclaw-gateway run stage
mise x -- npm --prefix crates/coconutclaw-gateway run check:cf
GATEWAY_LOCAL_EXECUTOR=/path/to/gateway-local-executor \
  mise x -- node --test crates/coconutclaw-gateway/{config,integration}.test.mjs
```

On Leo-Claw, all six pilot tests and all 15 existing config and integration
cases passed. The pilot suite includes the CLI over loopback with certificate
verification and a second-claim rejection after lease expiration and restart.
The project's pinned official `cf` CLI accepted both prebuilt modes at
322.00 KiB gzipped. It reported the 16-per-ten-second limiter only in pilot
mode. Staged metadata verifies the secret declarations. Hosted provisioning
and binding behavior require separate acceptance.

Two `cargo-mutants` 27.1.0 variants failed their matching pilot tests. A
no-op synthetic delivery exhausted the receipt polls. Removing the
single-attempt guard admitted a second claim after lease expiration.

## Remaining gates

- Hosted synthetic acceptance needs an authorized isolated route and proven
  pre-Worker controls, or an authorized private caller. Provision the disposable
  bearer and signing key and confirm the short window and isolated named ledger.
  Run the driver once, verify the hosted receipt and replay behavior, then close
  ingress and verify the disabled version. The independently verified isolated
  version is the rollback reference.
- Real Codex success and cancellation remain blocked. The loopback executable
  only launches the fixed synthetic child with an empty environment. The
  provider has timeouts and owned-process-group cancellation. Its real
  `CodexRunner` still needs a verified disposable state directory, a verified
  lane-specific provider identity, tool and network containment, and a hard
  usage ceiling before exercising the two-attempt success and cancellation
  path.
- Production cutover requires separately authorized test Bot and gateway
  identities, real webhook and same-topic Bot API acceptance, hosted capacity
  and pacing evidence, and the key rotation, native lifecycle, containment,
  routing, memory, scheduling and migration gates in the
  [Phase 2 loopback notes](cloud-gateway-phase-2-local.md#remaining-pilot-gates).
  Task #23 remains `Unknown`.
