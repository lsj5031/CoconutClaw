# Phase 2 hosted synthetic pilot

The default `cloudflare.config.ts` stages `coconutclaw-phase1-test` with
`PHASE=disabled` with no hostname. Only non-preview `phase2-pilot` declares
the Custom Domain `bigfur.liu.nz`. Both builds disable `workers.dev` and
version previews, omit route patterns and triggers, and export one SQLite
`BotLedger` class. The pilot uses the single named object
`hosted-synthetic-pilot` with its own agent and evidence signing domain.
Mars previously read back isolated Worker version
`9bb8cd05-c44d-4b39-90be-bece54f83ee6`. Verify the active disabled version
before using it as the rollback reference.

## Invocation

Cloudflare provisions DNS and TLS for a Custom Domain and sends all paths on
that hostname to the Worker. Read back the edge rules that apply to the
hostname before deployment. The documented pre-Worker rule matches the
exact hostname, approved driver egress IP, and HTTPS POST to
`/v1/pilot/telegram`, `/v1/pilot/agent` and `/v1/pilot/fixture` with no query.
Its rate target is 16 per source IP per ten seconds with a ten-second block.
Free WAF rate limiting counts per IP and per data center and enforcement
can lag.
[Custom Domains](https://developers.cloudflare.com/workers/configuration/routing/custom-domains/)
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
still reach the DO. The DO caps admissions per UTC day at 9,800 for ingress
and 10,000 including alarms. The singleton update, task, attempt and receipt
bound the pilot's data rows.
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
Stage only one mode at a time in the local build output directory. Before each
build, check `df -B1 "$PWD" /tmp` and stop below 16,106,127,360 bytes available
on either filesystem or on `ENOSPC`.

```sh
mise x -- npm --prefix crates/coconutclaw-gateway run stage:pilot
mise x -- node --test crates/coconutclaw-gateway/pilot.test.mjs
mise x -- npm --prefix crates/coconutclaw-gateway run check:cf:pilot
mise x -- npm --prefix crates/coconutclaw-gateway run stage
mise x -- npm --prefix crates/coconutclaw-gateway run check:cf
GATEWAY_LOCAL_EXECUTOR=/path/to/gateway-local-executor \
  mise x -- node --test crates/coconutclaw-gateway/{config,integration}.test.mjs
```

The config tests check the generated `.cloudflare/output/v0/` metadata and
default, other and pilot modes with and without preview. The pilot suite
includes the CLI over loopback with certificate verification and a
second-claim rejection after lease expiration and restart. The pinned
official `cf` `1.0.0-beta.5` accepted both prebuilt modes and reported the
16-per-ten-second limiter only in pilot mode. The manifests verify hostname
and secret declarations. Hosted DNS, TLS, bindings and receipt require readback
on Mars.

The ledger budget test caught a `cargo-mutants` 27.1.0 variant that bypassed
persistent request accounting. Earlier variants also failed their matching
tests for a no-op synthetic delivery and a second claim after lease expiration.

## Mars deployment

1. Confirm the accepted commit and the target account and active `liu.nz` zone.
   Check DNS and Worker route patterns for conflicts on `bigfur.liu.nz`.
   Confirm the isolated Worker has an active disabled version and a single
   SQLite `BotLedger` namespace with no objects. Record that version ID and
   namespace ID. Read back the hostname's edge controls.

   From the accepted checkout, use the Mars Node version and the gateway's
   locked dependencies. Install missing dependencies with `npm ci
   --ignore-scripts` under the same Node version. Set `CLOUDFLARE_ACCOUNT_ID`
   and `CLOUDFLARE_ZONE_ID` to the verified IDs, `DISABLED_VERSION_ID` to the
   active version, and `PILOT_NAMESPACE_ID` to the verified namespace.

   ```bash
   cd crates/coconutclaw-gateway
   pilot_node() { mise x node@26.10.0 -- "$@"; }
   pilot_cf() { CF_SEND_TELEMETRY=false pilot_node ./node_modules/.bin/cf "$@"; }
   : "${CLOUDFLARE_ACCOUNT_ID:?}" "${CLOUDFLARE_ZONE_ID:?}"
   : "${DISABLED_VERSION_ID:?}" "${PILOT_NAMESPACE_ID:?}"
   export CLOUDFLARE_ACCOUNT_ID CLOUDFLARE_ZONE_ID
   worker=coconutclaw-phase1-test
   pilot_cf --version
   pilot_cf auth whoami
   pilot_cf workers deployments list --worker "$worker"
   pilot_cf workers versions get "$DISABLED_VERSION_ID" --worker-id "$worker"
   pilot_cf durable-objects namespaces list
   pilot_cf durable-objects namespaces objects list --id "$PILOT_NAMESPACE_ID"
   ```

2. Stage serially using the existing cache and the Rust and Wasm tooling
   from local acceptance. Abort below 16,106,127,360 bytes available on the
   checkout or `/tmp`, or on `ENOSPC`. Run the pilot tests and dry run.

   ```bash
   (set -o pipefail
    df -B1 . /tmp | awk '1; NR > 1 && $4 < 16106127360 { low = 1 } END { exit low }' &&
      CARGO_BUILD_JOBS=1 pilot_node npm run stage:pilot) &&
     pilot_node node --test pilot.test.mjs &&
     pilot_node npm run check:cf:pilot &&
     jq . .cloudflare/output/v0/config.json &&
     jq '{name, domains, workersDev, previewUrls, env, exports, manifest}' \
       .cloudflare/output/v0/workers/default/worker.config.json
   ```

3. In a private shell with tracing off, create the disposable credentials
   immediately before deploy. Go to cleanup on any failure after deployment
   starts. Run the driver at most once and treat ambiguity as terminal.

   ```bash
   set +x
   umask 077
   pilot_dir=$(mktemp -d /tmp/coconutclaw-bigfur.XXXXXXXX)
   pilot_node node --input-type=module - "$pilot_dir" <<'JS'
   import { generateKeyPairSync, randomBytes } from "node:crypto";
   import { writeFileSync } from "node:fs";
   import { join } from "node:path";
   const directory = process.argv[2];
   const { privateKey, publicKey } = generateKeyPairSync("ed25519");
   const start = Date.now();
   writeFileSync(join(directory, "pilot.pem"),
       privateKey.export({ type: "pkcs8", format: "pem" }), { flag: "wx", mode: 0o600 });
   writeFileSync(join(directory, "secrets.json"), JSON.stringify({
       PILOT_TOKEN: randomBytes(32).toString("hex"),
       PILOT_WINDOW: `${start}:${start + 600_000}`,
       PILOT_ALPHA_KEY: publicKey.export({ type: "spki", format: "der" }).subarray(-32).toString("hex"),
   }), { flag: "wx", mode: 0o600 });
   JS
   pilot_cf deploy --prebuilt --mode phase2-pilot \
     --zone "$CLOUDFLARE_ZONE_ID" --secrets-file "$pilot_dir/secrets.json"
   pilot_cf workers deployments list --worker "$worker"
   ```

   Record `PILOT_VERSION_ID` from the deployment at 100% traffic. Read back
   `PHASE=phase2-pilot`, the `BOTS` binding, rate limiter and the three
   secret names. Under **Workers & Pages → coconutclaw-phase1-test → Settings →
   Domains & Routes**, verify the sole Custom Domain is `bigfur.liu.nz`,
   `workers.dev` and preview URLs are off, and TLS is ready within the window.
   [cf project commands](https://developers.cloudflare.com/cf/projects/)

   ```bash
   pilot_cf workers versions get "$PILOT_VERSION_ID" --worker-id "$worker"
   pilot_cf workers secrets list --worker "$worker"
   COCONUTCLAW_PILOT_TOKEN="$(jq -er .PILOT_TOKEN "$pilot_dir/secrets.json")" \
     pilot_node node pilot-driver.mjs https://bigfur.liu.nz "$pilot_dir/pilot.pem"
   pilot_cf durable-objects namespaces objects list --id "$PILOT_NAMESPACE_ID"
   ```

   Expect one execution within 16 requests and one `delivered` receipt
   with `sends=1`, chat `321`, topic `37` and `pilot: SYNTHETIC PILOT`.
   The driver checks update deduplication and signed result replay.
   Confirm exactly one object with stored data in the isolated namespace.

## Mars cleanup

1. Remove the `bigfur.liu.nz` Custom Domain under **Settings → Domains &
   Routes** and confirm detachment before removing the pilot-specific edge
   rules. Apply this step after success or failure. If detachment fails,
   proceed with the disabled deployment and keep the edge rules in place.
   [List domains](https://developers.cloudflare.com/api/resources/workers/subresources/domains/methods/list/) ·
   [Delete domain](https://developers.cloudflare.com/api/resources/workers/subresources/domains/methods/delete/)

2. Stage the disabled manifest from the existing generated modules. If they
   are unavailable, recheck the disk gate and use `npm run stage`. Confirm
   `PHASE=disabled`, only `BOTS` and `PHASE` bindings, and no domain, routes
   or triggers before deployment.

   ```bash
   pilot_node node stage.mjs
   pilot_node npm run check:cf
   jq . .cloudflare/output/v0/workers/default/worker.config.json
   pilot_cf deploy --prebuilt --zone "$CLOUDFLARE_ZONE_ID"
   pilot_cf workers deployments list --worker "$worker"
   ```

   Set `CLOSED_VERSION_ID` from the disabled version at 100% traffic.
   Recheck the detached domain and both disabled URL settings.

   ```bash
   pilot_cf workers versions get "$CLOSED_VERSION_ID" --worker-id "$worker"
   pilot_cf workers secrets list --worker "$worker"
   ```

   For any retained pilot secret, run
   `pilot_cf workers secrets delete NAME --worker "$worker"` with the
   reported name and verify the active disabled version and secret list
   again. If a pilot version was uploaded, verify its exact ID and that
   it is inactive, then retire it with
   `pilot_cf workers versions delete "$PILOT_VERSION_ID" --worker-id "$worker"`.
   If the scratch directory was created, remove the local credentials with
   `rm -f -- "$pilot_dir/secrets.json" "$pilot_dir/pilot.pem"` and
   `rmdir -- "$pilot_dir"`.

3. Disabling the Worker retains the retired SQLite object. Permanent removal
   requires deletion of the isolated Worker and its namespace because this
   pilot has no object reset endpoint. With approval for that deletion and
   confirmation that the Worker and namespace contain only disposable data
   and have no external references, run `pilot_cf workers delete "$worker"`.
   Verify the Worker, namespace and domain mapping are absent.
   [Durable Object storage cleanup](https://developers.cloudflare.com/durable-objects/best-practices/access-durable-objects-storage/#remove-a-durable-object)

## Remaining gates

- Mars verifies the account, zone, hostname mapping and edge rules, runs the
  hosted driver once, and verifies cleanup.
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
