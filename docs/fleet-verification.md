# Fleet verification, 19 November 2026

The verified fleet has one Linux coordinator and paired macOS and native Windows
runners. The coordinator alone owns Telegram ingress, task routing, memory and
delivery. Runners use their own Codex authentication and permitted workspaces.
This record separates live observations, hosted CI and bounded formal evidence;
none substitutes for the others.

For setup, use the [paired-node guide](production-nodes.md), including the
separate [native Windows checklist](windows-onboarding.md).

## Merged baseline: `18c460c`

[PR #24](https://github.com/lsj5031/CoconutClaw/pull/24) merged scoped memory
for assigned runners. [Hosted CI run 44455611317](https://github.com/lsj5031/CoconutClaw/actions/runs/44455611317)
passed on the exact merge commit: Ubuntu, macOS, Windows build, native Windows
tests and the required Windows rollup. This is not a claim that the earlier PR
check was green or that CI used live Telegram or authenticated Codex.

Independent bounded formal verification was accepted on `3a55ae9`, whose Git
tree is identical to `18c460c`. That evidence includes source mapping, 13 SANY
module checks and 50 TLC cases: 31 exhausted positive state spaces and 19
expected counterexamples. This is same-tree formal evidence, not a separate
formal run on the merge SHA. The finite models do not prove native transport,
process custody or the exact scoped-memory payload. The signed-runner loopback
fixture uses mock Codex and is separate from the live checks below. See the
[formal scope and limits](formal-verification.md).

### Live scoped memory

Both matching runners authenticated to the deployed coordinator. An authorized
save put a disposable fact in one Telegram topic. On `18c460c`:

1. Windows recalled the fact before a guarded coordinator restart.
2. After the restart, a fresh Mac task recalled it in the **same topic**.
3. A fresh task on the **same Mac** in an unrelated topic returned `UNKNOWN`.
   Its signed request omitted the saved fact.

The recall prompts did not include the fact; both authorized signed requests
did. All three tasks had signed start and completion evidence and recorded
Telegram delivery. The unrelated-topic task completed successfully: `UNKNOWN`
was its reply about recall, not an Unknown task outcome. This checks durable,
coordinator-owned [scoped memory](memory.md), not replication between runners.

## Earlier live fleet cases: `746c3b8`

These observations were recorded on 19 November 2026, before the scoped-memory
rollout. They were not rerun on `18c460c`.

| Case | Observed result |
| --- | --- |
| Immediate Mac task | Ordinary Telegram admission, one evidenced invocation, signed start and completion, and delivery to the initiating topic. |
| Scheduled Mac task | A one-off created through the local CLI admitted one occurrence and one evidenced invocation, completed with signed receipts and delivered to the configured topic. |
| Schedule due during coordinator downtime | A guarded stop/start admitted the due one-off once on Mac. Administrative reconciliation fetched the stored signed result without invoking Codex again or sending another copy of the already acknowledged Telegram reply. |
| Mac unavailable at dispatch | One occurrence was assigned, with no authenticated start or result. Telegram reported Unknown; there was no retry or local fallback. |
| Mac cancellation | Telegram `/cancel` produced a signed Cancelled result and recorded terminal delivery. This was the slash command, not an inline callback. |
| Native Windows completion and cancellation | Separate tasks completed and cancelled with signed receipts and recorded Telegram delivery. Cancellation used the inline **Cancel** button. This did not inspect real Windows-local process handles. |

## Open

- [Telegram](telegram-operations.md) has a sticky conversation/topic selection,
  not a destination override for one task. Recent assignments do not show who
  actually ran the task; task results have no signer-backed executor field.
- [Result reconciliation](production-nodes.md#state-and-recovery) can retrieve
  stored evidence, but there is no supported control to freshly resend an
  already acknowledged reply. The replay case did not test a fresh resend.
- Abrupt coordinator death during an in-flight task and Mac inline **Cancel**
  remain unverified live. The guarded restart and Windows inline case do not
  close those gaps.
- Durable artifact publication, retrieval, cross-run reuse and retention are
  separate work tracked in [issue #23](https://github.com/lsj5031/CoconutClaw/issues/23).
