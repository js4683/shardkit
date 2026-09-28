# Decisions to resolve

D1/D2 are decided by the project author (2026-09-26). D3–D8 are open.
Proposed defaults are planning assumptions, not approvals. The implementing
engineer prepares evidence for D3–D8 and seeks domain review before acceptance.

| ID | Decision | Proposed direction | Required evidence / blocking milestone |
|---|---|---|---|
| D1 | Employer/IP clearance and license | MIT on GitHub, no employer clearance needed (author, 2026-09-26) | MIT license text before release |
| D2 | Name, public owner and module path | Owner js4683; module github.com/js4683/shardkit (author, 2026-09-26) | Name free under account (404 on 2026-09-26); module path agreed, unblocks M0-04 |
| D3 | Exclusivity, fencing and crash takeover | Fail closed on unproven release; specify cooperative guarantee | Delayed-write and partition model plus external review; blocks M1 safety claim |
| D4 | Cache freshness after handoff | Per-resource supported barrier or direct-read strategy | Demonstration with pinned API/cache stack, deletes/recreates and missed watches; blocks M1 |
| D5 | Namespace identity, hash encoding, seed lifecycle and cohorts | 1,000 fixed buckets per rollout; exclude wins | Golden vectors and monotonic properties; blocks partition API |
| D6 | Legacy lease and revision/promotion identity | Explicit staged migration; immutable revision identity | Old/new coexistence and promotion recovery tests; blocks M1 integration |
| D7 | Supported toolchain and plugin protocol | CR v0.24.1 + k8s 1.36.4 + kindest/node v1.36.4 (M0-06 bring-up proves executable compat); Argo SDK `github.com/argoproj/argo-rollouts` v1.10.0 proposed ([contract](argo-plugin.md): SDK compiles against k8s 0.36.4, method bindings fixed) | Accepted 2026-09-28: M2 green on these pins (kind good-path + chaos-path); external review waived by author, pins stand |
| D8 | Shadow semantics and persistent budget accounting | No persistent writes; fail-closed concurrent budgets | Admission/external-effects experiment and restart tests; blocks M3 |

Record a dated ADR when a decision is accepted: context, selected approach,
alternatives, consequences, approving reviewer, and evidence. Do not mark a
decision accepted solely because a document proposes a default.

## Options under review (M0-02, 2026-09-26)

Still open; evidence from the [safety-model review](safety-model.md). These
options feed the M0-08 review packet, which must confirm or replace them.

### D3 — exclusivity, fencing, crash takeover

- **Option A — cooperative guard only (proposed for M1).** Gate at
  Reconcile entry plus a guarded write client plus drain/ack handoff.
  Upstream gives no dequeue hook (worker calls `Get` then the handler
  directly; predicates filter pre-enqueue only), so the gate lives in
  the Reconciler wrapper. Guarantee: no two tracks both consider
  themselves authorized for a namespace at once, plus a measured
  residual window for in-flight and delayed requests.
- **Option B — A plus admission enforcement (experiment).** A
  ValidatingAdmissionPolicy keyed to per-track ServiceAccounts rejects
  *new* writes after an ownership flip. Open mechanism questions: how
  the server evaluates ownership (hash windows are not expressible in
  CEL; likely needs a webhook or coarse labels), whether it reads
  ownership authoritatively, and confirmation that it cannot fence
  already-admitted requests.
- **Option C — server-side fencing token (rejected for M1).** No native
  Kubernetes primitive exists; would require aggregation or API
  changes. Out of scope.
- **Crash takeover:** fail closed (no automatic takeover) until a valid
  release proof exists. Lease-expiry takeover stays unresolved pending
  a delayed-write experiment (SIGKILL versus pause, kubelet grace,
  TCP timeouts). A timeout must never become a successful
  acknowledgement.

### D4 — cache freshness after handoff

- **Option A — per-GVK server-side list barrier (proposed for M1).**
  The releasing track records its maximum written resourceVersion per
  GVK in its release ack. The acquirer polls
  `LIST(resourceVersion=<rv>&resourceVersionMatch=NotOlderThan)` per
  GVK until the returned collection version is at or past the recorded
  version (compared with
  `resourceversion.CompareResourceVersion`), then acquires. Grounded:
  same-type ordering is a documented API guarantee and a 1.35+
  conformance requirement; a list at collection version R reflects all
  changes at or before R, including deletions.
- **Option B — client-side informer version (fallback).** Poll
  `LastSyncResourceVersion()` per watched GVK until it passes the
  recorded version. Caveats: documented as neither synchronized with
  store access nor thread-safe; controller-runtime exposure at the
  pinned version must be confirmed in M0-04.
- **Option C — direct reads (narrow cases).** `GET` with an explicit
  version gives not-older-than semantics for known objects (already
  required for confirmed-delete); it cannot cover unknown object sets
  across gained namespaces.
- **Open questions:** barrier scope for extension API servers with
  non-numeric versions (restrict to kube-apiserver types plus a
  direct-read strategy for the rest); per-GVK list cost at 1k/10k
  namespaces (measure in M0-07); watch/`BOOKMARK`-based barrier as a
  later optimization.
