# External review packet (M0-08, draft — refreshed through M4)

> Author decision 2026-09-28: external review WAIVED — do not
> send. The packet below is retained as a design record. No
> external messages or issues were ever sent.

## What shardkit is

Progressive delivery for Kubernetes controllers: a new controller
version first owns a small slice of real objects (namespaces), is
observed, then widens or aborts in seconds. It targets leader-elected
operators, where replica-weighted canaries fail because one leader
reconciles everything. Routing and safety live in a controller-runtime
library plus a ShardPlan CRD; progression and analysis stay in Argo
Rollouts (traffic-router plugin) with a `kubectl` CLI fallback.

- Design: [design](design.md), [safety model](safety-model.md),
  [partition spec](partition-spec.md),
  [ShardPlan spec](shardplan-spec.md)
- Evidence:
  [M0-06 handoff trace](traces/m0-06-handoff-trace.md),
  [M0-07 handoff cost](traces/m0-07-handoff-cost.md),
  [M2 Argo demo](traces/m2-argo-demo.md) (good path + abort),
  [M3 budgets](traces/m3-budgets.md) (kind trip + race fix),
  [M4 scale](traces/m4-scale.md) (1k/10k measurements)
- Built since M0: library (`pkg/shardkit`, `pkg/partition`),
  Argo plugin (`plugins/argo-rollouts`, SDK v1.10.0),
  [CLI](cli.md), [webhook experiment](webhook.md) (optional),
  [admission example](../examples/admission/README.md),
  [conformance suite](../test/conformance/conformance_test.go),
  [KEP-5866 qualification](kep-5866.md)
- Decisions: [D1-D8](decisions.md) (D1/D2 decided: MIT, owner
  js4683, module `github.com/js4683/shardkit`; D3-D8 implemented
  but still open pending domain review before acceptance)

## Questions for reviewers

General (all reviewers):

1. The M1 safety claim is cooperative: no two tracks both consider
   themselves authorized for a namespace at once, plus a measured
   residual window for in-flight and delayed requests (FA1.1-FA1.4).
   Is that guarantee, stated that way, useful and shippable, or does
   it over-promise?
2. The M0-06 trace shows entry-gate checks alone do not fence
   in-flight work (5/5 delayed writes landed post-handoff without
   the guard; 0/5 with it). Is a guard re-check plus drain/ack
   handoff the right M1 mechanism, or would you require admission
   enforcement before calling any of this safe?

For controller-runtime maintainers:

3. There is no dequeue hook (worker calls `Get`, then the handler;
   predicates filter pre-enqueue only), so the M1 gate lives in a
   Reconciler wrapper with cancellation plus in-flight write
   accounting (built and tested). Is there appetite for an upstream
   gate/dequeue hook, or should we design permanently around the
   wrapper?
4. For handoff freshness we implemented per-GVK `LIST` barriers at
   the releaser's recorded resourceVersions (D4 option A). Does
   that match how you would gate acquisition, and what breaks for
   extension API servers with non-numeric versions?

For Argo Rollouts maintainers:

5. We map `setWeight` to weight-per-mille, `setMirrorRoute` to
   shadow, `setHeaderRoute` to explicit cohorts, `UpdateHash` to
   track revisions, and `VerifyWeight` to dual-track epoch
   acknowledgement. Does that mapping abuse any call's contract,
   especially `VerifyWeight` staleness and abort redelivery?
6. M2 pinned the plugin SDK (`argo-rollouts` v1.10.0) with the
   mapping above; the plugin inherits Rollouts-controller RBAC and
   keeps no persisted struct state. Post-hoc: does the mapping
   abuse any call's contract, and what would you change before we
   encourage external adoption?

For sharding prior-art maintainers:

7. We reuse the drain/ack handover idea from consistent-hash
   scale-out sharding but route by version weight instead of
   relabeling objects. What failure modes from operating sharded
   controllers (hot shards, flapping membership, split brain) should
   the weight model explicitly handle or document?

## Open decisions needing domain input

| ID | Question | Current proposal |
|---|---|---|
| D3 | Fencing and crash takeover | Cooperative guard; fail closed (no lease-expiry takeover) until proven |
| D4 | Cache freshness barrier | Per-GVK server-side list barrier over recorded versions |
| D5 | Hash identity and cohorts | Namespace name, FNV-1a-64, 1000 buckets, exclude wins |
| D6 | Legacy lease migration and promotion identity | Explicit staged migration; immutable revision identity |
| D7 | Toolchain and plugin protocol | controller-runtime v0.24.1 + k8s 1.36.4 proven; Argo SDK pinned v1.10.0 (M2 demo green); reviewer acceptance of the mapping still open |
| D8 | Shadow semantics and budget accounting | Implemented: shadow dry-runs every verb with no evidence writes; budgets with confirmed delete, fail-closed under concurrency (kind trip + race fixes); reviewer acceptance still open |

## Measured evidence (kind, pinned stack)

- Ownership: 21 namespaces across Off/cohort/widen/abort matched
  the spec exactly (0 mismatches, 4 phases).
- Delayed-write: guard suppressed 5/5 straddling writes;
  unguarded, all 5 landed after the new owner's write (worst 6s
  late), then were re-stamped within milliseconds.
- Cost at 21 namespaces: full-flip wave 70-135 ms (median 77),
  exactly 21 API writes per flip, second track ~12.8 MB RSS,
  zero restarts. Scale trends are M4 work, not claimed here.
- Failure found and fixed during M0: unconditional status writes
  plus self-watch formed a hot loop (>400k writes/widget); the fix
  (stamp only on change) is covered by unit tests and a 30s idle
  assertion in the trace.
- M2 Argo demo (kind): rollout 1→5→25→50→100% with 4 Successful
  AnalysisRuns, plus analysis-triggered abort back to Off/0;
  `SetWeight(0)` quiescence and plugin-loss CLI recovery proven.
- M3 budgets (kind): injected mass deletion tripped the cap
  (21→16 widgets, `used=3`); two race fixes landed (check-inside-RV
  loop; 8-attempt charge retry with backoff after lockstep-wave
  starvation leaked raw conflicts).
- M4 scale: partition assignment ~25 ns/namespace (10k sweep
  0.25 ms); 1000-namespace kind fleet lists in 0.20 s, full
  `simulate` in 0.52 s. Conformance suite 9/9 (I1/I2/I4–I8,
  cluster keys, skew tolerance). Admission webhook verified live
  (V9 deny + no-op allow) but stays optional.

## Status through M4

Roadmap M0 exit (accepted design, ownership/license decisions,
recorded proof assumptions, runnable experiment results): met in
tree, pending external review, which is itself an M0 exit criterion.
M1–M4 are built with the evidence above; the verification matrix
([verification](plans/verification.md)) binds every invariant to
its tests and traces. Remaining before v1: this review's feedback,
2–3 external adopters, and security review. Deviations from the
original brief to be aware of: MIT license per author decision
(brief said Apache-2.0), owner `js4683`, working name `shardkit`
still provisional.

## Proposed reviewers (author picks; no contact yet)

1. `timebertt` (kubernetes-controller-sharding author): drain/ack
   handover experience, failure modes of sharded controllers.
2. Argo Rollouts maintainers (CNCF Slack #argo-rollouts or a
   GitHub discussion): plugin mapping, `VerifyWeight`/abort
   semantics, SDK pin.
3. controller-runtime maintainers (kubernetes-sigs
   #controller-runtime): gate-hook appetite, freshness barrier,
   cache contracts.
4. KEP-5866 (server-side sharded list/watch) contacts via
   sig-api-machinery: we qualified the optional backend in
   [kep-5866](kep-5866.md) (implementable/alpha; coordination
   stays client-side; hash-mapping proof open) and deferred the
   flag to beta — is that the right call, and what would change
   the answer?

## Revised estimates

M0–M4 were built in focused sessions against allowances that
assumed reviewer latency would dominate; reviewer latency is now
the critical path, not the build. No further build estimates are
offered: the system exists with the evidence above. The remaining
cost is review incorporation (this packet), adopter support
(2–3 external adopters for v1), and security review. The two M0
adjustments held throughout: (a) explicit work was budgeted for
cache-lag windows between plan observation and action (D4
barrier, freshness evidence), and (b) a no-redundant-write
discipline with idle assertions followed the M0 hot loop.

## Outreach checklist

- [ ] Author approves reviewer list and packet contents
- [ ] Author approves MIT license text, DCO, OWNERS, conduct
      enforcement, private reporting channel before publication
- [ ] Packet sent (record where/when per reviewer)
- [ ] Feedback incorporated or explicitly deferred with reasons
