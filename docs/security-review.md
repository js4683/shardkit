# Security review (self, 2026-09-28)

Author: automated self-review by the maintainer's coding agent,
per the standing request to do the project's own security pass
before any external review. Method: trust-boundary walk of the
tree at M4 (`pkg/`, `api/`, `plugins/`, `cmd/`, `examples/`,
`config/`, `test/`), secrets grep, RBAC inventory, fail-open
audit of the guard paths, dependency pin check. No penetration
testing, no fuzzing beyond the existing randomized audit.

## Assets and trust boundaries

- **ShardPlan specs**: the fleet's source of truth. Writers:
  Argo plugin (bound rollouts), CLI operators, anyone with
  `update` on `shardplans` in the plan namespace.
- **Workload objects** (widgets/customer CRs): guarded by the
  library gate, not by RBAC granularity.
- **Leases/sessions**: liveness + attribution, read from the API
  server (trusted).
- **No secrets in scope**: plans, leases, metrics, and logs carry
  rollout metadata only (revisions, epochs, holder pod names).
  Grep for password/key/token/bearer patterns across code,
  manifests, and scripts is clean.

## Findings

| ID | Severity | Finding | Evidence | Disposition |
|---|---|---|---|---|
| S1 | Medium → Fixed | V9 (frozen-tuple/epoch monotonicity) was unenforced server-side with the webhook off (default). A writer with ShardPlan update rights could land a non-monotone spec, and the gate adopted it (single-version validation cannot see transitions). | `docs/webhook.md` limits; CEL rules in `config/crd/` | Fixed 2026-09-28: read-time V9 — the gate retains the last adopted spec and refuses any same-UID version failing `ValidateTransition` (`api/v1alpha1/validation.go`), holding all baselines (`ReasonPlanMalformed`). Pinned by `TestOwned_SameEpochGenerationBump`, `TestGate_V9FrozenTamper`, the I7 conformance extension, and a live kind probe (tamper accepted by the API, refused by both operators, repair resumes). Webhook stays optional; RBAC guidance (trusted writers only) still applies. |
| S2 | Low → Fixed (leases) | Demo operator ClusterRole granted `delete` on all leases cluster-wide; widgets/events necessarily coarse (fleet spans namespaces; recorder emits in workload namespaces). | `examples/widget-operator/config/rbac/role.yaml` | Fixed 2026-09-28: leases moved to a `widget-system` Role+Binding (leader election + observer leases both live there); live-verified (`can-i delete leases`: yes in widget-system, no elsewhere) with the fleet in sync and zero RBAC errors. Widgets/events remain cluster-wide by necessity; operator-pod compromise is still full fleet compromise (standard). |
| S3 | Low → Mitigated | Metrics bind `:8080` on all interfaces, unauthenticated. Labels carry track/revision/decision only — no sensitive data. | `examples/widget-operator/main.go:40` | Mitigated with `examples/widget-operator/config/networkpolicy.yaml` (ingress to operator pods from `monitoring/prometheus-server` only; dry-run validated; kind has no policy engine so it ships unapplied for production). |
| S4 | Info | Plugin needs cluster-scoped read-only `shardplans` (`get/list/watch`) because `InitPlugin` probes with no namespace. | `config/argo/shardplan-rbac.yaml`, live 2026-09-27 outage | Minimal grant, documented; a controller compromise exposes rollout metadata only. Writes stay namespaced. |
| S5 | Info → Partial | No image signing (cosign) and no dependency-update automation. Pins are current and `dist/` ships verified SHA256SUMS. | `Makefile`, `go.mod` | Dependabot added 2026-09-28 (`.github/dependabot.yml`, weekly gomod + actions). Cosign still open: needs signing keys the agent cannot create — author action. |
| S6 | Info | Multi-cluster session keys assume cooperating clusters (a hostile cluster reusing a name is unattested). | `docs/plans/verification.md` residuals | Documented; out of scope for the library. |
| S7 | Medium → Fixed | Observer/gate split-brain on refused versions (found live while verifying S1): the observer handshook a tampered version the gate refused, advancing baselines and acks past ownership that never moved; later membership-steady versions converged the acks while 5 canary widgets stayed stranded under Off. The singleton path had the same asymmetry (no read-time V9, no baseline adoption). | kind probe 2026-09-28 (first live run); `pkg/shardkit/observer.go` transition vs `gate.go` adoption | Fixed 2026-09-28: the observer drives adoption through the gate (`gateAdopted` probe) and advances only gate-adopted versions — verdicts identical by construction, covering V9, external holds, and B3 lag; singleton path aligned via shared `adoptLocked`. Pinned by `TestObserver_SameEpochGenerationBump` (rewritten to the fail-closed contract), `TestObserver_SeedTamperHeld`, `TestGate_SingletonV9MatchesOwned`, `TestObserver_TamperLockstep_RealServer` (envtest), and the kind probe (tamper: ownership frozen incl. write counters, acks frozen at the pre-tamper generation, `advance_refused` counted; repair converges; abort moves ownership). |
| S8 | Medium → Fixed | Acquire-abandonment skipped version bookkeeping (pre-existing, found live): when the plan moved on mid-acquire, the next version's delta was computed against the pre-abandon held set, so namespaces that changed only in the skipped version were never enqueued — 4 widgets stranded canary-stamped under Off with both tracks acked. | kind probe 2026-09-28 (second live run); transition counters (`acquire_abandoned=1`, `release=2`) | Fixed 2026-09-28: abandon narrows `held` to the abandoned grant (release ran and published; gains stay deferred), so the next version acquires the union behind the loser's fresh release, whose monotone evidence still fences the abandoned writes. Pinned by `TestObserver_AbandonedAcquireNarrowsHeld` (deterministic two-track staging; verified to fail with the fix disabled) and the kind probe (`NO-STRAND PASS`, canary=0 under Off after rapid-fire versions). |

## Verified controls (no finding)

- **Fail-closed gate**: `Owned` returns a `ClosedError` plus
  zero-value `Owned:false` on every read/validation failure
  (`pkg/shardkit/gate.go:148-183`) — callers that check errors
  stop, callers that ignore errors still get "not owned".
- **Deny-by-default writes**: `GuardedClient` authorizes every
  verb (S7 revision fence, ownership, singletons, budgets) with
  a `DeniedError` taxonomy; shadow mode performs real dry-runs
  with zero persistent writes (conformance I6 + envtest proof).
- **Session binding**: acks bind holder identity + lease
  transitions read live at ack time (S8); cross-cluster and
  stale sessions void instead of trusting.
- **CLI trust**: standard kubeconfig chain with explicit
  `--kubeconfig`/`--context`, fails closed on load errors, no
  impersonation flags — it cannot exceed the user's own creds.
- **Supply chain**: go 1.27.1, k8s.io 0.37.1, controller-runtime
  0.25.1, Argo Rollouts v1.10.0, `alpine:3.21`, kindest node pin;
  committed `go.sum`; release SHA256SUMS verified.

## Residual risk

The accepted items above (S1–S3) plus the documented
verification residuals (pause-straddling, hostile-cluster names).
Re-review triggers: any `api/` change (rebuild the baked plugin
image — skew drops unknown fields on write, seen live), any
observer/gate handshake change (S7/S8 show the failure mode is
stranded ownership under converged acks — always re-probe a tamper
plus rapid versions on kind), new network listeners, new
credentials, or a second writer of ShardPlan spec.
