# M2 Argo demo trace (kind `shardkit-dev`)

One Rollout (`widget-system/widget-demo`, UID
`083af792-5307-42e0-942f-b182748eb063`) drives the
widget-operator ShardPlan through the `js4683/shardkit`
traffic-router plugin: good path 1→5→25→50→100% with four
Successful analyses and promotion, then a chaos path ending in
analysis-triggered abort and CLI recovery. Driver:
`examples/widget-operator/hack/argo-demo.sh good|chaos`.

Pins: Argo Rollouts v1.10.0, controller image
`shardkit-argo-rollouts:v0.2.2` (plugin baked in, `REVISION`
stamped), Prometheus `monitoring/prometheus-server` scraping both
track metrics Services.

Workload honesty: the Rollout's own pods are pause stubs — Argo's
progression vehicle only. Real canary membership is the plan's
namespace cohort (20 `demo-*` + `sandbox-a` = 21 widget
namespaces; partition scope counts 29 cluster namespaces).
Assertions below are on plan epochs (method-attributed via the
plugin write-audit log), track acks, AnalysisRuns, and Prometheus
values — never on stub pods.

## Attempt 1 (gen1): two failures, both kept

The first run bound the plan to a rollout whose template matched
stable. Argo therefore saw it **fully promoted from the first
sync**: steps never executed, no AnalysisRun was ever created
(`get analysisrun -A` empty), and every sync ran
`RemoveManagedRoutes` + bottom `SetWeight(0)`
(`rollout/trafficrouting.go`: the `IsFullyPromoted` branch runs
before the bottom weight sync). The old plugin wrote `Active`/0
for zero, so the plan flapped `Off`↔`Active` at ~1 epoch/5s —
each call defeated the other's no-op guard. Diagnosis: breaking
the UID binding froze epochs (plugin-mediated); the write-audit
log (added for this) showed alternating
`RemoveManagedRoutes`/`SetWeight(0)` pairs; Argo source confirmed
the call pattern.

The same attempt exposed a stale binary: spec
`tracks.stable.revision` became the stub hash, which no code path
in the current source can write (zero assignments repo-wide;
`UpdateHash` is canary-only). The baked v0.2.0 image predated the
narrowing. Stable operator `rev-a` was fenced until the revision
was restored (`rev-a`, epoch+1, documented as manual recovery —
the CLI has no track-revision writer; M3 candidate).

Fixes (all landed, all gated):
- `SetWeight(0)` normalizes to `Off`/0 (zero weight means no
  canary). Post-promotion syncs then agree: exactly one abort
  epoch, then no-ops forever. Pinned by
  `TestPlugin_SteadyStateQuiesces` plus a `SetWeight(0)` subtest.
- Plugin write-audit log (`updateWithRetry`: op, plan, epoch,
  mode, weight, both revisions, attempt, err) and a startup
  version line, so a stale bake is visible in the controller log.
- Demo script: timestamped template label (an unchanged template
  can never progress — fully promoted at creation), `assert_quiet`
  churn detector (≤1 epoch/45s, fails loud), stable-revision
  guard after every rotation.

## Good path (gen2, hash `6b8dd5bd48`)

Pre-rollout quiet proven: epoch 279→279 over 45s. Rotation epoch
280 rebound canary only (`stable still rev-a` gate passed);
canary operator `REVISION` synced, ack converged.

| epoch | plugin op | plan state |
|---|---|---|
| 280 | `UpdateHash(canary=6b8dd5bd48)` | Off/0, canary rev rotated |
| 281 | `SetWeight(1)` | Active/10, canary owns 0 |
| 282 | `SetWeight(5)` | Active/50, canary owns 0 |
| 283 | `SetWeight(25)` | Active/250, canary owns 9 |
| 284 | `SetWeight(50)` | Active/500, canary owns 15 |
| 285 | `SetWeight(100)` | Active/1000 |
| 286 | `RemoveManagedRoutes` | Off/0 (promotion cleanup) |

Four AnalysisRuns (`widget-demo-6b8dd5bd48-2-{1,3,5,7}`), one
per analysis step covering both templates, all Successful.
Rollout `Healthy`, `stableRS=6b8dd5bd48`. Post-promotion quiet:
286→286 over 45s — the oscillation is gone.

Prometheus after promotion: `shardkit_held_namespaces`
stable/`rev-a`=29, canary/`6b8dd5bd48`=0 (the canary series
already carries the new revision — the S7 bridge flowed into
metrics); canary error rate `0`; canary degraded `0`.

## Chaos path (gen3, hash `766dccc4b8`)

Rotation epoch 287 clean (stable still `rev-a`), operator
synced. Weights 10→50→250 progressed with Successful analyses.
`CHAOS=mass-delete` injected on canary at 250‰: canary deleted
widgets in its namespaces and errored on every reconcile (foreign
deletes denied by the guard, still counted as errors).
Analysis `widget-demo-766dccc4b8-3-7` Failed:
`Metric "canary-error-rate" assessed Failed due to failed (2) >
failureLimit` (successCondition `result[0] <= 0.01`). Argo
aborted (`Degraded`): `RemoveManagedRoutes` wrote epoch 292
`Off`/0; the bottom `SetWeight(0)` no-oped (no epoch 293 —
the quiescence rule working on the abort path).

Recovery: `CHAOS` cleared, 11 deleted widgets reseeded (census
21/21: `DEMO_N=20` + `sandbox-a`), CLI `abort` correctly a
no-op (`already mode Off weight 0`), plan re-converged:
epoch 292, `Off`/0, stable `rev-a`/29, canary
`766dccc4b8`/0, verdict `in sync`.

## Residual notes

- Promotion resets the plan to `Off` (stable reclaims); the 100%
  step itself was held and verified (`Active`/1000, epoch 285)
  before cleanup. Per-step weights above are the measured
  progression, not stub traffic.
- `VerifyWeight(0)` reports `NotVerified` under `Off` (requires
  `Active`); post-promotion/abort Argo only records this in
  status, nothing blocks on it.
- Raw captures (rollout describes, plan YAMLs, analysisruns,
  progress logs): `/tmp/shardkit-argo-good2/`,
  `/tmp/shardkit-argo-chaos/` (ephemeral; verdicts transcribed
  here per repo convention).

## Addendum 2026-09-27: good-path re-run on rebuilt operators

After the M3/M4 library changes (budgets retry, cluster keys),
both operator images were rebuilt from the current tree,
redeployed, and the good path re-driven (fresh `TRACE_DIR`
`/tmp/shardkit-argo-good3/`; the shared default dir also holds
older runs). Same contract observed: 1→5→25→50→100% with four
Successful AnalysisRuns, `Healthy` at step 9, promotion cleanup
to `Off`/0 (stable owns 29), post-promotion quiet 313→313.
One driver-environment note: the run started from an aborted
rollout, on which unpause-await-plugin-write wedges (Argo never
resumes aborted revisions that way); a fresh stub-gen revision
recovered it with no driver change. Controller image at the
time was v0.2.2.

## Addendum 2026-09-27: v0.2.3 + probe RBAC + skew proof

Two live findings followed. First, `spec.budget` vanished across
plugin-driven epochs: image v0.2.2 embeds pre-budget `api/`
types, so its decode-then-full-update drops the M3 field —
version skew with a live fault, now an I8 case. Fix: rebuilt
`shardkit-argo-rollouts:v0.2.3` from the current tree (rule:
rebuild the baked image on any `api/` change). Second, every
fresh controller pod failed `InitPlugin`: its reachability probe
lists ShardPlans with no namespace (cluster-scoped by
construction) but the install granted only a namespaced Role, so
no step ever advanced. Fix: read-only
`argo-rollouts-shardplans-read` ClusterRole in
`config/argo/shardplan-rbac.yaml` (writes stay namespaced).
Proof: v0.2.3 drove epoch 337→339 with `maxDeletions: 3`
intact.

Chaos path re-run on the same build (fresh `TRACE_DIR`
`/tmp/shardkit-argo-chaos3/`): template change rotated the canary
hash to `b4ff8ff5c`, `CHAOS=mass-delete` tripped the error-rate
analysis (`widget-demo-b4ff8ff5c-6-7 Failed`), abort returned the
plan to `Off`/0, recovery cleared CHAOS, reseeded widgets, and
the CLI abort confirmed no-op — final verdict in sync at epoch
319, stable owns 29, canary `Released` at 0. Cluster at rest.

## Addendum 2026-09-27: live budget trip on v0.2.3 (caps armed)

With `maxDeletions: 3` armed, the chaos path's `CHAOS=mass-delete`
drove the canary to exactly `used=3` in the chaos epoch (further
deletes denied under continuing pressure), the fresh error-rate
analysis failed, abort returned `Off`, and recovery reseeded 6
widgets destroyed across epochs — per-epoch reset is the designed
semantic (3/epoch), not a bypass — ending in sync with stable
owning 29. Caps disarmed afterwards (epoch bump, field removed);
rest state identical to pre-experiment. This re-proves the M3
headline on the current tree with the current retry code.

## Addendum 2026-09-27: plugin-loss recovery on rebuilt operators

Controller scaled to 0, then pure-CLI operation: `set-weight 250
--mode Active` → epoch 322 acked in sync; 60s freeze at 322
(sole-writer proof — and confirmation the plugin authored the
earlier 321); `abort` → `Off` 323 acked in sync; controller
restored to 1 with 60s resume-quiet at 323 (zero churn).
Incidental contract note: with the controller alive, it had
overwritten the CLI's Active/250 with `Off`/0 (epoch 321) —
correct single-owner behavior for a plan bound to a completed
rollout, and exactly why loss-recovery runs with the plugin
dead. Cluster at rest.
