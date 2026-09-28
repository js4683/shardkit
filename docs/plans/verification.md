# Verification plan

## Current setup

`make check` checks local Markdown file links and whitespace. Manually review
scope, milestone dependencies and technical claims. Runtime suites are
`go test ./...` (unit, incl. `test/conformance/`) plus
`make test-envtest` (API-server tests with envtest 1.36.x); the
M3 matrix below binds them to I1–I8, the M4 section above to
scale/keys/admission/conformance.

## Required implementation evidence

| Layer | Scenarios | Acceptance / artifact |
|---|---|---|
| Pure model | Every weight, deterministic vectors, seed stability, cohorts, namespace recreation, abort/promotion | Disjoint desired ownership, coverage and monotone movement properties; persisted random seeds |
| Guard | All verbs/subresources, singleton work, stale epoch, cancelled context, bypass attempts | Assert API effects and denied writes, including operations begun before handoff |
| Multi-manager envtest | Concurrent status, delayed writes/cache, old pod ack, plan deletion/recreation, leader change | Audit includes operation intervals and session identity; no overlapping accepted writers under declared assumptions |
| Chaos | Kill mid-drain, API partition, process pause, slow ack, flapping plan, hung external request | Fail closed or recover according to protocol; retain traces showing actual behavior |
| kind / Argo | Shadow/cohort flow, weighted progression to 100%, failures, abort, promotion, plugin restart | Scripted demo in CI with explicit cluster context and analysis evidence; no-data/timeout holds |
| Budgets/shadow | Parallel deletes, restart, empty denominator, direct-read errors, all side effects | Caps enforced atomically; persisted state unchanged in shadow; emitted counts/diffs useful |
| Compatibility | Previous-version readers, finalizers, unknown fields, SSA conflicts, migration rollback | Old/new fixtures recover without losing fields or stranding resources |
| Scale | 1k/10k namespaces, same workloads with one/two caches | RSS, API QPS, handoff p50/p95/p99, queue recovery and failure traces with pinned environment |

The envtest audit must check overlap across epochs, not merely group writes by
epoch. Real API-server acceptance and stale requests matter more than a local
gate's decision log. Keep kind scenarios for semantics envtest cannot reproduce.

## Release gates

Bind results to a commit and pinned toolchain. Do not suppress failures to achieve
a milestone. M1 requires the safety model's assumptions to be explicit; M3 requires
chaos evidence for every invariant. M4 performance thresholds must be selected
from baseline measurements before evaluating success. v1 requires external
adoption and security review in addition to automated checks.

## M3 chaos evidence (invariant → test)

| Invariant | Chaos injected | Evidence |
|---|---|---|
| I1 one owner | Crash-both + drive-off abort finale; plugin Off↔Active call pairs | `TestRandomized_TwoManagerAudit` (ordered-acquire audit, 0 ungained writes; 22 seeds green: 11 historical + 2001–2011 on 2026-09-27); `TestPlugin_SteadyStateQuiesces`; kind oscillation trace `docs/traces/m2-argo-demo.md` |
| I2 versioned handoff | Superseded-revision writers; stale acks | `TestRevisionFence`, randomized I2a/I2b fencing invariants; kind canary fenced pre-`REVISION` sync |
| I3 bounded abort | Crash-both; error-rate trip; plugin loss (controller to 0) | Randomized abort finale → quiesce; kind analysis abort → `Off`/0 (`docs/traces/m2-argo-demo.md`); CLI abort recovery; scale-to-zero freeze diagnostic |
| I4 singleton | Foreign singleton writes | `TestGuardedClient_RealServer` singleton legs; `SingletonElsewhere` denials |
| I5 budgets | Mass delete under caps; 6-way concurrent chargers; confirm-read partition; 5-way concurrent reconciles on kind | Absolute/percent trip, window reset, idempotent no-op, foreign denial, read-failure requeue, empty denominator (unit); charge-then-trip + concurrent-chargers exactness (envtest); kind 21→16 trip with `used=3` + FA5.1 race fix (`docs/traces/m3-budgets.md`) |
| I6 shadow | Dry-run every verb incl. status/subresource; unsupported shapes | 6 fake tests (DryRun sent, denials pre-call, no evidence); `TestShadow_DryRunRealServer` (live objects unchanged) |
| I7 monotone | Frozen-tuple change under live rollout ID | V9 unit cases; `TestWebhook_Mapping` (webhook adds exactly V9 over CEL); S10 supersede in randomized audit |
| Budgets/shadow row | Parallel deletes, restart, empty denominator, direct-read errors | Covered under I5/I6 above |
| kind/Argo row | Abort, promotion, plugin restart, fully-promoted steady sync | `docs/traces/m2-argo-demo.md` (good + chaos paths, quiescence proofs) |

## M4 evidence (scale, keys, admission, conformance)

| Item | Evidence |
|---|---|
| 1k/10k benchmarks | `pkg/partition/scale_bench_test.go`, `pkg/shardkit/scale_bench_test.go`: Owner/Explain ~25 ns/ns; `Gate.Owned` 11.5 µs; `ValidateCreate` 273 ns; `ConfirmDelete` 1.33 ms; shadow 18.7 µs; fake List+assign 2.1 µs/ns |
| kind 1k measurement | 1000 labeled namespaces + `simulate -q` over 1029: raw list 0.20 s, simulate 0.52 s wall; all scale namespaces removed (`docs/traces/m4-scale.md`) |
| Multi-cluster keys | `ClusterName` on `AckOptions`/`AckCheck`/`ObserverOptions`: holders written `cluster/pod`, steady/acquire compare qualified, cross-cluster voids S8; `TestClusterQualifiedSession`, observer validation cases |
| Admission example | `examples/admission/` (manager + manifests + README): live kind run denied a V9 seed-tamper (`Forbidden`, seed unchanged) and admitted a no-op; resources removed afterwards |
| Conformance suite | `test/conformance/` (no envtest needed): I1 one-owner, I2 fence, I4 singleton, I5 budget exactness (6-way to exactly 2), I6 shadow purity, I7 monotone, M4 cluster keys, I8 skew tolerance, 1k sweep — 9/9 green |
| I8 version skew | `TestConformance_SkewTolerance`: unknown future fields tolerated, known session/budget fields survive round-trip; `ValidateClusterName` keeps qualification reversible. Live case 2026-09-27: controller image v0.2.2 (pre-budget types) dropped `spec.budget` on every write via typed decode+full update; v0.2.3 (current types) preserves it across UpdateHash/SetWeight (337→339 with caps intact). Rule: rebuild the baked plugin image on any `api/` change |
| Charge retry bound | Lockstep-wave starvation: 6 racers × cap 3 burned the 3-attempt `checkAndCharge` loop and leaked raw conflicts → 8 attempts + linear backoff (matches `PublishOwnEntry`), racer green 3× plus `-count=3` repeatability via unique namespaces |
| Live demo re-run | `hack/demo.sh` green on freshly rebuilt operator images embedding the M3/M4 lib (2026-09-27): rollout to weight 500 converged, abort to Off converged, epoch 303 in sync at rest with new leader sessions; unqualified session holders confirm the M1 shape is preserved when `ClusterName` is unset |
| Argo install script | `hack/install-argo.sh` captures the imperative M2 install (upstream v1.10.0, plugin build+load, ConfigMap, namespaced + probe-cluster RBAC, image pin, smoke); live-tested 2026-09-27 incl. the server-side-apply fix for upstream CRDs exceeding the last-applied annotation limit |

Residuals (not covered, carried forward): process-pause
straddling beyond the guard re-check unit paths; multi-cluster
keys assume cooperating clusters (a hostile cluster reusing a
name is out of scope — attested by deployment, not by the
library); external adopters and security review (v1 gates).
