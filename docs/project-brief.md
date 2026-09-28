# shardkit — Progressive Delivery for Kubernetes Controllers (OSS Plan)

> Working name `shardkit` (placeholder). Status: plan / pre-design.

## 1. Pitch and scope

**What it is:** progressive delivery for Kubernetes controllers. A new controller version first
owns a small, controlled slice of real objects (for example 1% of namespaces). It is observed,
then the slice widens or the rollout aborts in seconds. It targets leader-elected operators, where
replica-weighted canaries do not work because a single leader reconciles everything.

**What it is not:**

- A replacement for Argo Rollouts or Flagger for HTTP services.
- Scale-out sharding for throughput (timebertt/kubernetes-controller-sharding covers that).
- Fleet or multi-cluster promotion (Kargo, OCM, Argo CD cover that).
- A way to canary CRDs, RBAC, or webhook configurations. These are cluster-global; they are out of
  scope and documented as such (ship them backward-compatible, ahead of the code that uses them).

**Positioning versus prior art:**

| Project | Approach | How shardkit differs |
|---|---|---|
| KusionStack controller-mesh / openkruise controllermesh | Sidecar proxy injects label selectors; filtered caches; manual resharding with pod restarts; dormant since 2024 | In-process library, full caches, acknowledged handoff, weighted steps, automated analysis via Argo Rollouts |
| timebertt/kubernetes-controller-sharding | Consistent-hash scale-out across identical replicas; label-assigned; drain/ack handover | Weighted *version* canaries; no object relabeling; reuses the drain/ack idea |
| TiDB Operator canary, Istio revisions, Flux sharding | Manual "second instance takes a labeled subset" patterns | Generalized, automated, with safety invariants and tests |
| Argo Rollouts / Flagger | Replica- or traffic-weighted canaries for request-serving workloads | Adds a traffic-router plugin that routes *objects* instead of requests |
| KEP-5866 (server-side sharded list/watch) | API-server primitive (alpha) | Optional future backend to avoid doubled caches |

## 2. Architecture

```
 Argo Rollouts ──► traffic-router plugin ──► ShardPlan (CRD, spec)      ◄── kubectl shardplan (manual / fallback)
                        ▲ VerifyWeight              │ watched by
                        └──────── acks ◄── ShardPlan.status ◄─┐
   Rollout: stable RS (rev A) + canary RS (rev B), each pod embeds the shardkit library:
     lease per revision · gate · write guard · handoff · budgets · shadow · metrics{track,revision}
   Analysis: stock Argo Rollouts AnalysisTemplates (Prometheus by default, any provider)
```

| Component | Purpose |
|---|---|
| **Go library** (controller-runtime) | Core. Decides which track owns each object and enforces that ownership. |
| **ShardPlan CRD** | Orchestrator writes `spec`; each track acknowledges its epoch in `status`. Orchestrator-agnostic. |
| **Argo Rollouts traffic-router plugin** | Maps Rollout steps onto the ShardPlan. Deliberately thin. |
| **`kubectl shardplan` CLI** | `status`, `explain`, `simulate`, `set-weight`, `abort`. Works without Argo, so a plugin-API change degrades to manual steps rather than an outage. |
| **Webhook router helper** (later) | Maintains stable and canary copies of each webhook configuration, split per namespace via `matchConditions`. |

## 3. API sketch

```yaml
apiVersion: shardkit.dev/v1alpha1
kind: ShardPlan
metadata: {name: widget-operator, namespace: widget-system}
spec:
  key: namespace                  # namespace | label:<k> | cluster (multi-cluster managers)
  epoch: 7
  seed: "9a1f2e"                  # rotates the hash window per rollout
  tracks: {stable: {revision: 6d4b8c}, canary: {revision: 9a1f2e}}
  canary:
    mode: Active                  # Off | Shadow | Active
    weightPerMille: 10            # 1%
    include: {namespaces: [sandbox-a]}
    exclude: {selector: {matchLabels: {tier: critical}}}
  singletonOwner: stable
status:
  tracks:
  - {name: stable, observedEpoch: 7, released: true, ownedNamespaces: 412}
  - {name: canary, observedEpoch: 7, ownedNamespaces: 4}
```

Integration target: fewer than 50 lines in `main.go`.

```go
track := shardkit.TrackFromEnv() // pod-template-hash via downward API
mgr, _ := ctrl.NewManager(cfg, ctrl.Options{
    LeaderElection: true, LeaderElectionID: track.LeaseName("widget-operator"),
})
rt, _ := shardkit.Attach(mgr, track, shardkit.Options{
    Plan: client.ObjectKey{Namespace: podNS, Name: "widget-operator"},
    Key:  shardkit.ByNamespace, MigrateFromLease: "widget-operator", // legacy lease stays a mutex
})
c := rt.Client() // write guard + budgets + shadow (dry-run) mode
_ = ctrl.NewControllerManagedBy(mgr).
    For(&v1.Widget{}).Owns(&appsv1.Deployment{}).
    WithEventFilter(rt.Predicate()).
    WatchesRawSource(rt.HandoffSource()). // enqueue newly gained namespaces
    Complete(rt.Gate(&WidgetReconciler{Client: c}))
_ = mgr.Add(rt.SingletonOnly(gcLoop)) // cluster-scoped / periodic work
```

**Plugin mapping:**

| Rollout call | ShardPlan effect |
|---|---|
| `setWeight` | `canary.weightPerMille` |
| `setMirrorRoute` | Shadow mode |
| `setHeaderRoute` | Explicit cohorts (`include`) |
| `UpdateHash` | Track revisions |
| `VerifyWeight` | Returns true only after both tracks acknowledge the epoch |
| Abort | Weight 0; everything returns to stable |

Example Rollout:

```yaml
kind: Rollout
spec:
  strategy:
    canary:
      abortScaleDownDelaySeconds: 120   # >= handoff timeout + lease TTL
      trafficRouting:
        managedRoutes: [{name: shadow}, {name: cohort}]
        plugins:
          shardkit/controller-shards: {plan: widget-operator}
      steps:
      - setCanaryScale: {replicas: 2}
      - setMirrorRoute: {name: shadow, percentage: 100}
      - analysis: {templates: [{templateName: shadow-diff-budget}]}
      - setHeaderRoute: {name: cohort, match: [{headerName: namespace, headerValue: {regex: "^sandbox-.*"}}]}
      - analysis: {templates: [{templateName: evidence-soak}]}
      - setWeight: 1
      - analysis: {templates: [{templateName: evidence-soak}]}
      - setWeight: 5
      - analysis: {templates: [{templateName: evidence-soak}]}
      - setWeight: 25
      - setWeight: 50
      - pause: {}
```

## 4. Safety invariants

These are the product. Each one needs a test.

1. **Exactly one track owns a namespace at any time.** Enforced when work is dequeued and in the
   write client; optionally at admission via a ValidatingAdmissionPolicy keyed to each track's
   ServiceAccount.
2. **Two-phase handoff per epoch.**
   1. The releasing track drains in-flight work and acknowledges.
   2. The acquiring track waits for that acknowledgement, or for the releasing track's lease to
      expire plus a grace period.
   3. It waits until its cache has caught up to the old owner's last write (comparable
      resourceVersions).
   4. It enqueues everything in the gained namespaces at a bounded rate and normal priority.
3. **Bounded abort:** at most lease TTL plus drain timeout. The canary's scale-down delay must be at
   least that bound.
4. **Singleton and cluster-scoped work runs only on the singleton-owner track.**
5. **Destructive writes are budgeted** per track, per kind and verb, with absolute and percentage
   caps. Budgets fail closed. A delete justified by "the object is gone from the cache" must first
   be confirmed with a direct API read.
6. **Shadow mode never writes:** dry-run client, no-op event recorder, hooks for integrators to stub
   external clients. It reports write counts and diffs.
7. **Monotone movement within a rollout:** namespaces move only from stable to canary; back only on
   abort; promotion hands the canary everything.
8. **Version-skew contract**, documented and linted where possible:
   - The previous version must read what the new version writes.
   - No finalizers only the canary understands.
   - Patch or server-side apply, never full typed Update.
   - Readers ship before writers (expand/contract).

## 5. Analysis model

- **Hard invariants** (background analysis, fail fast): panics, new error classes, budget trips,
  objects failing to become Ready, ownership violations.
- **Evidence-based soak:** a step completes when the canary has handled at least N distinct object
  changes and M create/update/delete/finalize operations, spans at least one daily peak, and has a
  maximum duration after which it holds (never auto-promotes).
- **Comparative analysis:** canary vs a concurrently running control cohort, scoped by the
  `track`/`revision` metric labels. Count distinct failing objects, not raw error counts (retries
  correlate). Exclude warm-up after a leader starts.
- Ship example AnalysisTemplates for Prometheus built on controller-runtime metrics plus shardkit
  metrics; document other providers.

## 6. Roadmap

Effort in focused weeks for one engineer.

| Milestone | Scope | Exit criteria |
|---|---|---|
| **M0: Design** (2–3 wks) | Design doc, invariants, API draft, hash/bucket maths (FNV-1a-64 over namespace, rotating windows). Feedback from Argo Rollouts, controller-runtime, and sharding maintainers. | Design merged; name, license (Apache-2.0), and org decided |
| **M1: Library MVP, v0.1** (6–8) | Plan types, gate, per-track leases plus legacy-lease migration, write guard, epoch/ack handoff, metrics, CLI, sample operator | Two managers in envtest; invariant 1 holds under randomized handoffs |
| **M2: Argo plugin, v0.2** (4) | Traffic-router plugin, kind end-to-end demo with Prometheus analysis, auto-abort on an injected bug, published binaries with checksums | Demo of 1% → 100% and abort running in CI |
| **M3: Shadow and guardrails, v0.3** (4–6) | Shadow mode with diff metrics, budgets, freeze, confirmed-delete helper, webhook routing helper, chaos suite | Every invariant covered by a chaos test |
| **M4: Hardening, v0.4–0.5** | 10k-namespace benchmarks, cluster-keyed shards for multi-cluster managers, KEP-5866 backend behind a flag, admission-enforcement example, conformance suite for integrators | 2–3 external adopters |
| **v1.0** | Stable API, compatibility matrix (Kubernetes, controller-runtime, Argo Rollouts), security review | At least 3 production adopters |

## 7. Testing strategy

- **Property tests** for bucket maths: disjoint, complete coverage, monotone growth within a rollout.
- **Multi-manager envtest:** an audit sink records every write with its track; assert no namespace
  receives writes from two tracks within an epoch.
- **Chaos:** kill the canary mid-drain; partition the canary from the API server; slow
  acknowledgements; flap the plan.
- **End to end in kind:** sample "widget" operator + Argo Rollouts + Prometheus, including a
  deliberately bad version that mass-deletes objects and must trip the budget and abort.
- **Benchmarks:** memory and API-server cost of two full caches; handoff latency at 1k and 10k
  namespaces.
- **Later:** Sieve-style perturbation testing and SimKube trace replay.

## 8. Repository layout

```
api/v1alpha1/
pkg/shardkit/{plan,gate,guard,handoff,lease,shadow,metrics}/
cmd/kubectl-shardplan/
plugins/argo-rollouts/
charts/
config/crd/
examples/widget-operator/
test/{e2e,chaos,bench}/
docs/{design,safety-model,integration,ops}/
```

## 9. Community and governance

- Start as a monorepo. Later split the plugin into `rollouts-plugin-trafficrouter-shardkit` and
  offer it to argoproj-labs, where other traffic-router plugins live.
- Engage controller-runtime maintainers early about an upstream gate hook.
- Hygiene: Apache-2.0, DCO, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY.md, OWNERS. Recruit a second
  maintainer before v0.3.
- Visibility: a blog post with the M2 demo; a conference talk ("Canarying controllers when there's
  no traffic to split").
- Before writing code, check your employer's open-source policy (IP assignment, approval).

## 10. Risks

| Risk | Mitigation |
|---|---|
| Argo Rollouts plugin API is Alpha/Beta | Thin plugin, CLI fallback, help stabilize the interface upstream |
| Kubernetes has no fencing tokens | Write guard + lease-expiry fencing + optional admission enforcement; document the residual window |
| Two full caches cost memory and API load | Strip managedFields via cache transform; KEP-5866 backend later |
| Integration friction | Sub-50-line API; possibly a Kubebuilder scaffold plugin |
| Scope creep into a full delivery platform | Stay "routing + safety"; steps and analysis stay in Argo Rollouts |

## 11. First two weeks

1. Write the design doc and safety invariants.
2. Prototype the gate, per-track leases, and a manually edited ShardPlan on a sample operator in kind.
3. Measure handoff behaviour and memory overhead.
4. Send the draft to three or four external reviewers.

## Glossary

- **Track:** one controller version (stable or canary), identified by pod-template revision.
- **Shadow mode ("blast-radius zeroing"):** the canary runs against real objects but only dry-runs
  its writes, reporting what it would change.
- **Epoch:** a monotonically increasing ShardPlan version; handoffs are acknowledged per epoch.

## References

- [timebertt/kubernetes-controller-sharding](https://github.com/timebertt/kubernetes-controller-sharding)
- [KusionStack/controller-mesh](https://github.com/KusionStack/controller-mesh)
- [openkruise/controllermesh](https://github.com/openkruise/controllermesh)
- [Argo Rollouts traffic router plugins](https://argo-rollouts.readthedocs.io/en/stable/features/traffic-management/plugins/)
- [argoproj-labs Contour plugin (layout example)](https://github.com/argoproj-labs/rollouts-plugin-trafficrouter-contour)
- [TiDB Operator canary upgrade](https://docs.pingcap.com/tidb-in-kubernetes/stable/canary-upgrade-tidb-operator/)
- [Istio canary upgrades](https://istio.io/latest/docs/setup/upgrade/canary/)
- [Flux sharding](https://fluxcd.io/flux/installation/configuration/sharding/)
- [KEP-5866 Server-side sharded list/watch](https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/5866-server-side-sharded-list-and-watch/README.md)
