# Safety model and proof obligations

Status: requirements proposed in M0-02, implemented and evidenced through
M4 (2026-09-28). Preserve the original goals while distinguishing desired
ownership from permission to write. During draining, zero authorized
writers can be safer than overlapping writers; the exact-one-owner
wording needs an explicit availability interpretation.

Upstream-contract review (M0-02) recorded below on 2026-09-26. It adds failure
assumptions and decision options. Implementation evidence per invariant is
noted inline ("Implemented" / "Proven" markers); the remaining open
limitations are in the table's last column.

| ID | Required behavior | Evidence to implement | Open limitation |
|---|---|---|---|
| I1 | One desired owner; no overlapping authorized writers | Two-manager audit with operation start/end, revision, session, plan UID and epoch; stale-owner partition test | Cooperative guarantee only (FA1.1–FA1.4 stand): no two tracks consider themselves authorized at once, ordered by release-then-acquire. Residual window for in-flight/delayed requests; proven by the randomized audit, envtest handshake, kind probes, and S7/S8 (refused versions held, abandoned versions relinquish); mid-version relabels deny via the relabel pin (D10) instead of flipping unfenced |
| I2 | Release, acknowledge, establish freshness, then acquire and enqueue | Delayed reconcile/write/cache, duplicate status, crash at each state, missed-event recovery | Global numeric resourceVersion comparison is not a general barrier |
| I3 | Abort meets a declared time bound | Measure request-to-safe-stable latency across crashes and delayed writes | TTL + drain excludes API outages, cache delay and requeue; unconditional bound is unproven |
| I4 | Only singleton owner runs cluster and periodic work | Duplicate leaders, ownership transfer and blocked background loop tests | External work needs integrator cooperation |
| I5 | Destructive budgets fail closed; cache absence cannot justify deletion alone | Concurrent absolute/percentage caps, empty denominator, restart and API read failure; UID preconditions | Define budget persistence, denominator, scope and reset policy |
| I6 | Shadow persists no changes and emits no real events/side effects | Snapshot API state, intercept all mutating verbs and external/event clients, verify useful diff output | Dry-run can invoke admission; no-op clients can change reconcile behavior |
| I7 | Stable-to-canary growth until abort; complete promotion | Property tests through all weights, forced cohorts, excludes and namespace churn | Constrained by V9 since M3: the frozen tuple (key, seed, include, exclude) changes only with a fresh rollout ID, enforced at write (webhook/CLI/plugin) and at read (gate refuses, observer holds). Proven by `TestGate_V9FrozenTamper`, the tamper-lockstep envtest, and the kind tamper probe |
| I8 | Old readers survive new writes and finalizers | Old/new controller fixtures, round-trip unknown fields, patch/SSA conflicts and rollback | Cannot generally infer compatibility from static lint |

## Failure policy

On uncertainty, stop affected writes and report a condition with a reason. A timeout
must not become a successful acknowledgement. Retrying the same transition should
not double-charge a persisted budget or reacquire released work accidentally.
Define safe recovery for deleted plans, lost leases, clock pauses, and network
partitions before advertising automatic failover.

Admission enforcement is an experiment, not a proven cure for delayed requests.
Evaluate how the server knows current ownership and how in-flight requests are
fenced. Separate Kubernetes writes from arbitrary external effects that cannot
participate in the protocol. Publish the resulting assumptions with the guarantee.

These invariants are proven by the [verification plan](plans/verification.md)
gates: unit tests, envtest (including the randomized two-manager audit
and the tamper-lockstep suite), the conformance contract, and live kind
probes (tamper refusal, repair convergence, no-strand aborts). Residual
windows stay explicit in each row rather than advertised away.

## Failure assumptions (M0-02 upstream review, 2026-09-26)

Each invariant must hold under the assumptions below, or state the residual
window explicitly. Verbatim upstream quotes are in the evidence section.

### I1 — no overlapping authorized writers

- FA1.1: A process that lost its lease may still run and issue API writes.
  client-go leader election "does not guarantee that only one client is
  acting as a leader (a.k.a. fencing)". Lease expiry proves nothing about
  the old leader's activity.
- FA1.2: Requests accepted by the API server before an ownership change
  complete normally; the server knows nothing of shardkit ownership.
  Admission enforcement can only reject *new* requests after a flip, and
  only if it reads current ownership authoritatively rather than cached.
- FA1.3: Processes pause arbitrarily (GC, SIGSTOP, VM freeze, kubelet
  eviction delay). A paused old owner may resume after the new owner starts.
- FA1.4: A track partitioned from the API server keeps reconciling from a
  stale cache; its Kubernetes writes fail, but non-Kubernetes side effects
  may still occur outside the protocol.
- Consequence: the M1 guarantee is cooperative — no two tracks both
  *consider themselves* authorized for a namespace at once — plus a measured
  residual window for in-flight and delayed requests. See D3.

### I2 — release, acknowledge, freshness, acquire, enqueue

- FA2.1: Status writes may be lost, duplicated, delayed, or reordered.
  Acknowledgements must be idempotent and bound to plan UID, generation,
  epoch, revision, and leader session. Partial status (one track acked)
  never authorizes acquisition.
- FA2.2: The acquirer's cache lags the old owner's last write by an
  unbounded amount under partition or API outage. `WaitForCacheSync` and
  `HasSynced` cover initial sync only, never freshness relative to a later
  write.
- FA2.3: resourceVersion comparison is valid only within one API group and
  resource, for types served by kube-apiserver (required by Certified
  Kubernetes since 1.35). Cross-type numeric comparison is meaningless;
  extension API servers may use non-numeric versions (equality only).
- FA2.4: An informer delivers a per-object subsequence of authoritative
  states in order, with no ordering promise across different objects. In a
  list response, item versions record last-modification, not
  served-freshness; only the collection version anchors a snapshot.
- FA2.5: Watch history is bounded (etcd keeps about 5 minutes by default).
  A `410 Gone` forces a relist; `BOOKMARK` events mark progress up to a
  version.
- FA2.6: A crash may land between drain completion and ack persistence, or
  between ack persistence and actual release. Retries must be idempotent
  and must not double-charge budgets; a superseding epoch mid-handoff
  restarts the protocol rather than merging states.

### I3 — bounded abort

- FA3.1: Lease TTL plus drain timeout excludes API outages, cache lag,
  requeue backoff, hung external calls, and process pauses. Any published
  bound is conditional on API reachability and on reconciles honoring
  context cancellation within a declared deadline.
- FA3.2: How an Argo abort reaches a traffic-router plugin (re-driven
  `SetWeight(0)`, `RemoveManagedRoutes`, or nothing) is unverified; the
  abort path must be traced against the pinned Rollouts version in M2.

### I4 — singleton owner only

- FA4.1: Transient duplicate leaders (FA1.1) can double-run singleton loops
  and cluster-scoped writes during lease overlap. Singleton work must be
  idempotent or guarded by server-side compare-and-swap where the API
  allows it.
- FA4.2: An old singleton iteration may still be in flight when the new
  owner starts; same fencing gap as I1, same cooperative guarantee.
- FA4.3: Non-Kubernetes work cannot participate in the protocol; the
  integrator must stub or guard it (see I6).

### I5 — budgets fail closed

- FA5.1: Concurrent reconcilers race on budget counters, and local counters
  diverge across restarts and leader changes. Accounting needs atomic
  server-side state or a single-writer design with conflict retries.
- FA5.2: Percentage caps need a denominator observed from a possibly stale
  cache; an unknown or zero denominator denies the write.
- FA5.3: A failed direct API read during confirmed-delete denies the
  deletion (requeue, do not proceed from cache absence).
- FA5.4: Delete-versus-recreate races are guarded with UID preconditions,
  which the API supports on delete options.
- Implemented as `spec.budget` caps with per-track, per-epoch
  counters in each track's own status entry (M3, spec section 9):
  S1 single-writer ownership needs no new writer; epochs reset the
  window with no reset write; denominators come from a direct
  namespace list plus the pure ownership model. `ConfirmDelete`
  confirms (direct read), checks, charges, and deletes with a UID
  precondition; the widget chaos path uses it. Proven by fake
  tests (absolute/percentage trip, window reset, idempotent
  no-op, foreign denial) and envtest (charge then trip on the
  real server).

### I6 — shadow persists nothing

- FA6.1: `dryRun` requests still traverse admission webhooks and audit
  logging; third-party admission may observe (rarely, affect) them.
- FA6.2: No-op event recorders and stubbed external clients change
  reconcile control flow, so shadow diffs describe behavior "with stubbed
  externals" and must be labeled as such.
- FA6.3: Status writes, finalizer mutations, subresources, and raw REST
  escape hatches all route through the dry-run transport; unsupported
  write paths default to errors.
- Implemented as `GuardedClient.Shadowed(rec)` (M3): every mutating
  verb appends `DryRunAll` and records the attempt (verb, type,
  name, allowed/denied/error) without persisting; guard
  authorization runs first, so denials diff exactly; dry-run
  response RVs are never folded into fencing evidence and shadow
  attempts skip write metrics. Proven by fake tests (DryRun sent,
  denials recorded pre-call) and envtest (writes succeed yet live
  objects read back unchanged).

### I7 — monotone movement

- FA7.1: Seed, key, include, exclude, or cohort changes mid-rollout break
  monotonicity; freeze them or treat the change as a fresh transition
  with full handoff.
- FA7.2: Namespace churn during a rollout (create, delete, recreate)
  interacts with the name-versus-UID identity choice; see D5.
- FA7.3: Promotion must define the next rollout's stable revision and
  lease identity without restarting or orphaning the new stable lease.

### I8 — old readers survive new writes

- FA8.1: Compatibility cannot be inferred from static lint (unknown
  fields, defaulting, webhook behavior); old/new fixture round-trips
  are required.
- FA8.2: CRD and schema changes stay out of scope as cluster-global state;
  the contract requires backward-compatible schemas shipped ahead of the
  code that uses them.
- FA8.3: Versions sharing fields through server-side apply can conflict
  on field ownership; the patch/apply-only discipline needs a per-field
  ownership rule.

## Upstream evidence (M0-02, retrieved 2026-09-26)

Upstream `main` branches move; re-verify against pinned versions in M0-04.

- Leader election: "This implementation does not guarantee that only one
  client is acting as a leader (a.k.a. fencing)."
  ([client-go leaderelection](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection))
- resourceVersion: "orderable as monotonically increasing integers within
  the same resource type"; "Both resource versions must be from objects of
  the same API group and resource type"; since 1.35, orderability is a
  Certified Kubernetes conformance requirement; extension servers may be
  non-numeric. Comparison helper:
  `apimachinery/pkg/util/resourceversion#CompareResourceVersion`.
  ([API concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/))
- List barrier semantics: `resourceVersionMatch=NotOlderThan` guarantees
  the collection version is not older than requested, with no promise
  about item versions; per-item versions track last update, not
  served-freshness. (Same source.)
- Predicates: "filters events before enqueuing the keys" — already-queued
  keys are unaffected.
  ([controller-runtime predicate](https://github.com/kubernetes-sigs/controller-runtime/blob/main/pkg/predicate/predicate.go))
- Worker loop dequeues with `Queue.GetWithPriority()` and calls the
  reconcile handler directly; there is no dequeue hook. `Options` offers
  `ReconciliationTimeout` (per-Reconcile context deadline) and
  `RecoverPanic` (default true).
  ([internal controller](https://github.com/kubernetes-sigs/controller-runtime/blob/main/pkg/internal/controller/controller.go))
- Work queue interface is `Add/Len/Get/Done/ShutDown/ShutDownWithDrain/
  ShuttingDown`; no filter hook. A custom queue backend can reorder but
  dropping items at pop loses them (they already left the dirty set), so
  the gate belongs in the Reconciler wrapper, not the queue.
  ([client-go workqueue](https://github.com/kubernetes/client-go/blob/master/util/workqueue/queue.go))
- Informer contract: per-object states arrive as an ordered subsequence
  of authoritative states; "no promise about ordering between states seen
  for different objects". `LastSyncResourceVersion()` exists but "is not
  synchronized with access to the underlying store and is not
  thread-safe".
  ([client-go shared_informer](https://github.com/kubernetes/client-go/blob/master/tools/cache/shared_informer.go))
- Argo `TrafficRouterPlugin` interface: `InitPlugin`, `UpdateHash`,
  `SetWeight`, `SetHeaderRoute`, `SetMirrorRoute`, `VerifyWeight`,
  `RemoveManagedRoutes`, `Type`. Alpha since 1.5.0, alpha-quality.
  Plugin authors "should not depend on state being stored in the plugin
  struct as it will not be persisted between calls" (net/rpc); the
  plugin inherits the Rollouts controller ServiceAccount RBAC and must
  be written in Go.
  ([plugins](https://argo-rollouts.readthedocs.io/en/stable/plugins/),
  [traffic-router plugins](https://argo-rollouts.readthedocs.io/en/stable/features/traffic-management/plugins/))
