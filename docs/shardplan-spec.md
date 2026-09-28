# ShardPlan specification (M0-05)

Status: proposed, 2026-09-26. Defines the ShardPlan API, writer and
observer protocols, and handshake rules. The typed Go API, CRD, and
enforcement land in M1; this document is the contract they implement.
Ownership evaluation itself is specified in the
[partition specification](partition-spec.md).

Conventions: rules carry IDs for test traceability (`V` validation,
`W` writer protocol, `S` status/stale-ack, `B` bootstrap/recovery,
`T` transition). "Gate closes" means the track stops authorizing new
writes and reports a degraded condition; in-flight work drains under
its existing deadline.

## 1. Object identity and schema

Namespaced object, `shardkit.dev/v1alpha1`, kind `ShardPlan`. One plan
per operator deployment, in the operator's namespace.

```yaml
apiVersion: shardkit.dev/v1alpha1
kind: ShardPlan
metadata: {name: widget-operator, namespace: widget-system}
spec:
  key: namespace                  # M1: only "namespace"
  rollout: "r-001"                # opaque orchestrator-set rollout ID
  epoch: 7                        # writer-assigned, strictly increasing
  seed: "9a1f2e"                  # non-empty; frozen within a rollout
  tracks:
    stable: {revision: 6d4b8c}    # required
    canary: {revision: 9a1f2e}    # required unless mode is Off
  canary:
    mode: Active                  # Off | Shadow | Active
    weightPerMille: 10            # 0..1000; 0 when Off/Shadow
    include: {namespaces: [sandbox-a]}
    exclude: {selector: {matchLabels: {tier: critical}}}
  singletonOwner: stable          # stable | canary
status:
  tracks:
  - name: stable
    revision: 6d4b8c
    planUID: "a3f1…"              # UID of the plan this acks
    observedGeneration: 12
    observedEpoch: 7
    rollout: "r-001"
    phase: Released               # Draining | Released | Acquiring | Acquired
    released: true
    releasedWrites: {"deployments.apps": "1048576"}
    session: {holder: "widget-6d4b8c-x", leaseTransitions: 3}
    ownedNamespaces: 412
    conditions:
    - {type: Degraded, status: "False"}
  - name: canary
    revision: 9a1f2e
    planUID: "a3f1…"
    observedGeneration: 12
    observedEpoch: 7
    rollout: "r-001"
    phase: Acquired
    released: true
    ownedNamespaces: 4
    conditions:
    - {type: Degraded, status: "False"}
```

Field notes:

- `spec.key` is `namespace` in M1. `label:<k>` and `cluster` are
  reserved values, rejected until implemented.
- `spec.rollout` is opaque to the library (Argo rollout UID, CLI
  timestamp, …). It correlates audit trails and analysis; the
  definitional rollout for transition validation is the frozen tuple
  `(key, seed, include, exclude)`, compared structurally.
- `spec.tracks.*.revision` is the immutable deployment revision
  (pod-template-hash via downward API). Format: non-empty DNS-1123
  label.
- `spec.exclude.selector` supports `matchLabels` in M1;
  `matchExpressions` is rejected until implemented.
- `status.tracks[]` is keyed by `name` (`stable`, `canary`). Unknown
  names are ignored by readers.
- `releasedWrites` maps `resource.group` to the decimal
  resourceVersion of that track's last write of the type before
  release; absent entry means no writes of that type. M1 records
  every watched type; the D4 list barrier consumes this map.
- `session` copies the track lease's holder identity and transition
  count at ack time (lease naming is D6; the fields are fixed here).
  With M4 multi-cluster keys (`ClusterName` on the ack/observer
  options) the holder is written `cluster/pod` so shared readers
  of several clusters attribute sessions; empty keeps the M1 bare
  pod name. Cluster names must not contain `/`.

## 2. Spec validation (V)

Observed specs are untrusted input: the library validates every
version it acts on, and writers validate before writing. M1 should
additionally enforce V1–V8 as CRD CEL rules (epoch monotonicity via
`oldSelf`) so the server rejects bad writes at the boundary.

- V1 `key` is `namespace`; anything else is malformed.
- V2 `seed` is non-empty.
- V3 `mode` is one of `Off`, `Shadow`, `Active`.
- V4 `0 <= weightPerMille <= 1000`; `Off`/`Shadow` require 0.
- V5 `tracks.stable.revision` is a non-empty DNS-1123 label.
- V6 `Active`/`Shadow` require `tracks.canary.revision` as a
  non-empty DNS-1123 label. `Off` permits it (post-abort canary pods
  still exist) and ignores it for ownership.
- V7 `singletonOwner` is `stable` or `canary`. `canary` requires
  `tracks.canary.revision` present.
- V8 `exclude.selector` uses `matchLabels` only; `matchExpressions`
  present is malformed until implemented.
- V9 Frozen-tuple change (`key`, `seed`, `include`, `exclude`)
  without a `rollout` ID change is malformed: the orchestrator must
  mint a fresh rollout ID for a fresh rollout. (`rollout` change
  with an unchanged tuple is legal bookkeeping; ownership compares
  the tuple, not the ID.)
- V10 The observed `epoch` must exceed the track's last-acted epoch
  for the same plan UID, or the observation is stale (Section 6),
  not malformed. Stale reads are ignored; malformed content (V1–V9, V11)
  closes the gate.
- V11 Budget caps are ranges: `maxDeletions >= 0`,
  `0 <= maxDeletionPercent <= 100` when present. Absent caps are
  unbounded. Enforced by library validation and CRD range rules.

## 3. Epoch and writer protocol (W)

- W1 Writers (CLI, Argo plugin) own `spec`; the library never
  writes `spec`, only `status`. Writers own `status` never.
- W2 The writer sets `epoch` to observed epoch + 1 on every spec
  change, and writes with optimistic concurrency (resourceVersion
  precondition via read-modify-write or server-side apply).
- W3 On `409 Conflict`, the writer re-reads, re-checks that its
  intent still applies (W4), re-bases epoch, and retries with
  backoff up to a bounded attempt count. Persistent conflict
  surfaces as "another orchestrator is active" naming the plan;
  the writer never blindly overwrites.
- W4 Intent guard: before writing, the writer verifies the live
  spec still satisfies its precondition — `set-weight` requires
  mode `Active` with unchanged frozen tuple; `abort` requires not
  already `Off`; promotion requires the expected canary revision.
  Precondition failure aborts the command with the live state
  (CLI prints it; the plugin returns an RpcError), never a forced
  write.
- W5 Writers never decrease `epoch` and never reuse an epoch value
  within a plan UID. A plan recreated under the same name restarts
  at epoch 1 with a new UID (Section 7).

## 4. Status ownership and writer protocol (S1–S4)

- S1 Each track's leader owns exactly its own `status.tracks[]`
  entry, matched by `name`. A track creates its entry if absent
  and never mutates another track's entry.
- S2 Status writes use the status subresource with
  read-modify-write and conflict retries (bounded, with jitter).
  A track that cannot publish status within the M1 status timeout
  reports degraded locally and stops acquiring new work; it does
  not fabricate the other track's entry.
- S3 All binding fields (`planUID`, `observedGeneration`,
  `observedEpoch`, `rollout`, `revision`, `session`,
  `releasedWrites`) come from one observed spec version plus the
  live lease read at ack time. Parts assembled across versions are
  forbidden: an ack describes exactly one version.
- S4 Acks are idempotent: republishing the same binding and phase
  is a no-op. Retries reuse the same payload, never a recomputed
  one, so at-least-once delivery cannot double-apply.

## 5. Stale-ack rules (S5–S11)

An ack authorizes acquisition only if every rule passes. Failure
means "not released", never "released": the acquirer waits (bounded
by the M1 handoff timeout) and then reports degraded. A timeout
never becomes an acknowledgement.

- S5 UID binding: `entry.planUID` equals the live plan's UID, or
  the ack is void (plan recreated; Section 7).
- S6 Version binding: `observedGeneration`, `observedEpoch`, and
  `rollout` equal the transition being acquired, or the ack is
  void (not yet acked, or superseded).
- S7 Revision binding: `entry.revision` equals the spec's revision
  for that track name, or the ack is void (foreign pod from
  another rollout or a scaled-down ReplicaSet). The guarded
  client enforces the same binding on the write path
  (`ReasonRevisionMismatch`): a process whose gate revision
  differs from the spec revision for its track is superseded and
  may not write, even where the namespace still reads as owned —
  otherwise its stamps ping-pong against the current revision's
  (e.g. overlapping Argo ReplicaSets during an update). Ownership
  is checked first, so foreign writes keep their `NotOwned` /
  `SingletonElsewhere` reasons.
- S8 Session binding: the loser's live lease (direct API read, not
  cache) still shows `session.holder` with
  `session.leaseTransitions`, or the ack is void (acker lost
  leadership after acking). A missing lease voids the ack.
- S9 Phase binding: the loser's entry has `released: true` with
  `phase: Released` for the bound transition. `Draining` or a
  phase from another transition authorizes nothing.
- S10 Supersede: observing a newer valid epoch discards any
  in-progress handshake for the older transition and restarts the
  protocol; partial status from the older transition is ignored.
- S11 Partial status never authorizes: a missing loser entry, or
  an entry bound to another transition, is "not released".

## 6. Absence, deletion, recreation, bootstrap (B)

- B1 Attach requires the plan to exist and be valid (V1–V11).
  A missing or malformed plan fails attachment with an explicit
  error naming the plan and the blocking state. There is no
  implicit "run as stable without a plan". Any valid mode
  attaches: mid-rollout restarts are inevitable, and the
  observer's first-transition protocol (drain the complement of
  the grant, refresh evidence, ack, hold the grant) establishes
  safety without an `Off` round-trip.
- B2 Deletion after attachment closes every gate and reports
  degraded (`Reason: PlanDeleted`). Tracks keep their last
  evaluated ownership for reads but authorize no new writes.
- B3 Recreated plan (same name, new UID) is adopted only with
  mode `Off`: tracks reset baselines (epoch, ownership, handshake
  state) with no handoff needed, since stable owns everything. A
  recreated plan with `Shadow`/`Active` keeps gates closed and
  degraded (`Reason: PlanRecreated`) until the orchestrator
  drives the plan through `Off`.
- B4 Malformed content (V1–V9, V11) after attachment closes the gate
  (`Reason: PlanMalformed`) until a valid version is observed.
  Stale reads (V10) are ignored silently: transport staleness
  must not flap the gate.

## 7. Transition protocol (T)

One transition is one valid spec version (UID, generation, epoch).
The observer algorithm per track leader:

1. T1 Observe and validate (V1–V11). Invalid → B4. Stale → ignore.
2. T2 Compute desired ownership (partition spec) and the
   delta against currently held namespaces.
3. T3 If relinquishing: stop dequeuing relinquished work at
   Reconcile entry, cancel in-flight contexts, drain writes to
   completion or the drain timeout, then publish the release ack
   (`phase: Released`, `released: true`, S3 bindings including
   `releasedWrites` and `session`).
4. T4 If acquiring: wait for the loser's valid release ack
   (S5–S11), then establish the freshness barrier (D4: per-GVK
   list barrier over `releasedWrites`, or the M1-selected
   alternative), then set `phase: Acquired` and enqueue gained
   namespaces at a bounded rate.
5. T5 Supersede (S10) or timeout at any step returns to T1 or to
   degraded respectively; timed-out acquisition never writes.

Crash recovery falls out of the rules: release acks are persisted
before acquisition, acks are idempotent (S4), and a new leader
re-derives everything from the live plan, live status, and live
leases — never from local memory.

## 8. Singleton transfer handshake

`singletonOwner` changes follow the same pattern at loop scope:
the current owner finishes its in-flight iteration (bounded),
acks release of singleton work in its status entry, and the new
owner verifies (S5–S11, reading "ownership" as "singleton duty")
before scheduling. Default policy: transfer at promotion; explicit
mid-rollout transfer is allowed and follows the same handshake.
Singleton iterations must be idempotent across the transient
double-run window admitted by FA4.1.

## 9. Destructive budgets (M3, I5)

`spec.budget` caps destructive deletes per track per epoch:
`maxDeletions` (absolute) and `maxDeletionPercent` (of the
namespaces the track owns) apply concurrently; nil caps are
unbounded, so plans without budgets behave as before.

- Persistence: counters live in each track's own status entry
  (`budget: {epoch, deletionsUsed}`). S1 single-writer ownership
  needs no new writer: only the owning track charges its entry,
  with RV conflict retries (FA5.1).
- Scope: one track, one epoch. The counter keys on the spec
  epoch; any other epoch reads as zero and is overwritten on the
  next charge — new epochs reset with no reset write.
- Denominator: the track-owned namespace count from a direct
  list plus the pure ownership model (never cache). Zero or
  unknown denominators deny (FA5.2).
- `GuardedClient.ConfirmDelete` is the only budgeted delete path:
  authorize (ownership + S7 fence), direct-read confirm
  (`NotFound` is idempotent success; any other read failure is a
  plain error — requeue, never proceed, FA5.3), cap check, charge,
  then delete with a UID precondition so delete-versus-recreate
  races fail closed with Conflict (FA5.4). Over-cap denies with
  `ReasonBudgetExceeded`.
- Ack publishes preserve a charge they did not make (and charges
  preserve every other field), so the two read-modify-write loops
  never wipe each other; RV conflicts retry on both sides.

## 10. Open points for M1

- D6: per-track lease names, legacy-lease staged migration, and
  promotion lease continuity. This spec fixes the ack `session`
  fields; the lease objects they read are M1 work.
- Timeout values: drain, handoff-acquire, status-publish. M1
  tunes them; `abortScaleDownDelaySeconds` must cover the abort
  bound derived from them (I3).
- CRD CEL hardening for V1–V9 plus the V11 range rules (epoch via `oldSelf`) as
  defense in depth; library-side validation stays authoritative
  for reads.
- `releasedWrites` recording for every watched type, including
  types the track never writes (absent entry).
- `matchExpressions` and non-`namespace` keys stay rejected
  until specified and implemented.
