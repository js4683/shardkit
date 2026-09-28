# Design draft

Status: proposed, M0. This refines the [original brief](project-brief.md), whose
snippets describe desired interfaces rather than working APIs.

## Data flow and responsibilities

An orchestrator (initially the manual CLI, later the Argo plugin) writes ShardPlan
spec. Revision leaders observe it, stop newly disallowed work, drain outstanding
work, acknowledge release, and acquire permitted work. Status exposes evidence
for the orchestrator; it must never be an unconditional signal that writes are safe.

Keep routing and safety inside the library; leave progression, soak, and analysis
in Argo Rollouts. Each revision has a lease and a full cache. Dequeue gates and a
guarded client jointly enforce cooperative ownership. Cache filters and predicates
alone cannot stop queued work or a reconcile already in flight.

## Proposed partition contract

Namespace identity is the M1 key. Decide namespace name versus UID before golden
vectors; UID distinguishes delete/recreate but changes the original hash proposal.
Use a specified FNV-1a-64 input encoding and 1,000 buckets. A seed determines a
fixed rotating window for an entire rollout; increasing weight extends that window.
Hash collisions share a bucket and are acceptable. Percentage is an expectation
over namespaces, not a promise about object counts or reconcile load.

Precedence proposal: abort/Off assigns stable; exclusions override includes;
includes select canary in Active; otherwise the bucket decides. Shadow never
transfers ownership. Freeze seed, key, and cohort rules during progression, or
treat changes as a fresh transition with full handoff. Promotion is explicit and
must define the next rollout's stable revision without accidentally restarting its lease.

M0-03 resolves this sketch in the [partition specification](partition-spec.md):
namespace-name identity, FNV-1a-64 over UTF-8 bytes, seed-derived window
offset, the full precedence procedure, and normative golden vectors.

## API work to resolve

Retain the brief's namespaced ShardPlan and per-mille range 0–1000. Define plan
UID, rollout identity, immutable transition intent, epoch, and track/revision
identity separately. Validate revisions, mode/weight combinations, and monotonic
epochs. Missing, deleted, or malformed plans close the gate after attachment;
bootstrap into ordinary stable behavior must be explicit.

Status should acknowledge the exact plan UID, generation, epoch, and revision,
with conditions for draining, released, acquired, degraded, and timed out. Use
per-track entries and conflict retries so writers cannot overwrite each other's
acknowledgements. Stale status from an old pod or recreated plan cannot authorize acquisition.
Decide who increments epoch and use optimistic concurrency for all CLI/plugin writes.

M0-05 resolves this sketch in the [ShardPlan specification](shardplan-spec.md):
writers own spec and increment epoch under optimistic concurrency; per-track
status entries with exact (UID, generation, epoch, revision, session) binding;
void-if-stale ack rules; adopt-only-Off bootstrap and recreation recovery.

## Handoff proposal

1. Observe and validate a new transition; stop dequeuing work being relinquished.
2. Cancel/drain in-flight reconciles and writes; record completion before release.
3. Persist a release acknowledgement bound to the exact transition and leader session.
4. Acquire only after valid release and a defined cache freshness barrier.
5. Enqueue gained objects at bounded rate, normal priority, with retryable progress.

Crashes, timeouts, superseding epochs, and partial status writes need explicit
recovery states. Lease expiry alone is not a proven fencing barrier; keep automatic
takeover unresolved until D3. Do not numerically compare arbitrary resourceVersions
across objects to implement step 4; D4 must establish supported freshness semantics.

## Integration boundary

The <50-line main.go target is a usability acceptance test, not an implemented API.
Audit all writes: create, patch, update, delete, delete-collection, status,
subresources, raw REST calls, and external clients. Default unsupported write paths
to errors. Plan cooperative legacy-lease migration before two revisions run together.
Unmodified legacy controllers cannot be assumed to obey shard ownership.

Singleton ownership includes cluster-scoped resources and background loops.
Map child writes to the correct namespace; cross-namespace effects require an
explicit policy. Shadow requires a no-write transport boundary and stubbed external
clients; whether server dry-run is acceptable is a separate decision.

## Argo and operations

Verify the actual pinned traffic-router interface before implementing the mapping
from the brief. `VerifyWeight` must be false for stale or partial acknowledgements.
Document how scale-down, promotion, abort, repeated calls, and plugin restarts
interact. Manual CLI edits must detect conflicts with an active orchestrator.
Full promotion from 50% to 100% must be exercised even though the brief's sample
stops at a manual pause.

Analysis separates fail-fast invariants from evidence-based soak and comparative
control cohorts. Missing data, zero work, or maximum-duration expiry holds the
rollout; it must not silently promote. Bound metric cardinality; avoid namespace
and object labels on exported time series.

## Reference constraints

[client-go leader election](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection)
explicitly does not guarantee fencing. Thus a lease timeout is insufficient proof
of the brief's strict exclusivity claim. Consult the
[Kubernetes API contract](https://kubernetes.io/docs/reference/using-api/api-concepts/)
for cache/watch and resource-version behavior, and the
[Argo plugin documentation](https://argo-rollouts.readthedocs.io/en/stable/plugins/)
before pinning interfaces. Prior-art activity and KEP maturity from the brief are
research leads, not verified current facts.
