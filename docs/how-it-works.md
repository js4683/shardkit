# How shardkit works

Two copies of your controller run side by side — `stable` (current
revision) and `canary` (candidate) — and a `ShardPlan` custom
resource says which namespaces each copy reconciles. Shardkit moves
namespaces between the copies through a handshake both sides
acknowledge, and anything uncertain denies work instead of
guessing.

For component and handoff diagrams with links to the implementation, see
[Architecture](architecture.md).

## The pieces

- **Plan (spec).** Mode (`Active`/`Off`), canary weight in per
  mille, epoch, seed, track revisions. Writers (CLI, Argo plugin)
  own it; every change bumps the epoch. Full contract:
  [shardplan-spec](shardplan-spec.md).
- **Gate (read path).** Answers "may this track touch this
  namespace?" from the adopted plan version: ownership function,
  revision identity, singleton duty, drain state. Missing,
  malformed, tampered, or superseded inputs close the gate —
  writes stop instead of splitting the fleet.
- **Guarded client (write path).** Wraps the controller-runtime
  client and re-checks the gate on every mutating call, plus
  deletion budgets with confirmed deletes.
- **Observer (handshake).** Watches the plan. On a new version it
  drains what the track lost, publishes a release ack, then
  acquires what it gained behind the loser's ack plus a freshness
  barrier over recorded write versions. Acks land in plan status
  for the CLI and Argo to read.

## One handoff, end to end

1. Operator runs `kubectl shardplan set-weight PLAN 250 --mode Active`: plan goes
   `Active`/250 at epoch N+1.
2. Both observers see the new version. Stable drains the
   namespaces it lost and acks `Released`; canary waits for that
   ack, checks the barrier, acks `Acquired`, and starts
   reconciling its new namespaces.
3. `VerifyWeight` (or the CLI) reports verified only when the
   canary ack binds the live epoch, generation, and revision.
4. Abort is the same path in reverse: epoch N+2 at `Off`, stable
   reclaims everything, canary releases.

If the canary binary runs a stale revision, its acks do not bind
and verification never passes — the rollout stalls instead of
advancing blind. If the plan is edited outside the writer
contract (same-epoch change, frozen-field edit without a fresh
rollout), both tracks refuse it and hold their last state until a
contracted version arrives.

## Guarantees and limits

[safety-model](safety-model.md) states each invariant with its
proof obligation and residual window; the headline is a
*cooperative* guarantee (no two tracks consider themselves
authorized at once), not distributed fencing. Sharding is per
cluster over namespace names; multi-cluster orchestration and
non-namespace keys are out of scope.

## Relabel recovery

Relabeling a namespace across the exclude boundary mid-version
denies writes on both tracks (`LabelsDrifted`, or `NotAcquired`
when a track never evaluated it) instead of flipping ownership
without a handshake. Recover with one line — a lone epoch bump
with the identical spec, which carries the current labels
through drain, ack, and barrier:

```sh
kubectl shardplan bump-epoch widget-operator -n widget-system
kubectl shardplan status widget-operator -n widget-system  # watch it converge
```

Next: [Adopter onboarding](onboarding.md) drives all of the above
hands-on against kind.
