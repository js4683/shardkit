# M3 budgets trace (kind `shardkit-dev`)

V11 destructive budgets enforced live through
`GuardedClient.ConfirmDelete`: with `maxDeletions: 3` on the plan,
a `CHAOS=mass-delete` canary deleted exactly the budgeted objects,
then denied with `BudgetExceeded` — while the reconcile errors
kept tripping the alarm path. A first implementation of the
charge over-deleted under concurrency (5 deletes vs cap 3); the
fix and its regression test are below.

## Design (spec section 9, safety I5)

- Caps: `spec.budget.{maxDeletions, maxDeletionPercent}`,
  concurrent, per track per epoch; nil means unbounded.
- Persistence: counters in each track's own status entry
  (`budget: {epoch, deletionsUsed}`) — S1 single-writer
  ownership, no new writer, RV retries (FA5.1).
- Reset: the counter keys on the spec epoch; any other epoch
  reads as zero. No reset write.
- Denominator: track-owned namespace count from a direct list
  plus the pure ownership model; zero/unknown denies (FA5.2).
- `ConfirmDelete`: authorize → direct-read confirm (`NotFound`
  is idempotent success; other failures are plain errors for
  requeue, FA5.3) → cap check → charge → delete with UID
  precondition (FA5.4). Ack publishes preserve charges and vice
  versa. Widget chaos path uses it.

## The FA5.1 race (caught live, fixed, pinned)

First version checked caps once, then charged in a separate
retry loop from the stale count. Under `MaxConcurrentReconciles:
5`, five reconciles passed one check and all deleted: 5 objects
gone, `deletionsUsed: 3`. Fix: the check runs INSIDE the RV
retry loop (`checkAndCharge`) — every attempt re-reads, re-checks
against the live count, then writes, so RV conflicts serialize
racers and late ones deny. Regression:
`TestConfirmDelete_ConcurrentChargers` (envtest, 6 goroutines,
cap 3 → exactly 3 deleted, 3 `BudgetExceeded`, charge `{1 3}`).

## Kind run (manual, CLI-driven)

Pre: rest `Off`, 21 widgets, both tracks ack epoch 292.
Unbound from Argo first (`rollout: manual-budget-demo`): a bound
plan cannot be CLI-driven — Argo's fully-promoted steady sync
calls `RemoveManagedRoutes` every sync and reverts manual
`Active` weights within seconds (observed: set-weight 500
reverted to `Off` at epoch 295).

1. Patched `budget: {maxDeletions: 3}` (epoch 296). Note: the
   cluster CRD had to be re-applied first — the local regen
   added the fields but the server pruned them until then.
   Stale typed clients prune the same way (observed with a
   pre-budget CLI build).
2. `set-weight 500 Active` → epoch 297, canary owns 15, in sync.
3. `CHAOS=mass-delete` on canary. Result: 21 → 16 widgets;
   canary entry `budget: {epoch 297, deletionsUsed 3}`; canary
   log shows `write denied (BudgetExceeded): confirm-delete
   <ns>/w0: 3 deletions used, max 3 for epoch 297` repeating
   per reconcile — the alarm path stays loud while the blast
   radius stops at 3.
4. Recovery: `CHAOS` cleared, 5 widgets reseeded (21/21), CLI
   `abort` → `Off`, re-converged in sync (epoch 298), budget
   stanza removed (epoch 299). Final: `Off`, stable 29,
   canary 0, verdict in sync.

The 2-over-cap deletes from the pre-fix code were among the 5
reseeded; the post-fix image is what now runs on both tracks.

## Coverage

- Unit (fake): no-budget passthrough, absolute trip,
  percentage trip (50% of 2 owned), epoch-window reset,
  not-found idempotence, foreign denial, empty-denominator
  floor, publish-preserves-budget. V11 range/invalid cases.
- envtest (real server): charge-then-trip with UID-preconditioned
  delete; 6-way concurrent chargers exact.
- Widget: chaos path through `ConfirmDelete` (fake-tested);
  live images rebuilt with the fix and rolled to both tracks.
