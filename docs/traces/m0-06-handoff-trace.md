# M0-06 handoff trace (kind, 2026-09-27)

Status: passed. Two track revisions of the widget prototype ran in
kind, a scripted ShardPlan drove Off / Active / abort transitions,
ownership at each step matched the
[partition specification](../partition-spec.md) exactly, and the
delayed-write experiment produced both halves of the D3 evidence:
the guard suppresses straddling writes, and without the guard they
land after the new owner has written.

The durable record is this document plus the committed scripts and
unit tests. Raw artifacts (plan/namespace/widget snapshots, operator
logs, per-phase expect tables) live only in the ephemeral trace
directory `/tmp/shardkit-trace-20260927T070509Z/`; the verdict counts
below are transcribed from it.

## Environment

- Cluster `kind-shardkit-dev`, node image `kindest/node:v1.36.4`.
- `controller-runtime v0.24.1`, `k8s.io v0.36.4`, Go 1.27.1,
  kubectl 1.37.0 client. See [development](../development.md).
- Deployments `widget-stable` (image `shardkit-widget:rev-a`) and
  `widget-canary` (`shardkit-widget:rev-b`), one replica each, 5
  reconcile workers each, per-track leases
  `widget-operator-stable` / `widget-operator-canary`.
- 21 widgets: `demo-00`..`demo-19` plus `sandbox-a`, one `w0` each.
- Seed `9a1f2e`, window offset 37, buckets
  `demo-00:825 demo-01:614 demo-02:403 demo-03:192 demo-04:669
  demo-05:458 demo-06:247 demo-07:36 demo-08:513 demo-09:302
  demo-10:728 demo-11:939 demo-12:150 demo-13:361 demo-14:572
  demo-15:783 demo-16:994 demo-17:205 demo-18:416 demo-19:627
  sandbox-a:286`.

## Method

[`hack/trace-handoff.sh`](../../examples/widget-operator/hack/trace-handoff.sh)
(driven from a clean `Off`, epoch 1) runs these phases; every
ownership phase polls `hack/expect.py` (an independent Python
implementation of the partition spec) until the live widget statuses
match or a 150s timeout expires:

| Phase | Plan transition | Verdict |
|---|---|---|
| 1 Off baseline | rest state | all 21 stable, 0 mismatches |
| 2 cohort + window | Active w=10, include sandbox-a (rollout r-002) | predicted distribution, 0 mismatches |
| 3 widen | Active w=500 (same rollout) | predicted distribution, 0 mismatches |
| 4 abort | Off w=0 (include kept, must be ignored) | all 21 stable, 0 mismatches |
| 5 delayed-write, guard on | Off, then Active w=1000 mid-sleep | 0 late stable writes, 5 guard skips, 5 canary stamps |
| 6 delayed-write, guard off | Off, then Active w=1000 mid-sleep | 5 late stable writes, 5 canary-overlapping counterexamples |
| restore | Off, guard on, no delay | all 21 stable, 0 mismatches |
| idle | none (30s observation) | all 21 widgets bit-identical (status + resourceVersion) |

The delayed-write cohort is 5 widgets (`demo-00`..`demo-04`) for 5
workers with a 15s write delay: every touched reconcile starts
pre-flip and wakes post-flip, so the verdict counts are exact, not
bounds. The script exits nonzero on any verdict failure.

## Ownership detail (phases 2-3)

Phase 2 owned exactly the predicted set: canary holds only the
`sandbox-a` include (1/21) because no demo namespace falls in window
37..46 (nearest are `demo-07` at 36 and `demo-87`-style outsiders).
Phase 3 canary holds exactly the predicted 11/21: `demo-02`,
`demo-03`, `demo-05`, `demo-06`, `demo-08`, `demo-09`, `demo-12`,
`demo-13`, `demo-17`, `demo-18`, `sandbox-a`. Every other namespace
stayed stable in both phases.

Newly acquired widgets stamp exactly once (`#1`); untouched widgets
keep their previous counters, which is itself idempotency evidence
(see below).

## Delayed-write experiment (phases 5-6)

Phase 5 (guard on). Flip at 07:07:29Z; the 5 sleeping stable
reconciles woke at 07:07:41Z, re-checked ownership, and skipped:

- late stable writes: 0 (parser-verified against the zap log format)
- `SKIP guarded-after-delay`: 5
- canary post-flip stamps of the cohort: 5 (07:07:34-35Z), proving
  the cohort really changed hands underneath the sleepers

Phase 6 (guard off) reproduced the counterexample on all 5 cohort
widgets. Millisecond-precision interleave for `demo-00/w0`:

- 07:10:56.127Z canary stamps the cohort post-flip
- 07:11:02.609Z stable's pre-flip reconcile wakes and overwrites
  with a stable stamp (6s after losing ownership)
- 07:11:02.614Z canary re-stamps, 5ms later

The join over both tracks' logs reports 5/5 `OVERLAP` lines (stable
last-write and canary last-write both post-flip for every cohort
widget). Entry-gate checks alone do not fence in-flight work; the
guard re-check (phase 5) or the M1 drain/ack protocol must.

## Idle at rest

After restore, two full widget snapshots 30s apart were identical
across owner, revision, write count, and resourceVersion for all 21
widgets. Total operator log volume for the whole trace was 927 lines
(phase logs 38-292 lines each), versus 25,870 canary lines in 52
seconds on the first run before the hot-loop fix below.

## Failure encountered and fixed (first run, same day)

The first trace run passed phases 1-4 but failed phases 5, 6, and
restore. Root cause: the reconciler wrote widget status
unconditionally on every reconcile, and the controller watches its
own status writes, forming a self-sustaining hot loop (>400k writes
on some widgets, 10 MB logs per phase). The "drain" sleep never
drained, touched reconciles queued behind the storm and skipped at
entry instead of straddling the flip (0 guard skips, 0 late writes),
and conflict-backoff starvation froze `demo-00` canary-stamped
(4 restore mismatches).

A second, independent bug: the log parsers matched `"WRITE"`
(quoted), but zap dev-mode logs a bare tab-delimited word, so late
writes were unobservable even had they occurred.

Fixes (all in this tree, all covered by the passing re-run):

- `markOwned` stamps only on ownership change and logs `SKIP
  already-stamped` otherwise; `Writes` resets to 1 per acquisition.
- Unit tests (`reconciler_test.go`) pin one-write-per-acquisition
  and no-write-when-unowned against a counting fake client.
- Parsers match the real `\tWRITE\t` format (verified against
  first-run log lines before the re-run); phase 5 adds a canary
  positive control so a blind parser cannot yield a vacuous pass.
- Cohort narrowed 6 to 5 (worker count), delay 10s to 15s, drain
  75s to 120s, with margins derived from worker math; added the
  idle phase and a `rollout restart` in `bring-up.sh` so rebuilt
  images actually redeploy under reused tags.

The large `#N` counters still visible on never-reflipped widgets
(e.g. `demo-14#453775`) are leftover increments from the first run's
storm, preserved because the fixed code never rewrites an
already-correct stamp.

## Honest prototype limitations

This trace exercises the partition function, the entry/guard checks,
per-track leases, and status reporting. It does not demonstrate M1
safety. Marked `PROTOTYPE` in code and still open:

- No epoch tracking: every plan version is evaluated fresh; stale
  reads are not distinguished (spec V10 is M1).
- No drain/release/acquire state machine and no freshness barrier;
  the guard re-check is a single cooperative re-read, not fencing.
- No re-enqueue of gained namespaces; phase transitions in this
  trace were pushed by annotation touches, not by the library.
- Reporter phase is always `Acquired` with `released: true`; a
  missing lease yields a nil session instead of failing (spec S8
  requires the session in M1).
- Delayed-write timing assumes a quiet queue and sub-second watch
  latency; margins are documented in the script, not enforced.

## Pointers

- Scripts:
  [`bring-up.sh`](../../examples/widget-operator/hack/bring-up.sh),
  [`trace-handoff.sh`](../../examples/widget-operator/hack/trace-handoff.sh),
  [`expect.py`](../../examples/widget-operator/hack/expect.py)
- Contracts:
  [partition spec](../partition-spec.md),
  [ShardPlan spec](../shardplan-spec.md),
  [safety model](../safety-model.md) (D3 failure assumptions FA1.1-FA1.4)
- Next: M0-07 handoff and double-cache measurements on this
  prototype; M1 replaces the prototype gate with the specified
  epoch/drain/ack protocol.
