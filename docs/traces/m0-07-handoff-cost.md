# M0-07 handoff and double-cache cost (kind, 2026-09-27)

Status: passed. Three runs of
[`hack/measure-handoff.sh`](../../examples/widget-operator/hack/measure-handoff.sh)
converged every flip; the numbers below are the final run
(`/tmp/shardkit-measure-20260927T075210Z/`, plan epochs 76-97).
Raw artifacts (per-flip snapshots, log spreads, memory samples) live
only in that ephemeral directory; the tables below transcribe them.

Headline: at 21 namespaces a full ownership flip converges in about
a tenth of a second with exactly 21 API writes, and the second track
costs about 13 MB RSS. The Go runtime baseline dominates at this
scale; per-object cache cost needs the M4 scale runs.

## Environment

Same pins as the [M0-06 trace](m0-06-handoff-trace.md): kind
`shardkit-dev` on `kindest/node:v1.36.4`, controller-runtime v0.24.1,
Go 1.27.1, one replica per track, 5 reconcile workers each, 21
widgets. Mac-to-node clock skew measured per run via `docker exec`
 GNU `date +%s.%N` versus mac `time.time()`: -86 ms (node behind).

## Method

Each of 10 round trips flips the plan Off to Active(w=1000) and back
(single rollout `m-007`; the frozen tuple never changes). After each
flip one `kubectl annotate --all` call re-triggers all 21 widgets,
then `expect.py` polls (2s interval) until live statuses match.

The annotation touch stands in for the M1 HandoffSource the
prototype lacks: nothing re-enqueues gained namespaces on a plan
change, so without the touch there is no convergence to measure.
Reported latencies therefore cover trigger propagation plus the
reconcile wave, not gain detection.

WRITE lines are attributed to flips by their logged epoch (exact
under full-swap flips: a winner write implies a fresh plan read),
and wave timing comes from fractional kubectl timestamps. Poll-based
converge times have 2s granularity; log spreads are millisecond.

## Handoff latency (21 widgets, 5 workers)

Poll-based touch-to-converged: 18 flips at 0s, 2 at 1s (n=20).

Per-flip reconcile-wave spread (first to last WRITE, node clock),
all 20 Phase B flips in ms:

```text
70 73 73 73 74 75 76 77 77 77 77 81 82 83 84 91 93 101 112 135
```

min 70, median 77, p95 112, max 135. The two Phase D flips add
110 and 155 ms (22 waves total, max 155). Last write lands about
30 ms after the touch call returns (last_plus 12-38 ms); the whole
21-widget wave fits in roughly a tenth of a second because 5
workers drive ~5 rounds of sub-10 ms API writes against local kind.

First-write offsets are negative (-40 to -105 ms after
skew correction). That is a measurement artifact, not time travel:
the touch instant is sampled after `kubectl annotate` returns while
watches fire at the earlier commit, so early reconciles legitimately
predate the timestamp. Spread and last_plus are unaffected.

Cold start (first run, canary mid-restart, not yet reporting): one
flip took 18s to converge, covering leader election plus cache sync.
Later runs wait for the restarted canary to report before measuring.

## Memory: one track vs two (cgroup bytes)

`memory.current` (with page cache) and `anon` (RSS-like) per pod,
three samples 10s apart:

| Config | stable current | stable anon | canary current | canary anon |
|---|---|---|---|---|
| dual-1 | 13,717,504 | 12,537,856 | 12,595,200 | 11,538,432 |
| dual-2 | 13,950,976 | 12,537,856 | 12,828,672 | 11,689,984 |
| dual-3 | 13,840,384 | 12,537,856 | 13,029,376 | 11,694,080 |
| single-1 | 13,852,672 | 12,582,912 | scaled to 0 | - |
| single-2 | 13,787,136 | 12,587,008 | scaled to 0 | - |
| single-3 | 13,692,928 | 12,587,008 | scaled to 0 | - |

The second track costs about 12.8 MB current / 11.6 MB anon:
dual-track total ~26.6 MB versus ~13.8 MB single, i.e. near-linear
in tracks at tiny object counts. Stable's footprint is unchanged by
canary's presence (12.54 vs 12.59 MB anon, within GC noise), and no
pod restarted during the runs. Cache contents at this scale (21
widgets, 22 namespaces, 1 plan, 2 leases, times two tracks) are
kilobytes; the megabytes are Go runtime plus controller-runtime
machinery, so per-object cache cost is unmeasurable here and belongs
to the M4 scale runs.

## API writes per full handoff

Both directions: the winner logs exactly 21 WRITE lines for 21
unique widgets; the loser logs 0 lines at the flip epoch while its
21 prior-epoch lines stand unchanged, ruling out stale-cache writes
as well as redundant ones. A full 21-namespace handoff is exactly
21 status writes plus the plan patch and the touches.

## Measurement pitfalls fixed along the way

- Kubectl timestamps carry 9 fractional digits; `strptime %f`
  takes at most 6, silently dropping every line. Timestamps are now
  truncated to microseconds (and attribution is by epoch anyway).
- Wall-time `--since-time` windows bleed across flips: a wave
  landing early in a wall-clock second falls inside the next flip's
  floored window and was once miscounted as 21 phantom loser writes.
  All counting is now by logged epoch.
- `kubectl get` RV comparisons cannot count status writes: the
  annotation touch bumps resourceVersions by itself.

## What this does not show

- Any scale trend: 21 namespaces fit in L2 cache, figuratively.
  M4 measures 1k/10k with these figures as the baseline input.
- API-server-side cost (QPS, watch load, etcd): no audit or
  apiserver metrics were scraped; client-side write counts only.
- Gain detection latency: the touch models an enqueue trigger M1
  has yet to build; a slow HandoffSource would add to these times.
- Steady-state churn: the idle check in M0-06 covers rest behavior.

## Pointers

- Script:
  [`measure-handoff.sh`](../../examples/widget-operator/hack/measure-handoff.sh)
- Prior: [M0-06 handoff trace](m0-06-handoff-trace.md) (correctness),
  [partition spec](../partition-spec.md),
  [ShardPlan spec](../shardplan-spec.md)
- Next: M0-08 external review packet; M1 productizes the gate with
  epoch tracking, drain/ack, and the HandoffSource timed here by proxy.
