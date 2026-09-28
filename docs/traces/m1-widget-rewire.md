# M1 widget rewire: operator on the library (kind, 2026-09-27)

Status: done. The widget sample operator no longer carries its own
gate, reporter, or handshake logic: it integrates `pkg/shardkit`
through `Attach`, a guarded client, one observer per track leader,
and the observer's handoff event channel. The M0-06 scripted
handoff trace re-ran green against the rewired operator, and the
runnable CLI recovery demo ([demo.sh](../../examples/widget-operator/hack/demo.sh))
drives rollout, convergence, explain, and abort on the live
cluster with only `kubectl-shardplan`.

The durable record is this document plus the committed code,
scripts, and tests. Raw trace artifacts live only in the ephemeral
trace directory `/tmp/shardkit-trace-20260927T104334Z/`; the
verdict counts below are transcribed from its verdict files.

## Environment

- Cluster `kind-shardkit-dev`, node image `kindest/node:v1.36.4`.
- `controller-runtime v0.24.1`, `k8s.io v0.36.4`, Go 1.27.1.
  See [development](../development.md).
- Deployments `widget-stable` / `widget-canary`, one replica each,
  5 reconcile workers each, per-track leases
  `widget-operator-stable` / `widget-operator-canary`,
  `OBSERVER_POLL=1s`.
- ShardPlan `widget-system/widget-operator`, seed `9a1f2e`.

## What changed

M0's operator hand-rolled the safety boundary inside the example:
`gate.go` (ownership reads), `reporter.go` (status acks), and
`gate_test.go` are deleted. In their place:

- `main.go` attaches the library gate with the manager's
  **API reader** (`mgr.GetAPIReader()`), records metrics, wraps
  all writes in the gate's guarded client, builds one
  `shardkit.Observer`, runs it as a leader-elected runnable, and
  feeds `obs.Events()` into the controller via
  `WatchesRawSource(source.Channel(...))`. The wiring is
  `main.go` lines 73–113, about 40 lines: Attach, SetMetrics,
  Client, NewObserver, Add, WatchesRawSource — under the M1
  under-50-line integration bar with every write path covered
  (the reconciler holds only the guarded client).
- `reconciler.go` keeps the M0 reconcile shape (entry check,
  optional straddle delay, guard re-check, idempotent stamp) but
  its client is `*shardkit.GuardedClient` and its gate is
  `*shardkit.Gate`. Unscoped reconciles stay on purpose: the D3
  delayed-write experiment needs its sleep to survive the drain.
- `Gate.Attach` takes a `client.Reader`, not a `client.Client`:
  attach runs before the manager cache starts, so a cached client
  fails with `cache is not started`. The operator passes the
  direct API reader; the observer's enumeration likewise lists
  directly so transition deltas never rest on cache staleness.
- Metrics ride the manager's metrics server:
  `shardkit.NewMetrics(prometheus.DefaultRegisterer)` with the
  standard `--metrics-bind-address`.
- `reconciler_test.go` replaces `gate_test.go` (green with
  `go test ./...`).

## Verification

`hack/trace-handoff.sh` re-ran unmodified against the rewired
operator and passed: ownership at each phase matched `expect.py`
with 0 mismatches, and the delayed-write verdict files read:

| Phase | Verdict file | Count |
|---|---|---|
| 5 guard on | `phase5-late.txt` | `late-stable-writes: 0` |
| 5 guard on | `phase5-canary-subset.txt` | 5 subset widgets stamped |
| 5 guard on | `phase5-canary-total.txt` / postflip | 21 / 21 (whole M1 handoff wave) |
| 6 guard off | `phase6-late.txt` | `late-stable-writes: 0` |
| 6 guard off | `phase6-join.txt` | `joined-overlaps: 0` |
| 6 guard off | `phase6-canary-subset.txt` | 5 subset widgets stamped |
| 6 guard off | `phase6-canary-total.txt` / postflip | 21 / 21 |
| restore/idle | — | all stable, 0 mismatches; 30s idle bit-identical |

Phase 6 stays at zero late/join lines because the M1 handoff wave
re-stamps every gained widget (the positive control is subset 5 +
total 21 + postflip lines equal to total), not because the guard
is off: the guard-off counterexamples are covered by the unit
fencing tests, while the trace asserts the wave is complete.

The CLI recovery demo ran the same day against the same cluster:

| Step | Command | Result |
|---|---|---|
| baseline | `status` | Off epoch 115, in sync |
| preview | `simulate --weight 500 --mode Active -q` | `stable: 12 canary: 15 (27 namespaces, no writes made)` |
| rollout | `set-weight 500 --mode Active` | epoch 116, waiting → in sync |
| explain | `explain demo-12` | `canary (rule: window)`, bucket 150 in window [37, 537) |
| recover | `abort` | epoch 117, waiting → in sync; stable Acquired 27, canary Released 0 |

`demo.sh` exits nonzero on any missed convergence (3-minute poll
per handshake) and ends at the Off rest state, so re-running is
safe.

## Consequences

- The example is now a pure library consumer: future handshake
  changes land in `pkg/shardkit` with its unit and envtest
  coverage, not in sample code.
- Operators that attach before cache start must pass a direct
  reader; the `client.Reader` parameter type enforces that at
  compile time.
- `OBSERVER_POLL=1s` keeps kind handshakes to seconds; production
  deployments keep the 30s-scale defaults.
