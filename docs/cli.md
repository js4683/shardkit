# kubectl-shardplan CLI

Manual ShardPlan operations: `status`, `explain`, `simulate`,
`set-weight`, `abort`. The CLI works without Argo, so a plugin-API
change degrades to manual steps rather than an outage (see the
[roadmap](plans/roadmap.md) M1 package 5 and the
[ShardPlan spec](shardplan-spec.md)).

Build and install on `PATH` to run it as `kubectl shardplan`:

```sh
go build -o $HOME/bin/kubectl-shardplan ./cmd/kubectl-shardplan
```

Global flags (`--kubeconfig`, `--context`, `-n`/`--namespace`) work
before or after the command. The plan namespace defaults to the
context namespace.

## Commands

`status PLAN` shows versions, per-track acks, and one actionable
verdict line per problem (waiting, stale, degraded, in progress).
Exit 0 reports coherent truth even mid-handoff.

```sh
kubectl shardplan status widget-operator -n widget-system
```

`explain PLAN NAMESPACE` shows why a namespace is owned by its
track: the winning rule plus the window math.

```sh
kubectl shardplan explain widget-operator demo-87 -n widget-system
```

`simulate PLAN` reports the ownership distribution for the live plan
or a hypothetical `--weight`/`--mode`/`--seed`, over all namespaces
or `--namespaces a,b`. It only reads; `no writes made` is printed
on every run. `-q` prints counts only.

```sh
kubectl shardplan simulate widget-operator --weight 100 --mode Active -q -n widget-system
```

`set-weight PLAN N` moves the plan to weight N (0..1000) as a new
epoch (live + 1 by default, or an explicit higher `--epoch`).
`--mode` overrides the live mode; `--rollout` overrides the rollout
ID. The update is read-modify-write with conflict retries; setting
the live weight and mode is a no-op, not a new epoch.

```sh
kubectl shardplan set-weight widget-operator 100 --mode Active -n widget-system
```

`abort PLAN` returns everything to stable (mode `Off`, weight 0) as
a new epoch. Aborting an already-aborted plan is a no-op, so
recovery scripts can run abort unconditionally.

```sh
kubectl shardplan abort widget-operator -n widget-system
```

## Exit codes

- `0`: coherent truth (including no-op mutations).
- `1`: the CLI failed, the plan is invalid, or the report is
  incoherent. The message names the resource and the next action.
- `2`: the invocation was wrong (usage). Unknown flags fail loud
  with the valid set inline.

## Recovery runbook

Runnable version: `examples/widget-operator/hack/demo.sh` performs
a weight rollout plus abort-recovery against the kind cluster and
polls `status` to convergence at each step.

1. `abort` the plan, then `status` until both tracks ack the new
   epoch. Stable reclaims every namespace; the canary drains first.
2. `status` says `waiting`: re-run to watch. If a track never
   advances, check that track Deployment's logs and its leadership
   lease, then re-run.
3. `status` says `degraded`: the reason names the cause
   (`AcquireWaiting` means the winner is still fenced behind the
   loser's ack; `PlanRecreated` means the plan object was replaced).
4. `status` says a track `runs revision X, spec wants Y`: the S7
   binding voids that track's acks until the specified revision
   runs. Check the Deployments, not the plan.
5. `set-weight`/`abort` reports a concurrent change: re-run the
   printed command; the conflict loser lost nothing.

## Troubleshooting

- `kubectl apply` rejects `mode: Off` with `"boolean"`: YAML 1.1
  parses unquoted `Off` as false. Quote it: `mode: "Off"`.
- `simulate --weight N` on an `Off` plan fails validation: pass
  `--mode Active` with it (the error says so).
- `set-weight N` on an `Off` plan fails without `--mode Active`:
  starting a rollout from rest is explicit.
- `set-weight 0` warns when the include list still routes
  namespaces to canary: weight 0 empties the window, but explicit
  includes win over the window. Use `abort` to return everything.
