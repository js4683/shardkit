# Adopter onboarding

You run two copies of your operator (stable + canary tracks) and a
`ShardPlan` decides which namespaces each copy reconciles. This
page takes you from zero to a live handoff on kind, then to wiring
the library into your own operator. The worked example is
`examples/widget-operator/`; the contract it satisfies is pinned by
`test/conformance/`.

## 1. Prerequisites

Go 1.27.1, `kind`, `kubectl`, Docker. All commands use the
`kind-shardkit-dev` context; the scripts never touch your current
context. Pinned versions live in [decisions](decisions.md).

## 2. Bring up the demo fleet

```sh
./examples/widget-operator/hack/bring-up.sh   # kind cluster, CRD, both operators, 20 widgets
./examples/widget-operator/hack/demo.sh       # CLI rollout to 50%, explain, abort, re-converge
```

`demo.sh` ends at rest (`Off`, stable owns all) and prints the
verdict after every step. If any step fails, stop: the failure is
a real bug, not a flake to re-run past.

## 3. Drive a handoff yourself

```sh
go run ./cmd/kubectl-shardplan simulate widget-operator -n widget-system  # dry run, no writes
go run ./cmd/kubectl-shardplan set-weight widget-operator 250 --mode Active -n widget-system
go run ./cmd/kubectl-shardplan status widget-operator -n widget-system    # watch both tracks ack
go run ./cmd/kubectl-shardplan abort widget-operator -n widget-system     # back to Off
```

Reads are safe anytime; `set-weight`/`abort` move the plan to a
new epoch. Full command reference: [cli](cli.md).

## 4. Wire the library into your operator

Each track runs the same binary with its own `TRACK`/`REVISION`
and attaches a gate; all writes go through the guarded client,
and one observer per track runs the handshake:

```go
gate, err := shardkit.Attach(ctx, mgr.GetAPIReader(), planKey, track, revision)
if err != nil { /* fail closed: do not reconcile */ }
guarded := gate.Client(mgr.GetClient(), mgr.GetAPIReader())

obs, err := shardkit.NewObserver(shardkit.ObserverOptions{
    Client: guarded, APIReader: mgr.GetAPIReader(),
    Gate: gate, Guarded: guarded, PlanKey: planKey,
    Track: track, LeaseBase: "my-operator",
    Types: []schema.GroupVersionKind{myGVK},
    PollInterval: time.Second, Metrics: metrics,
})
go obs.Run(ctx) // steady acks, handoff acquire/release

// in Reconcile, for each namespace:
owned, err := gate.Owned(ctx, namespace)
if err != nil || !owned.Owned { return ctrl.Result{}, err }
if err := guarded.Update(ctx, obj); err != nil { /* DeniedError: skip, do not retry blindly */ }
```

Rules that keep you safe: never write except through `guarded`;
treat every gate/client error as "stop" (the library fails
closed, your wrapper must too); keep one observer per track;
name leases via `shardkit.LeaseName(base, track)` in the plan's
namespace. Deletions beyond a trickle go through
`guarded.ConfirmDelete` with a `spec.budget` cap
([budgets](../docs/traces/m3-budgets.md)).

## 5. Prove your integration

- `go test ./test/conformance/` — the adopter contract (single
  owner, version fence, budgets, shadow purity, skew tolerance).
- `make test-envtest` — the same behaviors against a real API
  server, plus the randomized two-manager audit
  (`SHARDKIT_TEST_SEED=<n>` for more seeds).
- Kind: point the widget demo at your types, or run your own
  operator twice and repeat section 3.

## 6. Going further

- Argo Rollouts driving the plan: [argo-plugin](argo-plugin.md),
  install via `examples/widget-operator/hack/install-argo.sh`.
- Scale numbers and their baselines:
  [m4-scale](traces/m4-scale.md). The 10k-fleet sweep costs
  ~0.25 ms of CPU; the API list dominates.
- What the optional webhook adds (exactly V9) and why it stays
  optional: [webhook](webhook.md).
- Multi-cluster fleets: set `ClusterName` so session holders
  read `cluster/pod` ([spec](shardplan-spec.md)).

Questions or problems: open an issue. Security-sensitive:
follow [SECURITY](../SECURITY.md), never a public issue.
