# M4 scale trace — 1k/10k cache/API/handoff measurements

Date: 2026-09-27. Machine: Apple M1 Pro (darwin/arm64), in-process
benches via `go test -bench` (`pkg/partition/scale_bench_test.go`,
`pkg/shardkit/scale_bench_test.go`); cluster half on
`kind-shardkit-dev` (node image `kindest/node:v1.36.4`).

## In-process (fake client where noted)

| Operation | Size | Result |
|---|---|---|
| `Spec.Owner` full sweep | 1k ns | 26.9 µs total (~27 ns/ns) |
| `Spec.Owner` full sweep | 10k ns | 247.5 µs total (~25 ns/ns) |
| `Spec.Explain` full sweep | 1k ns | 24.2 µs total (~24 ns/ns) |
| `Spec.Explain` full sweep | 10k ns | 251.7 µs total (~25 ns/ns) |
| `Gate.Owned` per check (fake) | 1 ns | 11.5 µs |
| `ValidateCreate` V1–V11 (fake) | 1 plan | 273 ns |
| `ConfirmDelete` charge+delete (fake) | 1 obj | 1.33 ms |
| `Shadowed.Delete` dry-run (fake) | 1 obj | 18.7 µs |
| List + assign sweep (fake) | 1k ns | 2.09 ms total (~2.1 µs/ns) |
| List + assign sweep (fake) | 10k ns | 21.1 ms total (~2.1 µs/ns) |

Partition assignment is O(namespaces) at ~25 ns each: a 10k-fleet
sweep costs ~0.25 ms of CPU. The fake-client List dominates the
sweep bench (~2 µs/ns); the real API cost is measured below.

## Cluster (kind, 1000 extra namespaces)

Created 1000 labeled namespaces (`shardkit-scale-test=true`,
single `kubectl apply`), then timed with the fleet at 1029:

- `kubectl get namespaces -o name`: **0.20 s** (raw API list).
- `kubectl-shardplan simulate widget-operator -n widget-system -q`
  (plan fetch + full list + per-namespace assignment, no writes):
  **0.52 s** wall, reporting `stable: 1029 canary: 0`.
- Partition assignment over the 1029 is ~26 µs of that; the rest
  is binary/client startup plus the API list.

All 1000 scale namespaces deleted afterwards by label selector;
fleet back to rest (21 widgets).

## Reading

- Handoff CPU scales linearly and is negligible next to one API
  round trip (<0.1% of a 1k simulate).
- `Gate.Owned` at 11.5 µs keeps per-reconcile gate overhead far
  below a single API read; no caching layer is needed at 10k.
- `ConfirmDelete` at ~1.3 ms per eviction is the most expensive
  per-object path (RV-guarded status write + delete); a 10k-object
  mass cleanup serializes to ~13 s of API work, which is why M3
  budgets cap the blast radius instead of speeding it up.
- `make check` (22 files) and full gates re-run green after the
  `testing.TB` helper widening (see `docs/plans/verification.md`).
