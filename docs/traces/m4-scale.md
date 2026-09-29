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

## Addendum 2026-09-29 — gate reads stay live (D9)

External review asked for cached gate reads plus frozen labels. We
built it on a branch (observer-published versions, per-version frozen
verdicts) and the randomized two-manager audit failed
deterministically: seed `1790654963648388000`, step 15, `aud-02`
owned by both tracks (live 3/3, stable adopted 2/2, canary adopted
3/3). The same seed passes on live reads. Root cause is structural,
not a bug: live reads are the loser's self-fence — both tracks
converge at the flip. Any async adoption delays loser convergence,
and strict I1 plus cached reads plus loser-down availability cannot
all hold (only the loser's release proves de-authorization, which
needs the loser alive; frozen labels diverge the same way across a
relabel). This matches the standing FA1.2 constraint: admission must
read ownership authoritatively, not cached.

Adopted instead: in-flight dedup (singleflight) over unchanged live
reads. `TestGate_StormSharesReads` piles 100 concurrent `Owned`
calls onto one blocked read: **2 plan GETs total** (attach + one
shared storm read), and 5 sequential calls still pay 5 reads —
sharing never caches. Steady-state `Gate.Owned` still measures per
call above; storms now cost one plan GET, not one per reconcile.
