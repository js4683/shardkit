# Argo Rollouts traffic-router plugin contract (M2)

The shardkit plugin routes *objects* instead of requests: it
implements Argo Rollouts' `TrafficRouterPlugin` by writing
`ShardPlan` specs, so analysis-gated weight steps and aborts flow
through the same acknowledged handoff the CLI drives. This
document pins the SDK and binds every plugin method to a plan
operation; the plugin binary follows it. D7 (toolchain and plugin
protocol) stays open pending domain review — this is the proposed
direction with evidence, not an acceptance.

## SDK pin (proposed)

- Module `github.com/argoproj/argo-rollouts` at **v1.10.0**
  (newest stable; the `argoproj-labs` reference plugin tracks it).
- Imported packages: `rollout/trafficrouting/plugin/rpc`
  (`TrafficRouterPlugin`, gob wire types),
  `utils/plugin/types` (`RpcTrafficRoutingReconciler`,
  `RpcError`, `RpcVerified`), `pkg/apis/rollouts/v1alpha1`
  (`Rollout`, `WeightDestination`). There is no separate
  `rollouts-plugin-trafficrouter/rpc/v2` module; the earlier
  `/v2` path does not exist on the proxy or pkg.go.dev.
- v1.10.0 itself builds on k8s.io v0.34.5, but MVS keeps our
  v0.36.4 pins: the three SDK packages were compiled against
  k8s.io v0.36.4 in a scratch module (`SKEW-BUILD-OK`, no source
  incompatibilities). The plugin side never imports the
  controller-side `plugin` package (client-go, `utils/record`).
- The plugin serves over hashicorp `go-plugin` (net/rpc + gob,
  pulled in by the `rpc` package), exactly like the upstream
  `test/cmd/trafficrouter-plugin-sample`. Gob names must match
  the controller byte-for-byte, which is why the SDK is pinned,
  not copied.
- Landing command for the implementation slice:
  `go get github.com/argoproj/argo-rollouts@v1.10.0` + `go mod tidy`.

## Binding

One Rollout drives one ShardPlan. The plugin lists ShardPlans and
selects the one whose opaque `spec.rollout` equals the calling
Rollout's UID ([spec](shardplan-spec.md): rollout UID, CLI
writes it at plan creation). Zero matches or more than one is an
`RpcError` naming the rollout UID and the count — never a guess.
The plugin inherits the Rollouts controller ServiceAccount RBAC
and is written in Go (upstream constraints, see
[safety-model](safety-model.md)).

No call keeps state in the plugin struct: net/rpc does not
persist it between calls. Every method derives its write from the
call arguments plus one live plan read, so retries and duplicate
deliveries converge.

## Method bindings

Argo weights are percent (0–100); plan weights are per mille, so
`SetWeight`/`VerifyWeight` scale by exactly 10 (M2 exit steps
1→5→25→50→100 map to 10→50→250→500→1000), except `SetWeight(0)`,
which normalizes to `Off`/0: zero weight means no canary.

Quiescence rule: after promotion Argo calls `RemoveManagedRoutes`
then `SetWeight(0)` on every sync. Both land on `Off`/0, so a
completed rollout leaves the plan quiet — exactly one abort epoch,
then no-ops forever. Writing `Active`/0 for zero would flap
`Off`↔`Active` on every sync (each call defeats the other's
no-op guard); this exact failure was observed on kind and is
pinned by `TestPlugin_SteadyStateQuiesces`.

Registered plugin name is `js4683/shardkit` (the `<org>/<name>`
form Argo requires): it keys both the `argo-rollouts-config`
`trafficRouterPlugins` entry and the Rollout's
`spec.strategy.canary.trafficRouting.plugins` map. `Type()`
returns the short `"shardkit"`; dispatch uses the registered
name, not `Type()`.

| Method | Plan operation | Idempotency / errors |
|---|---|---|
| `Type()` | returns `"shardkit"` | pure |
| `InitPlugin()` | list ShardPlans (connectivity + RBAC probe) | error surfaces Argo-side when the API is unreachable. The probe list is cluster-scoped (no namespace at init), so the install needs the read-only `argo-rollouts-shardplans-read` ClusterRole in `config/argo/shardplan-rbac.yaml` — without it every fresh controller pod fails init and no step ever advances (found live 2026-09-27) |
| `SetWeight(rollout, w, addl)` | epoch+1: `Active` at weight `w*10` for `w > 0`, `Off` at 0 for `w == 0`; rollout ID preserved | no-op when live already matches; nonempty `addl` is an `RpcError` (v1 has no per-destination objects; cohorts stay CLI-managed) |
| `VerifyWeight(rollout, w, addl)` | compare live spec + canary ack | `Verified` only when spec weight is `w*10` **and** the canary entry acks the live epoch, generation, and revision. Anything stale reports `NotVerified`, never true (M2 exit: stale VerifyWeight false); nonempty `addl` is an `RpcError` (unverifiable destinations must not read as verified); read errors return `RpcError` (fail closed). Operational consequence (seen live 2026-09-27): Argo logs `Desired weight N not yet verified` every sync and never advances while the canary operator runs a stale `REVISION` (S7 voids its acks) — hand-patched rollout templates must be followed by an operator revision sync, which `argo-demo.sh` does per iteration |
| `UpdateHash(rollout, canary, stable, addl)` | epoch+1 preserving mode/weight, canary revision set to the canary hash (S7 binding) | no-op when the canary hash already matches spec; nonempty `addl` and empty hashes are `RpcError`. The stable hash is validated non-empty but otherwise ignored: the stable track is externally managed (plain Deployment / CLI flow), while `stable` names Argo's own stable ReplicaSet — writing it into `spec.tracks.stable` would void the real stable operator and stall every handoff |
| `SetHeaderRoute` / `SetMirrorRoute` | none | `RpcError`: object routing has no headers or mirrors; silent success would lie to the rollout |
| `RemoveManagedRoutes(rollout)` | epoch+1 `Off`, weight 0 (abort) | called on rollout deletion, on abort, and on **every sync of a fully-promoted rollout** (`reconcileTrafficRouting`: the `IsFullyPromoted` branch runs before the bottom `SetWeight(0)`); no-op when already `Off`; stable reclaims everything and the CLI can take over from there (M2 exit: plugin loss recoverable through CLI). An analysis abort reaches this method through Argo's abort branch (observed live: `RemoveManagedRoutes` wrote the abort epoch; the bottom `SetWeight(0)` then no-ops against `Off`/0); deletion is the other caller |

Promotion is hash rotation: Argo's promotion changes the stable
hash, which arrives as `UpdateHash` and rebinds revisions; the
next rollout's `SetWeight` steps from there. `Shadow` is not
produced by any plugin call in v1 — it stays a CLI-managed mode
(first-two-weeks cohort/shadow/promotion question, narrowed).

## Test hooks (for the implementation slice)

- Unit: table of (live plan, call) → (expected spec write or
  no-op, expected `RpcError`), reusing the CLI's optimistic
  concurrency (conflict retry, same no-op rules as
  `set-weight`/`abort`).
- envtest: one Rollout UID bound to one plan; `SetWeight`
  5→`VerifyWeight` false pre-ack, true post-ack; `UpdateHash`
  churn is a no-op; `RemoveManagedRoutes` aborts; error paths
  (zero/two plans bound) name the UID.
- kind (M2 demo): Argo Rollouts at the pinned version,
  1→5→25→50→100% plus analysis-triggered abort on an injected
  reconcile failure.
