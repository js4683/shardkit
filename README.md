# shardkit

[![CI](https://github.com/js4683/shardkit/actions/workflows/ci.yml/badge.svg)](https://github.com/js4683/shardkit/actions/workflows/ci.yml)

Fleet-safe canary ownership for Kubernetes operators: run two copies of your
controller (stable + canary) and let a `ShardPlan` decide which namespaces
each copy reconciles, with fenced handoffs, acked epochs, and fail-closed
reads.

**Status: M0–M4 + hardening done.** ShardPlan API, Go library (ownership
gate, guarded client, handshake observer, budgets, shadow diff, admission
helper), `kubectl-shardplan` CLI, Argo Rollouts traffic-router plugin, and a
runnable kind demo — unit, envtest, and live-probe verified. See the
[roadmap](docs/plans/roadmap.md) and [verification notes](docs/plans/verification.md).

## Layout

- `api/` — ShardPlan/Widget CRD types + V1–V11 validation (source of truth;
  CRDs in `config/crd/` regenerate via `make manifests`).
- `pkg/shardkit/` — the library: gate, guarded client, observer, leases,
  freshness barrier, budgets, shadow diff, metrics, webhook helper.
- `pkg/partition/` — the pure ownership function the gate enforces and the
  CLI simulates.
- `cmd/kubectl-shardplan/` — operator CLI (`status`, `explain`, `simulate`,
  `set-weight`, `abort`).
- `plugins/argo-rollouts/` — traffic-router plugin so Argo Rollouts steps
  drive the plan.
- `examples/widget-operator/` — runnable two-track demo on kind
  (`hack/bring-up.sh`, `hack/demo.sh`).
- `test/conformance/` — adopter contract; `test/envtest/` — real-API-server
  suite including the randomized two-manager audit.
- `docs/` — specs, onboarding, traces, security review.

## Quickstart

Prereqs: Go 1.27.1, `kind`, `kubectl`, Docker (pins in
[decisions](docs/decisions.md)).

```sh
./examples/widget-operator/hack/bring-up.sh   # kind cluster, CRD, both operators, 20 widgets
./examples/widget-operator/hack/demo.sh       # CLI rollout to 50%, explain, abort, re-converge
```

Then follow [Adopter onboarding](docs/onboarding.md) to drive handoffs
yourself and wire the library into your own operator.

## Verification

```sh
make check        # docs links + whitespace (28 files)
gofmt -l . && go vet ./... && go test ./...   # unit gate
make test-envtest # real API server (needs KUBEBUILDER_ASSETS via setup-envtest 1.36.x)
make release      # release matrix + verified SHA256SUMS (output in gitignored dist/)
```

CI runs all of the above plus a codegen-clean check on every push and pull
request. Tagging `v*` builds and attaches release binaries.

## Governance

MIT ([LICENSE](LICENSE)) · [Code of Conduct](CODE_OF_CONDUCT.md)
(Kubernetes/CNCF) · [Contributing](CONTRIBUTING.md) (DCO sign-off required:
`git commit -s`) · [Security](SECURITY.md) (private advisories; see the
[self security review](docs/security-review.md)) · Owners: [OWNERS](OWNERS).
