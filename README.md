# shardkit

[![CI](https://github.com/js4683/shardkit/actions/workflows/ci.yml/badge.svg)](https://github.com/js4683/shardkit/actions/workflows/ci.yml)

Canary releases for Kubernetes operators, done safely. You run two copies of
your controller — the current revision and the candidate — and a `ShardPlan`
says which namespaces each copy reconciles. Shardkit moves namespaces between
the two copies with a handshake, so both sides agree on every handoff and a
stale or tampered plan stops all writes instead of causing a split brain.

**Status:** M0–M4 + hardening complete and released ([v0.2.0](https://github.com/js4683/shardkit/releases/tag/v0.2.0)).
See the [roadmap](docs/plans/roadmap.md) for history and
[verification notes](docs/plans/verification.md) for how it was tested.

## Try it in 5 minutes

You need Go 1.27.1, `kind`, `kubectl`, and Docker (full pins in
[decisions](docs/decisions.md)).

```sh
./examples/widget-operator/hack/bring-up.sh   # kind cluster, CRD, both operators, 20 widgets
./examples/widget-operator/hack/demo.sh       # CLI rollout to 50%, explain, abort, re-converge
```

`demo.sh` prints a verdict after every step and ends at rest (`Off`, stable
owns everything). If any step fails, stop — the failure is a real bug, not a
flake to re-run past.

Next: [Adopter onboarding](docs/onboarding.md) walks you through driving a
handoff yourself and wiring the library into your own operator.

## How it fits together

- `api/` — the ShardPlan/Widget CRD types and validation rules
  (source of truth; CRDs in `config/crd/` regenerate with `make manifests`).
- `pkg/shardkit/` — the library your operator links: ownership gate,
  guarded client, handshake observer, budgets, shadow diff, metrics.
- `pkg/partition/` — the pure ownership function both the gate and the
  CLI simulate with, so previews match enforcement exactly.
- `cmd/kubectl-shardplan/` — the operator CLI (`status`, `explain`,
  `simulate`, `set-weight`, `abort`).
- `plugins/argo-rollouts/` — lets Argo Rollouts steps drive the plan.
- `examples/widget-operator/` — the runnable two-track demo used above.
- `test/conformance/` + `test/envtest/` — the adopter contract and the
  real-API-server suite, including a randomized two-manager audit.

The safety argument lives in [safety-model](docs/safety-model.md); the full
contract is [shardplan-spec](docs/shardplan-spec.md).

## Checking your own changes

```sh
make check        # docs links + whitespace
gofmt -l . && go vet ./... && go test ./...   # unit gate
make test-envtest # real API server (needs KUBEBUILDER_ASSETS via setup-envtest 1.36.x)
make release      # release matrix + verified SHA256SUMS (output in gitignored dist/)
```

CI runs all of the above plus a codegen-clean check on every push and pull
request. Tagging `v*` builds and attaches release binaries.

## Governance and help

- MIT ([LICENSE](LICENSE)); owners in [OWNERS](OWNERS).
- [Code of Conduct](CODE_OF_CONDUCT.md) (Kubernetes/CNCF).
- [Contributing](CONTRIBUTING.md) — DCO sign-off required (`git commit -s`).
- Questions or bugs: [open an issue](https://github.com/js4683/shardkit/issues).
- Security-sensitive: follow [SECURITY](SECURITY.md) (private advisories),
  never a public issue. Background: [self security review](docs/security-review.md).
