# shardkit

[![CI](https://github.com/js4683/shardkit/actions/workflows/ci.yml/badge.svg)](https://github.com/js4683/shardkit/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/js4683/shardkit)](https://github.com/js4683/shardkit/releases)
[![Go](https://img.shields.io/github/go-mod/go-version/js4683/shardkit)](https://go.dev)
[![License](https://img.shields.io/github/license/js4683/shardkit)](LICENSE)

Shardkit brings safe canary releases to Kubernetes operators. Run two copies
of your controller — the current revision and the candidate — and a
`ShardPlan` decides which namespaces each copy reconciles. Namespaces move
between the two copies through a handshake both sides acknowledge, so a
stale or tampered plan stops all writes instead of causing a split brain.

## Features

- **Deterministic ownership** — a pure partition function maps every
  namespace to exactly one track; the CLI simulates with the same code
  the gate enforces, so previews never lie.
- **Fail-closed gate** — missing, deleted, recreated, or malformed plans
  deny work; tampered versions are refused against the writer contract.
- **Guarded client** — every write re-checks ownership, revision identity,
  singleton duty, and deletion budgets at the API boundary.
- **Acked handoffs** — tracks drain, release, and acquire behind each
  other's acknowledgements plus a freshness barrier over write versions.
- **Operator tooling** — `kubectl-shardplan` CLI (`status`, `explain`,
  `simulate`, `set-weight`, `abort`), an Argo Rollouts traffic-router
  plugin, shadow diffing, budgets with confirmed deletes, and Prometheus
  metrics.

## Quick Start

Prerequisites: Go 1.27.1, `kind`, `kubectl`, Docker (full pins in
[decisions](docs/decisions.md)).

```sh
./examples/widget-operator/hack/bring-up.sh   # kind cluster, CRD, both operators, 20 widgets
./examples/widget-operator/hack/demo.sh       # CLI rollout to 50%, explain, abort, re-converge
```

`demo.sh` prints a verdict after every step and ends at rest (`Off`, stable
owns everything). If any step fails, stop — the failure is a real bug, not a
flake to re-run past.

## Documentation

Start with [Adopter onboarding](docs/onboarding.md): drive a handoff
yourself, then wire the library into your own operator.

- [ShardPlan spec](docs/shardplan-spec.md) — the API contract
- [Safety model](docs/safety-model.md) — what is guaranteed and why
- [CLI reference](docs/cli.md) — every command and flag
- [Argo Rollouts plugin](docs/argo-plugin.md) — letting rollout steps drive the plan
- [Webhook](docs/webhook.md) — what the optional webhook adds (and why it stays optional)
- [Security review](docs/security-review.md) — self-review findings S1–S8, all fixed
- [Roadmap](docs/plans/roadmap.md) and [verification notes](docs/plans/verification.md) — history and test evidence

## Community

Questions or bugs: please [open an issue](https://github.com/js4683/shardkit/issues).
Security-sensitive reports follow [SECURITY](SECURITY.md) — private
advisories, never a public issue. Contributions are welcome; see below.

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md). All commits need DCO sign-off
(`git commit -s`), and `main` requires green CI (`check`, `codegen`,
`envtest`) — force pushes and deletions are disabled.

## Code of Conduct

Shardkit follows the [Kubernetes / CNCF Code of Conduct](CODE_OF_CONDUCT.md).

## License

MIT — see [LICENSE](LICENSE). Owners are listed in [OWNERS](OWNERS).
