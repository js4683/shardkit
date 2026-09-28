# shardkit

Progressive delivery for leader-elected Kubernetes controllers: assign a controlled
slice of objects to a canary revision, observe it, then expand or abort.

**Status: M0 done; M1 library and CLI in progress.** The ShardPlan API, the
partition/gate/guarded-client/observer library, and the `kubectl-shardplan`
CLI are implemented and tested (unit, envtest, kind smoke). Metrics, the
widget integration, and the Argo plugin are pending. The working name and
API are provisional; this repository is not an installable operator yet.

## Start here

1. Read the [original brief](docs/project-brief.md) and [design draft](docs/design.md).
2. Review the [safety model](docs/safety-model.md), especially the unresolved fencing proof.
3. Work through the [first two weeks](docs/plans/first-two-weeks.md).
4. Track subsequent work in the [milestones](docs/plans/roadmap.md).

Operators drive rollouts with the [CLI](docs/cli.md) (`status`, `explain`,
`simulate`, `set-weight`, `abort`).

From this directory, run `make check` to validate local documentation links and
whitespace. It requires Python 3 and Make, and performs no network or cluster writes.
See [development setup](docs/development.md) for tool inventory and implementation gates.

## Scope

The planned core is a controller-runtime Go library, an orchestrator-independent
ShardPlan API, and a manual CLI. Argo Rollouts supplies promotion steps and analysis
through a thin traffic-router plugin. Full caches are the initial design.

HTTP traffic delivery, throughput sharding, fleet promotion, and canarying CRDs,
RBAC, or webhook configurations are outside the core. A later webhook helper is
an explicitly separate experiment, not permission to roll out incompatible schemas.

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md), [decisions](docs/decisions.md), and
[security guidance](SECURITY.md). MIT on GitHub is the decided license and host;
confirm the owner/module path and employer/IP clearance before implementation
or publication.
The repository has no remote or public release configured.
