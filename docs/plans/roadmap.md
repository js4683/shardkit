# Delivery roadmap

Effort estimates from the brief assume one focused engineer; they are not deadlines.
M0–M2 landed in dependency order (traces under `docs/traces/`);
M3+ remain. Complete milestones in dependency order.

M2 evidence: SDK `argo-rollouts v1.10.0` pinned with skew build;
idempotent plugin calls with fake tests
(`plugins/argo-rollouts/`); Prometheus examples
(`examples/widget-operator/config/{analysis,prometheus}/`);
kind demo good path 1→5→25→50→100% plus analysis-triggered
abort (`docs/traces/m2-argo-demo.md`, driver
`examples/widget-operator/hack/argo-demo.sh`); stale
`VerifyWeight` false by unit test; plugin-loss recovery through
the CLI demonstrated live; checksummed binaries via
`make release` (`dist/`, gitignored).

| Milestone | Work packages | Exit evidence |
|---|---|---|
| M0, 2–3 weeks | D1–D7 research; design/safety review; API draft; partition and handoff experiments; reviewer feedback | Accepted design and ownership/license decisions; recorded proof assumptions and runnable experiment results |
| M1 / v0.1, 6–8 weeks | Plan types/schema; partition; gate; guarded client; revision leases and legacy migration; epoch state machine; metrics; manual CLI; widget sample | Two real managers in envtest; randomized audit for I1/I2/I4/I7; CLI recovery demo; <50-line integration with all write paths covered |
| M2 / v0.2, 4 weeks | Pin Argo plugin SDK; implement idempotent calls; Prometheus examples; kind demo; checksummed binaries | CI evidence for 1→5→25→50→100% and abort; stale VerifyWeight false; plugin loss recoverable through CLI |
| M3 / v0.3, 4–6 weeks | Shadow diff; absolute/percentage budgets; freeze; confirmed-delete helper; webhook helper experiment; chaos suite | Every invariant has chaos coverage; injected mass deletion trips budgets and aborts; webhook limits documented |
| M4 / v0.4–0.5, estimate after M3 | 10k-namespace benchmarks; multi-cluster keys; optional KEP backend research; admission example; conformance suite | 1k/10k cache/API/handoff measurements (`docs/traces/m4-scale.md`); cluster-qualified session holders; KEP-5866 qualification researched, flag deferred to beta (`docs/kep-5866.md`); admission example verified live, stays optional (`examples/admission/`); `test/conformance/` 9/9 green. Carried forward: backend qualification past research, 2–3 external adopters |
| v1.0, adoption-driven | Stable API; migration policy; compatibility matrix; security review; release/runbooks | Three production adopters, reviewed safety assumptions, verified compatibility and recovery |

## M1 implementation order

1. Pure partition and transition model: property tests and explicit invalid inputs.
2. Typed ShardPlan with validation, status conflict handling and fake-independent envtest.
3. Gate and client boundary with cancellation and in-flight write accounting.
4. Leases/migration and drain/ack acquisition with freshness evidence.
5. CLI `status`, `explain`, `simulate`, `set-weight`, `abort`; optimistic concurrency
   and actionable failures. Simulation reports distribution without cluster writes.
6. Metrics and widget integration; multi-manager randomized tests; runnable local demo.

Each package must be usable and tested before declaring completion. Add source
directories from the brief as work lands, not as empty scaffold trees.

## Scope and timing dependencies

M2 can demonstrate analysis-triggered abort on an injected reconcile failure.
The mass-delete budget-trip scenario depends on M3's budgets; do not claim that
scenario in M2 without explicitly bringing the necessary budget implementation forward.
The webhook helper does not relax compatibility rules for cluster-global resources.

## Community and release work

During M0 prepare a design review packet for Argo, controller-runtime, and sharding
maintainers; sending it requires author approval. Before publication establish
MIT license text, DCO, OWNERS, conduct enforcement and private reporting. Recruit a second
maintainer before v0.3. Plan the M2 demo blog and conference proposal after working
evidence exists. Consider splitting the plugin only after its interface stabilizes.
