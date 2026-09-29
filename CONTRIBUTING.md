# Contributing

Start with [Adopter onboarding](docs/onboarding.md) for behavior context and the
[roadmap](docs/plans/roadmap.md) for history. Design changes must
identify the invariant affected, failure scenario, and evidence needed for acceptance.
Keep unresolved choices in [decisions](docs/decisions.md).

Owner `js4683`, module `github.com/js4683/shardkit`, Apache-2.0 license
(adopted 2026-09-29, superseding MIT per external review; text in
[LICENSE](LICENSE)), DCO sign-off (adopted 2026-09-28), [OWNERS](OWNERS),
[code of conduct](CODE_OF_CONDUCT.md), and [security
policy](SECURITY.md) with private reporting are all in place.

Use focused branches and commits. Certify your own sign-off with
`git commit -s` (Developer Certificate of Origin); never sign on
behalf of someone else.
PR titles follow `NOISSUE - [low|medium|high] - Title` unless an issue number exists.
Explain context, behavior, tests, risk, and rollback. Safety/API changes are high risk.

Run `make check` for documentation. Add focused
behavioral tests and run the applicable Go, envtest, and kind checks described in
the [verification plan](docs/plans/verification.md), summarized as gates in the
[README](README.md).
Run those gates locally before pushing. Do not
create a dummy remote or publish merely to satisfy the gate's prerequisites.

Keep functions cohesive, favor guard clauses, target complexity below 10, and
avoid nesting deeper than three levels. Test effects at the API boundary rather
than assertions over implementation text. Do not delegate without user opt-in.
