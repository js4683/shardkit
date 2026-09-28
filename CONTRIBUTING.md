# Contributing

Start with the [M0 backlog](docs/plans/first-two-weeks.md). Design changes must
identify the invariant affected, failure scenario, and evidence needed for acceptance.
Keep unresolved choices in [decisions](docs/decisions.md).

Owner `js4683`, module `github.com/js4683/shardkit`, MIT license
(adopted 2026-09-26, text in [LICENSE](LICENSE) with the copyright
holder), DCO sign-off (adopted 2026-09-28), [OWNERS](OWNERS),
[code of conduct](CODE_OF_CONDUCT.md), and [security
policy](SECURITY.md) with private reporting are all in place.

Use focused branches and commits. Certify your own sign-off with
`git commit -s` (Developer Certificate of Origin); never sign on
behalf of someone else.
PR titles follow `NOISSUE - [low|medium|high] - Title` unless an issue number exists.
Explain context, behavior, tests, risk, and rollback. Safety/API changes are high risk.

Run `make check` for documentation. Once executable behavior exists, add focused
behavioral tests and run the applicable Go, envtest, and kind checks described in
the [verification plan](docs/plans/verification.md).
Use no-mistakes for the delivery gate after a real origin is configured. Do not
create a dummy remote or publish merely to satisfy the gate's prerequisites.

Keep functions cohesive, favor guard clauses, target complexity below 10, and
avoid nesting deeper than three levels. Test effects at the API boundary rather
than assertions over implementation text. Do not delegate without user opt-in.
