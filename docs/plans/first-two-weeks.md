# First two weeks

Start with M0-01. These are ready-to-use local work items, not published issues.
The project author resolves ownership questions; one implementing engineer owns
the technical tasks. Estimates are focused days and allow review tasks to overlap.

| ID / days | Task | Dependencies | Definition of done |
|---|---|---|---|
| M0-01 / day 1 | Confirm IP permission, provisional name, license and public owner | Author input | D1/D2 recorded; Go module path can be chosen without renaming guesses |
| M0-02 / days 1–2 | Review safety model against upstream contracts | None | I1–I8 have failure assumptions; D3/D4 options and unanswered questions documented |
| M0-03 / days 2–3 | Specify partition input encoding, seed/window and cohort rules | D5 discussion | Golden-vector specification; include/exclude/Off/Shadow/abort/promotion semantics agreed |
| M0-04 / days 3–4 | Pin toolchain, initialize module and pure model tests | M0-01, M0-03 | Reproducible Go test command; all 0–1000 weights and invalid transitions exercised |
| M0-05 / days 4–5 | Draft ShardPlan schema and transition protocol | M0-02 | Status ownership, plan recreation, stale ack and epoch conflict rules specified |
| M0-06 / days 6–8 | Prototype manual plan, gate and per-revision leases in widget/kind | M0-04/05; Docker available | Two revisions run; scripted manual handoff trace; partition and delayed-write counterexamples captured |
| M0-07 / days 8–9 | Measure handoff and double-cache cost | M0-06 | Reproducible command, pinned versions, raw results at baseline and dual-cache configurations |
| M0-08 / days 9–10 | Prepare external review and revise milestone estimates | M0-02/07 | Review packet with design, traces, unresolved questions and 3–4 proposed reviewers; author approves outreach |

## First work session

Run `make check`, read the design and safety model, and resolve D1/D2 with the author.
While those are pending, M0-02 and the partition specification can proceed. Do not
claim the prototype provides exclusive writes until D3's failure cases are resolved.

## Review packet questions

- Does the cooperative guard permit the guarantee we want under process pauses
  and delayed API requests? What would server-enforced fencing require?
- What cache freshness mechanism works for the selected resource types and versions?
- Can controller-runtime expose dequeue and in-flight tracking hooks cleanly?
- How should the pinned Argo interface model explicit cohorts, shadow and promotion?

No external messages or issues have been sent. Reviewer agreement is a future M0
exit criterion, not a prerequisite to using this planning workspace.
