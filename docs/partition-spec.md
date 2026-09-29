# Partition specification (M0-03)

Status: proposed, 2026-09-26. Resolves the [design](design.md) partition
contract sketch for the M1 namespace key. Section 5 vectors are normative:
the M0-04 Go model must reproduce them exactly.

## 1. Identity and hash

- M1 key: namespace **name**. Rationale: deterministic, simulatable
  without a cluster (`simulate` needs no API access), and stable across
  delete/recreate (a recreated tenant name keeps its cohort). Namespace
  UID is recorded in audits to detect recreation, but is not a hash
  input. Label-keyed and cluster-keyed shards are later work, not M1.
- Hash: FNV-1a-64 over the UTF-8 bytes of the name, with no prefix,
  suffix, or separator. Offset basis `14695981039346656037`, prime
  `1099511628211`, 64-bit wraparound per byte.
- Bucket: `hash mod 1000`, giving buckets `0..999`. Collisions share a
  bucket and are acceptable.
- Percentages are expectations over namespace counts, not promises
  about object counts or reconcile load.

## 2. Seed and window

- Each rollout fixes one opaque, non-empty seed string (for example
  `9a1f2e`). Empty seed is invalid.
- Window offset: `FNV-1a-64(UTF-8 bytes of seed) mod 1000`, using the
  section 1 hash. The offset is fixed for the whole rollout.
- Canary window at weight `w` (`0..1000`): buckets
  `(offset + i) mod 1000` for `0 <= i < w`. Weight 0 owns nothing;
  weight 1000 owns everything.
- Monotonicity: the offset never moves within a rollout, so raising
  `w` only appends buckets at the window's leading edge. Lowering `w`
  outside abort is invalid and rejected by plan validation (M0-05).
- Seed, key, include, and exclude rules are frozen during progression.
  Any change defines a fresh rollout with full handoff from stable,
  never an in-place mutation.

## 3. Ownership function

Inputs: mode (`Off` | `Shadow` | `Active`), weight `w`, include name
set `I`, exclude label selector `X`, namespace name `n` with labels
`L(n)`, bucket `b(n)`.

1. Mode `Off` → stable. Abort and the pre-rollout rest state take
   this path; cohorts are ignored. Abort is one transition to
   `Off` + `w = 0` with a new epoch.
2. Mode `Shadow` → stable owns; the canary dry-run-evaluates all
   namespaces without taking ownership. Shadow-all is the M1 rule;
   bucket- or cohort-scoped shadow is a possible later extension.
3. `L(n)` matches `X` → stable. Exclude wins over include and over
   the window.
4. Mode `Active` and `n ∈ I` → canary.
5. Mode `Active` and `b(n)` in the canary window → canary.
6. Otherwise → stable.

Include matching is an exact name set; exclude matching is a label
selector evaluated against current namespace labels. If namespace
labels cannot be read (missing from cache), the gate retains the last
evaluated owner for that namespace and reports a degraded condition;
it never silently flips ownership on a transient read error.
Relabels across the exclude boundary mid-version deny with
`LabelsDrifted` on both tracks until a new version carries the new
labels through the handshake (the relabel pin, D10); label churn
that does not cross the boundary is unaffected.
Cluster-scoped and cross-namespace effects follow the singleton
policy (I4), not this function.

## 4. Abort and promotion

- Abort: the orchestrator writes one transition (`Off`, `w = 0`, new
  epoch). Every namespace evaluates to stable and the handoff protocol
  moves it back. Includes and excludes are ignored while `Off`, not
  deleted; any post-abort rollout is a fresh rollout with a new seed.
- Promotion: an explicit transition to `Off` with
  `tracks.stable.revision` set to the canary revision and the canary
  section cleared; the promoted stable owns everything. Promotion must
  define the next rollout's stable revision and lease identity without
  restarting or orphaning the new stable lease (D6, M0-05).

## 5. Golden vectors (normative)

Computed from sections 1–3 with an independent Python implementation
(`/tmp/gen_vectors.py`, kept outside the repo as a scratch probe).
`M0-04` transcribes these tables into `testdata/` plus Go tests; the
tables here stay the authority on conflicts.

### 5.1 Hash vectors

Namespace → FNV-1a-64 hex → bucket. The empty string is invalid as a
real namespace and only proves the function is total.

| Namespace | FNV-1a-64 | Bucket |
|---|---|---|
| `default` | `ebada5168620c5fe` | 782 |
| `kube-system` | `df65c7bc1d82b35e` | 918 |
| `kube-public` | `917fb081f1a65f5a` | 234 |
| `kube-node-lease` | `021fb6dd71b80174` | 260 |
| `sandbox-a` | `2633b11b40dd40fe` | 286 |
| `sandbox-b` | `2633b01b40dd3f4b` | 75 |
| `team-payments` | `561aba95a305e2dc` | 956 |
| `team-search` | `bef17cbefcc254cb` | 523 |
| `critical-db` | `927d770b114f84f9` | 313 |
| `prod-eu-west-1` | `1fdb0ee33661503d` | 933 |
| `staging` | `7c90917f05bfe1a6` | 862 |
| `a` | `af63dc4c8601ec8c` | 996 |
| `z9` | `08f78b07b592bcbc` | 124 |
| `ns-0123456789-abcdef-0123456789-abcdef-0123456789-abcdef-0123` | `e8129f46f81eb4cd` | 997 |
| `demo-87` | `30b1d07345d1c5c4` | 44 |
| `test-ns-123` | `62509e3d8d99f10e` | 46 |
| `canary-154` | `4f38ebd034311b16` | 38 |
| `` (empty) | `cbf29ce484222325` | 37 |

### 5.2 Seed vectors

Seed → window offset.

| Seed | Offset |
|---|---|
| `9a1f2e` | 37 |
| `000000` | 549 |
| `rollout-2026-09-26-001` | 194 |
| `ffffffffffffffff` | 701 |

### 5.3 Window vectors

Seed `9a1f2e` (offset 37); weight → owned buckets.

| Weight | Owned buckets |
|---|---|
| 0 | none |
| 1 | 37 |
| 10 | 37..46 |
| 500 | 37..536 |
| 999 | all except 36 (wraps: 37..999, 0..35) |
| 1000 | all 0..999 |

Wrapping check, seed `ffffffffffffffff` (offset 701), weight 500:
owned ranges are 701..999 and 0..200. Bucket 200 is owned; buckets
201 and 700 are not.

### 5.4 Ownership vectors

Seed `9a1f2e` (offset 37), includes `{sandbox-a}`,
excludes `{matchLabels: {tier: critical}}`. `critical-db` carries
label `tier=critical`; `demo-87` (bucket 44) sits inside the
weight-10 window 37..46; `sandbox-a` (bucket 286) sits outside it.

| Mode | Weight | Namespace | Owner | Reason |
|---|---|---|---|---|
| Off | 0 | `sandbox-a` | stable | Off ignores cohorts (§3.1) |
| Off | 0 | `demo-87` | stable | Off ignores the window (§3.1) |
| Shadow | 0 | `demo-87` | stable | shadow never transfers (§3.2) |
| Active | 10 | `demo-87` | canary | bucket 44 in 37..46 (§3.5) |
| Active | 10 | `sandbox-a` | canary | include wins over window miss (§3.4) |
| Active | 10 | `default` | stable | bucket 782 outside window (§3.6) |
| Active | 500 | `critical-db` | stable | exclude wins over window hit (§3.3) |
| Active | 500 | `team-search` | canary | bucket 523 in 37..536 (§3.5) |
| Active | 500 | `staging` | stable | bucket 862 outside window (§3.6) |
| Active | 1000 | `staging` | canary | full window (§3.5) |
| Active | 1000 | `critical-db` | stable | exclude wins even at full weight (§3.3) |

## 6. Open points for M0-05

This spec fixes the ownership function. It does not fix the ShardPlan
schema that carries it: rollout identity, epoch authorship, plan UID
binding, revision identity, and validation rules belong to M0-05. D5
stays open until the M0-04 model and property tests implement this
spec.
