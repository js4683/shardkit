# Validating webhook experiment (M3)

`pkg/shardkit.ShardPlanValidator` adapts library validation
(V1–V9, V11) to a controller-runtime admission webhook. It is
deliberately **not wired into any manager** — this document
records what it adds over CEL, how to wire it if reviewers want
it, and the limits that keep it optional.

## Wiring (M4 example, still optional)

`examples/admission/` implements this sketch: a manager serving
the validator plus the `ValidatingWebhookConfiguration`, with
`failurePolicy: Fail` chosen explicitly (see limit 3). The
mapping itself is unit-tested (`TestWebhook_Mapping`) and the
example was verified live on kind (V9 tamper denied, no-op
admitted, resources removed afterwards) — see
`examples/admission/README.md`. The webhook stays unwired in
production deployments for the limits below.

## What it adds over CEL: exactly V9

The CRD already enforces at the boundary, structurally or via
CEL: V1–V8 field rules, epoch never-decrease (`oldSelf`), V6/V7
conditionals, and the V11 range rules (`minimum`/`maximum`).
The only library rule with no server-side equivalent is **V9**:
frozen-tuple changes need a fresh rollout ID, and spec changes
need an epoch bump — both compare old against new beyond what
the shipped CEL expresses. That comparison is the webhook's
entire marginal value.

## Limits

1. **V10 staleness is unenforceable at admission.** It needs the
   controller's last-acted epoch memory; the webhook sees one
   object version, never history. Staleness stays a gate
   concern (spec B4/T1).
2. **UID/recreate checks need more than the object.** B3
   (recreated plan under the same name) compares UIDs across
   reads; admission has old+new but no liveness watch. Keep
   recreation detection in the gate.
3. **Availability cuts both ways.** `failurePolicy: Fail` makes
   a webhook outage block every ShardPlan write (fail closed,
   wide blast radius); `Ignore` silently passes malformed specs
   during an outage. CEL has no availability cost — it runs
   in-process in the API server. This alone argues for keeping
   the webhook unwired until V9 evasion is observed in practice.
4. **Version skew.** The webhook binary must track library
   validation rule-for-rule; a stale webhook either rejects
   valid new specs or passes new violations. The mapping test
   pins the current version, not deployment skew.
5. **Latency.** Every spec write pays a webhook round trip on
   top of CEL. Spec writes are rare (one per handoff step), so
   this is negligible — listed for completeness, not as an
   objection.

## Recommendation

Stay with CEL + library validation (current state). Revisit the
webhook only if a V9-class evasion appears — a writer bypassing
the CLI/plugin to mutate frozen cohorts under a live rollout
ID — since that is the one shape nothing else catches.
