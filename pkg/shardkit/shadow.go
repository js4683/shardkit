package shardkit

import (
	"sync"
)

// Shadow diffs (I6): a GuardedClient copy that dry-runs every
// mutating verb and records what would have happened, persisting
// nothing. Guard authorization still applies first, so the diff
// shows denials exactly where live would refuse.
//
// Safety rules, all fail-closed:
//   - Evidence RVs from dry-run responses are never folded into
//     LastWritten: nothing persisted, so there is nothing to cite
//     in a release ack.
//   - Metrics skip shadow attempts: the recorder is the record,
//     and periodic diffs must not inflate write counters.
//   - Unsupported write paths (unmappable types, cross-namespace
//     collections) are denied before any call, same as live.
//
// Caveats (FA6.1/FA6.2): dry-run requests still traverse
// admission and audit; diffs describe behavior with the live
// admitters in place, and stubbed externals would change control
// flow — label any stored diff accordingly.

// ShadowOp is one attempted mutation: the verb GuardedClient
// would have executed, the type key, the object name, and the
// writeOutcome decision/reason vocabulary (allowed/none,
// denied/<Reason>, error/<kind>).
type ShadowOp struct {
	Verb     string
	Key      string
	Name     string
	Decision string
	Reason   string
}

// ShadowRecorder collects ShadowOps in call order.
type ShadowRecorder struct {
	mu  sync.Mutex
	ops []ShadowOp
}

func (r *ShadowRecorder) add(op ShadowOp) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

// Ops returns the recorded attempts in call order.
func (r *ShadowRecorder) Ops() []ShadowOp {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ShadowOp(nil), r.ops...)
}

// Shadowed returns a client that routes every mutating verb
// through dry-run and records attempts in rec, persisting
// nothing. Reads pass through untouched. A nil rec returns c
// itself (live behavior). The shadow shares gate, reader, and
// metrics with c but keeps its own (empty) evidence map — and, by
// construction, never folds any.
func (c *GuardedClient) Shadowed(rec *ShadowRecorder) *GuardedClient {
	if rec == nil {
		return c
	}
	return &GuardedClient{
		Client:      c.Client,
		gate:        c.gate,
		reader:      c.reader,
		metrics:     c.metrics,
		shadow:      rec,
		lastWritten: map[string]string{},
	}
}

// shadowing reports whether this client dry-runs and records.
func (c *GuardedClient) shadowing() bool {
	return c.shadow != nil
}

// noteShadow records one attempt; no-op on live clients.
func (c *GuardedClient) noteShadow(verb, key, name, decision, reason string) {
	if c.shadow == nil {
		return
	}
	c.shadow.add(ShadowOp{Verb: verb, Key: key, Name: name, Decision: decision, Reason: reason})
}

// shadowName formats namespace/name, tolerating cluster scope.
func shadowName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

// Shadowing rule for mutating methods: when shadowing, append
// DryRunAll to the delegated call's options. The option types
// differ per verb, so each method inlines the same two lines:
//
//	if c.shadowing() {
//		opts = append(opts, client.DryRunAll)
//	}
//
// DryRunAll implements every mutating option interface in
// controller-runtime v0.24.1 (Create/Update/Patch/Delete/
// DeleteAllOf/Apply plus all subresource variants), so the plain
// append type-checks at every site with no helper.
