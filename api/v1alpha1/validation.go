package v1alpha1

import (
	"fmt"
	"maps"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/js4683/shardkit/pkg/partition"
)

// Validation enforces docs/shardplan-spec.md section 2. Observed
// specs are untrusted input: the library validates every version it
// acts on, and writers validate before writing. Errors carry their
// rule ID (V1-V9, V11). V10 staleness is an observer predicate,
// not an error: see StaleFor.

// ValidateCreate enforces rules V1-V8 on a new plan.
func (s *ShardPlan) ValidateCreate() error {
	return validateSpec(&s.Spec)
}

// ValidateUpdate enforces V1-V8 on the new spec plus V9 (frozen-tuple
// changes need a fresh rollout ID) and the writer epoch rule: any
// spec change must carry a strictly greater epoch (W2/W5). A nil old
// plan degrades to ValidateCreate.
func (s *ShardPlan) ValidateUpdate(old *ShardPlan) error {
	if err := validateSpec(&s.Spec); err != nil {
		return err
	}
	if old == nil {
		return nil
	}
	if err := validateSpec(&old.Spec); err != nil {
		return fmt.Errorf("V9: old spec invalid: %w", err)
	}
	return ValidateTransition(&old.Spec, &s.Spec)
}

// ValidateTransition enforces the V9 update clauses between two
// already-valid specs: frozen-tuple changes need a fresh rollout
// ID, and any spec change needs a strictly greater epoch.
// Writers call it via ValidateUpdate; gates call it on read to
// refuse versions that bypassed the writer contract (direct API
// edits with the webhook unwired), holding their last state
// instead of adopting a non-monotone remap.
func ValidateTransition(old, new *ShardPlanSpec) error {
	if frozenChanged(old, new) && new.Rollout == old.Rollout {
		return fmt.Errorf("V9: frozen tuple (key, seed, include, exclude) changed without a fresh rollout ID")
	}
	if specChanged(old, new) && new.Epoch <= old.Epoch {
		return fmt.Errorf("V9: spec change needs epoch > %d, got %d", old.Epoch, new.Epoch)
	}
	return nil
}

// StaleFor reports rule V10: an observation is stale when it carries
// the caller's plan UID with no newer version. Versions order by
// (epoch, generation): the epoch counts writer transitions (strictly
// increasing per ValidateUpdate), and the generation breaks ties for
// edits that bypass the writer contract, so a same-epoch generation
// bump is fresh. Fresh is not adopted: the gate still refuses
// versions that fail ValidateTransition (read-time V9), and the
// observer only advances gate-adopted versions. Stale reads are
// ignored silently; a new UID (recreated plan) is never stale and
// takes the B3 recovery path instead.
func (s *ShardPlan) StaleFor(uid string, lastActedEpoch, lastActedGeneration int64) bool {
	if string(s.UID) != uid {
		return false
	}
	if s.Spec.Epoch != lastActedEpoch {
		return s.Spec.Epoch < lastActedEpoch
	}
	return s.Generation <= lastActedGeneration
}

// validateSpec enforces V1-V8. The mode/seed/weight triple (V2-V4)
// delegates to the pure partition model so the rules cannot diverge;
// the remaining rules are typed-API shape checks.
func validateSpec(spec *ShardPlanSpec) error {
	if spec.Key != "namespace" { // V1
		return fmt.Errorf("V1: key %q, only %q is supported in M1", spec.Key, "namespace")
	}
	if spec.Seed == "" { // V2
		return fmt.Errorf("V2: empty seed")
	}
	triple := partition.Spec{
		Seed:           spec.Seed,
		WeightPerMille: int(spec.Canary.WeightPerMille),
	}
	switch spec.Canary.Mode { // V3
	case ModeOff:
		triple.Mode = partition.ModeOff
	case ModeShadow:
		triple.Mode = partition.ModeShadow
	case ModeActive:
		triple.Mode = partition.ModeActive
	default:
		return fmt.Errorf("V3: mode %q, want Off, Shadow, or Active", spec.Canary.Mode)
	}
	// Mode and seed are pre-screened, so the only remainder is V4.
	if err := triple.Validate(); err != nil {
		return fmt.Errorf("V4: %v", err)
	}
	if err := validRevision(spec.Tracks.Stable.Revision); err != nil { // V5
		return fmt.Errorf("V5: stable revision: %w", err)
	}
	if spec.Tracks.Canary != nil {
		if err := validRevision(spec.Tracks.Canary.Revision); err != nil {
			return fmt.Errorf("V6: canary revision: %w", err)
		}
	} else if spec.Canary.Mode != ModeOff { // V6
		return fmt.Errorf("V6: mode %s requires tracks.canary.revision", spec.Canary.Mode)
	}
	switch spec.SingletonOwner { // V7
	case TrackStable:
	case TrackCanary:
		if spec.Tracks.Canary == nil {
			return fmt.Errorf("V7: singletonOwner canary needs tracks.canary.revision")
		}
	default:
		return fmt.Errorf("V7: singletonOwner %q, want stable or canary", spec.SingletonOwner)
	}
	if sel := spec.Canary.Exclude.Selector; sel != nil && len(sel.MatchExpressions) > 0 { // V8
		return fmt.Errorf("V8: exclude.selector.matchExpressions is unsupported in M1")
	}
	if b := spec.Budget; b != nil { // V11
		if b.MaxDeletions != nil && *b.MaxDeletions < 0 {
			return fmt.Errorf("V11: maxDeletions %d, want >= 0", *b.MaxDeletions)
		}
		if b.MaxDeletionPercent != nil && (*b.MaxDeletionPercent < 0 || *b.MaxDeletionPercent > 100) {
			return fmt.Errorf("V11: maxDeletionPercent %d, want 0..100", *b.MaxDeletionPercent)
		}
	}
	return nil
}

// validRevision checks the non-empty DNS-1123 label format shared by
// track revisions (V5/V6).
func validRevision(rev string) error {
	if rev == "" {
		return fmt.Errorf("empty revision")
	}
	if msgs := validation.IsDNS1123Label(rev); len(msgs) > 0 {
		return fmt.Errorf("revision %q: %s", rev, msgs[0])
	}
	return nil
}

// frozenChanged compares the V9 frozen tuple structurally: key and
// seed by value, include as a set, exclude selectors by matchLabels
// (nil and empty selectors are equal; matchExpressions cannot appear
// here because V8 already rejected them).
func frozenChanged(a, b *ShardPlanSpec) bool {
	if a.Key != b.Key || a.Seed != b.Seed {
		return true
	}
	if !sameSet(a.Canary.Include.Namespaces, b.Canary.Include.Namespaces) {
		return true
	}
	return !maps.Equal(matchLabels(a), matchLabels(b))
}

// matchLabels normalizes a possibly absent exclude selector to its
// matchLabels map.
func matchLabels(spec *ShardPlanSpec) map[string]string {
	if spec.Canary.Exclude.Selector == nil {
		return nil
	}
	return spec.Canary.Exclude.Selector.MatchLabels
}

// sameSet compares string slices as sets, ignoring order and
// duplicates (mirrors the partition model's set semantics for the
// include list).
func sameSet(a, b []string) bool {
	setA := make(map[string]struct{}, len(a))
	for _, v := range a {
		setA[v] = struct{}{}
	}
	setB := make(map[string]struct{}, len(b))
	for _, v := range b {
		setB[v] = struct{}{}
	}
	return maps.Equal(setA, setB)
}

// specChanged reports whether any transition-defining spec field
// differs. Epoch and rollout ID are excluded: a lone epoch bump is a
// writer no-op, and a rollout relabel over an unchanged tuple is
// legal bookkeeping (V9), so neither may demand a further bump.
func specChanged(a, b *ShardPlanSpec) bool {
	return !deepSpecEqual(a, b)
}

// deepSpecEqual compares specs field by field with set-aware cohort
// equality. Epoch and rollout ID are deliberately not compared.
func deepSpecEqual(a, b *ShardPlanSpec) bool {
	if a.Key != b.Key || a.Seed != b.Seed || a.SingletonOwner != b.SingletonOwner {
		return false
	}
	if a.Tracks.Stable.Revision != b.Tracks.Stable.Revision {
		return false
	}
	if (a.Tracks.Canary == nil) != (b.Tracks.Canary == nil) {
		return false
	}
	if a.Tracks.Canary != nil && a.Tracks.Canary.Revision != b.Tracks.Canary.Revision {
		return false
	}
	if a.Canary.Mode != b.Canary.Mode || a.Canary.WeightPerMille != b.Canary.WeightPerMille {
		return false
	}
	if !sameSet(a.Canary.Include.Namespaces, b.Canary.Include.Namespaces) {
		return false
	}
	if !maps.Equal(matchLabels(a), matchLabels(b)) {
		return false
	}
	return true
}
