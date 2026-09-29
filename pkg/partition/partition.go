// Package partition implements the M1 namespace ownership function from the
// partition specification (docs/partition-spec.md). It is pure and
// dependency-free: hashing, window membership, ownership precedence, and
// rollout transition rules with no cluster access.
package partition

import (
	"fmt"
	"hash/fnv"
	"maps"
)

// Buckets is the fixed bucket count per rollout.
const Buckets = 1000

// MaxWeight is the largest weightPerMille value; it owns every bucket.
const MaxWeight = 1000

// Mode selects how a rollout assigns ownership.
type Mode int

const (
	// ModeOff assigns every namespace to stable. Abort and rest state.
	ModeOff Mode = iota
	// ModeShadow keeps stable ownership while the canary dry-runs.
	ModeShadow
	// ModeActive assigns ownership by cohorts, then the hash window.
	ModeActive
)

// String renders the mode name, or Mode(N) for unknown values.
func (m Mode) String() string {
	switch m {
	case ModeOff:
		return "Off"
	case ModeShadow:
		return "Shadow"
	case ModeActive:
		return "Active"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// Owner is the desired owning track for a namespace.
type Owner int

const (
	// Stable is the current production track.
	Stable Owner = iota
	// Canary is the rollout-under-test track.
	Canary
)

// String renders the owner name, or Owner(N) for unknown values.
func (o Owner) String() string {
	switch o {
	case Stable:
		return "stable"
	case Canary:
		return "canary"
	default:
		return fmt.Sprintf("Owner(%d)", int(o))
	}
}

// Spec carries one ShardPlan ownership state: mode, seed, weight, and
// cohort rules. ExcludeLabels uses matchLabels semantics: every pair
// must be present on the namespace. matchExpressions arrive with the
// typed API in M0-05 and are not modeled here.
type Spec struct {
	Mode           Mode
	Seed           string
	WeightPerMille int
	Include        []string
	ExcludeLabels  map[string]string
}

// Hash returns FNV-1a-64 over the UTF-8 bytes of s.
func Hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// Bucket maps a namespace name to its bucket in [0, Buckets).
func Bucket(namespace string) int {
	return int(Hash(namespace) % Buckets)
}

// Offset derives the rollout window offset from the seed. An empty
// seed is invalid.
func Offset(seed string) (int, error) {
	if seed == "" {
		return 0, fmt.Errorf("partition: empty seed")
	}
	return int(Hash(seed) % Buckets), nil
}

// windowOwns reports whether bucket falls in the window of weight w
// starting at offset. Callers must keep arguments in range.
func windowOwns(bucket, offset, w int) bool {
	return (bucket-offset+Buckets)%Buckets < w
}

// Validate rejects malformed specs. Off and Shadow require weight 0:
// they ignore cohorts and the window by definition, so a nonzero
// weight would describe a contradictory state.
func (s Spec) Validate() error {
	switch s.Mode {
	case ModeOff, ModeShadow, ModeActive:
	default:
		return fmt.Errorf("partition: unknown mode %s", s.Mode)
	}
	if s.Seed == "" {
		return fmt.Errorf("partition: empty seed")
	}
	if s.WeightPerMille < 0 || s.WeightPerMille > MaxWeight {
		return fmt.Errorf("partition: weight %d outside 0..%d",
			s.WeightPerMille, MaxWeight)
	}
	if s.Mode != ModeActive && s.WeightPerMille != 0 {
		return fmt.Errorf("partition: mode %s requires weight 0, got %d",
			s.Mode, s.WeightPerMille)
	}
	return nil
}

// Owner evaluates the precedence procedure from the spec: Off and
// Shadow assign stable; excludes win over includes; includes win over
// the window. Unknown label state is the caller's problem: pass the
// last evaluated owner through instead of guessing (spec section 3).
func (s Spec) Owner(namespace string, labels map[string]string) (Owner, error) {
	exp, err := s.Explain(namespace, labels)
	if err != nil {
		return Stable, err
	}
	return exp.Owner, nil
}

// Explanation records how one namespace evaluated: the winning rule
// plus the window inputs, so CLIs and debuggers can show the math
// instead of reimplementing it. Rule is one of "mode", "exclude",
// "include", "window".
type Explanation struct {
	Owner  Owner
	Rule   string
	Bucket int
	Offset int
}

// Explain evaluates Owner and names the rule that decided it. Bucket
// and Offset are populated for every rule (they cost one hash) so
// callers can show window math unconditionally.
func (s Spec) Explain(namespace string, labels map[string]string) (Explanation, error) {
	if err := s.Validate(); err != nil {
		return Explanation{}, err
	}
	offset, err := Offset(s.Seed)
	if err != nil {
		return Explanation{}, err
	}
	exp := Explanation{Bucket: Bucket(namespace), Offset: offset}
	switch s.Mode {
	case ModeOff, ModeShadow:
		exp.Owner, exp.Rule = Stable, "mode"
		return exp, nil
	}
	if Excluded(labels, s.ExcludeLabels) {
		exp.Owner, exp.Rule = Stable, "exclude"
		return exp, nil
	}
	for _, want := range s.Include {
		if namespace == want {
			exp.Owner, exp.Rule = Canary, "include"
			return exp, nil
		}
	}
	exp.Owner, exp.Rule = Stable, "window"
	if windowOwns(exp.Bucket, exp.Offset, s.WeightPerMille) {
		exp.Owner = Canary
	}
	return exp, nil
}

// excluded applies matchLabels semantics: an empty selector excludes
// nothing, and every pair must be present with an equal value.
// Excluded reports matchLabels semantics for the exclude selector:
// an empty selector excludes nothing; otherwise every pair must be
// present. Exported so the gate's relabel pin uses the exact
// predicate ownership evaluates.
func Excluded(labels, selector map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, want := range selector {
		got, ok := labels[k]
		if !ok || got != want {
			return false
		}
	}
	return true
}

// ValidNext reports whether next may follow s as the next rollout
// transition. Seed, include, and exclude rules are frozen during
// progression: changing any of them is a fresh rollout and requires
// s to be Off (stable owns everything, so no handoff is needed).
// Within a rollout, Off is always reachable (abort), Shadow may
// advance to Active but Active never regresses to Shadow, and weight
// never decreases.
func (s Spec) ValidNext(next Spec) error {
	if err := s.Validate(); err != nil {
		return fmt.Errorf("partition: current spec invalid: %w", err)
	}
	if err := next.Validate(); err != nil {
		return fmt.Errorf("partition: next spec invalid: %w", err)
	}
	if next.Seed != s.Seed ||
		!sameSet(next.Include, s.Include) ||
		!maps.Equal(next.ExcludeLabels, s.ExcludeLabels) {
		if s.Mode != ModeOff {
			return fmt.Errorf("partition: seed/cohort change requires Off, have %s", s.Mode)
		}
		return nil
	}
	if next.Mode == ModeOff {
		return nil
	}
	if s.Mode == ModeActive && next.Mode == ModeShadow {
		return fmt.Errorf("partition: mode must not regress Active to Shadow")
	}
	if next.WeightPerMille < s.WeightPerMille {
		return fmt.Errorf("partition: weight must not decrease %d to %d",
			s.WeightPerMille, next.WeightPerMille)
	}
	return nil
}

// sameSet compares string slices as sets, ignoring order and duplicates.
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
