package partition

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
)

// TestHash_GoldenVectors pins spec section 5.1: FNV-1a-64 hex digests
// and buckets computed by the independent Python probe.
func TestHash_GoldenVectors(t *testing.T) {
	vectors := []struct {
		namespace string
		hex       string
		bucket    int
	}{
		{"default", "ebada5168620c5fe", 782},
		{"kube-system", "df65c7bc1d82b35e", 918},
		{"kube-public", "917fb081f1a65f5a", 234},
		{"kube-node-lease", "021fb6dd71b80174", 260},
		{"sandbox-a", "2633b11b40dd40fe", 286},
		{"sandbox-b", "2633b01b40dd3f4b", 75},
		{"team-payments", "561aba95a305e2dc", 956},
		{"team-search", "bef17cbefcc254cb", 523},
		{"critical-db", "927d770b114f84f9", 313},
		{"prod-eu-west-1", "1fdb0ee33661503d", 933},
		{"staging", "7c90917f05bfe1a6", 862},
		{"a", "af63dc4c8601ec8c", 996},
		{"z9", "08f78b07b592bcbc", 124},
		{"ns-0123456789-abcdef-0123456789-abcdef-0123456789-abcdef-0123", "e8129f46f81eb4cd", 997},
		{"demo-87", "30b1d07345d1c5c4", 44},
		{"test-ns-123", "62509e3d8d99f10e", 46},
		{"canary-154", "4f38ebd034311b16", 38},
		{"", "cbf29ce484222325", 37},
	}
	for _, v := range vectors {
		if got := fmt.Sprintf("%016x", Hash(v.namespace)); got != v.hex {
			t.Errorf("Hash(%q) = %s, want %s", v.namespace, got, v.hex)
		}
		if got := Bucket(v.namespace); got != v.bucket {
			t.Errorf("Bucket(%q) = %d, want %d", v.namespace, got, v.bucket)
		}
	}
}

// TestOffset_GoldenVectors pins spec section 5.2.
func TestOffset_GoldenVectors(t *testing.T) {
	vectors := []struct {
		seed   string
		offset int
	}{
		{"9a1f2e", 37},
		{"000000", 549},
		{"rollout-2026-09-26-001", 194},
		{"ffffffffffffffff", 701},
	}
	for _, v := range vectors {
		got, err := Offset(v.seed)
		if err != nil {
			t.Errorf("Offset(%q) error: %v", v.seed, err)
			continue
		}
		if got != v.offset {
			t.Errorf("Offset(%q) = %d, want %d", v.seed, got, v.offset)
		}
	}
	if _, err := Offset(""); err == nil {
		t.Error("Offset(\"\") = nil error, want non-nil")
	}
}

// ownedSet collects every bucket the window owns at the given offset
// and weight.
func ownedSet(offset, w int) map[int]bool {
	owned := map[int]bool{}
	for b := 0; b < Buckets; b++ {
		if windowOwns(b, offset, w) {
			owned[b] = true
		}
	}
	return owned
}

// TestWindow_GoldenVectors pins spec section 5.3, including the
// wrapping window case.
func TestWindow_GoldenVectors(t *testing.T) {
	t.Run("seed 9a1f2e offset 37", func(t *testing.T) {
		if got := ownedSet(37, 0); len(got) != 0 {
			t.Errorf("weight 0 owns %d buckets, want 0", len(got))
		}
		if got := ownedSet(37, 1); len(got) != 1 || !got[37] {
			t.Errorf("weight 1 owns %v, want {37}", got)
		}
		got := ownedSet(37, 10)
		if len(got) != 10 {
			t.Fatalf("weight 10 owns %d buckets, want 10", len(got))
		}
		for b := 37; b <= 46; b++ {
			if !got[b] {
				t.Errorf("weight 10 missing bucket %d", b)
			}
		}
		got = ownedSet(37, 500)
		if len(got) != 500 {
			t.Fatalf("weight 500 owns %d buckets, want 500", len(got))
		}
		for b := 37; b <= 536; b++ {
			if !got[b] {
				t.Errorf("weight 500 missing bucket %d", b)
			}
		}
		got = ownedSet(37, 999)
		if len(got) != 999 || got[36] {
			t.Errorf("weight 999 must own all but 36, got %d buckets (36 owned: %v)",
				len(got), got[36])
		}
		if got := ownedSet(37, 1000); len(got) != 1000 {
			t.Errorf("weight 1000 owns %d buckets, want 1000", len(got))
		}
	})
	t.Run("wrapping window offset 701 weight 500", func(t *testing.T) {
		got := ownedSet(701, 500)
		if len(got) != 500 {
			t.Fatalf("owns %d buckets, want 500", len(got))
		}
		for _, b := range []int{701, 999, 0, 200} {
			if !got[b] {
				t.Errorf("bucket %d must be owned", b)
			}
		}
		for _, b := range []int{201, 700} {
			if got[b] {
				t.Errorf("bucket %d must not be owned", b)
			}
		}
	})
}

// TestOwner_GoldenVectors pins spec section 5.4: the full precedence
// procedure over mode, cohorts, and window.
func TestOwner_GoldenVectors(t *testing.T) {
	critical := map[string]string{"tier": "critical"}
	vectors := []struct {
		name      string
		mode      Mode
		weight    int
		namespace string
		labels    map[string]string
		want      Owner
	}{
		{"Off ignores include", ModeOff, 0, "sandbox-a", nil, Stable},
		{"Off ignores window", ModeOff, 0, "demo-87", nil, Stable},
		{"Shadow never transfers", ModeShadow, 0, "demo-87", nil, Stable},
		{"window hit", ModeActive, 10, "demo-87", nil, Canary},
		{"include wins over window miss", ModeActive, 10, "sandbox-a", nil, Canary},
		{"window miss", ModeActive, 10, "default", nil, Stable},
		{"exclude wins over window hit", ModeActive, 500, "critical-db", critical, Stable},
		{"window hit at 500", ModeActive, 500, "team-search", nil, Canary},
		{"window miss at 500", ModeActive, 500, "staging", nil, Stable},
		{"full window", ModeActive, 1000, "staging", nil, Canary},
		{"exclude wins at full weight", ModeActive, 1000, "critical-db", critical, Stable},
	}
	for _, v := range vectors {
		spec := Spec{
			Mode:           v.mode,
			Seed:           "9a1f2e",
			WeightPerMille: v.weight,
			Include:        []string{"sandbox-a"},
			ExcludeLabels:  map[string]string{"tier": "critical"},
		}
		got, err := spec.Owner(v.namespace, v.labels)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", v.name, err)
			continue
		}
		if got != v.want {
			t.Errorf("%s: Owner(%q) = %s, want %s", v.name, v.namespace, got, v.want)
		}
	}
}

// TestWeights_MonotoneAllWeights sweeps every weight 0..1000: each
// weight owns exactly w buckets, growth is monotone (each set
// contains the previous), and the endpoints own nothing/everything.
func TestWeights_MonotoneAllWeights(t *testing.T) {
	for _, seed := range []string{"9a1f2e", "000000", "rollout-2026-09-26-001", "ffffffffffffffff"} {
		offset, err := Offset(seed)
		if err != nil {
			t.Fatalf("Offset(%q): %v", seed, err)
		}
		var prev map[int]bool
		for w := 0; w <= MaxWeight; w++ {
			owned := ownedSet(offset, w)
			if len(owned) != w {
				t.Fatalf("seed %q weight %d owns %d buckets, want %d",
					seed, w, len(owned), w)
			}
			for b := range prev {
				if !owned[b] {
					t.Fatalf("seed %q weight %d lost bucket %d from weight %d",
						seed, w, b, w-1)
				}
			}
			prev = owned
		}
	}
}

// probePerBucket returns one deterministic namespace per bucket.
func probePerBucket(t *testing.T) map[int]string {
	t.Helper()
	probes := map[int]string{}
	for i := 0; len(probes) < Buckets; i++ {
		if i > 100*Buckets {
			t.Fatalf("only covered %d of %d buckets", len(probes), Buckets)
		}
		n := fmt.Sprintf("probe-%d", i)
		if _, ok := probes[Bucket(n)]; !ok {
			probes[Bucket(n)] = n
		}
	}
	return probes
}

// TestOwner_AgreesWithWindow checks the end-to-end path: with no
// cohorts, Owner follows the window for every bucket at sampled
// weights, so stable/canary coverage is disjoint and complete.
func TestOwner_AgreesWithWindow(t *testing.T) {
	probes := probePerBucket(t)
	for _, w := range []int{0, 1, 10, 250, 500, 999, 1000} {
		spec := Spec{Mode: ModeActive, Seed: "9a1f2e", WeightPerMille: w}
		canary := 0
		for b := 0; b < Buckets; b++ {
			got, err := spec.Owner(probes[b], nil)
			if err != nil {
				t.Fatalf("weight %d: %v", w, err)
			}
			want := Stable
			if windowOwns(b, 37, w) {
				want = Canary
			}
			if got != want {
				t.Fatalf("weight %d bucket %d: Owner = %s, want %s", w, b, got, want)
			}
			if got == Canary {
				canary++
			}
		}
		if canary != w {
			t.Errorf("weight %d: %d canary namespaces, want %d", w, canary, w)
		}
	}
}

// TestSpec_Validate exercises valid specs and every invalid shape:
// unknown modes, empty seeds, out-of-range weights, and nonzero
// weights under Off/Shadow.
func TestSpec_Validate(t *testing.T) {
	valid := []Spec{
		{Mode: ModeOff, Seed: "s", WeightPerMille: 0},
		{Mode: ModeShadow, Seed: "s", WeightPerMille: 0},
		{Mode: ModeActive, Seed: "s", WeightPerMille: 0},
		{Mode: ModeActive, Seed: "s", WeightPerMille: 1},
		{Mode: ModeActive, Seed: "s", WeightPerMille: 999},
		{Mode: ModeActive, Seed: "s", WeightPerMille: 1000},
		{Mode: ModeActive, Seed: "s", WeightPerMille: 10,
			Include: []string{"a"}, ExcludeLabels: map[string]string{"k": "v"}},
	}
	for i, s := range valid {
		if err := s.Validate(); err != nil {
			t.Errorf("valid[%d] (%+v): %v", i, s, err)
		}
	}
	invalid := []Spec{
		{Mode: Mode(99), Seed: "s", WeightPerMille: 0},
		{Mode: Mode(-1), Seed: "s", WeightPerMille: 0},
		{Mode: ModeActive, Seed: "", WeightPerMille: 0},
		{Mode: ModeActive, Seed: "s", WeightPerMille: -1},
		{Mode: ModeActive, Seed: "s", WeightPerMille: 1001},
		{Mode: ModeOff, Seed: "s", WeightPerMille: 1},
		{Mode: ModeOff, Seed: "s", WeightPerMille: 1000},
		{Mode: ModeShadow, Seed: "s", WeightPerMille: 5},
	}
	for i, s := range invalid {
		if err := s.Validate(); err == nil {
			t.Errorf("invalid[%d] (%+v): nil error, want non-nil", i, s)
		}
	}
}

// TestSpec_ValidNext exercises rollout transitions: abort, phase
// advance, monotone growth, fresh rollouts from Off, and every
// forbidden move (weight decrease, frozen-rule mutation, regression,
// invalid endpoints).
func TestSpec_ValidNext(t *testing.T) {
	off := Spec{Mode: ModeOff, Seed: "s", WeightPerMille: 0}
	shadow := Spec{Mode: ModeShadow, Seed: "s", WeightPerMille: 0}
	active10 := Spec{Mode: ModeActive, Seed: "s", WeightPerMille: 10}
	active500 := Spec{Mode: ModeActive, Seed: "s", WeightPerMille: 500}
	cohort := Spec{Mode: ModeActive, Seed: "s", WeightPerMille: 0,
		Include: []string{"sandbox-a"}}

	valid := []struct {
		name string
		prev Spec
		next Spec
	}{
		{"rest to shadow", off, shadow},
		{"rest to active", off, active10},
		{"rest to cohort-only", off, cohort},
		{"shadow to active", shadow, active10},
		{"shadow to shadow", shadow, shadow},
		{"weight growth", active10, active500},
		{"no-op rewrite", active500, active500},
		{"abort to off", active500, off},
		{"shadow abort to off", shadow, off},
		{"fresh rollout new seed from off", off,
			Spec{Mode: ModeActive, Seed: "new", WeightPerMille: 10}},
		{"fresh rollout new cohorts from off", off,
			Spec{Mode: ModeOff, Seed: "s", Include: []string{"other"}}},
		{"include reorder is not a change", cohort,
			Spec{Mode: ModeActive, Seed: "s", WeightPerMille: 0,
				Include: []string{"sandbox-a", "sandbox-a"}}},
	}
	for _, v := range valid {
		if err := v.prev.ValidNext(v.next); err != nil {
			t.Errorf("%s: unexpected error: %v", v.name, err)
		}
	}

	invalid := []struct {
		name string
		prev Spec
		next Spec
	}{
		{"weight decrease", active500, active10},
		{"active to shadow regresses", active10, shadow},
		{"seed change while active", active10,
			Spec{Mode: ModeActive, Seed: "new", WeightPerMille: 10}},
		{"seed change while shadow", shadow,
			Spec{Mode: ModeShadow, Seed: "new", WeightPerMille: 0}},
		{"include change while active", cohort,
			Spec{Mode: ModeActive, Seed: "s", WeightPerMille: 0,
				Include: []string{"sandbox-a", "sandbox-b"}}},
		{"exclude change while active", active10,
			Spec{Mode: ModeActive, Seed: "s", WeightPerMille: 10,
				ExcludeLabels: map[string]string{"tier": "critical"}}},
		{"next invalid", active10,
			Spec{Mode: ModeActive, Seed: "s", WeightPerMille: 1001}},
		{"prev invalid", Spec{Mode: ModeActive, Seed: "", WeightPerMille: 0}, active10},
	}
	for _, v := range invalid {
		if err := v.prev.ValidNext(v.next); err == nil {
			t.Errorf("%s: nil error, want non-nil", v.name)
		}
	}
}

// TestExclude_MatchLabelsSemantics pins exact Kubernetes matchLabels
// behavior: empty selector excludes nothing, absent keys never match
// (even nil labels), supersets match.
func TestExclude_MatchLabelsSemantics(t *testing.T) {
	base := Spec{Mode: ModeActive, Seed: "s", WeightPerMille: 1000}
	cases := []struct {
		name     string
		excludes map[string]string
		labels   map[string]string
		want     Owner
	}{
		{"nil selector excludes nothing", nil, map[string]string{"tier": "critical"}, Canary},
		{"empty selector excludes nothing", map[string]string{}, map[string]string{"tier": "critical"}, Canary},
		{"exact match excluded", map[string]string{"tier": "critical"}, map[string]string{"tier": "critical"}, Stable},
		{"superset labels excluded", map[string]string{"tier": "critical"},
			map[string]string{"tier": "critical", "team": "x"}, Stable},
		{"wrong value not excluded", map[string]string{"tier": "critical"},
			map[string]string{"tier": "prod"}, Canary},
		{"absent key not excluded", map[string]string{"tier": "critical"},
			map[string]string{"team": "x"}, Canary},
		{"nil labels not excluded", map[string]string{"tier": "critical"}, nil, Canary},
		{"absent key with empty want not excluded", map[string]string{"tier": ""}, nil, Canary},
		{"present empty value excluded", map[string]string{"tier": ""},
			map[string]string{"tier": ""}, Stable},
	}
	for _, c := range cases {
		spec := base
		spec.ExcludeLabels = c.excludes
		got, err := spec.Owner("any", c.labels)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: Owner = %s, want %s", c.name, got, c.want)
		}
	}
}

// TestOwner_InvalidSpecFailsClosed pins the fail-closed contract:
// an invalid spec denies canary ownership and reports an error.
func TestOwner_InvalidSpecFailsClosed(t *testing.T) {
	spec := Spec{Mode: ModeActive, Seed: "", WeightPerMille: 1000}
	got, err := spec.Owner("any", nil)
	if err == nil {
		t.Fatal("nil error, want non-nil")
	}
	if got != Stable {
		t.Errorf("Owner = %s, want stable (fail closed)", got)
	}
}

// FuzzOwner asserts the evaluator never panics and always returns a
// valid owner for a valid spec, over arbitrary namespace names and
// label keys/values.
func FuzzOwner(f *testing.F) {
	f.Add("sandbox-a", "tier", "critical")
	f.Add("", "", "")
	f.Add("a/b/c", "example.com/key", "v-=+.")
	spec := Spec{
		Mode:           ModeActive,
		Seed:           "9a1f2e",
		WeightPerMille: 500,
		Include:        []string{"sandbox-a"},
		ExcludeLabels:  map[string]string{"tier": "critical"},
	}
	f.Fuzz(func(t *testing.T, namespace, key, value string) {
		owner, err := spec.Owner(namespace, map[string]string{key: value})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if owner != Stable && owner != Canary {
			t.Fatalf("invalid owner %d", int(owner))
		}
	})
}

// testSeed returns the deterministic RNG seed for randomized
// progression trials: SHARDKIT_TEST_SEED overrides the default, and
// the effective seed is always logged so failures reproduce with
// SHARDKIT_TEST_SEED=<n> go test ./pkg/partition/ -run Progression.
func testSeed(t *testing.T) uint64 {
	t.Helper()
	const def = 20260927
	if v := os.Getenv("SHARDKIT_TEST_SEED"); v != "" {
		s, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("bad SHARDKIT_TEST_SEED=%q: %v", v, err)
		}
		t.Logf("seed=%d (from env)", s)
		return s
	}
	t.Logf("seed=%d (default)", def)
	return def
}

// canarySet evaluates full ownership over corpus: every namespace
// lands on exactly one track (disjoint, complete coverage).
func canarySet(t *testing.T, spec Spec, corpus []string, labels map[string]map[string]string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, ns := range corpus {
		owner, err := spec.Owner(ns, labels[ns])
		if err != nil {
			t.Fatalf("Owner(%q): %v", ns, err)
		}
		if owner == Canary {
			out[ns] = true
		}
	}
	return out
}

// TestProgression_RandomizedMonotone walks random valid progressions
// (Off, optional Shadow, rising Active weights, optional abort) over
// a random corpus with random cohorts, asserting the canary set only
// grows until abort, every step passes ValidNext, and ownership stays
// total. Deterministic under the logged seed.
func TestProgression_RandomizedMonotone(t *testing.T) {
	rng := rand.New(rand.NewPCG(testSeed(t), 0x9a1f2e))
	const trials = 200
	for trial := 0; trial < trials; trial++ {
		seed := fmt.Sprintf("seed-%d-%x", trial, rng.Uint64())
		n := 50 + rng.IntN(250)
		corpus := make([]string, n)
		labels := map[string]map[string]string{}
		for i := range corpus {
			corpus[i] = fmt.Sprintf("ns-%d-%d", trial, i)
			if rng.IntN(10) == 0 {
				labels[corpus[i]] = map[string]string{"tier": "critical"}
			}
		}
		var include []string
		for i := 0; i < rng.IntN(4); i++ {
			include = append(include, corpus[rng.IntN(n)])
		}
		var excludes map[string]string
		if rng.IntN(2) == 0 {
			excludes = map[string]string{"tier": "critical"}
		}
		steps := []Spec{{Mode: ModeOff, Seed: seed, WeightPerMille: 0,
			Include: include, ExcludeLabels: excludes}}
		if rng.IntN(2) == 0 {
			steps = append(steps, Spec{Mode: ModeShadow, Seed: seed,
				Include: include, ExcludeLabels: excludes})
		}
		w := 0
		for k := 0; k < 1+rng.IntN(4); k++ {
			if w < MaxWeight {
				w += 1 + rng.IntN(MaxWeight-w)
			}
			steps = append(steps, Spec{Mode: ModeActive, Seed: seed,
				WeightPerMille: w, Include: include, ExcludeLabels: excludes})
		}
		abort := rng.IntN(2) == 0
		if abort {
			steps = append(steps, Spec{Mode: ModeOff, Seed: seed,
				Include: include, ExcludeLabels: excludes})
		}
		var prev map[string]bool
		for s := 1; s < len(steps); s++ {
			if err := steps[s-1].ValidNext(steps[s]); err != nil {
				t.Fatalf("trial %d step %d: ValidNext: %v", trial, s, err)
			}
		}
		for s, spec := range steps {
			got := canarySet(t, spec, corpus, labels)
			if s > 0 && steps[s].Mode != ModeOff {
				for ns := range prev {
					if !got[ns] {
						t.Fatalf("trial %d step %d: %q left canary without abort", trial, s, ns)
					}
				}
			}
			if steps[s].Mode == ModeOff && len(got) != 0 {
				t.Fatalf("trial %d step %d: Off owns %d canary namespaces", trial, s, len(got))
			}
			prev = got
		}
		// A fresh rollout from Off may change seed/cohorts freely.
		fresh := Spec{Mode: ModeActive, Seed: seed + "-next", WeightPerMille: 5}
		last := steps[len(steps)-1]
		if last.Mode == ModeOff {
			if err := last.ValidNext(fresh); err != nil {
				t.Fatalf("trial %d: fresh rollout from Off: %v", trial, err)
			}
		} else if err := last.ValidNext(fresh); err == nil {
			t.Fatalf("trial %d: seed change while %s accepted", trial, last.Mode)
		}
	}
}

// ExampleSpec_Owner shows a 1% rollout with an explicit cohort:
// bucket membership and the include list both select the canary.
func ExampleSpec_Owner() {
	spec := Spec{
		Mode:           ModeActive,
		Seed:           "9a1f2e",
		WeightPerMille: 10,
		Include:        []string{"sandbox-a"},
		ExcludeLabels:  map[string]string{"tier": "critical"},
	}
	for _, ns := range []string{"demo-87", "default"} {
		owner, _ := spec.Owner(ns, nil)
		fmt.Println(ns, owner)
	}
	// Output:
	// demo-87 canary
	// default stable
}
