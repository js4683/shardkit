package v1alpha1

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// i32 boxes an int32 for optional budget caps.
func i32(i int32) *int32 { return &i }

// basePlan returns a valid Active plan; tests mutate one field at a
// time to pin the failing rule ID.
func basePlan() *ShardPlan {
	return &ShardPlan{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "widget-system", Name: "widget-operator",
			UID: types.UID("plan-uid-1"),
		},
		Spec: ShardPlanSpec{
			Key: "namespace", Rollout: "r-001", Epoch: 7, Seed: "9a1f2e",
			Tracks: TrackSet{
				Stable: TrackRevision{Revision: "rev-a"},
				Canary: &TrackRevision{Revision: "rev-b"},
			},
			Canary: CanarySpec{
				Mode: "Active", WeightPerMille: 10,
				Include: IncludeSpec{Namespaces: []string{"sandbox-a"}},
				Exclude: ExcludeSpec{Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"tier": "critical"},
				}},
			},
			SingletonOwner: "stable",
		},
	}
}

// TestValidateCreate_Rules pins V1-V8: each invalid shape fails with
// its rule ID, and the valid base plus mode variants pass.
func TestValidateCreate_Rules(t *testing.T) {
	valid := map[string]func(*ShardPlan){
		"active base": func(*ShardPlan) {},
		"off permits canary revision": func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeOff
			p.Spec.Canary.WeightPerMille = 0
		},
		"off without canary revision": func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeOff
			p.Spec.Canary.WeightPerMille = 0
			p.Spec.Tracks.Canary = nil
		},
		"shadow": func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeShadow
			p.Spec.Canary.WeightPerMille = 0
		},
		"singleton canary with revision": func(p *ShardPlan) {
			p.Spec.SingletonOwner = TrackCanary
		},
		"nil exclude selector": func(p *ShardPlan) {
			p.Spec.Canary.Exclude.Selector = nil
		},
		"empty cohorts": func(p *ShardPlan) {
			p.Spec.Canary.Include.Namespaces = nil
			p.Spec.Canary.Exclude.Selector = nil
		},
		"weight boundaries": func(p *ShardPlan) {
			p.Spec.Canary.WeightPerMille = 1000
		},
		"nil budget": func(*ShardPlan) {},
		"zero caps": func(p *ShardPlan) {
			p.Spec.Budget = &BudgetSpec{MaxDeletions: i32(0), MaxDeletionPercent: i32(0)}
		},
		"max caps": func(p *ShardPlan) {
			p.Spec.Budget = &BudgetSpec{MaxDeletions: i32(1000), MaxDeletionPercent: i32(100)}
		},
	}
	for name, mutate := range valid {
		p := basePlan()
		mutate(p)
		if err := p.ValidateCreate(); err != nil {
			t.Errorf("%s: unexpected error: %v", name, err)
		}
	}

	invalid := []struct {
		name   string
		rule   string
		mutate func(*ShardPlan)
	}{
		{"bad key", "V1", func(p *ShardPlan) { p.Spec.Key = "cluster" }},
		{"empty key", "V1", func(p *ShardPlan) { p.Spec.Key = "" }},
		{"empty seed", "V2", func(p *ShardPlan) { p.Spec.Seed = "" }},
		{"bad mode", "V3", func(p *ShardPlan) { p.Spec.Canary.Mode = "Warp" }},
		{"empty mode", "V3", func(p *ShardPlan) { p.Spec.Canary.Mode = "" }},
		{"negative weight", "V4", func(p *ShardPlan) { p.Spec.Canary.WeightPerMille = -1 }},
		{"weight over mille", "V4", func(p *ShardPlan) { p.Spec.Canary.WeightPerMille = 1001 }},
		{"off with weight", "V4", func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeOff
			p.Spec.Canary.WeightPerMille = 10
		}},
		{"shadow with weight", "V4", func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeShadow
			p.Spec.Canary.WeightPerMille = 5
		}},
		{"empty stable revision", "V5", func(p *ShardPlan) { p.Spec.Tracks.Stable.Revision = "" }},
		{"bad stable revision", "V5", func(p *ShardPlan) { p.Spec.Tracks.Stable.Revision = "Bad_Label!" }},
		{"active missing canary", "V6", func(p *ShardPlan) { p.Spec.Tracks.Canary = nil }},
		{"shadow missing canary", "V6", func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeShadow
			p.Spec.Canary.WeightPerMille = 0
			p.Spec.Tracks.Canary = nil
		}},
		{"empty canary revision", "V6", func(p *ShardPlan) { p.Spec.Tracks.Canary.Revision = "" }},
		{"bad canary revision", "V6", func(p *ShardPlan) { p.Spec.Tracks.Canary.Revision = "UPPER" }},
		{"bad singleton owner", "V7", func(p *ShardPlan) { p.Spec.SingletonOwner = "both" }},
		{"empty singleton owner", "V7", func(p *ShardPlan) { p.Spec.SingletonOwner = "" }},
		{"singleton canary without revision", "V7", func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeOff
			p.Spec.Canary.WeightPerMille = 0
			p.Spec.Tracks.Canary = nil
			p.Spec.SingletonOwner = TrackCanary
		}},
		{"matchExpressions", "V8", func(p *ShardPlan) {
			p.Spec.Canary.Exclude.Selector.MatchExpressions =
				[]metav1.LabelSelectorRequirement{{Key: "tier", Operator: "In"}}
		}},
		{"negative maxDeletions", "V11", func(p *ShardPlan) {
			p.Spec.Budget = &BudgetSpec{MaxDeletions: i32(-1)}
		}},
		{"negative percent", "V11", func(p *ShardPlan) {
			p.Spec.Budget = &BudgetSpec{MaxDeletionPercent: i32(-1)}
		}},
		{"percent over 100", "V11", func(p *ShardPlan) {
			p.Spec.Budget = &BudgetSpec{MaxDeletionPercent: i32(101)}
		}},
	}
	for _, c := range invalid {
		p := basePlan()
		c.mutate(p)
		err := p.ValidateCreate()
		if err == nil {
			t.Errorf("%s: nil error, want %s", c.name, c.rule)
			continue
		}
		if !strings.Contains(err.Error(), c.rule) {
			t.Errorf("%s: error %q lacks rule ID %s", c.name, err, c.rule)
		}
	}
}

// TestValidateUpdate_Transitions pins V9 plus the writer epoch rule:
// frozen-tuple changes need a fresh rollout ID, spec changes need a
// higher epoch, and bookkeeping writes pass through.
func TestValidateUpdate_Transitions(t *testing.T) {
	base := basePlan()
	clone := func(mutate func(*ShardPlan)) *ShardPlan {
		p := base.DeepCopy()
		mutate(p)
		return p
	}
	valid := map[string]*ShardPlan{
		"weight growth with bump": clone(func(p *ShardPlan) {
			p.Spec.Canary.WeightPerMille = 500
			p.Spec.Epoch = 8
		}),
		"abort with bump": clone(func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeOff
			p.Spec.Canary.WeightPerMille = 0
			p.Spec.Epoch = 8
		}),
		"no-op rewrite same epoch": clone(func(*ShardPlan) {}),
		"rollout relabel same tuple": clone(func(p *ShardPlan) {
			p.Spec.Rollout = "r-001-relabel"
		}),
		"fresh rollout new seed": clone(func(p *ShardPlan) {
			p.Spec.Seed = "other"
			p.Spec.Rollout = "r-002"
			p.Spec.Epoch = 8
		}),
		"fresh rollout new cohorts": clone(func(p *ShardPlan) {
			p.Spec.Canary.Include.Namespaces = []string{"sandbox-b"}
			p.Spec.Rollout = "r-002"
			p.Spec.Epoch = 8
		}),
		"include reorder is bookkeeping": clone(func(p *ShardPlan) {
			p.Spec.Canary.Include.Namespaces = []string{"sandbox-a", "sandbox-a"}
		}),
	}
	for name, next := range valid {
		if err := next.ValidateUpdate(base); err != nil {
			t.Errorf("%s: unexpected error: %v", name, err)
		}
	}

	invalid := []struct {
		name string
		next *ShardPlan
	}{
		{"seed change same rollout", clone(func(p *ShardPlan) {
			p.Spec.Seed = "other"
			p.Spec.Epoch = 8
		})},
		{"include change same rollout", clone(func(p *ShardPlan) {
			p.Spec.Canary.Include.Namespaces = []string{"sandbox-a", "sandbox-b"}
			p.Spec.Epoch = 8
		})},
		{"exclude change same rollout", clone(func(p *ShardPlan) {
			p.Spec.Canary.Exclude.Selector.MatchLabels = map[string]string{"tier": "prod"}
			p.Spec.Epoch = 8
		})},
		{"key change rejected as V1", clone(func(p *ShardPlan) {
			p.Spec.Key = "cluster"
			p.Spec.Epoch = 8
		})},
		{"weight growth without bump", clone(func(p *ShardPlan) {
			p.Spec.Canary.WeightPerMille = 500
		})},
		{"weight growth with lower epoch", clone(func(p *ShardPlan) {
			p.Spec.Canary.WeightPerMille = 500
			p.Spec.Epoch = 6
		})},
		{"mode change without bump", clone(func(p *ShardPlan) {
			p.Spec.Canary.Mode = ModeOff
			p.Spec.Canary.WeightPerMille = 0
		})},
		{"revision change without bump", clone(func(p *ShardPlan) {
			p.Spec.Tracks.Canary.Revision = "rev-c"
		})},
		{"new spec invalid", clone(func(p *ShardPlan) {
			p.Spec.Canary.WeightPerMille = 1001
			p.Spec.Epoch = 8
		})},
	}
	for _, c := range invalid {
		if err := c.next.ValidateUpdate(base); err == nil {
			t.Errorf("%s: nil error, want non-nil", c.name)
		}
	}
	// nil old plan degrades to create validation.
	if err := base.ValidateUpdate(nil); err != nil {
		t.Errorf("nil old: unexpected error: %v", err)
	}
	bad := base.DeepCopy()
	bad.Spec.Key = "cluster"
	if err := bad.ValidateUpdate(nil); err == nil {
		t.Error("nil old with bad spec: nil error, want V1")
	}
}

// TestValidateUpdate_SelectorNormalization pins nil-vs-empty
// exclude equivalence: both exclude nothing, so swapping them is
// bookkeeping, while dropping a real selector is a V9 tuple change.
func TestValidateUpdate_SelectorNormalization(t *testing.T) {
	plain := basePlan()
	plain.Spec.Canary.Exclude.Selector = nil
	empty := plain.DeepCopy()
	empty.Spec.Canary.Exclude.Selector = &metav1.LabelSelector{}
	if err := empty.ValidateUpdate(plain); err != nil {
		t.Errorf("nil to empty selector: unexpected error: %v", err)
	}
	dropped := basePlan().DeepCopy()
	dropped.Spec.Canary.Exclude.Selector = &metav1.LabelSelector{}
	dropped.Spec.Epoch = 8
	if err := dropped.ValidateUpdate(basePlan()); err == nil {
		t.Error("dropped exclude same rollout: nil error, want V9")
	} else if !strings.Contains(err.Error(), "V9") {
		t.Errorf("dropped exclude: error %q lacks V9", err)
	}
}

// TestShardPlan_StaleFor pins V10: same UID without a newer
// (epoch, generation) is stale; a newer epoch, a same-epoch newer
// generation, or a new UID is not.
func TestShardPlan_StaleFor(t *testing.T) {
	p := basePlan() // UID plan-uid-1, epoch 7, generation 3
	p.Generation = 3
	cases := []struct {
		name  string
		uid   string
		epoch int64
		gen   int64
		want  bool
	}{
		{"same version is stale", "plan-uid-1", 7, 3, true},
		{"older epoch is stale", "plan-uid-1", 9, 0, true},
		{"newer epoch is fresh", "plan-uid-1", 6, 99, false},
		{"same epoch newer generation is fresh", "plan-uid-1", 7, 2, false},
		{"same epoch older generation is stale", "plan-uid-1", 7, 4, true},
		{"new UID is fresh", "plan-uid-2", 100, 100, false},
	}
	for _, c := range cases {
		if got := p.StaleFor(c.uid, c.epoch, c.gen); got != c.want {
			t.Errorf("%s: StaleFor = %v, want %v", c.name, got, c.want)
		}
	}
}
