package v1alpha1

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestDeepCopy_RoundTrip proves the generated copies are equal and
// independent: mutating the copy leaves the original alone.
func TestDeepCopy_RoundTrip(t *testing.T) {
	plan := &ShardPlan{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", Labels: map[string]string{"a": "b"}},
		Spec: ShardPlanSpec{
			Key: "namespace", Rollout: "r-1", Epoch: 7, Seed: "s",
			Tracks: TrackSet{
				Stable: TrackRevision{Revision: "aaa"},
				Canary: &TrackRevision{Revision: "bbb"},
			},
			Canary: CanarySpec{
				Mode: "Active", WeightPerMille: 10,
				Include: IncludeSpec{Namespaces: []string{"x"}},
				Exclude: ExcludeSpec{Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"tier": "critical"},
				}},
			},
			SingletonOwner: "stable",
		},
		Status: ShardPlanStatus{Tracks: []TrackStatus{{
			Name: "stable", Revision: "aaa", PlanUID: "uid",
			ObservedGeneration: 3, ObservedEpoch: 7, Rollout: "r-1",
			Phase: "Acquired", Released: true,
			ReleasedWrites:  map[string]string{"widgets.shardkit.dev": "42"},
			Session:         &LeaderSession{Holder: "pod-0", LeaseTransitions: 1},
			OwnedNamespaces: 4,
			Conditions:      []metav1.Condition{{Type: "Degraded", Status: "False"}},
		}}},
	}
	dup := plan.DeepCopy()
	if !reflect.DeepEqual(plan, dup) {
		t.Fatal("ShardPlan copy differs from original")
	}
	dup.Labels["a"] = "mut"
	dup.Spec.Tracks.Canary.Revision = "mut"
	dup.Spec.Canary.Include.Namespaces[0] = "mut"
	dup.Spec.Canary.Exclude.Selector.MatchLabels["tier"] = "mut"
	dup.Status.Tracks[0].ReleasedWrites["widgets.shardkit.dev"] = "mut"
	dup.Status.Tracks[0].Session.Holder = "mut"
	dup.Status.Tracks[0].Conditions[0].Status = "True"
	if reflect.DeepEqual(plan, dup) {
		t.Fatal("ShardPlan copy shares memory with original")
	}
	if plan.DeepCopyObject() == nil || (&ShardPlanList{}).DeepCopyObject() == nil {
		t.Fatal("nil DeepCopyObject")
	}

	widget := &Widget{
		ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns"},
		Spec:       WidgetSpec{Value: "v"},
		Status: WidgetStatus{OwnerTrack: "stable", OwnerRevision: "aaa", Writes: 3,
			Conditions: []metav1.Condition{{Type: "Ready", Status: "True"}}},
	}
	wdup := widget.DeepCopy()
	if !reflect.DeepEqual(widget, wdup) {
		t.Fatal("Widget copy differs from original")
	}
	wdup.Status.Conditions[0].Status = "False"
	if reflect.DeepEqual(widget, wdup) {
		t.Fatal("Widget copy shares memory with original")
	}
	if widget.DeepCopyObject() == nil || (&WidgetList{}).DeepCopyObject() == nil {
		t.Fatal("nil DeepCopyObject")
	}
}
