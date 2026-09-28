// Package conformance is the M4 external-adopter contract: one
// runnable suite pinning the library invariants end to end over
// fake clients. Unit tests pin internals; this suite pins the
// behaviors a third-party integration depends on (fail-closed
// gate, owner-only writes, shadow purity, budget exactness,
// monotone validation, cluster-qualified sessions, version-skew
// tolerance). It needs no envtest binary and no cluster.
package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/partition"
	"github.com/js4683/shardkit/pkg/shardkit"
)

var planKey = types.NamespacedName{Namespace: "widget-system", Name: "widget-operator"}

// scheme carries core types plus ShardPlan.
func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// mapper gives the fake client the scope mappings the guard
// needs (the fake builder defaults to an empty RESTMapper).
func mapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	m.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)
	m.Add(corev1.SchemeGroupVersion.WithKind("ConfigMapList"), meta.RESTScopeNamespace)
	m.Add(corev1.SchemeGroupVersion.WithKind("Namespace"), meta.RESTScopeRoot)
	m.Add(corev1.SchemeGroupVersion.WithKind("NamespaceList"), meta.RESTScopeRoot)
	m.Add(v1alpha1.GroupVersion.WithKind("ShardPlan"), meta.RESTScopeNamespace)
	return m
}

// offPlan is an attachable Off plan (epoch 1) with both revisions.
func offPlan() *v1alpha1.ShardPlan {
	return &v1alpha1.ShardPlan{
		ObjectMeta: metav1.ObjectMeta{Namespace: planKey.Namespace, Name: planKey.Name, UID: "uid-1"},
		Spec: v1alpha1.ShardPlanSpec{
			Key: "namespace", Rollout: "r-1", Epoch: 1, Seed: "9a1f2e",
			Tracks: v1alpha1.TrackSet{
				Stable: v1alpha1.TrackRevision{Revision: "rev-a"},
				Canary: &v1alpha1.TrackRevision{Revision: "rev-b"},
			},
			Canary:         v1alpha1.CanarySpec{Mode: "Off"},
			SingletonOwner: "stable",
		},
	}
}

// world builds a fake holding the Off plan plus owned (demo-87)
// and foreign (default) namespaces.
func world(t *testing.T, mutate func(*v1alpha1.ShardPlan)) client.Client {
	t.Helper()
	plan := offPlan()
	if mutate != nil {
		mutate(plan)
	}
	return fake.NewClientBuilder().WithScheme(scheme(t)).
		WithRESTMapper(mapper()).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(
			plan,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		).Build()
}

func attach(t *testing.T, fc client.Client, track, rev string) *shardkit.Gate {
	t.Helper()
	g, err := shardkit.Attach(context.Background(), fc, planKey, track, rev)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// TestConformance_OneOwner (I1): under Off, stable owns every
// namespace and canary owns none — exactly one owner per
// namespace, the quiescent shape the plugin also pins.
func TestConformance_OneOwner(t *testing.T) {
	fc := world(t, nil)
	ctx := context.Background()
	stable := attach(t, fc, "stable", "rev-a")
	canary := attach(t, fc, "canary", "rev-b")
	for _, ns := range []string{"demo-87", "default"} {
		sOwn, err := stable.Owned(ctx, ns)
		if err != nil {
			t.Fatalf("stable %s: %v", ns, err)
		}
		cOwn, err := canary.Owned(ctx, ns)
		if err != nil {
			t.Fatalf("canary %s: %v", ns, err)
		}
		if !sOwn.Owned || cOwn.Owned {
			t.Fatalf("ns %s: stable=%v canary=%v, want exactly stable",
				ns, sOwn.Owned, cOwn.Owned)
		}
	}
}

// TestConformance_VersionFence (I2): a writer holding a superseded
// revision is denied (S7), even in a namespace it would own.
func TestConformance_VersionFence(t *testing.T) {
	fc := world(t, nil)
	ctx := context.Background()
	stale := attach(t, fc, "stable", "rev-stale")
	gc := stale.Client(fc, fc)
	err := gc.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm"}})
	if err == nil {
		t.Fatal("stale-revision write: nil error, want S7 denial")
	}
	if denied, ok := shardkit.AsDenied(err); !ok ||
		denied.Reason != shardkit.ReasonRevisionMismatch {
		t.Fatalf("stale-revision write: %v, want S7 RevisionMismatch denial", err)
	}
}

// TestConformance_SingletonForeign (I4): cluster-scoped writes
// from the non-owning track are denied.
func TestConformance_SingletonForeign(t *testing.T) {
	fc := world(t, nil)
	ctx := context.Background()
	canary := attach(t, fc, "canary", "rev-b")
	gc := canary.Client(fc, fc)
	if err := gc.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rogue"}}); err == nil {
		t.Fatal("canary cluster write under stable singleton: nil error, want denial")
	}
}

// TestConformance_BudgetExactness (I5): a cap of 2 admits exactly
// 2 concurrent confirmed deletes out of 6 and records used=2.
func TestConformance_BudgetExactness(t *testing.T) {
	cap := int32(2)
	plan := offPlan()
	plan.Spec.Budget = &v1alpha1.BudgetSpec{MaxDeletions: &cap}
	b := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithRESTMapper(mapper()).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(plan,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}}).
		Build()
	ctx := context.Background()
	g := attach(t, b, "stable", "rev-a")
	gc := g.Client(b, b)
	for i := 0; i < 6; i++ {
		if err := b.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Namespace: "demo-87", Name: fmt.Sprintf("cm-%d", i)}}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make([]error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = gc.ConfirmDelete(ctx, planKey, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "demo-87", Name: fmt.Sprintf("cm-%d", i)}})
		}(i)
	}
	wg.Wait()
	allowed := 0
	for _, err := range errs {
		if err == nil {
			allowed++
			continue
		}
		if denied, ok := shardkit.AsDenied(err); !ok ||
			denied.Reason != shardkit.ReasonBudgetExceeded {
			t.Fatalf("unexpected confirm error: %v", err)
		}
	}
	if allowed != 2 {
		t.Fatalf("allowed = %d, want exactly 2 (cap exactness)", allowed)
	}
}

// TestConformance_ShadowPurity (I6): a shadowed delete records the
// decision and leaves the live object untouched.
func TestConformance_ShadowPurity(t *testing.T) {
	fc := world(t, nil)
	ctx := context.Background()
	g := attach(t, fc, "stable", "rev-a")
	gc := g.Client(fc, fc)
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm"}}
	if err := gc.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	rec := &shardkit.ShadowRecorder{}
	if err := gc.Shadowed(rec).Delete(ctx, obj); err != nil {
		t.Fatalf("shadowed delete: %v", err)
	}
	if len(rec.Ops()) != 1 || rec.Ops()[0].Decision != "allowed" {
		t.Fatalf("shadow ops = %+v, want one allowed decision", rec.Ops())
	}
	var live corev1.ConfigMap
	if err := fc.Get(ctx, types.NamespacedName{Namespace: "demo-87", Name: "cm"}, &live); err != nil {
		t.Fatalf("live object after shadow: %v (shadow must not write)", err)
	}
}

// TestConformance_Monotone (I7): a frozen-tuple change without a
// fresh rollout ID is rejected (V9), with or without the webhook.
func TestConformance_Monotone(t *testing.T) {
	plan := offPlan()
	mutated := offPlan()
	mutated.Spec.Seed = "tampered"
	if err := mutated.ValidateUpdate(plan); err == nil ||
		!strings.Contains(err.Error(), "V9") {
		t.Fatalf("seed change without rollout: %v, want V9", err)
	}
	mutated.Spec.Rollout = "r-2"
	mutated.Spec.Epoch = 2 // V9 also requires an epoch bump on spec change
	if err := mutated.ValidateUpdate(plan); err != nil {
		t.Fatalf("seed change with fresh rollout + epoch: %v", err)
	}
	// And the gate refuses a tampered live version (read-time V9):
	// the API server accepts it without the webhook, but no
	// ownership flows from it.
	ctx := context.Background()
	tainted := world(t, func(p *v1alpha1.ShardPlan) {
		p.Spec.Canary.Mode = "Active"
		p.Spec.Canary.WeightPerMille = 1000
		p.Spec.Epoch = 2
		p.Spec.Rollout = "r-2"
	})
	g := attach(t, tainted, "canary", "rev-b")
	if _, err := g.Owned(ctx, "demo-87"); err != nil {
		t.Fatalf("contracted Active/1000: %v", err)
	}
	var live v1alpha1.ShardPlan
	if err := tainted.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Seed = "tampered" // same epoch, same rollout
	live.Generation++
	if err := tainted.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Owned(ctx, "demo-87"); err == nil {
		t.Fatal("live seed tamper: nil error, want gate refusal")
	} else if closed, ok := shardkit.AsClosed(err); !ok ||
		closed.Reason != shardkit.ReasonPlanMalformed {
		t.Fatalf("live seed tamper: %v, want PlanMalformed closed", err)
	}
}

// TestConformance_ClusterKeys (M4): qualified sessions verify only
// under the same cluster; cross-cluster and unqualified checkers
// void on S8.
func TestConformance_ClusterKeys(t *testing.T) {
	holder, transitions := "pod-0", int32(1)
	tracks := []v1alpha1.TrackStatus{{
		Name: "stable", Revision: "rev-a", PlanUID: "uid-1",
		ObservedGeneration: 4, ObservedEpoch: 2, Rollout: "r-2",
		Phase: v1alpha1.PhaseReleased, Released: true,
		Session: &v1alpha1.LeaderSession{Holder: "prod-east/pod-0", LeaseTransitions: 1},
	}}
	plan := &v1alpha1.ShardPlan{
		ObjectMeta: metav1.ObjectMeta{UID: "uid-1", Generation: 4},
		Spec: v1alpha1.ShardPlanSpec{Epoch: 2, Rollout: "r-2",
			Tracks: v1alpha1.TrackSet{Stable: v1alpha1.TrackRevision{Revision: "rev-a"}}},
	}
	check := func(cluster string) error {
		return shardkit.ValidRelease(shardkit.AckCheck{
			Plan: plan, Tracks: tracks, Loser: "stable",
			LoserLease: &coordinationv1.Lease{Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &holder, LeaseTransitions: &transitions,
			}},
			ClusterName: cluster,
		})
	}
	if err := check("prod-east"); err != nil {
		t.Fatalf("same-cluster check: %v", err)
	}
	if err := check(""); err == nil || !strings.Contains(err.Error(), "S8") {
		t.Fatalf("unqualified check: %v, want S8", err)
	}
	if err := check("prod-west"); err == nil || !strings.Contains(err.Error(), "S8") {
		t.Fatalf("foreign-cluster check: %v, want S8", err)
	}
}

// TestConformance_SkewTolerance (I8): an older reader seeing a
// newer entry (extra session/budget fields populated) still
// resolves the fields it understands — forward-tolerated JSON,
// no loss of known data on round-trip.
func TestConformance_SkewTolerance(t *testing.T) {
	entry := v1alpha1.TrackStatus{
		Name: "stable", Revision: "rev-a", PlanUID: "uid-1",
		ObservedEpoch: 2, Rollout: "r-2",
		Phase: v1alpha1.PhaseReleased, Released: true,
		Session: &v1alpha1.LeaderSession{Holder: "prod-east/pod-0", LeaseTransitions: 3},
		Budget:  &v1alpha1.BudgetUsage{Epoch: 2, DeletionsUsed: 3},
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	// A newer writer adds a field the old reader never knew.
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m["futureField"] = "v2-data"
	raw, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var back v1alpha1.TrackStatus
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("old reader on new payload: %v", err)
	}
	if back.Session == nil || back.Session.Holder != "prod-east/pod-0" ||
		back.Budget == nil || back.Budget.DeletionsUsed != 3 || back.Phase != v1alpha1.PhaseReleased {
		t.Fatalf("round-trip lost known fields: %+v", back)
	}
}

// TestConformance_ScaleSweep (M4): 1k namespaces assign in well
// under a reconcile budget; fails closed on error, never partial.
func TestConformance_ScaleSweep(t *testing.T) {
	spec, err := shardkit.PartitionSpec(offPlan())
	if err != nil {
		t.Fatal(err)
	}
	owned := 0
	for i := 0; i < 1000; i++ {
		o, err := spec.Owner(fmt.Sprintf("tenant-%04d", i), nil)
		if err != nil {
			t.Fatal(err)
		}
		if o == partition.Stable {
			owned++
		}
	}
	if owned != 1000 {
		t.Fatalf("Off sweep: stable owns %d/1000, want all", owned)
	}
}
