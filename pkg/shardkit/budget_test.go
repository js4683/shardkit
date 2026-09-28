package shardkit

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// budgetFixture builds an Off plan (epoch 1) with demo-87/default
// namespaces and two ConfigMaps, attaching track with its spec
// revision. mutate adjusts the plan (budget caps) before attach.
func budgetFixture(t *testing.T, track, rev string, mutate func(*v1alpha1.ShardPlan)) (*GuardedClient, client.Client) {
	t.Helper()
	plan := baseOffPlan()
	if mutate != nil {
		mutate(plan)
	}
	fc := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithRESTMapper(testRestMapper()).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(
			plan,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm1"}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm2"}},
		).Build()
	return newGuardedRev(t, track, rev, fc)
}

func withMaxDeletions(n int32) func(*v1alpha1.ShardPlan) {
	return func(p *v1alpha1.ShardPlan) {
		p.Spec.Budget = &v1alpha1.BudgetSpec{MaxDeletions: &n}
	}
}

func cmExists(t *testing.T, c client.Client, ns, name string) bool {
	t.Helper()
	var cm corev1.ConfigMap
	err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &cm)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

func entryBudget(t *testing.T, c client.Client) *v1alpha1.BudgetUsage {
	t.Helper()
	var live v1alpha1.ShardPlan
	if err := c.Get(context.Background(), planKey, &live); err != nil {
		t.Fatal(err)
	}
	for i := range live.Status.Tracks {
		if live.Status.Tracks[i].Name == v1alpha1.TrackStable {
			return live.Status.Tracks[i].Budget
		}
	}
	return nil
}

func confirmCM(t *testing.T, gc *GuardedClient, ns, name string) error {
	t.Helper()
	return gc.ConfirmDelete(context.Background(), planKey,
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}})
}

// TestConfirmDelete_NoBudget pins backward compatibility: without
// caps the confirmed delete confirms (direct read + UID) and
// deletes, charging nothing.
func TestConfirmDelete_NoBudget(t *testing.T) {
	gc, raw := budgetFixture(t, v1alpha1.TrackStable, "rev-a", nil)
	if err := confirmCM(t, gc, "demo-87", "cm1"); err != nil {
		t.Fatalf("confirm delete: %v", err)
	}
	if cmExists(t, raw, "demo-87", "cm1") {
		t.Fatal("cm1 still exists, want deleted")
	}
	if b := entryBudget(t, raw); b != nil {
		t.Fatalf("budget = %+v, want no charge without caps", b)
	}
}

// TestConfirmDelete_AbsoluteTrips pins the absolute cap: one
// delete charges used=1, the second denies BudgetExceeded and the
// object survives.
func TestConfirmDelete_AbsoluteTrips(t *testing.T) {
	gc, raw := budgetFixture(t, v1alpha1.TrackStable, "rev-a", withMaxDeletions(1))
	if err := confirmCM(t, gc, "demo-87", "cm1"); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if b := entryBudget(t, raw); b == nil || b.Epoch != 1 || b.DeletionsUsed != 1 {
		t.Fatalf("budget = %+v, want {1 1}", b)
	}
	err := confirmCM(t, gc, "demo-87", "cm2")
	if denied, ok := AsDenied(err); !ok || denied.Reason != ReasonBudgetExceeded {
		t.Fatalf("err = %v, want BudgetExceeded", err)
	}
	if !cmExists(t, raw, "demo-87", "cm2") {
		t.Fatal("cm2 deleted despite trip, want surviving")
	}
}

// TestConfirmDelete_PercentTrips pins the percentage cap over the
// track-owned denominator: two namespaces owned, 50% admits the
// first delete and denies the second.
func TestConfirmDelete_PercentTrips(t *testing.T) {
	pct := int32(50)
	gc, _ := budgetFixture(t, v1alpha1.TrackStable, "rev-a", func(p *v1alpha1.ShardPlan) {
		p.Spec.Budget = &v1alpha1.BudgetSpec{MaxDeletionPercent: &pct}
	})
	if err := confirmCM(t, gc, "demo-87", "cm1"); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	err := confirmCM(t, gc, "demo-87", "cm2")
	if denied, ok := AsDenied(err); !ok || denied.Reason != ReasonBudgetExceeded {
		t.Fatalf("err = %v, want BudgetExceeded", err)
	}
}

// TestConfirmDelete_WindowResets pins the epoch reset policy: a
// new epoch reads the old charge as zero and charges fresh.
func TestConfirmDelete_WindowResets(t *testing.T) {
	gc, raw := budgetFixture(t, v1alpha1.TrackStable, "rev-a", withMaxDeletions(1))
	if err := confirmCM(t, gc, "demo-87", "cm1"); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	var live v1alpha1.ShardPlan
	if err := raw.Get(context.Background(), planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	if err := raw.Update(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if err := confirmCM(t, gc, "demo-87", "cm2"); err != nil {
		t.Fatalf("post-epoch delete: %v", err)
	}
	if b := entryBudget(t, raw); b == nil || b.Epoch != 2 || b.DeletionsUsed != 1 {
		t.Fatalf("budget = %+v, want {2 1}", b)
	}
}

// TestConfirmDelete_NotFoundIdempotent pins the already-gone path:
// no charge, no error.
func TestConfirmDelete_NotFoundIdempotent(t *testing.T) {
	gc, raw := budgetFixture(t, v1alpha1.TrackStable, "rev-a", withMaxDeletions(1))
	if err := confirmCM(t, gc, "demo-87", "cm9"); err != nil {
		t.Fatalf("missing delete: %v", err)
	}
	if b := entryBudget(t, raw); b != nil {
		t.Fatalf("budget = %+v, want no charge for a no-op", b)
	}
}

// TestConfirmDelete_ForeignDenied pins authorization first: the
// other track is denied before any read, charge, or delete.
func TestConfirmDelete_ForeignDenied(t *testing.T) {
	gc, raw := budgetFixture(t, v1alpha1.TrackCanary, "rev-b", withMaxDeletions(10))
	err := confirmCM(t, gc, "demo-87", "cm1")
	if _, ok := AsDenied(err); !ok {
		t.Fatalf("err = %v, want *DeniedError", err)
	}
	if !cmExists(t, raw, "demo-87", "cm1") {
		t.Fatal("cm1 deleted by foreign track, want surviving")
	}
}

// TestConfirmDelete_ReadFailureRequeues pins FA5.3: when the
// direct confirm-read fails, the delete is refused with a plain
// (requeueable) error — never a denial, never a charge, never a
// delete. Only the victim's reads fail; plan and namespace reads
// behave, isolating the confirm leg.
func TestConfirmDelete_ReadFailureRequeues(t *testing.T) {
	plan := baseOffPlan()
	fc := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithRESTMapper(testRestMapper()).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(
			plan,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm1"}},
		).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == "cm1" {
					return errors.New("simulated API partition")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	gc, raw := newGuardedRev(t, v1alpha1.TrackStable, "rev-a", fc)
	err := gc.ConfirmDelete(context.Background(), planKey,
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm1"}})
	if err == nil {
		t.Fatal("nil error, want the read failure")
	}
	if _, ok := AsDenied(err); ok {
		t.Fatalf("err = %v, want plain (requeueable) error, not denial", err)
	}
	// Get is poisoned for cm1, so prove survival through List.
	var list corev1.ConfigMapList
	if err := raw.List(context.Background(), &list, client.InNamespace("demo-87")); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != "cm1" {
		t.Fatalf("list = %+v, want surviving cm1", list.Items)
	}
	if b := entryBudget(t, raw); b != nil {
		t.Fatalf("budget = %+v, want no charge on failed confirm", b)
	}
}

// TestOwnedNamespaceCount_EmptyDenies documents the FA5.2 floor:
// with no namespaces listed the denominator is zero, which the
// percentage check treats as deny.
func TestOwnedNamespaceCount_EmptyDenies(t *testing.T) {
	plan := baseOffPlan()
	fc := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithRESTMapper(testRestMapper()).
		WithObjects(plan).Build()
	gc, _ := newGuardedRev(t, v1alpha1.TrackStable, "rev-a", fc)
	n, err := gc.ownedNamespaceCount(context.Background(), plan)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("denominator = %d, want 0", n)
	}
}

// TestPublishOwnEntry_PreservesBudget pins the shared-entry
// protocol: an ack publish carries forward a charge it did not
// make, so publishes and charges never wipe each other.
func TestPublishOwnEntry_PreservesBudget(t *testing.T) {
	ctx := context.Background()
	fc := publishFixture(t)
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Status.Tracks = []v1alpha1.TrackStatus{{
		Name: "stable", Revision: "rev-a", PlanUID: "uid-1",
		ObservedGeneration: 0, ObservedEpoch: 2, Rollout: "r-1",
		Phase: v1alpha1.PhaseReleased, Released: true,
		Budget: &v1alpha1.BudgetUsage{Epoch: 2, DeletionsUsed: 3},
	}}
	if err := fc.Status().Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	opts := AckOptions{
		Client: fc, APIReader: fc, PlanKey: planKey,
		Track: "stable", Revision: "rev-a",
		Version: PlanVersion{UID: "uid-1", Generation: 0, Epoch: 2, Rollout: "r-1"},
		Phase:   v1alpha1.PhaseReleased, Released: true, OwnedNamespaces: 5,
		LeaseName: "widget-operator-stable", LeaseNamespace: planKey.Namespace,
	}
	if err := PublishOwnEntry(ctx, opts); err != nil {
		t.Fatal(err)
	}
	var after v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &after); err != nil {
		t.Fatal(err)
	}
	if len(after.Status.Tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(after.Status.Tracks))
	}
	e := after.Status.Tracks[0]
	if e.OwnedNamespaces != 5 {
		t.Fatalf("owned = %d, want republished 5", e.OwnedNamespaces)
	}
	if e.Budget == nil || e.Budget.Epoch != 2 || e.Budget.DeletionsUsed != 3 {
		t.Fatalf("budget = %+v, want preserved {2 3}", e.Budget)
	}
}
