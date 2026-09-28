package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

// updateCounter counts status writes through the wrapped writer.
type updateCounter struct {
	client.SubResourceWriter
	n *int
}

func (u updateCounter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	*u.n++
	return u.SubResourceWriter.Update(ctx, obj, opts...)
}

// countingClient delegates to the fake client but counts status
// writes, so the test observes write calls rather than inferring
// them from resourceVersions.
type countingClient struct {
	client.Client
	w client.SubResourceWriter
}

func (c countingClient) Status() client.SubResourceWriter { return c.w }

var widgetPlanKey = types.NamespacedName{Namespace: "widget-system", Name: "widget-operator"}

// widgetFixture builds an Off plan, one namespace, and one unstamped
// widget, with the mapper the guarded client needs.
func widgetFixture(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("Namespace"), meta.RESTScopeRoot)
	mapper.Add(corev1.SchemeGroupVersion.WithKind("NamespaceList"), meta.RESTScopeRoot)
	mapper.Add(v1alpha1.GroupVersion.WithKind("Widget"), meta.RESTScopeNamespace)
	mapper.Add(v1alpha1.GroupVersion.WithKind("ShardPlan"), meta.RESTScopeNamespace)
	return fake.NewClientBuilder().WithScheme(scheme).WithRESTMapper(mapper).
		WithStatusSubresource(&v1alpha1.Widget{}, &v1alpha1.ShardPlan{}).
		WithObjects(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-00"}},
			&v1alpha1.Widget{ObjectMeta: metav1.ObjectMeta{Name: "w0", Namespace: "demo-00"}},
			&v1alpha1.ShardPlan{
				ObjectMeta: metav1.ObjectMeta{Namespace: widgetPlanKey.Namespace, Name: widgetPlanKey.Name},
				Spec: v1alpha1.ShardPlanSpec{
					Key: "namespace", Rollout: "r-1", Epoch: 1, Seed: "9a1f2e",
					Tracks: v1alpha1.TrackSet{
						Stable: v1alpha1.TrackRevision{Revision: "rev-a"},
						Canary: &v1alpha1.TrackRevision{Revision: "rev-b"},
					},
					Canary:         v1alpha1.CanarySpec{Mode: "Off"},
					SingletonOwner: "stable",
				},
			},
		).Build()
}

func widgetReconciler(t *testing.T, c client.Client, writes *int, track, rev string) (*WidgetReconciler, client.Client) {
	t.Helper()
	cc := countingClient{Client: c, w: updateCounter{SubResourceWriter: c.Status(), n: writes}}
	gate, err := shardkit.Attach(context.Background(), cc, widgetPlanKey, track, rev)
	if err != nil {
		t.Fatal(err)
	}
	return &WidgetReconciler{Client: gate.Client(cc, cc), Gate: gate, Track: track, Revision: rev, PlanKey: widgetPlanKey}, cc
}

// TestReconcile_IdempotentStamp pins the no-hot-loop behavior: the
// first reconcile of an owned widget stamps it once, a repeat
// reconcile performs no write, and a widget held by the other track
// is re-stamped.
func TestReconcile_IdempotentStamp(t *testing.T) {
	fc := widgetFixture(t)
	var writes int
	rec, _ := widgetReconciler(t, fc, &writes, "stable", "rev-a")
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "demo-00", Name: "w0"}}
	ctx := context.Background()

	if _, err := rec.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if writes != 1 {
		t.Fatalf("first reconcile wrote %d times, want 1", writes)
	}
	var got v1alpha1.Widget
	if err := fc.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.OwnerTrack != "stable" || got.Status.OwnerRevision != "rev-a" || got.Status.Writes != 1 {
		t.Fatalf("unexpected stamp %+v", got.Status)
	}

	if _, err := rec.Reconcile(ctx, req); err != nil {
		t.Fatalf("repeat reconcile: %v", err)
	}
	if writes != 1 {
		t.Fatalf("repeat reconcile wrote (total %d), want no second write", writes)
	}

	got.Status.OwnerTrack = "canary"
	got.Status.OwnerRevision = "rev-b"
	if err := fc.Status().Update(ctx, &got); err != nil {
		t.Fatal(err)
	}
	if _, err := rec.Reconcile(ctx, req); err != nil {
		t.Fatalf("re-stamp reconcile: %v", err)
	}
	if writes != 2 {
		t.Fatalf("re-stamp wrote (total %d), want one more write", writes)
	}
	if err := fc.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.OwnerTrack != "stable" || got.Status.Writes != 1 {
		t.Fatalf("unexpected re-stamp %+v", got.Status)
	}
}

// TestReconcile_NotOwnedSkipsWithoutWrite checks that a track which
// does not own a widget performs no write for it.
func TestReconcile_NotOwnedSkipsWithoutWrite(t *testing.T) {
	fc := widgetFixture(t)
	var writes int
	rec, _ := widgetReconciler(t, fc, &writes, "canary", "rev-b")
	if _, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "demo-00", Name: "w0"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if writes != 0 {
		t.Fatalf("not-owned reconcile wrote %d times, want 0", writes)
	}
}

// TestReconcile_ChaosMassDelete pins the M2 demo's injected bug:
// with Chaos set, reconcile deletes the widget and reports an
// error instead of stamping (no status write happens).
func TestReconcile_ChaosMassDelete(t *testing.T) {
	fc := widgetFixture(t)
	var writes int
	rec, _ := widgetReconciler(t, fc, &writes, "stable", "rev-a")
	rec.Chaos = "mass-delete"
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "demo-00", Name: "w0"}}
	if _, err := rec.Reconcile(context.Background(), req); err == nil {
		t.Fatal("chaos reconcile must report an error")
	}
	if writes != 0 {
		t.Fatalf("chaos reconcile wrote %d times, want 0", writes)
	}
	var got v1alpha1.Widget
	if err := fc.Get(context.Background(), req.NamespacedName, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("widget must be deleted, get err = %v", err)
	}
}

// TestReconcile_GuardedClientDeniesForeign pins the M1 write
// boundary: even if the entry and re-checks passed, a stamp write
// for a foreign namespace is denied (NotOwned), never executed.
func TestReconcile_GuardedClientDeniesForeign(t *testing.T) {
	fc := widgetFixture(t)
	var writes int
	rec, _ := widgetReconciler(t, fc, &writes, "canary", "rev-b")
	var w v1alpha1.Widget
	if err := fc.Get(context.Background(),
		types.NamespacedName{Namespace: "demo-00", Name: "w0"}, &w); err != nil {
		t.Fatal(err)
	}
	if _, err := rec.markOwned(context.Background(), &w); err == nil {
		t.Fatal("foreign stamp wrote, want denied")
	} else if denied, ok := shardkit.AsDenied(err); !ok || denied.Reason != shardkit.ReasonNotOwned {
		t.Fatalf("foreign stamp err = %v, want NotOwned", err)
	}
	if writes != 0 {
		t.Fatalf("denied stamp wrote %d times, want 0", writes)
	}
}
