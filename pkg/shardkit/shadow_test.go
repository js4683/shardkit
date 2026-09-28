package shardkit

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// shadowFixture mirrors newGuarded but captures DryRun flags and
// call counts through interceptors: the fake tracker applies
// writes even with DryRun set, so the tests assert the mechanism
// (DryRun sent) plus the recorder, while envtest proves real
// non-persistence against the API server.
type shadowCapture struct {
	updateDryRun []string
	statusDryRun []string
	updateCalls  int
}

func shadowFixture(t *testing.T, track string, cap *shadowCapture) (*GuardedClient, client.Client) {
	t.Helper()
	scheme := testScheme(t)
	fc := fake.NewClientBuilder().WithScheme(scheme).
		WithRESTMapper(testRestMapper()).
		WithStatusSubresource(&corev1.ConfigMap{}).
		WithObjects(
			baseOffPlan(),
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm"}},
		).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				o := &client.UpdateOptions{}
				for _, opt := range opts {
					opt.ApplyToUpdate(o)
				}
				cap.updateDryRun = o.DryRun
				cap.updateCalls++
				return c.Update(ctx, obj, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, c client.Client, _ string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				o := &client.SubResourceUpdateOptions{}
				for _, opt := range opts {
					opt.ApplyToSubResourceUpdate(o)
				}
				cap.statusDryRun = o.DryRun
				return c.SubResource("status").Update(ctx, obj, opts...)
			},
		}).Build()
	rev := "rev-a"
	if track == v1alpha1.TrackCanary {
		rev = "rev-b"
	}
	return newGuardedRev(t, track, rev, fc)
}

func cmKey() types.NamespacedName {
	return types.NamespacedName{Namespace: "demo-87", Name: "cm"}
}

// TestShadow_AllowedUpdateDryRun pins the core promise: an owned
// write through a shadow client sends DryRunAll, succeeds, and is
// recorded as allowed — without folding evidence.
func TestShadow_AllowedUpdateDryRun(t *testing.T) {
	ctx := context.Background()
	var cap shadowCapture
	gc, raw := shadowFixture(t, v1alpha1.TrackStable, &cap)
	rec := &ShadowRecorder{}
	sh := gc.Shadowed(rec)

	var cm corev1.ConfigMap
	if err := raw.Get(ctx, cmKey(), &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data = map[string]string{"k": "v"}
	if err := sh.Update(ctx, &cm); err != nil {
		t.Fatalf("shadow update: %v", err)
	}
	if len(cap.updateDryRun) != 1 || cap.updateDryRun[0] != "All" {
		t.Fatalf("DryRun = %v, want [All]", cap.updateDryRun)
	}
	ops := rec.Ops()
	if len(ops) != 1 {
		t.Fatalf("ops = %v, want exactly one", ops)
	}
	op := ops[0]
	if op.Verb != "update" || op.Key != "configmaps" || op.Name != "demo-87/cm" ||
		op.Decision != "allowed" || op.Reason != "none" {
		t.Fatalf("op = %+v, want update/configmaps/demo-87/cm allowed/none", op)
	}
	if got := sh.LastWritten(); len(got) != 0 {
		t.Fatalf("shadow folded evidence %v, want none", got)
	}
}

// TestShadow_DeniedForeignRecorded pins fail-closed shadowing: a
// foreign write is denied before any API call and recorded with
// its reason.
func TestShadow_DeniedForeignRecorded(t *testing.T) {
	ctx := context.Background()
	var cap shadowCapture
	gc, raw := shadowFixture(t, v1alpha1.TrackCanary, &cap)
	rec := &ShadowRecorder{}
	sh := gc.Shadowed(rec)

	var cm corev1.ConfigMap
	if err := raw.Get(ctx, cmKey(), &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data = map[string]string{"k": "v"}
	err := sh.Update(ctx, &cm)
	if _, ok := AsDenied(err); !ok {
		t.Fatalf("err = %v, want *DeniedError", err)
	}
	if cap.updateCalls != 0 {
		t.Fatalf("denied write reached the API (%d calls)", cap.updateCalls)
	}
	ops := rec.Ops()
	if len(ops) != 1 || ops[0].Decision != "denied" || ops[0].Verb != "update" {
		t.Fatalf("ops = %+v, want one denied update", ops)
	}
}

// TestShadow_StatusUpdateRecorded pins the status path (FA6.3):
// status writes route through dry-run and record their verb.
func TestShadow_StatusUpdateRecorded(t *testing.T) {
	ctx := context.Background()
	var cap shadowCapture
	gc, raw := shadowFixture(t, v1alpha1.TrackStable, &cap)
	rec := &ShadowRecorder{}
	sh := gc.Shadowed(rec)

	var cm corev1.ConfigMap
	if err := raw.Get(ctx, cmKey(), &cm); err != nil {
		t.Fatal(err)
	}
	if err := sh.Status().Update(ctx, &cm); err != nil {
		t.Fatalf("shadow status-update: %v", err)
	}
	if len(cap.statusDryRun) != 1 || cap.statusDryRun[0] != "All" {
		t.Fatalf("status DryRun = %v, want [All]", cap.statusDryRun)
	}
	ops := rec.Ops()
	if len(ops) != 1 || ops[0].Verb != "status-update" || ops[0].Decision != "allowed" {
		t.Fatalf("ops = %+v, want one allowed status-update", ops)
	}
}

// TestShadow_LiveClientWritesEvidence pins the negative: the same
// write on the unshadowed client folds evidence as before, so the
// shadow skip is behavioral, not structural.
func TestShadow_LiveClientWritesEvidence(t *testing.T) {
	ctx := context.Background()
	var cap shadowCapture
	gc, raw := shadowFixture(t, v1alpha1.TrackStable, &cap)

	var cm corev1.ConfigMap
	if err := raw.Get(ctx, cmKey(), &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data = map[string]string{"k": "v"}
	if err := gc.Status().Update(ctx, &cm); err != nil {
		t.Fatalf("live status-update: %v", err)
	}
	if got := gc.LastWritten(); len(got) == 0 {
		t.Fatal("live client folded no evidence, want an entry")
	}
}

// TestShadow_NilRecorderIsLive pins the safe default:
// Shadowed(nil) behaves exactly like the live client.
func TestShadow_NilRecorderIsLive(t *testing.T) {
	ctx := context.Background()
	var cap shadowCapture
	gc, raw := shadowFixture(t, v1alpha1.TrackStable, &cap)
	sh := gc.Shadowed(nil)
	if sh.shadowing() {
		t.Fatal("Shadowed(nil) reports shadowing, want live")
	}

	var cm corev1.ConfigMap
	if err := raw.Get(ctx, cmKey(), &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data = map[string]string{"k": "v"}
	if err := sh.Update(ctx, &cm); err != nil {
		t.Fatalf("Shadowed(nil) update: %v", err)
	}
	if len(cap.updateDryRun) != 0 {
		t.Fatalf("live DryRun = %v, want none", cap.updateDryRun)
	}
}

// TestShadow_DeleteCollectionDenied pins the unattributable path:
// a cross-namespace collection is denied and recorded without an
// API call.
func TestShadow_DeleteCollectionDenied(t *testing.T) {
	ctx := context.Background()
	var cap shadowCapture
	gc, _ := shadowFixture(t, v1alpha1.TrackStable, &cap)
	rec := &ShadowRecorder{}
	sh := gc.Shadowed(rec)

	err := sh.DeleteAllOf(ctx, &corev1.ConfigMap{})
	if denied, ok := AsDenied(err); !ok || denied.Reason != ReasonUnsupportedWrite {
		t.Fatalf("err = %v, want UnsupportedWrite", err)
	}
	ops := rec.Ops()
	if len(ops) != 1 || ops[0].Verb != "deletecollection" || ops[0].Decision != "denied" {
		t.Fatalf("ops = %+v, want one denied deletecollection", ops)
	}
}
