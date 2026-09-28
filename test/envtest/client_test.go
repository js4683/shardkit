package envtest_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

// TestGuardedClient_RealServer pins the write boundary against the
// real API server: ownership allow/deny on live objects, an admitted
// server-side apply, label-exclusion routing, and singleton
// cluster-scope routing.
func TestGuardedClient_RealServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, ns := range []corev1.Namespace{
		{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "team-b", Labels: map[string]string{"tier": "critical"}}},
	} {
		if err := k8sClient.Create(ctx, &ns); err != nil {
			t.Fatal(err)
		}
	}
	plan := basePlan("guarded")
	plan.Spec.Epoch = 1
	plan.Spec.Canary.Mode = v1alpha1.ModeActive
	plan.Spec.Canary.WeightPerMille = 1000
	plan.Spec.Canary.Exclude.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{"tier": "critical"},
	}
	requireCreate(t, plan)
	key := types.NamespacedName{Namespace: testNS, Name: "guarded"}

	// B1 (refined in M1): attach adopts any valid plan; the
	// observer's resume protocol establishes safety, so no Off
	// round-trip is needed. Attach directly to the Active plan,
	// then bump the epoch to keep the "gates follow plan updates"
	// leg below meaningful.
	stableGate, err := shardkit.Attach(ctx, k8sClient, key, "stable", "rev-a")
	if err != nil {
		t.Fatalf("attach stable to Active plan: %v", err)
	}
	canaryGate, err := shardkit.Attach(ctx, k8sClient, key, "canary", "rev-b")
	if err != nil {
		t.Fatalf("attach canary to Active plan: %v", err)
	}
	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	if err := k8sClient.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	stable, canary := stableGate.Client(k8sClient, k8sClient), canaryGate.Client(k8sClient, k8sClient)

	if err := canary.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "c1"}}); err != nil {
		t.Errorf("canary create owned: %v", err)
	}
	if err := stable.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "s1"}}); err == nil {
		t.Error("stable create foreign: nil error, want NotOwned")
	} else if denied, ok := shardkit.AsDenied(err); !ok || denied.Reason != shardkit.ReasonNotOwned {
		t.Errorf("stable create foreign: %v, want NotOwned", err)
	}
	// team-b carries tier=critical and matches the exclude selector:
	// stable keeps it at full canary weight.
	if err := stable.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "s2"}}); err != nil {
		t.Errorf("stable create excluded: %v", err)
	}
	if err := canary.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "c2"}}); err == nil {
		t.Error("canary create excluded: nil error, want NotOwned")
	}
	// Admitted server-side apply, then read-back of applied data.
	ac := corev1ac.ConfigMap("ac-1", "team-a").WithData(map[string]string{"k": "v"})
	if err := canary.Apply(ctx, ac, client.FieldOwner("envtest")); err != nil {
		t.Errorf("canary apply owned: %v", err)
	}
	var got corev1.ConfigMap
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "team-a", Name: "ac-1"}, &got); err != nil {
		t.Errorf("applied object missing: %v", err)
	} else if got.Data["k"] != "v" {
		t.Errorf("applied data = %v, want k=v", got.Data)
	}
	if err := stable.Apply(ctx, corev1ac.ConfigMap("ac-2", "team-a")); err == nil {
		t.Error("stable apply foreign: nil error, want NotOwned")
	}
	// Fencing evidence on the real server: response-RV verbs, the
	// apply post-read, and delete collection versions all land.
	if got := canary.LastWritten(); got["configmaps"] == "" {
		t.Errorf("canary evidence = %v, want configmaps RV", got)
	}
	if err := canary.Delete(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "c1"}}); err != nil {
		t.Fatal(err)
	}
	if got := canary.LastWritten(); got["configmaps"] == "" {
		t.Errorf("evidence after delete = %v, want configmaps RV", got)
	}
	// Cluster scope follows the singleton owner (stable).
	if err := stable.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "envtest-gc-1"}}); err != nil {
		t.Errorf("stable namespace create: %v", err)
	}
	if err := canary.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "envtest-gc-2"}}); err == nil {
		t.Error("canary namespace create: nil error, want SingletonElsewhere")
	}
	// Status subresource on an owned object. ConfigMaps have no
	// /status endpoint, so this leg uses the Widget CRD (installed
	// with the suite's CRDs), which does.
	w := &v1alpha1.Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "w0"},
		Spec:       v1alpha1.WidgetSpec{Value: "team-a"},
	}
	if err := canary.Create(ctx, w); err != nil {
		t.Fatalf("canary widget create owned: %v", err)
	}
	var widget v1alpha1.Widget
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "team-a", Name: "w0"}, &widget); err != nil {
		t.Fatal(err)
	}
	widget.Status.OwnerTrack = "canary"
	widget.Status.OwnerRevision = "rev-b"
	widget.Status.Writes = 1
	if err := canary.Status().Update(ctx, &widget); err != nil {
		t.Errorf("canary status update owned: %v", err)
	}
	if err := stable.Status().Update(ctx, &widget); err == nil {
		t.Error("stable status update foreign: nil error, want NotOwned")
	}
}

// TestShadow_DryRunRealServer proves I6 against the real API
// server: a shadowed status write and a shadowed data write both
// succeed, both record allowed attempts, and neither persists —
// the live objects read back unchanged and no fencing evidence is
// folded.
func TestShadow_DryRunRealServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ns := corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shadow-a"}}
	if err := k8sClient.Create(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	plan := basePlan("shadow")
	plan.Spec.Epoch = 1
	plan.Spec.Canary.Mode = v1alpha1.ModeActive
	plan.Spec.Canary.WeightPerMille = 1000
	requireCreate(t, plan)
	key := types.NamespacedName{Namespace: testNS, Name: "shadow"}
	canaryGate, err := shardkit.Attach(ctx, k8sClient, key, "canary", "rev-b")
	if err != nil {
		t.Fatalf("attach canary: %v", err)
	}
	rec := &shardkit.ShadowRecorder{}
	shadow := canaryGate.Client(k8sClient, k8sClient).Shadowed(rec)

	w := &v1alpha1.Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shadow-a", Name: "w0"},
		Spec:       v1alpha1.WidgetSpec{Value: "shadow-a"},
	}
	if err := k8sClient.Create(ctx, w); err != nil {
		t.Fatalf("seed widget: %v", err)
	}
	var live v1alpha1.Widget
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "shadow-a", Name: "w0"}, &live); err != nil {
		t.Fatal(err)
	}
	live.Status.OwnerTrack = "canary"
	live.Status.OwnerRevision = "rev-b"
	live.Status.Writes = 1
	if err := shadow.Status().Update(ctx, &live); err != nil {
		t.Fatalf("shadow status update: %v", err)
	}
	var after v1alpha1.Widget
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "shadow-a", Name: "w0"}, &after); err != nil {
		t.Fatal(err)
	}
	if after.Status.OwnerTrack != "" || after.Status.Writes != 0 {
		t.Errorf("dry-run status persisted: %+v", after.Status)
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "shadow-a", Name: "c0"}}
	if err := k8sClient.Create(ctx, cm); err != nil {
		t.Fatalf("seed configmap: %v", err)
	}
	var liveCM corev1.ConfigMap
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "shadow-a", Name: "c0"}, &liveCM); err != nil {
		t.Fatal(err)
	}
	liveCM.Data = map[string]string{"k": "shadowed"}
	if err := shadow.Update(ctx, &liveCM); err != nil {
		t.Fatalf("shadow update: %v", err)
	}
	var afterCM corev1.ConfigMap
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "shadow-a", Name: "c0"}, &afterCM); err != nil {
		t.Fatal(err)
	}
	if len(afterCM.Data) != 0 {
		t.Errorf("dry-run data persisted: %v", afterCM.Data)
	}

	ops := rec.Ops()
	if len(ops) != 2 {
		t.Fatalf("ops = %v, want two allowed attempts", ops)
	}
	for _, op := range ops {
		if op.Decision != "allowed" {
			t.Errorf("op = %+v, want allowed", op)
		}
	}
	if ops[0].Verb != "status-update" || ops[1].Verb != "update" {
		t.Errorf("verbs = %q %q, want status-update update", ops[0].Verb, ops[1].Verb)
	}
	if got := shadow.LastWritten(); len(got) != 0 {
		t.Errorf("shadow folded evidence %v, want none", got)
	}
}

// TestConfirmDelete_BudgetsRealServer proves I5 against the real
// API server: with MaxDeletions 1 the first confirmed delete
// charges used=1 and deletes through the UID precondition, and
// the second denies BudgetExceeded while its object survives.
func TestConfirmDelete_BudgetsRealServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ns := corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "budget-a"}}
	if err := k8sClient.Create(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	max := int32(1)
	plan := basePlan("budget")
	plan.Spec.Epoch = 1
	plan.Spec.Canary.Mode = v1alpha1.ModeActive
	plan.Spec.Canary.WeightPerMille = 1000
	plan.Spec.Budget = &v1alpha1.BudgetSpec{MaxDeletions: &max}
	requireCreate(t, plan)
	key := types.NamespacedName{Namespace: testNS, Name: "budget"}
	canaryGate, err := shardkit.Attach(ctx, k8sClient, key, "canary", "rev-b")
	if err != nil {
		t.Fatalf("attach canary: %v", err)
	}
	canary := canaryGate.Client(k8sClient, k8sClient)

	mkWidget := func(name string) {
		t.Helper()
		w := &v1alpha1.Widget{
			ObjectMeta: metav1.ObjectMeta{Namespace: "budget-a", Name: name},
			Spec:       v1alpha1.WidgetSpec{Value: name},
		}
		if err := k8sClient.Create(ctx, w); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	mkWidget("w1")
	mkWidget("w2")
	del := func(name string) error {
		t.Helper()
		return canary.ConfirmDelete(ctx, key,
			&v1alpha1.Widget{ObjectMeta: metav1.ObjectMeta{Namespace: "budget-a", Name: name}})
	}
	if err := del("w1"); err != nil {
		t.Fatalf("first confirmed delete: %v", err)
	}
	var gone v1alpha1.Widget
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "budget-a", Name: "w1"}, &gone); err == nil {
		t.Fatal("w1 still exists, want deleted through UID precondition")
	}
	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	charged := false
	for _, e := range live.Status.Tracks {
		if e.Name == "canary" && e.Budget != nil &&
			e.Budget.Epoch == 1 && e.Budget.DeletionsUsed == 1 {
			charged = true
		}
	}
	if !charged {
		t.Errorf("canary entry lacks {1 1} charge: %+v", live.Status.Tracks)
	}
	if err := del("w2"); err == nil {
		t.Fatal("second delete: nil error, want BudgetExceeded")
	} else if denied, ok := shardkit.AsDenied(err); !ok || denied.Reason != shardkit.ReasonBudgetExceeded {
		t.Fatalf("second delete: %v, want BudgetExceeded", err)
	}
	var kept v1alpha1.Widget
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "budget-a", Name: "w2"}, &kept); err != nil {
		t.Fatalf("w2 missing after trip: %v", err)
	}
}

// TestConfirmDelete_ConcurrentChargers pins the FA5.1 race the
// kind run caught (5 deletes against a cap of 3 under
// MaxConcurrentReconciles=5): six concurrent confirmed deletes
// against MaxDeletions 3 must delete exactly three. RV conflicts
// serialize the check-and-charge loop; every retry re-derives the
// count from a fresh read, so late racers deny instead of
// over-deleting.
// chargerRun disambiguates namespaces across -count=N repeats sharing
// one envtest server (namespace termination lingers there).
var chargerRun atomic.Int32

func TestConfirmDelete_ConcurrentChargers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// Unique per iteration: the shared envtest server persists
	// across -count=N runs and namespace termination lingers
	// (no finalizer controller), so a fixed name collides.
	nsName := fmt.Sprintf("budget-b-%d", chargerRun.Add(1))
	ns := corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}
	if err := k8sClient.Create(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	// The plan name is fixed, so remove it for the next iteration
	// (plan deletion is synchronous: no finalizers).
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = k8sClient.Delete(ctx, &v1alpha1.ShardPlan{ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS, Name: "budget-race"}})
	})
	max := int32(3)
	plan := basePlan("budget-race")
	plan.Spec.Epoch = 1
	plan.Spec.Canary.Mode = v1alpha1.ModeActive
	plan.Spec.Canary.WeightPerMille = 1000
	plan.Spec.Budget = &v1alpha1.BudgetSpec{MaxDeletions: &max}
	requireCreate(t, plan)
	key := types.NamespacedName{Namespace: testNS, Name: "budget-race"}
	canaryGate, err := shardkit.Attach(ctx, k8sClient, key, "canary", "rev-b")
	if err != nil {
		t.Fatalf("attach canary: %v", err)
	}
	canary := canaryGate.Client(k8sClient, k8sClient)

	names := []string{"c1", "c2", "c3", "c4", "c5", "c6"}
	for _, name := range names {
		w := &v1alpha1.Widget{
			ObjectMeta: metav1.ObjectMeta{Namespace: nsName, Name: name},
			Spec:       v1alpha1.WidgetSpec{Value: name},
		}
		if err := k8sClient.Create(ctx, w); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	errs := make([]error, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			errs[i] = canary.ConfirmDelete(ctx, key,
				&v1alpha1.Widget{ObjectMeta: metav1.ObjectMeta{Namespace: nsName, Name: name}})
		}(i, name)
	}
	wg.Wait()

	var deleted, denied int
	for _, err := range errs {
		switch {
		case err == nil:
			deleted++
		default:
			if d, ok := shardkit.AsDenied(err); !ok || d.Reason != shardkit.ReasonBudgetExceeded {
				t.Fatalf("racer err = %v, want nil or BudgetExceeded", err)
			}
			denied++
		}
	}
	if deleted != 3 || denied != 3 {
		t.Fatalf("deleted=%d denied=%d, want exactly 3 and 3", deleted, denied)
	}
	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	for _, e := range live.Status.Tracks {
		if e.Name == "canary" {
			if e.Budget == nil || e.Budget.Epoch != 1 || e.Budget.DeletionsUsed != 3 {
				t.Fatalf("canary budget = %+v, want {1 3}", e.Budget)
			}
		}
	}
}
