package shardkit

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// testRestMapper gives the fake client the scope mappings the guard
// needs: the fake builder defaults to an empty RESTMapper, on which
// every IsObjectNamespaced fails. Real managers carry a discovery
// mapper, so production is unaffected.
func testRestMapper() meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)
	mapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMapList"), meta.RESTScopeNamespace)
	mapper.Add(corev1.SchemeGroupVersion.WithKind("Namespace"), meta.RESTScopeRoot)
	mapper.Add(corev1.SchemeGroupVersion.WithKind("NamespaceList"), meta.RESTScopeRoot)
	mapper.Add(v1alpha1.GroupVersion.WithKind("ShardPlan"), meta.RESTScopeNamespace)
	return mapper
}

// newGuarded builds an isolated fixture: Off plan, demo-87/default
// namespaces, one ConfigMap in demo-87, and a guarded client for
// track. The gate attaches with the spec revision for the track:
// the S7 fence denies unbound writers, so the fixture binds like a
// real process. Status subresource is registered for ConfigMaps.
func newGuarded(t *testing.T, track string) (*GuardedClient, client.Client) {
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
		).Build()
	rev := "rev-a"
	if track == v1alpha1.TrackCanary {
		rev = "rev-b"
	}
	return newGuardedRev(t, track, rev, fc)
}

// newGuardedRev attaches with an explicit revision: a revision that
// differs from the spec exercises the S7 write fence.
func newGuardedRev(t testing.TB, track, rev string, fc client.Client) (*GuardedClient, client.Client) {
	t.Helper()
	g, err := Attach(context.Background(), fc, planKey, track, rev)
	if err != nil {
		t.Fatal(err)
	}
	return g.Client(fc, fc), fc
}

func deniedReason(t *testing.T, err error) string {
	t.Helper()
	denied, ok := AsDenied(err)
	if !ok {
		t.Fatalf("err = %v, want *DeniedError", err)
	}
	return denied.Reason
}

// TestVerbs_AllowAndDeny pins the write boundary: every mutating verb
// on an owned namespace succeeds; the same verbs from the foreign
// track fail with NotOwned. Flipping the plan reverses both sides.
func TestVerbs_AllowAndDeny(t *testing.T) {
	ctx := context.Background()
	runVerbs := func(t *testing.T, gc *GuardedClient, raw client.Client, wantErr bool) {
		t.Helper()
		key := types.NamespacedName{Namespace: "demo-87", Name: "cm"}
		mk := func() *corev1.ConfigMap {
			var cm corev1.ConfigMap
			if err := raw.Get(ctx, key, &cm); err != nil {
				t.Fatal(err)
			}
			return &cm
		}
		if err := gc.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "new-cm"}}); (err != nil) != wantErr {
			t.Errorf("create err = %v, wantErr %v", err, wantErr)
		}
		cm := mk()
		cm.Data = map[string]string{"k": "v"}
		if err := gc.Update(ctx, cm); (err != nil) != wantErr {
			t.Errorf("update err = %v, wantErr %v", err, wantErr)
		}
		cm = mk()
		if err := gc.Patch(ctx, cm, client.RawPatch(types.MergePatchType, []byte(`{"data":{"p":"1"}}`))); (err != nil) != wantErr {
			t.Errorf("patch err = %v, wantErr %v", err, wantErr)
		}
		cm = mk()
		if err := gc.Status().Update(ctx, cm); (err != nil) != wantErr {
			t.Errorf("status-update err = %v, wantErr %v", err, wantErr)
		}
		cm = mk()
		if err := gc.Delete(ctx, cm); (err != nil) != wantErr {
			t.Errorf("delete err = %v, wantErr %v", err, wantErr)
		}
	}
	t.Run("off stable allows canary denies", func(t *testing.T) {
		stable, raw := newGuarded(t, "stable")
		runVerbs(t, stable, raw, false)
		canary, raw2 := newGuarded(t, "canary")
		runVerbs(t, canary, raw2, true)
		var cm corev1.ConfigMap
		if err := raw2.Get(ctx, types.NamespacedName{Namespace: "demo-87", Name: "cm"}, &cm); err != nil {
			t.Fatal(err)
		}
		if got := deniedReason(t, canary.Delete(ctx, &cm)); got != ReasonNotOwned {
			t.Fatalf("reason = %s, want NotOwned", got)
		}
	})
	t.Run("active-1000 reverses sides", func(t *testing.T) {
		stable, raw := newGuarded(t, "stable")
		flipFull(t, raw)
		runVerbs(t, stable, raw, true)
		canary, raw2 := newGuarded(t, "canary")
		flipFull(t, raw2)
		runVerbs(t, canary, raw2, false)
	})
}

// flipFull rewrites the live plan to Active w=1000 at epoch 2.
func flipFull(t *testing.T, c client.Client) {
	t.Helper()
	ctx := context.Background()
	var plan v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &plan); err != nil {
		t.Fatal(err)
	}
	plan.Spec.Epoch = 2
	plan.Spec.Canary.Mode = "Active"
	plan.Spec.Canary.WeightPerMille = 1000
	if err := c.Update(ctx, &plan); err != nil {
		t.Fatal(err)
	}
}

// TestClusterScoped_Singleton pins cluster-scope routing: Namespace
// writes need singleton duty, and duty transfer flips both sides.
func TestClusterScoped_Singleton(t *testing.T) {
	ctx := context.Background()
	stable, _ := newGuarded(t, "stable")
	canary, _ := newGuarded(t, "canary")
	if err := stable.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "extra-1"}}); err != nil {
		t.Fatalf("stable singleton create: %v", err)
	}
	if got := deniedReason(t, canary.Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "extra-2"}})); got != ReasonSingletonElsewhere {
		t.Fatalf("reason = %s, want SingletonElsewhere", got)
	}
}

// TestRevisionFence pins S7 at the write boundary: a superseded
// revision (previous image, previous Argo ReplicaSet) reads an
// owned namespace as owned but must not write, or its stamps
// ping-pong against the current revision's.
func TestRevisionFence(t *testing.T) {
	ctx := context.Background()
	newStale := func(t *testing.T, track string) (*GuardedClient, client.Client) {
		t.Helper()
		_, raw := newGuarded(t, track)
		return newGuardedRev(t, track, "rev-stale", raw)
	}
	t.Run("namespaced write denied", func(t *testing.T) {
		stale, _ := newStale(t, "stable")
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "fenced"}}
		if got := deniedReason(t, stale.Create(ctx, cm)); got != ReasonRevisionMismatch {
			t.Fatalf("reason = %s, want RevisionMismatch", got)
		}
	})
	t.Run("ownership checked first", func(t *testing.T) {
		stale, _ := newStale(t, "canary")
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "fenced"}}
		if got := deniedReason(t, stale.Create(ctx, cm)); got != ReasonNotOwned {
			t.Fatalf("reason = %s, want NotOwned", got)
		}
	})
	t.Run("singleton write denied", func(t *testing.T) {
		stale, _ := newStale(t, "stable")
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "extra-fenced"}}
		if got := deniedReason(t, stale.Create(ctx, ns)); got != ReasonRevisionMismatch {
			t.Fatalf("reason = %s, want RevisionMismatch", got)
		}
	})
}

// TestDeleteAllOf_Scoping pins collection deletes: single-namespace
// selections evaluate like any namespaced write; broader selections
// are unattributable and denied.
func TestDeleteAllOf_Scoping(t *testing.T) {
	ctx := context.Background()
	t.Run("owned namespace allowed", func(t *testing.T) {
		stable, raw := newGuarded(t, "stable")
		if err := stable.DeleteAllOf(ctx, &corev1.ConfigMap{},
			client.InNamespace("demo-87")); err != nil {
			t.Fatalf("deletecollection owned: %v", err)
		}
		var list corev1.ConfigMapList
		if err := raw.List(ctx, &list, client.InNamespace("demo-87")); err != nil {
			t.Fatal(err)
		}
		if len(list.Items) != 0 {
			t.Fatalf("%d items remain, want 0", len(list.Items))
		}
	})
	t.Run("foreign namespace denied", func(t *testing.T) {
		canary, _ := newGuarded(t, "canary")
		err := canary.DeleteAllOf(ctx, &corev1.ConfigMap{}, client.InNamespace("demo-87"))
		if got := deniedReason(t, err); got != ReasonNotOwned {
			t.Fatalf("reason = %s, want NotOwned", got)
		}
	})
	t.Run("all namespaces denied", func(t *testing.T) {
		stable, _ := newGuarded(t, "stable")
		err := stable.DeleteAllOf(ctx, &corev1.ConfigMap{})
		if got := deniedReason(t, err); got != ReasonUnsupportedWrite {
			t.Fatalf("reason = %s, want UnsupportedWrite", got)
		}
	})
	t.Run("labels within owned namespace allowed", func(t *testing.T) {
		stable, _ := newGuarded(t, "stable")
		if err := stable.DeleteAllOf(ctx, &corev1.ConfigMap{},
			client.InNamespace("demo-87"), client.MatchingLabels{"k": "v"}); err != nil {
			t.Fatalf("scoped labeled deletecollection: %v", err)
		}
	})
}

// exoticApply implements runtime.ApplyConfiguration without the
// standard namespace accessor.
type exoticApply struct{}

func (exoticApply) IsApplyConfiguration() {}

// TestApply_Attribution pins server-side apply routing: generated
// apply configs attribute by namespace, exotic shapes are denied.
// (Admitted applies run against envtest's real server; see
// test/envtest/client_test.go, since the fake client lacks Apply.)
func TestApply_Attribution(t *testing.T) {
	ctx := context.Background()
	canary, _ := newGuarded(t, "canary")
	foreign := corev1ac.ConfigMap("ac-1", "demo-87").WithData(map[string]string{"k": "v"})
	if got := deniedReason(t, canary.Apply(ctx, foreign)); got != ReasonNotOwned {
		t.Fatalf("reason = %s, want NotOwned", got)
	}
	stable, _ := newGuarded(t, "stable")
	if got := deniedReason(t, stable.Apply(ctx, exoticApply{})); got != ReasonUnsupportedWrite {
		t.Fatalf("reason = %s, want UnsupportedWrite", got)
	}
}

// TestReads_Passthrough pins that Get and List never consult
// ownership: observability stays available on foreign namespaces.
func TestReads_Passthrough(t *testing.T) {
	ctx := context.Background()
	canary, _ := newGuarded(t, "canary")
	var cm corev1.ConfigMap
	if err := canary.Get(ctx, types.NamespacedName{Namespace: "demo-87", Name: "cm"}, &cm); err != nil {
		t.Fatalf("get foreign: %v", err)
	}
	var list corev1.ConfigMapList
	if err := canary.List(ctx, &list, client.InNamespace("demo-87")); err != nil {
		t.Fatalf("list foreign: %v", err)
	}
}

// TestGateClosed_Denies pins fail-closed writes: a deleted plan
// denies with GateClosed naming the underlying reason.
func TestGateClosed_Denies(t *testing.T) {
	ctx := context.Background()
	stable, raw := newGuarded(t, "stable")
	var plan v1alpha1.ShardPlan
	if err := raw.Get(ctx, planKey, &plan); err != nil {
		t.Fatal(err)
	}
	if err := raw.Delete(ctx, &plan); err != nil {
		t.Fatal(err)
	}
	err := stable.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "x"}})
	denied, ok := AsDenied(err)
	if !ok || denied.Reason != ReasonGateClosed {
		t.Fatalf("err = %v, want denied GateClosed", err)
	}
	if !strings.Contains(denied.Msg, ReasonPlanDeleted) {
		t.Fatalf("msg %q lacks PlanDeleted", denied.Msg)
	}
}

// TestLastWritten pins fencing evidence: admitted writes record
// per-type resourceVersions keyed by resource.group, denied writes
// record nothing.
func TestLastWritten(t *testing.T) {
	ctx := context.Background()
	stable, _ := newGuarded(t, "stable")
	if got := stable.LastWritten(); len(got) != 0 {
		t.Fatalf("initial evidence = %v, want empty", got)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "ev"}}
	if err := stable.Create(ctx, cm); err != nil {
		t.Fatal(err)
	}
	cm.Data = map[string]string{"k": "v"}
	if err := stable.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	if err := stable.Status().Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	got := stable.LastWritten()
	rv, ok := got["configmaps"]
	if !ok || rv == "" {
		t.Fatalf("evidence = %v, want non-empty configmaps RV", got)
	}
	if _, ok := parseRV(rv); !ok {
		t.Fatalf("evidence RV %q not numeric", rv)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ev-ns"}}
	if err := stable.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	if got := stable.LastWritten(); got["namespaces"] == "" {
		t.Fatalf("evidence = %v, want namespaces RV", got)
	}
	canary, _ := newGuarded(t, "canary")
	_ = canary.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "denied"}})
	if got := canary.LastWritten(); len(got) != 0 {
		t.Fatalf("denied evidence = %v, want empty", got)
	}
}

// TestLastWritten_Deletes exercises the delete-evidence path on the
// fake client (post-delete collection list). Fake lists carry no
// resourceVersion, so the recorded-RV assertions live in envtest
// (TestGuardedClient_RealServer); here the deletes must simply
// succeed without disturbing surrounding evidence.
func TestLastWritten_Deletes(t *testing.T) {
	ctx := context.Background()
	stable, _ := newGuarded(t, "stable")
	var cm corev1.ConfigMap
	if err := stable.Get(ctx, types.NamespacedName{Namespace: "demo-87", Name: "cm"}, &cm); err != nil {
		t.Fatal(err)
	}
	if err := stable.Delete(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	if err := stable.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm2"}}); err != nil {
		t.Fatal(err)
	}
	if err := stable.DeleteAllOf(ctx, &corev1.ConfigMap{}, client.InNamespace("demo-87")); err != nil {
		t.Fatal(err)
	}
	if got := stable.LastWritten(); got["configmaps"] == "" {
		t.Fatalf("evidence = %v, want configmaps RV from the create", got)
	}
}

// blockClient stalls Create on a channel to make in-flight writes
// observable.
type blockClient struct {
	client.Client
	entered chan struct{}
	release chan struct{}
	once    chan struct{}
}

func (b *blockClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	select {
	case <-b.once:
	default:
		close(b.once)
		close(b.entered)
		<-b.release
	}
	return b.Client.Create(ctx, obj, opts...)
}

// TestInflight_Accounting pins write accounting: an admitted,
// slow write counts as in flight until it returns, and the idle
// wait honors its deadline.
func TestInflight_Accounting(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	fc := fake.NewClientBuilder().WithScheme(scheme).
		WithRESTMapper(testRestMapper()).
		WithObjects(
			baseOffPlan(),
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
		).Build()
	g, err := Attach(ctx, fc, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	block := &blockClient{Client: fc,
		entered: make(chan struct{}), release: make(chan struct{}), once: make(chan struct{})}
	gc := g.Client(block, fc)
	done := make(chan error, 1)
	go func() {
		done <- gc.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "slow"}})
	}()
	select {
	case <-block.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write never reached the delegate")
	}
	if got := gc.InflightWrites(); got != 1 {
		t.Fatalf("inflight = %d, want 1", got)
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := gc.WaitWritesIdle(short); err == nil {
		t.Fatal("idle wait with blocked write: nil error, want deadline")
	}
	close(block.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write never returned")
	}
	if err := gc.WaitWritesIdle(ctx); err != nil {
		t.Fatalf("idle wait: %v", err)
	}
}
