package shardkit

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// TestMetrics_GateAndClient pins the decision/write recorders across
// owned, foreign, denied, allowed, and error-adjacent paths.
func TestMetrics_GateAndClient(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithRESTMapper(testRestMapper()).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(baseOffPlan(),
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
		).Build()
	stable, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	canary, err := Attach(ctx, c, planKey, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	stable.SetMetrics(m)
	canary.SetMetrics(m)

	if obs, _ := stable.Owned(ctx, "demo-87"); !obs.Owned {
		t.Fatal("stable should own demo-87 Off")
	}
	if obs, _ := canary.Owned(ctx, "demo-87"); obs.Owned {
		t.Fatal("canary should not own demo-87 Off")
	}
	if _, err := stable.SingletonOwned(ctx); err != nil {
		t.Fatal(err)
	}
	sc, cc := stable.Client(c, c), canary.Client(c, c)
	if err := sc.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "m1"}}); err != nil {
		t.Fatalf("allowed create: %v", err)
	}
	if err := cc.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "m2"}}); err == nil {
		t.Fatal("foreign create allowed, want denied")
	}
	var plan v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &plan); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, &plan); err != nil {
		t.Fatal(err)
	}
	if _, err := stable.Owned(ctx, "demo-87"); err == nil {
		t.Fatal("owned after delete, want closed")
	}

	cases := []struct {
		name string
		got  prometheus.Counter
		want float64
	}{
		{"gate owned", m.GateDecisions("stable", "rev-a", "namespace", "owned", "none"), 2},
		{"gate foreign", m.GateDecisions("canary", "rev-b", "namespace", "foreign", "none"), 2},
		{"gate denied", m.GateDecisions("stable", "rev-a", "namespace", "denied", ReasonPlanDeleted), 1},
		{"gate singleton", m.GateDecisions("stable", "rev-a", "singleton", "owned", "none"), 1},
		{"client allowed", m.ClientWrites("stable", "rev-a", "create", "configmaps", "allowed", "none"), 1},
		{"client denied", m.ClientWrites("canary", "rev-b", "create", "configmaps", "denied", ReasonNotOwned), 1},
	}
	for _, tc := range cases {
		if got := testutil.ToFloat64(tc.got); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestMetrics_Observer pins handshake events and held gauges: resume
// counts once, idle steady counts nothing, session rotation counts a
// refresh, and release/acquire count through a flip.
func TestMetrics_Observer(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	obs, g, fc := observerFixture(t, "stable")
	obs.metrics = m
	g.SetMetrics(m)
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.Transitions("stable", "rev-x", "resume")); got != 1 {
		t.Fatalf("resume events = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.Transitions("stable", "rev-x", "steady_refresh")); got != 0 {
		t.Fatalf("steady_refresh events = %v, want 0 after idle poll", got)
	}
	var lease coordinationv1.Lease
	key := client.ObjectKey{Namespace: planKey.Namespace, Name: "widget-operator-stable"}
	if err := fc.Get(ctx, key, &lease); err != nil {
		t.Fatal(err)
	}
	holder, transitions := "pod-1", int32(1)
	lease.Spec.HolderIdentity, lease.Spec.LeaseTransitions = &holder, &transitions
	if err := fc.Update(ctx, &lease); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.Transitions("stable", "rev-x", "steady_refresh")); got != 1 {
		t.Fatalf("steady_refresh events = %v, want 1 after rotation", got)
	}
	if got := testutil.ToFloat64(m.HeldNamespaces("stable", "rev-x")); got != 2 {
		t.Errorf("held namespaces = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.HeldSingleton("stable", "rev-x")); got != 1 {
		t.Errorf("held singleton = %v, want 1", got)
	}
}

// TestMetrics_Barrier pins success/error barrier observations.
func TestMetrics_Barrier(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	c := fixture(t, nil)
	ok := BarrierOptions{Reader: c, Mapper: testRestMapper(),
		Types: []schema.GroupVersionKind{cmGVK}, Track: "canary", Metrics: m}
	if err := WaitFresh(ctx, ok); err != nil {
		t.Fatal(err)
	}
	bad := ok
	bad.Writes = map[string]string{"widgets.shardkit.dev": "9"}
	if err := WaitFresh(ctx, bad); err == nil {
		t.Fatal("barrier over unwatched type passed, want closed")
	}
	count := func(outcome string) uint64 {
		t.Helper()
		var pb dto.Metric
		if err := m.Barrier("canary", outcome).(prometheus.Histogram).Write(&pb); err != nil {
			t.Fatal(err)
		}
		return pb.GetHistogram().GetSampleCount()
	}
	if got := count("success"); got != 1 {
		t.Errorf("success observations = %d, want 1", got)
	}
	if got := count("error"); got != 1 {
		t.Errorf("error observations = %d, want 1", got)
	}
}

// TestMetrics_NilRecorder pins the no-op contract: every record
// method tolerates a nil receiver.
func TestMetrics_NilRecorder(t *testing.T) {
	var m *Metrics
	m.ObserveGateDecision("stable", "rev-a", "namespace", "owned", "none")
	m.ObserveClientWrite("stable", "rev-a", "Create", "configmaps", "allowed", "none")
	m.ObserveTransition("stable", "rev-a", "resume")
	m.ObserveBarrier("stable", 0, "success")
	m.SetHeld("stable", "rev-a", 2, true)
}
