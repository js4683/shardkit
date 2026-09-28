// Package envtest observer handoff: two track observers against a
// real API server drive one full Off -> Active flip. Stable
// releases with real fencing evidence, canary acquires behind the
// loser's ack plus a real quorum barrier, and the handoff event for
// stable's object arrives on the canary observer's event channel.
package envtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

var cmGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

// TestObserver_Handoff_RealServer pins the end-to-end handshake on a
// real server: resume, stable release with evidence RVs, canary
// acquire behind S5-S9 plus the freshness barrier, handoff events,
// and the gate flip (canary writes, stable denied).
func TestObserver_Handoff_RealServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "hand-a"}}
	if err := k8sClient.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	holder, transitions := "pod-0", int32(0)
	for _, track := range []string{"stable", "canary"} {
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: testNS, Name: "handoff-" + track,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &holder, LeaseTransitions: &transitions,
			},
		}
		if err := k8sClient.Create(ctx, lease); err != nil {
			t.Fatalf("lease %s: %v", track, err)
		}
	}
	plan := basePlan("handoff")
	plan.Spec.Canary.Mode = v1alpha1.ModeOff
	plan.Spec.Canary.WeightPerMille = 0
	plan.Spec.Canary.Include = v1alpha1.IncludeSpec{}
	requireCreate(t, plan)
	key := types.NamespacedName{Namespace: testNS, Name: "handoff"}

	reg := prometheus.NewRegistry()
	metrics := shardkit.NewMetrics(reg)
	stableGate, err := shardkit.Attach(ctx, k8sClient, key, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	canaryGate, err := shardkit.Attach(ctx, k8sClient, key, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	stableGate.SetMetrics(metrics)
	canaryGate.SetMetrics(metrics)
	mkObserver := func(g *shardkit.Gate, track string) *shardkit.Observer {
		t.Helper()
		obs, err := shardkit.NewObserver(shardkit.ObserverOptions{
			Client: k8sClient, APIReader: k8sClient,
			Gate: g, Guarded: g.Client(k8sClient, k8sClient),
			PlanKey: key, Track: track, LeaseBase: "handoff",
			Types:          []schema.GroupVersionKind{cmGVK},
			PollInterval:   10 * time.Millisecond,
			DrainTimeout:   10 * time.Second,
			AcquireTimeout: 30 * time.Second,
			Metrics:        metrics,
		})
		if err != nil {
			t.Fatal(err)
		}
		return obs
	}
	stableObs, canaryObs := mkObserver(stableGate, "stable"), mkObserver(canaryGate, "canary")
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable resume: %v", err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary resume: %v", err)
	}

	// Stable fences a real write before the flip; the release ack
	// must carry its evidence and the barrier must fence it.
	stable := stableGate.Client(k8sClient, k8sClient)
	hero := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "hand-a", Name: "hero"},
		Data:       map[string]string{"by": "stable"},
	}
	if err := stable.Create(ctx, hero); err != nil {
		t.Fatalf("stable fenced write: %v", err)
	}

	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	live.Spec.Canary.Mode = v1alpha1.ModeActive
	live.Spec.Canary.WeightPerMille = 1000
	if err := k8sClient.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable release: %v", err)
	}
	entry := func(track string) v1alpha1.TrackStatus {
		t.Helper()
		p := requireGet(t, "handoff")
		for _, e := range p.Status.Tracks {
			if e.Name == track {
				return e
			}
		}
		t.Fatalf("no %s entry in %+v", track, p.Status.Tracks)
		return v1alpha1.TrackStatus{}
	}
	rel := entry("stable")
	if rel.ObservedEpoch != 2 || !rel.Released || rel.Phase != v1alpha1.PhaseReleased {
		t.Fatalf("stable entry = %+v, want Released for epoch 2", rel)
	}
	if rel.ReleasedWrites["configmaps"] == "" {
		t.Fatalf("stable evidence = %v, want real configmaps RV", rel.ReleasedWrites)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary acquire: %v", err)
	}
	acq := entry("canary")
	if acq.ObservedEpoch != 2 || acq.Phase != v1alpha1.PhaseAcquired {
		t.Fatalf("canary entry = %+v, want Acquired for epoch 2", acq)
	}
	// The realistic assembly records: one release, one acquire,
	// stable holding nothing, canary holding namespaces.
	if got := testutil.ToFloat64(metrics.Transitions("stable", "rev-a", "release")); got != 1 {
		t.Errorf("stable release events = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Transitions("canary", "rev-b", "acquire")); got != 1 {
		t.Errorf("canary acquire events = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.HeldNamespaces("stable", "rev-a")); got != 0 {
		t.Errorf("stable held = %v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.HeldNamespaces("canary", "rev-b")); got <= 0 {
		t.Errorf("canary held = %v, want > 0", got)
	}

	// The handoff wave carries stable's object to the canary
	// reconciler; the barrier already fenced it, so read-back sees
	// stable's data.
	deadline := time.Now().Add(15 * time.Second)
	sawHero := false
drain:
	for {
		select {
		case ev := <-canaryObs.Events():
			if ev.Object.GetNamespace() == "hand-a" && ev.Object.GetName() == "hero" {
				sawHero = true
				break drain
			}
		case <-time.After(time.Until(deadline)):
			break drain
		}
	}
	if !sawHero {
		t.Fatal("no handoff event for hand-a/hero")
	}
	var got corev1.ConfigMap
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "hand-a", Name: "hero"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Data["by"] != "stable" {
		t.Fatalf("hero data = %v, want stable's write", got.Data)
	}

	// Gates flipped with the handshake: canary writes, stable denied.
	canary := canaryGate.Client(k8sClient, k8sClient)
	if err := canary.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "hand-a", Name: "c-touches"}}); err != nil {
		t.Errorf("canary create after acquire: %v", err)
	}
	if err := stable.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "hand-a", Name: "s-touches"}}); err == nil {
		t.Error("stable create after release: nil error, want NotOwned")
	} else if denied, ok := shardkit.AsDenied(err); !ok || denied.Reason != shardkit.ReasonNotOwned {
		t.Errorf("stable create after release: %v, want NotOwned", err)
	}
}

// TestObserver_TamperLockstep_RealServer pins the read-time V9
// lockstep on a real server (regression for the 2026-09-28 kind
// stranding, where the observer handshook a tampered version the
// gate refused and later Off acks converged while canary-owned
// objects never moved). A same-epoch seed tamper freezes both
// tracks' acks at the pre-tamper version and denies all writes; a
// contracted carry-forward converges; and the Off abort after it
// moves ownership back instead of stranding it.
func TestObserver_TamperLockstep_RealServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tamper-a"}}
	if err := k8sClient.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	holder, transitions := "pod-0", int32(0)
	for _, track := range []string{"stable", "canary"} {
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: testNS, Name: "tamperlock-" + track,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &holder, LeaseTransitions: &transitions,
			},
		}
		if err := k8sClient.Create(ctx, lease); err != nil {
			t.Fatalf("lease %s: %v", track, err)
		}
	}
	plan := basePlan("tamperlock")
	plan.Spec.Canary.Mode = v1alpha1.ModeOff
	plan.Spec.Canary.WeightPerMille = 0
	plan.Spec.Canary.Include = v1alpha1.IncludeSpec{}
	requireCreate(t, plan)
	key := types.NamespacedName{Namespace: testNS, Name: "tamperlock"}

	reg := prometheus.NewRegistry()
	metrics := shardkit.NewMetrics(reg)
	stableGate, err := shardkit.Attach(ctx, k8sClient, key, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	canaryGate, err := shardkit.Attach(ctx, k8sClient, key, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	mkObserver := func(g *shardkit.Gate, track string) *shardkit.Observer {
		t.Helper()
		obs, err := shardkit.NewObserver(shardkit.ObserverOptions{
			Client: k8sClient, APIReader: k8sClient,
			Gate: g, Guarded: g.Client(k8sClient, k8sClient),
			PlanKey: key, Track: track, LeaseBase: "tamperlock",
			Types:          []schema.GroupVersionKind{cmGVK},
			PollInterval:   10 * time.Millisecond,
			DrainTimeout:   10 * time.Second,
			AcquireTimeout: 30 * time.Second,
			Metrics:        metrics,
		})
		if err != nil {
			t.Fatal(err)
		}
		return obs
	}
	stableObs, canaryObs := mkObserver(stableGate, "stable"), mkObserver(canaryGate, "canary")
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable resume: %v", err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary resume: %v", err)
	}
	entry := func(track string) v1alpha1.TrackStatus {
		t.Helper()
		p := requireGet(t, "tamperlock")
		for _, e := range p.Status.Tracks {
			if e.Name == track {
				return e
			}
		}
		t.Fatalf("no %s entry in %+v", track, p.Status.Tracks)
		return v1alpha1.TrackStatus{}
	}

	// Contracted flip to Active/1000 at epoch 2; canary takes all.
	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	live.Spec.Canary.Mode = v1alpha1.ModeActive
	live.Spec.Canary.WeightPerMille = 1000
	if err := k8sClient.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable release: %v", err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary acquire: %v", err)
	}
	if e := entry("stable"); e.ObservedEpoch != 2 || !e.Released {
		t.Fatalf("stable entry = %+v, want Released epoch 2", e)
	}
	if e := entry("canary"); e.ObservedEpoch != 2 || e.Phase != v1alpha1.PhaseAcquired {
		t.Fatalf("canary entry = %+v, want Acquired epoch 2", e)
	}
	stable, canary := stableGate.Client(k8sClient, k8sClient), canaryGate.Client(k8sClient, k8sClient)
	if err := canary.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tamper-a", Name: "c-holds"}}); err != nil {
		t.Fatalf("canary create at epoch 2: %v", err)
	}

	// Tamper: same-epoch seed change. Both tracks hold: acks frozen
	// at the epoch-2 version (the live generation moved on), one
	// refusal counted per track, and every write denied.
	preGen := requireGet(t, "tamperlock").Generation
	if err := k8sClient.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Seed = "tampered"
	if err := k8sClient.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if got := requireGet(t, "tamperlock").Generation; got == preGen {
		t.Fatalf("live generation stayed %d, want a tamper bump", got)
	}
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable held step: %v", err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary held step: %v", err)
	}
	if e := entry("stable"); e.ObservedEpoch != 2 || e.ObservedGeneration != preGen {
		t.Fatalf("stable entry = epoch %d gen %d, want frozen epoch 2 gen %d",
			e.ObservedEpoch, e.ObservedGeneration, preGen)
	}
	if e := entry("canary"); e.ObservedEpoch != 2 || e.ObservedGeneration != preGen {
		t.Fatalf("canary entry = epoch %d gen %d, want frozen epoch 2 gen %d",
			e.ObservedEpoch, e.ObservedGeneration, preGen)
	}
	if got := testutil.ToFloat64(metrics.Transitions("stable", "rev-a", "advance_refused")); got != 1 {
		t.Errorf("stable advance_refused = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Transitions("canary", "rev-b", "advance_refused")); got != 1 {
		t.Errorf("canary advance_refused = %v, want 1", got)
	}
	if err := canary.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tamper-a", Name: "c-during-tamper"}}); err == nil {
		t.Error("canary create during tamper: nil error, want denied")
	}
	if err := stable.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tamper-a", Name: "s-during-tamper"}}); err == nil {
		t.Error("stable create during tamper: nil error, want denied")
	}

	// Contracted carry-forward: same seed with a fresh rollout and
	// epoch 3 converges; canary writes again.
	if err := k8sClient.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Rollout = "tamperlock-r3"
	live.Spec.Epoch = 3
	if err := k8sClient.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable carry-forward: %v", err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary carry-forward: %v", err)
	}
	if e := entry("stable"); e.ObservedEpoch != 3 {
		t.Fatalf("stable entry = %+v, want epoch 3", e)
	}
	if e := entry("canary"); e.ObservedEpoch != 3 {
		t.Fatalf("canary entry = %+v, want epoch 3", e)
	}
	if err := canary.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tamper-a", Name: "c-after-repair"}}); err != nil {
		t.Errorf("canary create after repair: %v", err)
	}

	// Off abort at epoch 4 moves ownership back: canary releases,
	// stable acquires, and the write polarity flips — nothing
	// strands under converged acks.
	if err := k8sClient.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 4
	live.Spec.Canary.Mode = v1alpha1.ModeOff
	live.Spec.Canary.WeightPerMille = 0
	if err := k8sClient.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary abort release: %v", err)
	}
	if e := entry("canary"); e.ObservedEpoch != 4 || !e.Released ||
		e.Phase != v1alpha1.PhaseReleased {
		t.Fatalf("canary entry = %+v, want Released epoch 4", e)
	}
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable abort acquire: %v", err)
	}
	if e := entry("stable"); e.ObservedEpoch != 4 || e.Phase != v1alpha1.PhaseAcquired {
		t.Fatalf("stable entry = %+v, want Acquired epoch 4", e)
	}
	if err := stable.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tamper-a", Name: "s-after-abort"}}); err != nil {
		t.Errorf("stable create after abort: %v", err)
	}
	if err := canary.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tamper-a", Name: "c-after-abort"}}); err == nil {
		t.Error("canary create after abort: nil error, want NotOwned")
	} else if denied, ok := shardkit.AsDenied(err); !ok || denied.Reason != shardkit.ReasonNotOwned {
		t.Errorf("canary create after abort: %v, want NotOwned", err)
	}
}
