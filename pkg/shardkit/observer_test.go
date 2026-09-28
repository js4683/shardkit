package shardkit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/partition"
)

var cmGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

// observerFixture builds an isolated observer world: Off plan,
// demo-87/default namespaces, both track leases held, and the gate,
// guarded client, and observer for track. Leases carry fixed holder
// identity so S8 bindings are stable.
func observerFixture(t *testing.T, track string) (*Observer, *Gate, client.Client) {
	t.Helper()
	holder, transitions := "pod-0", int32(0)
	mkLease := func(name string) *coordinationv1.Lease {
		return &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: planKey.Namespace, Name: "widget-operator-" + name},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &transitions},
		}
	}
	fc := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithRESTMapper(testRestMapper()).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(
			baseOffPlan(),
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
			mkLease("stable"), mkLease("canary"),
		).Build()
	ctx := context.Background()
	g, err := Attach(ctx, fc, planKey, track, "rev-x")
	if err != nil {
		t.Fatal(err)
	}
	obs, err := NewObserver(ObserverOptions{
		Client: fc, APIReader: fc, Gate: g, Guarded: g.Client(fc, fc),
		PlanKey: planKey, Track: track, LeaseBase: "widget-operator",
		Types:        []schema.GroupVersionKind{cmGVK},
		PollInterval: 5 * time.Millisecond, AcquireTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return obs, g, fc
}

// ownEntry fetches this track's status entry or fails.
func ownEntry(t *testing.T, c client.Client, track string) v1alpha1.TrackStatus {
	t.Helper()
	var live v1alpha1.ShardPlan
	if err := c.Get(context.Background(), planKey, &live); err != nil {
		t.Fatal(err)
	}
	for _, e := range live.Status.Tracks {
		if e.Name == track {
			return e
		}
	}
	t.Fatalf("no %s entry in %+v", track, live.Status.Tracks)
	return v1alpha1.TrackStatus{}
}

// TestNewObserver_Validation pins fail-fast integrator config: every
// required option missing is an explicit error.
func TestNewObserver_Validation(t *testing.T) {
	_, g, fc := observerFixture(t, "stable")
	good := ObserverOptions{
		Client: fc, APIReader: fc, Gate: g, Guarded: g.Client(fc, fc),
		PlanKey: planKey, Track: "stable", LeaseBase: "widget-operator",
		Types: []schema.GroupVersionKind{cmGVK},
	}
	if _, err := NewObserver(good); err != nil {
		t.Fatalf("valid options: %v", err)
	}
	clustered := good
	clustered.ClusterName = "prod-east"
	if _, err := NewObserver(clustered); err != nil {
		t.Fatalf("clustered options: %v", err)
	}
	bad := []struct {
		name   string
		mutate func(*ObserverOptions)
	}{
		{"nil client", func(o *ObserverOptions) { o.Client = nil }},
		{"nil reader", func(o *ObserverOptions) { o.APIReader = nil }},
		{"nil gate", func(o *ObserverOptions) { o.Gate = nil }},
		{"nil guarded", func(o *ObserverOptions) { o.Guarded = nil }},
		{"bad track", func(o *ObserverOptions) { o.Track = "warp" }},
		{"empty lease base", func(o *ObserverOptions) { o.LeaseBase = "" }},
		{"no types", func(o *ObserverOptions) { o.Types = nil }},
		{"gate mismatch", func(o *ObserverOptions) { o.Track = "canary" }},
		{"slashed cluster", func(o *ObserverOptions) { o.ClusterName = "east/west" }},
	}
	for _, c := range bad {
		o := good
		c.mutate(&o)
		if _, err := NewObserver(o); err == nil {
			t.Errorf("%s: nil error, want explicit failure", c.name)
		}
	}
}

// TestObserver_Resume pins first-poll adoption: the observer takes
// the live version, drains the complement of its grant, and acks.
// Stable holds everything Off; canary holds nothing and denies all.
func TestObserver_Resume(t *testing.T) {
	ctx := context.Background()
	t.Run("stable holds Off", func(t *testing.T) {
		obs, g, fc := observerFixture(t, "stable")
		if err := obs.Step(ctx); err != nil {
			t.Fatal(err)
		}
		e := ownEntry(t, fc, "stable")
		if e.ObservedEpoch != 1 || !e.Released || e.Phase != v1alpha1.PhaseReleased || e.OwnedNamespaces != 2 {
			t.Fatalf("entry = %+v, want Released epoch 1 owned 2", e)
		}
		if e.Session == nil || e.Session.Holder != "pod-0" {
			t.Fatalf("session = %+v", e.Session)
		}
		if obs2, err := g.Owned(ctx, "demo-87"); err != nil || !obs2.Owned {
			t.Fatalf("gate after resume: %+v %v, want owned", obs2, err)
		}
		// Second step is steady: no duplicate transition.
		if err := obs.Step(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("canary denies Off", func(t *testing.T) {
		obs, g, fc := observerFixture(t, "canary")
		if err := obs.Step(ctx); err != nil {
			t.Fatal(err)
		}
		e := ownEntry(t, fc, "canary")
		if e.ObservedEpoch != 1 || !e.Released || e.OwnedNamespaces != 0 {
			t.Fatalf("entry = %+v, want Released epoch 1 owned 0", e)
		}
		if obs, err := g.Owned(ctx, "demo-87"); err != nil || obs.Owned {
			t.Fatalf("gate after resume: %+v %v, want foreign", obs, err)
		}
	})
}

// TestObserver_Release pins the relinquish path: a flip that takes
// namespaces away drains and acks the new version with owned=0.
func TestObserver_Release(t *testing.T) {
	ctx := context.Background()
	obs, g, fc := observerFixture(t, "stable")
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	flipFull(t, fc)
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	e := ownEntry(t, fc, "stable")
	if e.ObservedEpoch != 2 || !e.Released || e.Phase != v1alpha1.PhaseReleased || e.OwnedNamespaces != 0 {
		t.Fatalf("entry = %+v, want Released epoch 2 owned 0", e)
	}
	if obs, err := g.Owned(ctx, "demo-87"); err != nil || obs.Owned {
		t.Fatalf("gate after release: %+v %v, want foreign", obs, err)
	}
	// Completed transitions clear the drain set; the plan itself now
	// denies the relinquished namespaces.
	if len(g.DrainingSet()) != 0 {
		t.Fatalf("draining = %v, want clear after completed transition", g.DrainingSet())
	}
}

// forgeAck writes a valid release entry for track at the live
// version, simulating the counterpart's drain completion.
func forgeAck(t *testing.T, c client.Client, track, rev string) {
	t.Helper()
	ctx := context.Background()
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	var lease coordinationv1.Lease
	if err := c.Get(ctx, client.ObjectKey{Namespace: planKey.Namespace,
		Name: "widget-operator-" + track}, &lease); err != nil {
		t.Fatal(err)
	}
	entry := v1alpha1.TrackStatus{
		Name: track, Revision: rev, PlanUID: string(live.UID),
		ObservedGeneration: live.Generation, ObservedEpoch: live.Spec.Epoch,
		Rollout: live.Spec.Rollout, Phase: v1alpha1.PhaseReleased, Released: true,
		ReleasedWrites: map[string]string{},
		Session: &v1alpha1.LeaderSession{
			Holder: *lease.Spec.HolderIdentity, LeaseTransitions: *lease.Spec.LeaseTransitions,
		},
	}
	found := false
	for i := range live.Status.Tracks {
		if live.Status.Tracks[i].Name == track {
			live.Status.Tracks[i] = entry
			found = true
		}
	}
	if !found {
		live.Status.Tracks = append(live.Status.Tracks, entry)
	}
	if err := c.Status().Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
}

// TestObserver_Acquire pins the gain path: behind a valid loser ack
// (with empty evidence, so the barrier passes vacuously), the
// observer takes the gained set, undrains, and acks Acquired.
func TestObserver_Acquire(t *testing.T) {
	ctx := context.Background()
	obs, g, fc := observerFixture(t, "canary")
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	flipFull(t, fc)
	forgeAck(t, fc, "stable", "rev-a")
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	e := ownEntry(t, fc, "canary")
	if e.ObservedEpoch != 2 || e.Phase != v1alpha1.PhaseAcquired || e.OwnedNamespaces != 2 {
		t.Fatalf("entry = %+v, want Acquired epoch 2 owned 2", e)
	}
	if obs, err := g.Owned(ctx, "demo-87"); err != nil || !obs.Owned {
		t.Fatalf("gate after acquire: %+v %v, want owned", obs, err)
	}
	if len(g.DrainingSet()) != 0 {
		t.Fatalf("draining = %v, want empty after acquire", g.DrainingSet())
	}
	// Handoff events on the fake client enumerate nothing (the fake
	// mapper guesses list resources): content is proven in envtest.
}

// TestObserver_AcquireWaits pins no-unilateral-acquire: without the
// loser ack the observer waits (degrading past the deadline) and a
// canceled context surfaces instead of acquiring.
func TestObserver_AcquireWaits(t *testing.T) {
	ctx := context.Background()
	obs, g, fc := observerFixture(t, "canary")
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	flipFull(t, fc)
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- obs.Step(wctx) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("step err = %v, want context.Canceled", err)
	}
	e := ownEntry(t, fc, "canary")
	// Past the acquire deadline the waiter reports degraded (never
	// unilateral): epoch 2, Draining, unreleased, AcquireWaiting.
	if e.ObservedEpoch != 2 || e.Phase != v1alpha1.PhaseDraining || e.Released {
		t.Fatalf("entry = %+v, want degraded waiting entry for epoch 2", e)
	}
	waiting := false
	for _, c := range e.Conditions {
		if c.Type == "Degraded" && c.Status == metav1.ConditionTrue && c.Reason == "AcquireWaiting" {
			waiting = true
		}
	}
	if !waiting {
		t.Fatalf("conditions = %+v, want Degraded/AcquireWaiting", e.Conditions)
	}
	if obs, _ := g.Owned(ctx, "demo-87"); obs.Owned {
		t.Fatal("gate allows demo-87 without loser ack")
	}
}

// TestObserver_AcquireMovesOn pins mid-wait plan movement: when the
// plan advances during an acquire wait, the step returns cleanly and
// the next poll re-evaluates the newer version.
func TestObserver_AcquireMovesOn(t *testing.T) {
	ctx := context.Background()
	obs, _, fc := observerFixture(t, "canary")
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	flipFull(t, fc)
	done := make(chan error, 1)
	go func() { done <- obs.Step(ctx) }()
	time.Sleep(50 * time.Millisecond)
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 3
	live.Spec.Canary.WeightPerMille = 500
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("step err = %v, want clean move-on", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("step never returned after plan moved on")
	}
}

// TestObserver_SteadyRefresh pins S8 freshness: a rotated lease
// session triggers republish; a fresh entry does not churn.
func TestObserver_SteadyRefresh(t *testing.T) {
	ctx := context.Background()
	obs, _, fc := observerFixture(t, "stable")
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	before := ownEntry(t, fc, "stable")
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
	after := ownEntry(t, fc, "stable")
	if after.Session.Holder != "pod-1" || after.Session.LeaseTransitions != 1 {
		t.Fatalf("session = %+v, want rotated holder", after.Session)
	}
	if after.ObservedEpoch != before.ObservedEpoch {
		t.Fatalf("epoch moved %d -> %d on session refresh", before.ObservedEpoch, after.ObservedEpoch)
	}
}

// TestObserver_RecreatedPlan pins strict B3 for live processes: a new
// UID outside Off degrades without adopting; driven through Off, it
// adopts with fresh baselines.
func TestObserver_RecreatedPlan(t *testing.T) {
	ctx := context.Background()
	obs, _, fc := observerFixture(t, "stable")
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	if err := fc.Delete(ctx, &live); err != nil {
		t.Fatal(err)
	}
	recreated := baseOffPlan()
	recreated.UID = "uid-2"
	recreated.Spec.Canary.Mode = v1alpha1.ModeActive
	recreated.Spec.Canary.WeightPerMille = 1000
	if err := fc.Create(ctx, recreated); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	e := ownEntry(t, fc, "stable")
	if e.PlanUID != "uid-2" || e.Phase != v1alpha1.PhaseDraining {
		t.Fatalf("entry = %+v, want Draining for uid-2", e)
	}
	degraded := false
	for _, c := range e.Conditions {
		degraded = degraded || (c.Type == "Degraded" && c.Status == metav1.ConditionTrue)
	}
	if !degraded {
		t.Fatalf("conditions = %+v, want Degraded=True", e.Conditions)
	}
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Canary.Mode = v1alpha1.ModeOff
	live.Spec.Canary.WeightPerMille = 0
	live.Spec.Epoch = 2
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	e = ownEntry(t, fc, "stable")
	if e.PlanUID != "uid-2" || e.ObservedEpoch != 2 || e.Phase != v1alpha1.PhaseReleased {
		t.Fatalf("entry = %+v, want Acquired uid-2 epoch 2", e)
	}
}

// TestObserver_MalformedAndDeleted pins handshake silence: malformed
// and deleted plans publish nothing new (the gate is closed; acks
// for unvalid versions would be lies).
func TestObserver_MalformedAndDeleted(t *testing.T) {
	ctx := context.Background()
	obs, _, fc := observerFixture(t, "stable")
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	before := ownEntry(t, fc, "stable")
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	live.Spec.Canary.WeightPerMille = 1001
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if e := ownEntry(t, fc, "stable"); e.ObservedEpoch != before.ObservedEpoch {
		t.Fatalf("malformed step published epoch %d", e.ObservedEpoch)
	}
	if err := fc.Delete(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestObserver_SameEpochGenerationBump pins the read-time V9
// lockstep: one transition is one (UID, generation, epoch), but a
// singleton flip at the same epoch (a writer bypassing the
// strict-epoch contract) is held, not handshaked — both tracks keep
// their baseline acks and duties, and count advance_refused. The
// next contracted version (epoch bump) then drives the full
// handshake from the held baselines, and both entries bind the new
// generation.
func TestObserver_SameEpochGenerationBump(t *testing.T) {
	ctx := context.Background()
	_, _, fc := observerFixture(t, "stable")
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	mkObserver := func(track, rev string) (*Observer, *Gate) {
		t.Helper()
		g, err := Attach(ctx, fc, planKey, track, rev)
		if err != nil {
			t.Fatal(err)
		}
		obs, err := NewObserver(ObserverOptions{
			Client: fc, APIReader: fc, Gate: g, Guarded: g.Client(fc, fc),
			PlanKey: planKey, Track: track, LeaseBase: "widget-operator",
			Types:          []schema.GroupVersionKind{cmGVK},
			PollInterval:   5 * time.Millisecond,
			AcquireTimeout: 50 * time.Millisecond,
			Metrics:        metrics,
		})
		if err != nil {
			t.Fatal(err)
		}
		return obs, g
	}
	stableObs, stableGate := mkObserver("stable", "rev-a")
	canaryObs, canaryGate := mkObserver("canary", "rev-b")
	if err := stableObs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if sing, _ := stableGate.SingletonOwned(ctx); !sing {
		t.Fatal("stable lacks singleton duty at baseline")
	}
	baseStable, baseCanary := ownEntry(t, fc, "stable"), ownEntry(t, fc, "canary")

	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.SingletonOwner = "canary"
	live.Generation++
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}

	// The bypassing version is held: no ack advance, no duty flip,
	// no handoff events, one refusal counted per track.
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable held step: %v", err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary held step: %v", err)
	}
	if got := ownEntry(t, fc, "stable"); got.ObservedEpoch != baseStable.ObservedEpoch ||
		got.ObservedGeneration != baseStable.ObservedGeneration || got.Phase != baseStable.Phase {
		t.Fatalf("stable entry moved to %+v, want held %+v", got, baseStable)
	}
	if got := ownEntry(t, fc, "canary"); got.ObservedEpoch != baseCanary.ObservedEpoch ||
		got.ObservedGeneration != baseCanary.ObservedGeneration || got.Phase != baseCanary.Phase {
		t.Fatalf("canary entry moved to %+v, want held %+v", got, baseCanary)
	}
	// Held versions close the gate on both paths: duty reads refuse
	// instead of following the tampered version.
	if _, err := stableGate.SingletonOwned(ctx); err == nil {
		t.Fatal("stable SingletonOwned on held version: nil error, want gate-closed refusal")
	} else if cerr, ok := AsClosed(err); !ok || cerr.Reason != ReasonPlanMalformed {
		t.Fatalf("stable SingletonOwned on held version = %v, want PlanMalformed", err)
	}
	// Canary reads foreign (it drains a duty it never held); the
	// stable refusal above pins the V9 verdict on this path.
	if sing, _ := canaryGate.SingletonOwned(ctx); sing {
		t.Fatal("canary gained singleton duty on a held version")
	}
	for _, ev := range []<-chan event.GenericEvent{stableObs.Events(), canaryObs.Events()} {
		select {
		case got := <-ev:
			t.Fatalf("handoff event on held version: %v", got.Object)
		default:
		}
	}
	if got := testutil.ToFloat64(metrics.Transitions("stable", "rev-a", "advance_refused")); got != 1 {
		t.Errorf("stable advance_refused = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.Transitions("canary", "rev-b", "advance_refused")); got != 1 {
		t.Errorf("canary advance_refused = %v, want 1", got)
	}

	// The contracted version converges from the held baselines.
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	live.Generation++
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	var wantGen int64
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	wantGen = live.Generation

	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable relinquish: %v", err)
	}
	rel := ownEntry(t, fc, "stable")
	if rel.ObservedEpoch != 2 || rel.ObservedGeneration != wantGen ||
		!rel.Released || rel.Phase != v1alpha1.PhaseReleased {
		t.Fatalf("stable entry = %+v, want Released epoch 2 gen %d", rel, wantGen)
	}
	if sing, _ := stableGate.SingletonOwned(ctx); sing {
		t.Fatal("stable keeps singleton duty after relinquish")
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary acquire: %v", err)
	}
	acq := ownEntry(t, fc, "canary")
	if acq.ObservedEpoch != 2 || acq.ObservedGeneration != wantGen ||
		acq.Phase != v1alpha1.PhaseAcquired {
		t.Fatalf("canary entry = %+v, want Acquired epoch 2 gen %d", acq, wantGen)
	}
	if sing, _ := canaryGate.SingletonOwned(ctx); !sing {
		t.Fatal("canary lacks singleton duty after acquire")
	}
}

// TestObserver_SeedTamperHeld pins the lockstep on one track: a
// same-epoch seed tamper is held (acks frozen at the baseline
// version, the gate unmoved, no handoff events), and the contracted
// carry-forward — same seed with a fresh rollout and epoch bump —
// converges from the held baselines.
func TestObserver_SeedTamperHeld(t *testing.T) {
	ctx := context.Background()
	obs, g, fc := observerFixture(t, "stable")
	if err := obs.Step(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}
	base := ownEntry(t, fc, "stable")

	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Seed = "tampered"
	live.Generation++
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil {
		t.Fatalf("held step: %v", err)
	}
	if got := ownEntry(t, fc, "stable"); got.ObservedEpoch != base.ObservedEpoch ||
		got.ObservedGeneration != base.ObservedGeneration {
		t.Fatalf("entry moved to epoch %d gen %d, want held epoch %d gen %d",
			got.ObservedEpoch, got.ObservedGeneration, base.ObservedEpoch, base.ObservedGeneration)
	}
	if ver, _ := g.Adopted(); ver.Epoch != base.ObservedEpoch ||
		ver.Generation != base.ObservedGeneration {
		t.Fatalf("gate adopted epoch %d gen %d, want held epoch %d gen %d",
			ver.Epoch, ver.Generation, base.ObservedEpoch, base.ObservedGeneration)
	}
	select {
	case got := <-obs.Events():
		t.Fatalf("handoff event on held version: %v", got.Object)
	default:
	}

	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Rollout = "r-2"
	live.Spec.Epoch = 2
	live.Generation++
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	wantGen := live.Generation
	if err := obs.Step(ctx); err != nil {
		t.Fatalf("carry-forward step: %v", err)
	}
	got := ownEntry(t, fc, "stable")
	if got.ObservedEpoch != 2 || got.ObservedGeneration != wantGen {
		t.Fatalf("entry = epoch %d gen %d, want converged epoch 2 gen %d",
			got.ObservedEpoch, got.ObservedGeneration, wantGen)
	}
}

// TestObserver_AbandonedAcquireNarrowsHeld pins abandon bookkeeping
// (regression for the 2026-09-28 kind stranding): when the plan
// moves on mid-acquire, the abandoned version still relinquishes —
// the next version's delta covers namespaces that changed only in
// the skipped version instead of stranding them under converged
// acks. Staging is deterministic: the loser never steps the
// abandoned version, so the winner's acquire cannot complete before
// the superseding write lands.
func TestObserver_AbandonedAcquireNarrowsHeld(t *testing.T) {
	ctx := context.Background()
	// Two seeds splitting {demo-87, default} oppositely at
	// Active/500: s0 grants demo-87 to stable, s1 grants default.
	stableOf := func(seed string) []string {
		spec := partition.Spec{Mode: partition.ModeActive, Seed: seed, WeightPerMille: 500}
		var out []string
		for _, ns := range []string{"demo-87", "default"} {
			owner, err := spec.Owner(ns, nil)
			if err != nil {
				t.Fatal(err)
			}
			if owner == partition.Stable {
				out = append(out, ns)
			}
		}
		return out
	}
	var s0, s1 string
	for i := 0; i < 2000 && (s0 == "" || s1 == ""); i++ {
		seed := fmt.Sprintf("abandon-%d", i)
		if st := stableOf(seed); len(st) == 1 {
			if st[0] == "demo-87" && s0 == "" {
				s0 = seed
			}
			if st[0] == "default" && s1 == "" {
				s1 = seed
			}
		}
	}
	if s0 == "" || s1 == "" {
		t.Fatal("no opposite-split seeds found")
	}

	holder, transitions := "pod-0", int32(0)
	mkLease := func(name string) *coordinationv1.Lease {
		return &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: planKey.Namespace, Name: "widget-operator-" + name},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &transitions},
		}
	}
	base := baseOffPlan()
	base.Spec.Seed = s0
	base.Spec.Canary.Mode = v1alpha1.ModeActive
	base.Spec.Canary.WeightPerMille = 500
	fc := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithRESTMapper(testRestMapper()).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(
			base,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm-a"}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cm-b"}},
			mkLease("stable"), mkLease("canary"),
		).Build()
	mkObserver := func(track, rev string) (*Observer, *Gate) {
		t.Helper()
		g, err := Attach(ctx, fc, planKey, track, rev)
		if err != nil {
			t.Fatal(err)
		}
		obs, err := NewObserver(ObserverOptions{
			Client: fc, APIReader: fc, Gate: g, Guarded: g.Client(fc, fc),
			PlanKey: planKey, Track: track, LeaseBase: "widget-operator",
			Types:          []schema.GroupVersionKind{cmGVK},
			PollInterval:   5 * time.Millisecond,
			AcquireTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		return obs, g
	}
	stableObs, _ := mkObserver("stable", "rev-a")
	canaryObs, _ := mkObserver("canary", "rev-b")
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable resume: %v", err)
	}
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary resume: %v", err)
	}

	writeSpec := func(mut func(*v1alpha1.ShardPlanSpec)) {
		t.Helper()
		var live v1alpha1.ShardPlan
		if err := fc.Get(ctx, planKey, &live); err != nil {
			t.Fatal(err)
		}
		mut(&live.Spec)
		live.Generation++
		if err := fc.Update(ctx, &live); err != nil {
			t.Fatal(err)
		}
	}
	// M flips the split: stable relinquishes demo-87, gains default.
	writeSpec(func(spec *v1alpha1.ShardPlanSpec) {
		spec.Seed = s1
		spec.Rollout = "r-2"
		spec.Epoch = 2
	})
	stepErr := make(chan error, 1)
	go func() { stepErr <- stableObs.Step(ctx) }()
	// Wait for stable's M release: it is now parked in acquire,
	// which can only abandon (canary never steps M, so no M ack for
	// the loser can ever appear).
	deadline := time.Now().Add(10 * time.Second)
	for {
		if e := ownEntry(t, fc, "stable"); e.ObservedEpoch == 2 && e.Released {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stable never published its M release")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// N aborts to Off, superseding M mid-acquire.
	writeSpec(func(spec *v1alpha1.ShardPlanSpec) {
		spec.Canary.Mode = v1alpha1.ModeOff
		spec.Canary.WeightPerMille = 0
		spec.Epoch = 3
	})
	select {
	case err := <-stepErr:
		if err != nil {
			t.Fatalf("stable abandoned step: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stable M step never returned")
	}

	// Both converge on N; stable's handoff wave must carry both
	// namespaces — default (N's own delta) and demo-87 (changed
	// only in the abandoned M).
	if err := canaryObs.Step(ctx); err != nil {
		t.Fatalf("canary N step: %v", err)
	}
	if err := stableObs.Step(ctx); err != nil {
		t.Fatalf("stable N step: %v", err)
	}
	if e := ownEntry(t, fc, "stable"); e.ObservedEpoch != 3 || e.Phase != v1alpha1.PhaseAcquired {
		t.Fatalf("stable entry = %+v, want Acquired epoch 3", e)
	}
	if e := ownEntry(t, fc, "canary"); e.ObservedEpoch != 3 || !e.Released {
		t.Fatalf("canary entry = %+v, want Released epoch 3", e)
	}
	got := map[string]bool{}
	drain := time.After(5 * time.Second)
	for len(got) < 2 {
		select {
		case ev := <-stableObs.Events():
			got[ev.Object.GetNamespace()] = true
		case <-drain:
			t.Fatalf("stable handoff namespaces = %v, want demo-87 and default", got)
		}
	}
}
