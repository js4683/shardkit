package shardkit

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

var planKey = types.NamespacedName{Namespace: "widget-system", Name: "widget-operator"}

// baseOffPlan is an attachable Off plan (epoch 1) carrying a canary
// revision so tests can flip it Active.
func baseOffPlan() *v1alpha1.ShardPlan {
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

func testScheme(t testing.TB) *runtime.Scheme {
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

// fixture builds a fake client with the Off plan plus demo-87
// (bucket 44), default (782), and critical-db namespaces.
func fixture(t testing.TB, mutate func(*v1alpha1.ShardPlan)) client.Client {
	t.Helper()
	plan := baseOffPlan()
	if mutate != nil {
		mutate(plan)
	}
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		plan,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "critical-db"}},
	).Build()
}

// flipActive rewrites the live plan to Active w=10 at epoch 2.
func flipActive(t *testing.T, c client.Client) {
	t.Helper()
	ctx := context.Background()
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	live.Spec.Canary.Mode = v1alpha1.ModeActive
	live.Spec.Canary.WeightPerMille = 10
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
}

// TestAttach pins B1: missing plans, non-Off modes, and bad tracks
// fail loudly; a valid Off plan attaches and evaluates.
func TestAttach(t *testing.T) {
	ctx := context.Background()
	t.Run("missing plan names the plan", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
		_, err := Attach(ctx, c, planKey, "stable", "rev-a")
		if err == nil || !strings.Contains(err.Error(), planKey.Name) {
			t.Fatalf("err = %v, want B1 error naming the plan", err)
		}
	})
	t.Run("non-Off mode adopts (observer runs resume protocol)", func(t *testing.T) {
		c := fixture(t, func(p *v1alpha1.ShardPlan) {
			p.Spec.Canary.Mode = v1alpha1.ModeActive
			p.Spec.Canary.WeightPerMille = 10
		})
		g, err := Attach(ctx, c, planKey, "canary", "rev-b")
		if err != nil {
			t.Fatalf("attach Active: %v", err)
		}
		obs, err := g.Owned(ctx, "demo-87")
		if err != nil || !obs.Owned {
			t.Fatalf("obs = %+v err = %v, want owned window hit", obs, err)
		}
	})
	t.Run("bad track rejected", func(t *testing.T) {
		c := fixture(t, nil)
		if _, err := Attach(ctx, c, planKey, "warp", "rev-a"); err == nil {
			t.Fatal("nil error, want track rejection")
		}
	})
	t.Run("malformed plan rejected", func(t *testing.T) {
		c := fixture(t, func(p *v1alpha1.ShardPlan) { p.Spec.Seed = "" })
		if _, err := Attach(ctx, c, planKey, "stable", "rev-a"); err == nil {
			t.Fatal("nil error, want malformed rejection")
		}
	})
	t.Run("valid attach evaluates Off", func(t *testing.T) {
		c := fixture(t, nil)
		g, err := Attach(ctx, c, planKey, "stable", "rev-a")
		if err != nil {
			t.Fatal(err)
		}
		obs, err := g.Owned(ctx, "demo-87")
		if err != nil {
			t.Fatal(err)
		}
		if !obs.Owned || obs.Epoch != 1 || obs.Mode != "Off" {
			t.Fatalf("obs = %+v, want owned Off epoch 1", obs)
		}
	})
}

// TestOwned_TracksAndWindow pins T1-T2 across a flip: window hits
// and misses route to opposite tracks with the live version attached.
func TestOwned_TracksAndWindow(t *testing.T) {
	ctx := context.Background()
	for _, track := range []string{"stable", "canary"} {
		c := fixture(t, nil)
		g, err := Attach(ctx, c, planKey, track, "rev-x")
		if err != nil {
			t.Fatal(err)
		}
		flipActive(t, c)
		hit, err := g.Owned(ctx, "demo-87") // bucket 44, window 37..46
		if err != nil {
			t.Fatal(err)
		}
		miss, err := g.Owned(ctx, "default") // bucket 782
		if err != nil {
			t.Fatal(err)
		}
		wantHit := track == "canary"
		if hit.Owned != wantHit || miss.Owned == wantHit {
			t.Fatalf("track %s: hit=%v miss=%v, want hit=%v", track, hit.Owned, miss.Owned, wantHit)
		}
		if hit.Epoch != 2 || hit.Mode != "Active" || hit.Weight != 10 || hit.Stale {
			t.Fatalf("track %s: obs = %+v, want fresh epoch 2", track, hit)
		}
	}
}

// TestOwned_StaleIgnored pins V10: a backward epoch reuses retained
// state (marked stale) and never moves the baseline back.
func TestOwned_StaleIgnored(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	flipActive(t, c)
	if obs, _ := g.Owned(ctx, "demo-87"); !obs.Owned || obs.Epoch != 2 {
		t.Fatalf("fresh obs = %+v, want owned epoch 2", obs)
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 1 // transport delivers an old version
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	obs, err := g.Owned(ctx, "demo-87")
	if err != nil {
		t.Fatal(err)
	}
	if !obs.Owned || !obs.Stale || obs.Epoch != 2 {
		t.Fatalf("stale obs = %+v, want retained owned epoch 2", obs)
	}
	live.Spec.Epoch = 3
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if obs, _ := g.Owned(ctx, "demo-87"); !obs.Owned || obs.Stale || obs.Epoch != 3 {
		t.Fatalf("recovered obs = %+v, want fresh owned epoch 3", obs)
	}
}

// TestOwned_SameEpochGenerationBump pins V10's generation tiebreak
// under read-time V9: a same-epoch generation bump WITHOUT a spec
// change (metadata-only) re-evaluates, while a same-epoch SPEC
// change (a writer bypassing the strict-epoch contract) is refused
// and the gate holds its last state until a proper epoch carries
// the change.
func TestOwned_SameEpochGenerationBump(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	if obs, _ := g.Owned(ctx, "demo-87"); obs.Owned {
		t.Fatalf("Off obs = %+v, want foreign", obs)
	}
	bump := func(mut func(*v1alpha1.ShardPlan)) {
		t.Helper()
		var live v1alpha1.ShardPlan
		if err := c.Get(ctx, planKey, &live); err != nil {
			t.Fatal(err)
		}
		mut(&live)
		live.Generation++
		if err := c.Update(ctx, &live); err != nil {
			t.Fatal(err)
		}
	}
	// Metadata-only: same spec, new generation re-evaluates.
	bump(func(live *v1alpha1.ShardPlan) {
		live.Annotations = map[string]string{"note": "x"}
	})
	if obs, err := g.Owned(ctx, "demo-87"); err != nil || obs.Owned || obs.Stale || obs.Epoch != 1 {
		t.Fatalf("meta bump obs = %+v err = %v, want fresh foreign epoch 1", obs, err)
	}
	// Spec change at the same epoch: refused, last state held.
	bump(func(live *v1alpha1.ShardPlan) {
		live.Spec.Canary.Mode = v1alpha1.ModeActive
		live.Spec.Canary.WeightPerMille = 1000
	})
	if obs, err := g.Owned(ctx, "demo-87"); err == nil {
		t.Fatalf("tampered obs = %+v, want V9 refusal", obs)
	} else if closed, ok := AsClosed(err); !ok || closed.Reason != ReasonPlanMalformed {
		t.Fatalf("tampered err = %v, want PlanMalformed closed", err)
	}
	// The contracted write (epoch bumped) adopts normally.
	bump(func(live *v1alpha1.ShardPlan) {
		live.Spec.Epoch = 2
	})
	if obs, err := g.Owned(ctx, "demo-87"); err != nil || !obs.Owned || obs.Stale || obs.Epoch != 2 {
		t.Fatalf("repaired obs = %+v err = %v, want fresh owned epoch 2", obs, err)
	}
}

// TestGate_V9FrozenTamper pins read-time V9 for the frozen tuple: a
// seed change without a fresh rollout ID is refused even with an
// epoch bump, and adopted once the rollout rotates.
func TestGate_V9FrozenTamper(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	if obs, _ := g.Owned(ctx, "demo-87"); !obs.Owned {
		t.Fatalf("Off obs = %+v, want owned", obs)
	}
	bump := func(mut func(*v1alpha1.ShardPlan)) {
		t.Helper()
		var live v1alpha1.ShardPlan
		if err := c.Get(ctx, planKey, &live); err != nil {
			t.Fatal(err)
		}
		mut(&live)
		live.Generation++
		if err := c.Update(ctx, &live); err != nil {
			t.Fatal(err)
		}
	}
	bump(func(live *v1alpha1.ShardPlan) {
		live.Spec.Seed = "tampered"
		live.Spec.Epoch = 2
	})
	if _, err := g.Owned(ctx, "demo-87"); err == nil {
		t.Fatal("seed tamper with stale rollout: nil error, want V9 refusal")
	} else if closed, ok := AsClosed(err); !ok || closed.Reason != ReasonPlanMalformed {
		t.Fatalf("seed tamper err = %v, want PlanMalformed closed", err)
	}
	bump(func(live *v1alpha1.ShardPlan) {
		live.Spec.Rollout = "r-2"
		live.Spec.Epoch = 3
	})
	if obs, err := g.Owned(ctx, "demo-87"); err != nil || !obs.Owned || obs.Epoch != 3 {
		t.Fatalf("rotated obs = %+v err = %v, want fresh owned epoch 3", obs, err)
	}
}

// TestGate_SingletonV9MatchesOwned pins adoption symmetry: the
// singleton path refuses the same contract-bypassing versions Owned
// refuses, and adopts contracted versions into the shared baselines,
// so the two paths never disagree about what is stale.
func TestGate_SingletonV9MatchesOwned(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	bump := func(mut func(*v1alpha1.ShardPlan)) {
		t.Helper()
		var live v1alpha1.ShardPlan
		if err := c.Get(ctx, planKey, &live); err != nil {
			t.Fatal(err)
		}
		mut(&live)
		live.Generation++
		if err := c.Update(ctx, &live); err != nil {
			t.Fatal(err)
		}
	}
	bump(func(live *v1alpha1.ShardPlan) {
		live.Spec.Seed = "tampered"
	})
	if _, err := g.SingletonOwned(ctx); err == nil {
		t.Fatal("singleton same-epoch tamper: nil error, want V9 refusal")
	} else if closed, ok := AsClosed(err); !ok || closed.Reason != ReasonPlanMalformed {
		t.Fatalf("singleton tamper err = %v, want PlanMalformed closed", err)
	}
	if _, err := g.Owned(ctx, "demo-87"); err == nil {
		t.Fatal("namespace same-epoch tamper: nil error, want V9 refusal")
	} else if closed, ok := AsClosed(err); !ok || closed.Reason != ReasonPlanMalformed {
		t.Fatalf("namespace tamper err = %v, want PlanMalformed closed", err)
	}
	bump(func(live *v1alpha1.ShardPlan) {
		live.Spec.Rollout = "r-2"
		live.Spec.Epoch = 2
	})
	if sing, err := g.SingletonOwned(ctx); err != nil || !sing {
		t.Fatalf("singleton contracted = %v err = %v, want duty", sing, err)
	}
	// Singleton-first adoption marks the namespace read stale, but
	// against the current spec: still owned, at the fresh epoch.
	obs, err := g.Owned(ctx, "demo-87")
	if err != nil || !obs.Owned || !obs.Stale || obs.Epoch != 2 {
		t.Fatalf("post-singleton obs = %+v err = %v, want stale owned epoch 2", obs, err)
	}
}

// TestOwned_MalformedCloses pins B4: malformed versions close the
// gate with a reason, and a later valid version reopens it.
func TestOwned_MalformedCloses(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	live.Spec.Canary.WeightPerMille = 1001
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	_, err = g.Owned(ctx, "default")
	closed, ok := AsClosed(err)
	if !ok || closed.Reason != ReasonPlanMalformed {
		t.Fatalf("err = %v, want closed PlanMalformed", err)
	}
	if g.Degraded() != ReasonPlanMalformed {
		t.Fatalf("degraded = %q", g.Degraded())
	}
	live.Spec.Canary.WeightPerMille = 0
	live.Spec.Epoch = 3
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if obs, err := g.Owned(ctx, "default"); err != nil || !obs.Owned {
		t.Fatalf("recovered obs = %+v err = %v", obs, err)
	}
	if g.Degraded() != "" {
		t.Fatalf("degraded = %q, want healthy", g.Degraded())
	}
}

// TestOwned_DeletedCloses pins B2.
func TestOwned_DeletedCloses(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, &live); err != nil {
		t.Fatal(err)
	}
	_, err = g.Owned(ctx, "default")
	closed, ok := AsClosed(err)
	if !ok || closed.Reason != ReasonPlanDeleted {
		t.Fatalf("err = %v, want closed PlanDeleted", err)
	}
}

// TestOwned_RecreatedPlan pins B3: a new UID adopts only through Off,
// and adoption resets baselines.
func TestOwned_RecreatedPlan(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	flipActive(t, c)
	if obs, _ := g.Owned(ctx, "demo-87"); !obs.Owned {
		t.Fatal("canary should own demo-87 before recreation")
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, &live); err != nil {
		t.Fatal(err)
	}
	recreated := baseOffPlan()
	recreated.UID = "uid-2"
	recreated.Spec.Canary.Mode = v1alpha1.ModeActive
	recreated.Spec.Canary.WeightPerMille = 1000
	recreated.Spec.Epoch = 1
	if err := c.Create(ctx, recreated); err != nil {
		t.Fatal(err)
	}
	_, err = g.Owned(ctx, "demo-87")
	closed, ok := AsClosed(err)
	if !ok || closed.Reason != ReasonPlanRecreated {
		t.Fatalf("err = %v, want closed PlanRecreated", err)
	}
	var live2 v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live2); err != nil {
		t.Fatal(err)
	}
	live2.Spec.Canary.Mode = v1alpha1.ModeOff
	live2.Spec.Canary.WeightPerMille = 0
	live2.Spec.Epoch = 2
	if err := c.Update(ctx, &live2); err != nil {
		t.Fatal(err)
	}
	obs, err := g.Owned(ctx, "demo-87")
	if err != nil {
		t.Fatal(err)
	}
	if obs.Owned || obs.Epoch != 2 {
		t.Fatalf("adopted obs = %+v, want stable-owned Off epoch 2", obs)
	}
}

// TestOwned_LabelsUnreadableRetains pins the partition-spec rule:
// missing labels retain the last owner (marked stale/degraded), and
// namespaces never seen before fail closed.
func TestOwned_LabelsUnreadableRetains(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	flipActive(t, c)
	if obs, _ := g.Owned(ctx, "demo-87"); !obs.Owned {
		t.Fatal("setup: canary should own demo-87")
	}
	var ns corev1.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: "demo-87"}, &ns); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	obs, err := g.Owned(ctx, "demo-87")
	if err != nil {
		t.Fatal(err)
	}
	if !obs.Owned || !obs.Stale || obs.Degraded != ReasonLabelsUnreadable {
		t.Fatalf("retained obs = %+v, want owned stale degraded", obs)
	}
	if _, err := g.Owned(ctx, "ghost-never-seen"); err == nil {
		t.Fatal("unknown namespace with no labels: nil error, want fail-closed")
	} else if _, ok := AsClosed(err); !ok {
		t.Fatalf("err = %v, want ClosedError", err)
	}
}

// TestSingletonOwned pins cluster-scope routing: only the
// singleton-owner track passes, and bad plans close.
func TestSingletonOwned(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	stable, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	canary, err := Attach(ctx, c, planKey, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := stable.SingletonOwned(ctx); !ok {
		t.Error("stable should own singleton duty")
	}
	if ok, _ := canary.SingletonOwned(ctx); ok {
		t.Error("canary should not own singleton duty")
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	live.Spec.SingletonOwner = "canary"
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if ok, _ := stable.SingletonOwned(ctx); ok {
		t.Error("stable should lose singleton duty")
	}
	if ok, _ := canary.SingletonOwned(ctx); !ok {
		t.Error("canary should gain singleton duty")
	}
}

// TestExternalHold pins the migration seam: while a hold is set and
// unsatisfied, every observation fails closed with ReasonExternalHold.
func TestExternalHold(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	hold := false
	g.SetExternalHold(func() bool { return hold })
	if _, err := g.Owned(ctx, "default"); err == nil {
		t.Fatal("owned with failing hold: nil error, want closed")
	} else if closed, ok := AsClosed(err); !ok || closed.Reason != ReasonExternalHold {
		t.Fatalf("err = %v, want closed ExternalHold", err)
	}
	if _, err := g.SingletonOwned(ctx); err == nil {
		t.Fatal("singleton with failing hold: nil error, want closed")
	}
	if g.Degraded() != ReasonExternalHold {
		t.Fatalf("degraded = %q", g.Degraded())
	}
	hold = true
	if obs, err := g.Owned(ctx, "default"); err != nil || !obs.Owned {
		t.Fatalf("obs = %+v err = %v, want owned", obs, err)
	}
	g.SetExternalHold(nil)
	if obs, err := g.Owned(ctx, "default"); err != nil || !obs.Owned {
		t.Fatalf("obs = %+v err = %v, want owned after clear", obs, err)
	}
}

// stormReader counts Gets by kind and blocks plan reads until
// released, so a storm of concurrent Owned calls piles onto one
// shared flight. Arming starts after attach (attach itself reads
// through).
type stormReader struct {
	client.Reader
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	plans   int
}

func (r *stormReader) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1alpha1.ShardPlan); ok {
		r.mu.Lock()
		r.plans++
		r.mu.Unlock()
		if r.armed.Load() {
			r.once.Do(func() { close(r.entered) })
			select {
			case <-r.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *stormReader) planReads() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.plans
}

// TestGate_StormSharesReads pins the singleflight contract: 100
// concurrent Owned calls share a handful of plan reads (one per
// pile-up, not one per call), every caller still observes the same
// live version, and sequential calls always re-read (sharing is
// in-flight only — never a cache).
func TestGate_StormSharesReads(t *testing.T) {
	ctx := context.Background()
	sr := &stormReader{Reader: fixture(t, nil),
		entered: make(chan struct{}), release: make(chan struct{})}
	g, err := Attach(ctx, sr, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	sr.armed.Store(true)
	const n = 100
	ready := make(chan struct{}, n)
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			ready <- struct{}{}
			_, err := g.Owned(ctx, "demo-87")
			done <- err
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case <-ready:
		case <-time.After(10 * time.Second):
			t.Fatal("storm goroutines never started")
		}
	}
	select {
	case <-sr.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no plan read entered the delegate")
	}
	// Every goroutine is scheduled and running; reaching the flight
	// takes microseconds, so this window piles the whole storm onto
	// the blocked leader with overwhelming margin.
	time.Sleep(time.Second)
	close(sr.release)
	for i := 0; i < n; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("storm call: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("storm call never returned")
		}
	}
	if plans := sr.planReads(); plans > 5 {
		t.Fatalf("storm plan reads = %d, want a shared handful (<=5 for 100 calls)", plans)
	} else {
		t.Logf("storm plan reads = %d for 100 calls", plans)
	}
	// Sequential calls never share: each observes live, so a flip
	// between two calls is visible to the second.
	sr.armed.Store(false)
	before := sr.planReads()
	for i := 0; i < 5; i++ {
		if _, err := g.Owned(ctx, "demo-87"); err != nil {
			t.Fatal(err)
		}
	}
	if got := sr.planReads() - before; got != 5 {
		t.Fatalf("sequential plan reads = %d, want exactly 5 (no caching)", got)
	}
}

// TestGate_SharedReadLeaderCancel pins detached sharing: the leader
// of a shared read cancels mid-flight, its own call fails, and the
// follower still succeeds on the shared bytes with no second read.
func TestGate_SharedReadLeaderCancel(t *testing.T) {
	ctx := context.Background()
	sr := &stormReader{Reader: fixture(t, nil),
		entered: make(chan struct{}), release: make(chan struct{})}
	g, err := Attach(ctx, sr, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	sr.armed.Store(true)
	leaderCtx, cancel := context.WithCancel(ctx)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := g.Owned(leaderCtx, "demo-87")
		leaderDone <- err
	}()
	select {
	case <-sr.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("leader read never entered the delegate")
	}
	followerReady := make(chan struct{})
	followerDone := make(chan error, 1)
	go func() {
		close(followerReady)
		_, err := g.Owned(ctx, "demo-87")
		followerDone <- err
	}()
	select {
	case <-followerReady:
	case <-time.After(10 * time.Second):
		t.Fatal("follower never started")
	}
	time.Sleep(200 * time.Millisecond) // pile the follower onto the flight
	cancel()
	select {
	case err := <-leaderDone:
		if err == nil {
			t.Fatal("canceled leader: nil error, want fail-closed")
		} else if _, ok := AsClosed(err); !ok {
			t.Fatalf("canceled leader err = %v, want ClosedError", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled leader never returned")
	}
	close(sr.release)
	select {
	case err := <-followerDone:
		if err != nil {
			t.Fatalf("follower after leader cancel: %v, want success on shared bytes", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("follower never returned")
	}
	if got := sr.planReads(); got != 2 {
		t.Fatalf("plan reads = %d, want 2 (attach + one shared storm read)", got)
	}
}

// TestGate_SharedReadFollowerCancel pins prompt per-waiter
// cancellation: a follower canceled mid-flight returns before the
// shared read completes, and the leader still succeeds on it.
func TestGate_SharedReadFollowerCancel(t *testing.T) {
	ctx := context.Background()
	sr := &stormReader{Reader: fixture(t, nil),
		entered: make(chan struct{}), release: make(chan struct{})}
	g, err := Attach(ctx, sr, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	sr.armed.Store(true)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := g.Owned(ctx, "demo-87")
		leaderDone <- err
	}()
	select {
	case <-sr.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("leader read never entered the delegate")
	}
	followerCtx, cancel := context.WithCancel(ctx)
	followerReady := make(chan struct{})
	followerDone := make(chan error, 1)
	go func() {
		close(followerReady)
		_, err := g.Owned(followerCtx, "demo-87")
		followerDone <- err
	}()
	select {
	case <-followerReady:
	case <-time.After(10 * time.Second):
		t.Fatal("follower never started")
	}
	time.Sleep(200 * time.Millisecond) // pile the follower onto the flight
	cancel()
	select {
	case err := <-followerDone:
		if err == nil {
			t.Fatal("canceled follower: nil error, want fail-closed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled follower hung on the blocked read (still coupled?)")
	}
	close(sr.release)
	select {
	case err := <-leaderDone:
		if err != nil {
			t.Fatalf("leader after follower cancel: %v, want success", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("leader never returned")
	}
	if got := sr.planReads(); got != 2 {
		t.Fatalf("plan reads = %d, want 2 (attach + one shared storm read)", got)
	}
}

// TestGate_CanceledContext pins fail-closed cancellation: an Owned
// call whose context already expired fails instead of evaluating.
func TestGate_CanceledContext(t *testing.T) {
	g, err := Attach(context.Background(), fixture(t, nil), planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Owned(ctx, "demo-87"); err == nil {
		t.Fatal("canceled Owned: nil error, want fail-closed")
	} else if _, ok := AsClosed(err); !ok {
		t.Fatalf("canceled Owned err = %v, want ClosedError", err)
	}
}

// TestOwned_RelabelDriftDenies pins the relabel pin: a namespace
// relabeled across the exclude boundary mid-version denies on both
// tracks (no overlap, no unfenced flip) until a new version carries
// the labels through the handshake. Label churn that does not cross
// the boundary is unaffected.
func TestOwned_RelabelDriftDenies(t *testing.T) {
	ctx := context.Background()
	excludes := func(p *v1alpha1.ShardPlan) {
		p.Spec.Canary.Exclude.Selector = &metav1.LabelSelector{
			MatchLabels: map[string]string{"tier": "critical"},
		}
	}
	c := fixture(t, excludes)
	canary, err := Attach(ctx, c, planKey, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	stable, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	flipActive(t, c)
	if obs, _ := canary.Owned(ctx, "demo-87"); !obs.Owned {
		t.Fatal("setup: canary should own unlabeled demo-87")
	}
	if obs, _ := stable.Owned(ctx, "demo-87"); obs.Owned {
		t.Fatal("setup: stable should not own demo-87")
	}
	// Unrelated label churn does not cross the boundary: no denial.
	var ns corev1.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: "demo-87"}, &ns); err != nil {
		t.Fatal(err)
	}
	ns.Labels = map[string]string{"team": "a"}
	if err := c.Update(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	if obs, err := canary.Owned(ctx, "demo-87"); err != nil || !obs.Owned {
		t.Fatalf("churned obs = %+v err = %v, want owned", obs, err)
	}
	// Across the boundary: both tracks deny (fail closed, no flip).
	ns.Labels["tier"] = "critical"
	if err := c.Update(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	for name, g := range map[string]*Gate{"canary": canary, "stable": stable} {
		if _, err := g.Owned(ctx, "demo-87"); err == nil {
			t.Fatalf("%s: relabeled Owned: nil error, want LabelsDrifted", name)
		} else if closed, ok := AsClosed(err); !ok || closed.Reason != ReasonLabelsDrifted {
			t.Fatalf("%s: relabeled err = %v, want closed LabelsDrifted", name, err)
		}
	}
	if canary.Degraded() != ReasonLabelsDrifted {
		t.Fatalf("degraded = %q, want LabelsDrifted", canary.Degraded())
	}
	// The next version re-pins and reopens through the handshake:
	// excluded demo-87 belongs to stable.
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 3
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if obs, err := canary.Owned(ctx, "demo-87"); err != nil || obs.Owned || obs.Epoch != 3 {
		t.Fatalf("adopted canary obs = %+v err = %v, want foreign epoch 3", obs, err)
	}
	if obs, err := stable.Owned(ctx, "demo-87"); err != nil || !obs.Owned {
		t.Fatalf("adopted stable obs = %+v err = %v, want owned", obs, err)
	}
	if canary.Degraded() != "" {
		t.Fatalf("degraded = %q, want healthy", canary.Degraded())
	}
}

// TestOwned_AdoptionClearsMemory pins version-scoped memory: adopting
// a new version drops pins and retained owners, so post-flip read
// failures close instead of serving the pre-flip owner, and deleted
// namespaces stop accumulating records.
func TestOwned_AdoptionClearsMemory(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "canary", "rev-b")
	if err != nil {
		t.Fatal(err)
	}
	flipActive(t, c)
	if obs, _ := g.Owned(ctx, "demo-87"); !obs.Owned {
		t.Fatal("setup: canary should own demo-87")
	}
	if obs, _ := g.Owned(ctx, "default"); obs.Owned {
		t.Fatal("setup: canary should not own default")
	}
	if n := len(g.lastPin); n != 2 {
		t.Fatalf("pins = %d, want 2 before adoption", n)
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 3
	if err := c.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	var ns corev1.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: "demo-87"}, &ns); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	// Adoption runs before the namespace read: the pre-flip owner
	// is already forgotten, so the missing namespace closes.
	if _, err := g.Owned(ctx, "demo-87"); err == nil {
		t.Fatal("deleted namespace after adoption: nil error, want fail-closed")
	} else if closed, ok := AsClosed(err); !ok || closed.Reason != ReasonPlanUnreadable {
		t.Fatalf("err = %v, want closed PlanUnreadable", err)
	}
	if obs, err := g.Owned(ctx, "default"); err != nil || obs.Owned || obs.Epoch != 3 {
		t.Fatalf("adopted obs = %+v err = %v, want foreign epoch 3", obs, err)
	}
	if n := len(g.lastPin); n != 1 {
		t.Fatalf("pins = %d, want 1 (default re-pinned, demo-87 pruned)", n)
	}
	if n := len(g.lastOwner); n != 1 {
		t.Fatalf("owners = %d, want 1 (demo-87 pruned)", n)
	}
}

// TestOwned_FirstSightingNotAcquired pins the first-sighting rule:
// a namespace this track never evaluated, relabeled into its grant
// after the observer computed the version's grant, denies instead of
// taking over without the freshness barrier. Namespaces created
// after the computation are genuinely new and serve, and the next
// version reopens the relabeled namespace through the handshake.
func TestOwned_FirstSightingNotAcquired(t *testing.T) {
	ctx := context.Background()
	obs, g, fc := observerFixture(t, "stable")
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 2
	live.Spec.Rollout = "r-2"
	live.Spec.Canary.Mode = v1alpha1.ModeActive
	live.Spec.Canary.WeightPerMille = 1000
	live.Spec.Canary.Exclude.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{"tier": "critical"},
	}
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil { // resume at V2; stable holds nothing
		t.Fatal(err)
	}
	// Membership-steady V3 completes a transition (clearing the
	// resume drain), so the relabel below reaches the pin logic.
	// Re-read first: the resume's status ack moved the object.
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 3
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := obs.Step(ctx); err != nil {
		t.Fatal(err)
	}
	// demo-87 was never evaluated by this gate; relabeling it into
	// the exclude set must deny, not authorize a fenceless takeover.
	var ns corev1.Namespace
	if err := fc.Get(ctx, types.NamespacedName{Name: "demo-87"}, &ns); err != nil {
		t.Fatal(err)
	}
	ns.Labels = map[string]string{"tier": "critical"}
	if err := fc.Update(ctx, &ns); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Owned(ctx, "demo-87"); err == nil {
		t.Fatal("relabeled first sighting: nil error, want NotAcquired")
	} else if closed, ok := AsClosed(err); !ok || closed.Reason != ReasonNotAcquired {
		t.Fatalf("relabeled first sighting err = %v, want closed NotAcquired", err)
	}
	if g.Degraded() != ReasonNotAcquired {
		t.Fatalf("degraded = %q, want NotAcquired", g.Degraded())
	}
	// Created after the observer's listing: genuinely new, serves.
	late := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "late-ns", Labels: map[string]string{"tier": "critical"}}}
	if err := fc.Create(ctx, late); err != nil {
		t.Fatal(err)
	}
	if obs, err := g.Owned(ctx, "late-ns"); err != nil || !obs.Owned {
		t.Fatalf("late obs = %+v err = %v, want owned", obs, err)
	}
	// The next version carries the relabel through the handshake.
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Epoch = 4
	if err := fc.Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if obs, err := g.Owned(ctx, "demo-87"); err != nil || !obs.Owned || obs.Epoch != 4 {
		t.Fatalf("adopted obs = %+v err = %v, want owned epoch 4", obs, err)
	}
	if g.Degraded() != "" {
		t.Fatalf("degraded = %q, want healthy", g.Degraded())
	}
}

// TestDraining pins the drain seam: draining namespaces and singleton
// duty read as foreign without error, while the rest is unaffected.
func TestDraining(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	g.SetDraining(map[string]bool{"demo-87": true})
	if obs, err := g.Owned(ctx, "demo-87"); err != nil || obs.Owned {
		t.Fatalf("draining obs = %+v err = %v, want foreign without error", obs, err)
	} else if obs.Epoch != 1 || obs.Mode != "Off" {
		t.Fatalf("draining obs = %+v, want live version attached", obs)
	}
	if obs, err := g.Owned(ctx, "default"); err != nil || !obs.Owned {
		t.Fatalf("steady obs = %+v err = %v, want owned", obs, err)
	}
	g.SetDraining(nil)
	if obs, err := g.Owned(ctx, "demo-87"); err != nil || !obs.Owned {
		t.Fatalf("cleared obs = %+v err = %v, want owned", obs, err)
	}
	g.SetDrainingSingleton(true)
	if ok, err := g.SingletonOwned(ctx); err != nil || ok {
		t.Fatalf("draining singleton = %v err = %v, want foreign", ok, err)
	}
	g.SetDrainingSingleton(false)
	if ok, err := g.SingletonOwned(ctx); err != nil || !ok {
		t.Fatalf("cleared singleton = %v err = %v, want owned", ok, err)
	}
}

// countReconciler runs until released, reporting entry.
type countReconciler struct {
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (r *countReconciler) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	r.calls++
	close(r.entered)
	select {
	case <-ctx.Done():
		return reconcile.Result{}, ctx.Err()
	case <-r.release:
		return reconcile.Result{}, nil
	}
}

// TestWrap_EntryCheck pins the dequeue gate: foreign and closed-gate
// work skips without calling inner; owned and singleton work runs.
func TestWrap_EntryCheck(t *testing.T) {
	ctx := context.Background()
	mkInner := func() *countReconciler {
		return &countReconciler{entered: make(chan struct{}), release: make(chan struct{})}
	}
	t.Run("foreign namespace skipped", func(t *testing.T) {
		c := fixture(t, nil)
		g, err := Attach(ctx, c, planKey, "canary", "rev-b")
		if err != nil {
			t.Fatal(err)
		}
		inner := mkInner()
		close(inner.release)
		res, err := g.Wrap(inner).Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "w0"}})
		if err != nil || res != (reconcile.Result{}) {
			t.Fatalf("res = %+v err = %v, want empty success", res, err)
		}
		if inner.calls != 0 {
			t.Fatal("inner ran for foreign namespace")
		}
	})
	t.Run("owned namespace runs", func(t *testing.T) {
		c := fixture(t, nil)
		g, err := Attach(ctx, c, planKey, "stable", "rev-a")
		if err != nil {
			t.Fatal(err)
		}
		inner := mkInner()
		close(inner.release)
		if _, err := g.Wrap(inner).Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "w0"}}); err != nil {
			t.Fatal(err)
		}
		if inner.calls != 1 {
			t.Fatalf("calls = %d, want 1", inner.calls)
		}
	})
	t.Run("cluster scope follows singleton", func(t *testing.T) {
		c := fixture(t, nil)
		g, err := Attach(ctx, c, planKey, "canary", "rev-b")
		if err != nil {
			t.Fatal(err)
		}
		inner := mkInner()
		close(inner.release)
		if _, err := g.Wrap(inner).Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "cluster-wide"}}); err != nil {
			t.Fatal(err)
		}
		if inner.calls != 0 {
			t.Fatal("inner ran without singleton duty")
		}
	})
	t.Run("closed gate skips", func(t *testing.T) {
		c := fixture(t, nil)
		g, err := Attach(ctx, c, planKey, "stable", "rev-a")
		if err != nil {
			t.Fatal(err)
		}
		var live v1alpha1.ShardPlan
		if err := c.Get(ctx, planKey, &live); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ctx, &live); err != nil {
			t.Fatal(err)
		}
		inner := mkInner()
		close(inner.release)
		if _, err := g.Wrap(inner).Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "w0"}}); err != nil {
			t.Fatal(err)
		}
		if inner.calls != 0 {
			t.Fatal("inner ran with closed gate")
		}
	})
}

// TestRevoke_CancelsInflight pins the drain seam: Revoke cancels
// scoped contexts, in-flight accounting tracks the wrapper, and the
// idle wait honors its deadline.
func TestRevoke_CancelsInflight(t *testing.T) {
	ctx := context.Background()
	c := fixture(t, nil)
	g, err := Attach(ctx, c, planKey, "stable", "rev-a")
	if err != nil {
		t.Fatal(err)
	}
	inner := &countReconciler{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := g.Wrap(inner).Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "w0"}})
		done <- err
	}()
	select {
	case <-inner.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("inner never entered")
	}
	if g.InflightReconciles() != 1 {
		t.Fatalf("inflight = %d, want 1", g.InflightReconciles())
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := g.WaitReconcilesIdle(short); err == nil {
		t.Fatal("idle wait with blocked reconcile: nil error, want deadline")
	}
	g.Revoke()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("revoked reconcile returned nil, want ctx.Canceled")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("revoked reconcile never returned")
	}
	if err := g.WaitReconcilesIdle(ctx); err != nil {
		t.Fatalf("idle wait after revoke: %v", err)
	}
	// A fresh scoped context survives the old revocation.
	scoped, cancel2 := g.Scoped(ctx)
	defer cancel2()
	select {
	case <-scoped.Done():
		t.Fatal("fresh scoped context already canceled")
	default:
	}
}
