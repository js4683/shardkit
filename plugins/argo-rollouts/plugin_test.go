package main

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rolloutv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	pluginTypes "github.com/argoproj/argo-rollouts/utils/plugin/types"
	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

const (
	pluginNS   = "plugin-test"
	rolloutUID = "ro-1"
	planUID    = "plan-uid-1"
)

// pluginFixture builds a fake cluster with the given objects.
func pluginFixture(t *testing.T, objs ...client.Object) *Plugin {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.Add(v1alpha1.GroupVersion.WithKind("ShardPlan"), meta.RESTScopeNamespace)
	c := fake.NewClientBuilder().WithScheme(s).WithRESTMapper(mapper).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).WithObjects(objs...).Build()
	return &Plugin{Client: c}
}

// mkRollout returns the calling Rollout bound to the fixture plan.
func mkRollout() *rolloutv1alpha1.Rollout {
	return &rolloutv1alpha1.Rollout{
		ObjectMeta: metav1.ObjectMeta{Namespace: pluginNS, Name: "web", UID: rolloutUID},
	}
}

func mkRolloutWithMax(max int32) *rolloutv1alpha1.Rollout {
	r := mkRollout()
	r.Spec.Strategy.Canary = &rolloutv1alpha1.CanaryStrategy{
		TrafficRouting: &rolloutv1alpha1.RolloutTrafficRouting{MaxTrafficWeight: &max},
	}
	return r
}

// mkBoundPlan returns a valid Active plan bound to mkRollout;
// mutate adjusts it before use.
func mkBoundPlan(mutate func(*v1alpha1.ShardPlan)) *v1alpha1.ShardPlan {
	p := &v1alpha1.ShardPlan{
		ObjectMeta: metav1.ObjectMeta{Namespace: pluginNS, Name: "web", UID: planUID, Generation: 7},
		Spec: v1alpha1.ShardPlanSpec{
			Key: "namespace", Rollout: rolloutUID, Epoch: 3, Seed: "9a1f2e",
			Tracks: v1alpha1.TrackSet{
				Stable: v1alpha1.TrackRevision{Revision: "rev-a"},
				Canary: &v1alpha1.TrackRevision{Revision: "rev-b"},
			},
			Canary:         v1alpha1.CanarySpec{Mode: "Active", WeightPerMille: 100},
			SingletonOwner: "stable",
		},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

// setPlanEntries replaces the fixture plan's status entries.
func setPlanEntries(t *testing.T, p *Plugin, entries ...v1alpha1.TrackStatus) {
	t.Helper()
	ctx := context.Background()
	var live v1alpha1.ShardPlan
	if err := p.Client.Get(ctx, types.NamespacedName{Namespace: pluginNS, Name: "web"}, &live); err != nil {
		t.Fatal(err)
	}
	live.Status.Tracks = entries
	if err := p.Client.Status().Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
}

// ackEntries is a converged status for epoch 3 / generation 7.
func ackEntries() []v1alpha1.TrackStatus {
	sess := &v1alpha1.LeaderSession{Holder: "pod-0", LeaseTransitions: 0}
	return []v1alpha1.TrackStatus{
		{Name: "stable", Revision: "rev-a", PlanUID: planUID,
			ObservedGeneration: 7, ObservedEpoch: 3, Rollout: rolloutUID,
			Phase: "Released", Released: true, OwnedNamespaces: 1, Session: sess},
		{Name: "canary", Revision: "rev-b", PlanUID: planUID,
			ObservedGeneration: 7, ObservedEpoch: 3, Rollout: rolloutUID,
			Phase: "Acquired", Released: true, OwnedNamespaces: 1, Session: sess},
	}
}

func getPlan(t *testing.T, p *Plugin) *v1alpha1.ShardPlan {
	t.Helper()
	var live v1alpha1.ShardPlan
	if err := p.Client.Get(context.Background(),
		types.NamespacedName{Namespace: pluginNS, Name: "web"}, &live); err != nil {
		t.Fatal(err)
	}
	return &live
}

func TestPlugin_Type(t *testing.T) {
	p := pluginFixture(t)
	if got := p.Type(); got != "shardkit" {
		t.Fatalf("Type() = %q, want shardkit", got)
	}
}

func TestPlugin_InitPlugin(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	if err := p.InitPlugin(); err.HasError() {
		t.Fatalf("InitPlugin: %v", err)
	}
}

func TestPlugin_ResolvePlan(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	got, err := p.resolvePlan(context.Background(), mkRollout())
	if err.HasError() {
		t.Fatalf("resolve: %v", err)
	}
	if got.Name != "web" {
		t.Fatalf("resolved %q, want web", got.Name)
	}
	if _, err := p.resolvePlan(context.Background(), &rolloutv1alpha1.Rollout{
		ObjectMeta: metav1.ObjectMeta{Namespace: pluginNS, Name: "other", UID: "ro-9"},
	}); !err.HasError() || !strings.Contains(err.ErrorString, "ro-9") {
		t.Fatalf("unbound resolve err = %v, want UID in message", err)
	}
	dup := mkBoundPlan(nil)
	dup.Name = "web-dup"
	p2 := pluginFixture(t, mkBoundPlan(nil), dup)
	if _, err := p2.resolvePlan(context.Background(), mkRollout()); !err.HasError() {
		t.Fatal("duplicate binding must fail")
	}
}

func TestPlugin_SetWeight(t *testing.T) {
	t.Run("noop at desired weight", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		if err := p.SetWeight(mkRollout(), 10, nil); err.HasError() {
			t.Fatalf("SetWeight: %v", err)
		}
		if got := getPlan(t, p); got.Spec.Epoch != 3 {
			t.Fatalf("no-op wrote epoch %d", got.Spec.Epoch)
		}
	})
	t.Run("new epoch on change", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		if err := p.SetWeight(mkRollout(), 25, nil); err.HasError() {
			t.Fatalf("SetWeight: %v", err)
		}
		got := getPlan(t, p)
		if got.Spec.Epoch != 4 || got.Spec.Canary.Mode != v1alpha1.ModeActive ||
			got.Spec.Canary.WeightPerMille != 250 {
			t.Fatalf("got epoch=%d mode=%s weight=%d",
				got.Spec.Epoch, got.Spec.Canary.Mode, got.Spec.Canary.WeightPerMille)
		}
	})
	t.Run("zero normalizes to off", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		if err := p.SetWeight(mkRollout(), 0, nil); err.HasError() {
			t.Fatalf("SetWeight: %v", err)
		}
		got := getPlan(t, p)
		if got.Spec.Epoch != 4 || got.Spec.Canary.Mode != v1alpha1.ModeOff ||
			got.Spec.Canary.WeightPerMille != 0 {
			t.Fatalf("got epoch=%d mode=%s weight=%d",
				got.Spec.Epoch, got.Spec.Canary.Mode, got.Spec.Canary.WeightPerMille)
		}
		if err := p.SetWeight(mkRollout(), 0, nil); err.HasError() {
			t.Fatalf("SetWeight: %v", err)
		}
		if got := getPlan(t, p); got.Spec.Epoch != 4 {
			t.Fatalf("second zero wrote epoch %d", got.Spec.Epoch)
		}
	})
	t.Run("rejects destinations and range", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		addl := []rolloutv1alpha1.WeightDestination{{Weight: 1}}
		if err := p.SetWeight(mkRollout(), 10, addl); !err.HasError() {
			t.Fatal("additional destinations must fail closed")
		}
		for _, w := range []int32{-1, 101} {
			if err := p.SetWeight(mkRollout(), w, nil); !err.HasError() {
				t.Fatalf("weight %d must fail", w)
			}
		}
	})
	t.Run("scales permille by maxTrafficWeight", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		ro := mkRolloutWithMax(1000)
		if err := p.SetWeight(ro, 1, nil); err.HasError() {
			t.Fatalf("SetWeight: %v", err)
		}
		if got := getPlan(t, p); got.Spec.Canary.WeightPerMille != 1 {
			t.Fatalf("weight = %d, want 1 (0.1%% step)", got.Spec.Canary.WeightPerMille)
		}
		if err := p.SetWeight(ro, 1000, nil); err.HasError() {
			t.Fatalf("SetWeight: %v", err)
		}
		if got := getPlan(t, p); got.Spec.Canary.WeightPerMille != 1000 {
			t.Fatalf("weight = %d, want 1000", got.Spec.Canary.WeightPerMille)
		}
		for _, w := range []int32{-1, 1001} {
			if err := p.SetWeight(ro, w, nil); !err.HasError() {
				t.Fatalf("weight %d must fail against max 1000", w)
			}
		}
	})
	t.Run("rejects inexact scale", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		for _, max := range []int32{0, 3, -100} {
			if err := p.SetWeight(mkRolloutWithMax(max), 1, nil); !err.HasError() {
				t.Fatalf("maxTrafficWeight %d must fail closed", max)
			}
		}
		if got := getPlan(t, p); got.Spec.Epoch != 3 {
			t.Fatalf("refused scale wrote epoch %d", got.Spec.Epoch)
		}
	})
}

func TestPlugin_VerifyWeight(t *testing.T) {
	t.Run("verified post-ack", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		setPlanEntries(t, p, ackEntries()...)
		v, err := p.VerifyWeight(mkRollout(), 10, nil)
		if err.HasError() {
			t.Fatalf("VerifyWeight: %v", err)
		}
		if v != pluginTypes.Verified {
			t.Fatalf("got %v, want Verified", v)
		}
	})
	t.Run("not verified pre-ack", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		if err := p.SetWeight(mkRollout(), 25, nil); err.HasError() {
			t.Fatal(err)
		}
		v, err := p.VerifyWeight(mkRollout(), 25, nil)
		if err.HasError() {
			t.Fatalf("VerifyWeight: %v", err)
		}
		if v != pluginTypes.NotVerified {
			t.Fatalf("stale ack reported %v, want NotVerified", v)
		}
	})
	t.Run("not verified on weight and revision drift", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		setPlanEntries(t, p, ackEntries()...)
		if v, _ := p.VerifyWeight(mkRollout(), 50, nil); v != pluginTypes.NotVerified {
			t.Fatalf("wrong weight reported %v", v)
		}
		stale := ackEntries()
		stale[1].Revision = "rev-zzz"
		setPlanEntries(t, p, stale...)
		if v, _ := p.VerifyWeight(mkRollout(), 10, nil); v != pluginTypes.NotVerified {
			t.Fatalf("revision drift reported %v", v)
		}
	})
	t.Run("destinations fail closed", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		addl := []rolloutv1alpha1.WeightDestination{{Weight: 1}}
		if _, err := p.VerifyWeight(mkRollout(), 10, addl); !err.HasError() {
			t.Fatal("additional destinations must fail closed")
		}
	})
	t.Run("verifies against scaled weight", func(t *testing.T) {
		one := mkBoundPlan(func(p *v1alpha1.ShardPlan) { p.Spec.Canary.WeightPerMille = 1 })
		p := pluginFixture(t, one)
		setPlanEntries(t, p, ackEntries()...)
		ro := mkRolloutWithMax(1000)
		if v, err := p.VerifyWeight(ro, 1, nil); err.HasError() || v != pluginTypes.Verified {
			t.Fatalf("got %v err %v, want Verified", v, err)
		}
		if v, _ := p.VerifyWeight(ro, 2, nil); v != pluginTypes.NotVerified {
			t.Fatalf("wrong scaled weight reported %v", v)
		}
		if _, err := p.VerifyWeight(mkRolloutWithMax(3), 1, nil); !err.HasError() {
			t.Fatal("inexact scale must fail closed")
		}
	})
}

func TestPlugin_UpdateHash(t *testing.T) {
	t.Run("noop on equal canary hash", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		// A foreign stable hash changes nothing: the stable track
		// is externally managed.
		if err := p.UpdateHash(mkRollout(), "rev-b", "rs-other", nil); err.HasError() {
			t.Fatalf("UpdateHash: %v", err)
		}
		if got := getPlan(t, p); got.Spec.Epoch != 3 {
			t.Fatalf("no-op wrote epoch %d", got.Spec.Epoch)
		}
	})
	t.Run("new epoch rebinds canary only", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		if err := p.UpdateHash(mkRollout(), "rev-c", "rs-other", nil); err.HasError() {
			t.Fatalf("UpdateHash: %v", err)
		}
		got := getPlan(t, p)
		if got.Spec.Epoch != 4 || got.Spec.Tracks.Canary.Revision != "rev-c" ||
			got.Spec.Tracks.Stable.Revision != "rev-a" ||
			got.Spec.Canary.Mode != v1alpha1.ModeActive || got.Spec.Canary.WeightPerMille != 100 {
			t.Fatalf("got epoch=%d stable=%s canary=%s mode=%s weight=%d",
				got.Spec.Epoch, got.Spec.Tracks.Stable.Revision,
				got.Spec.Tracks.Canary.Revision,
				got.Spec.Canary.Mode, got.Spec.Canary.WeightPerMille)
		}
	})
	t.Run("empty hashes fail", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		if err := p.UpdateHash(mkRollout(), "", "rs-x", nil); !err.HasError() {
			t.Fatal("empty canary hash must fail")
		}
		if err := p.UpdateHash(mkRollout(), "rev-b", "", nil); !err.HasError() {
			t.Fatal("empty stable hash must fail")
		}
	})
}

func TestPlugin_RemoveManagedRoutes(t *testing.T) {
	t.Run("aborts active plan", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(nil))
		if err := p.RemoveManagedRoutes(mkRollout()); err.HasError() {
			t.Fatalf("RemoveManagedRoutes: %v", err)
		}
		got := getPlan(t, p)
		if got.Spec.Epoch != 4 || got.Spec.Canary.Mode != v1alpha1.ModeOff ||
			got.Spec.Canary.WeightPerMille != 0 {
			t.Fatalf("got epoch=%d mode=%s weight=%d",
				got.Spec.Epoch, got.Spec.Canary.Mode, got.Spec.Canary.WeightPerMille)
		}
	})
	t.Run("noop when already off", func(t *testing.T) {
		p := pluginFixture(t, mkBoundPlan(func(p *v1alpha1.ShardPlan) {
			p.Spec.Canary.Mode = v1alpha1.ModeOff
			p.Spec.Canary.WeightPerMille = 0
		}))
		if err := p.RemoveManagedRoutes(mkRollout()); err.HasError() {
			t.Fatalf("RemoveManagedRoutes: %v", err)
		}
		if got := getPlan(t, p); got.Spec.Epoch != 3 {
			t.Fatalf("no-op wrote epoch %d", got.Spec.Epoch)
		}
	})
}

// TestPlugin_SteadyStateQuiesces pins the M2 kind finding: on a
// fully-promoted rollout Argo calls RemoveManagedRoutes followed
// by SetWeight(0) on every sync. Both must agree on Off/0, or the
// plan flaps Off<->Active forever (each call defeats the other's
// no-op guard). After the first abort exactly one epoch is
// written; every repeat call is a no-op in either order.
func TestPlugin_SteadyStateQuiesces(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	if err := p.RemoveManagedRoutes(mkRollout()); err.HasError() {
		t.Fatalf("RemoveManagedRoutes: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := p.SetWeight(mkRollout(), 0, nil); err.HasError() {
			t.Fatalf("SetWeight(0): %v", err)
		}
		if err := p.RemoveManagedRoutes(mkRollout()); err.HasError() {
			t.Fatalf("RemoveManagedRoutes: %v", err)
		}
	}
	if got := getPlan(t, p); got.Spec.Epoch != 4 ||
		got.Spec.Canary.Mode != v1alpha1.ModeOff ||
		got.Spec.Canary.WeightPerMille != 0 {
		t.Fatalf("got epoch=%d mode=%s weight=%d, want exactly one abort epoch",
			got.Spec.Epoch, got.Spec.Canary.Mode, got.Spec.Canary.WeightPerMille)
	}
}

func TestPlugin_HeaderMirrorUnsupported(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	if err := p.SetHeaderRoute(mkRollout(), nil); !err.HasError() {
		t.Fatal("SetHeaderRoute must fail closed")
	}
	if err := p.SetMirrorRoute(mkRollout(), nil); !err.HasError() {
		t.Fatal("SetMirrorRoute must fail closed")
	}
}
