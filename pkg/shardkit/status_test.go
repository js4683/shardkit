package shardkit

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// validCheck builds a release check that passes: stable released
// epoch 2 with a matching live lease.
func validCheck() AckCheck {
	holder, transitions := "pod-0", int32(3)
	return AckCheck{
		Plan: &v1alpha1.ShardPlan{
			ObjectMeta: metav1.ObjectMeta{UID: "uid-1", Generation: 4},
			Spec: v1alpha1.ShardPlanSpec{
				Epoch: 2, Rollout: "r-2",
				Tracks: v1alpha1.TrackSet{
					Stable: v1alpha1.TrackRevision{Revision: "rev-a"},
					Canary: &v1alpha1.TrackRevision{Revision: "rev-b"},
				},
			},
		},
		Tracks: []v1alpha1.TrackStatus{{
			Name: "stable", Revision: "rev-a", PlanUID: "uid-1",
			ObservedGeneration: 4, ObservedEpoch: 2, Rollout: "r-2",
			Phase: v1alpha1.PhaseReleased, Released: true,
			Session: &v1alpha1.LeaderSession{Holder: holder, LeaseTransitions: transitions},
		}},
		Loser: "stable",
		LoserLease: &coordinationv1.Lease{
			Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &transitions},
		},
	}
}

// TestValidRelease pins S5-S11: the valid handshake passes, and every
// binding failure voids with its rule ID.
func TestValidRelease(t *testing.T) {
	if err := ValidRelease(validCheck()); err != nil {
		t.Fatalf("valid ack: %v", err)
	}
	cases := []struct {
		name string
		rule string
		mute func(*AckCheck)
	}{
		{"missing entry", "S11", func(a *AckCheck) { a.Tracks = nil }},
		{"wrong entry only", "S11", func(a *AckCheck) { a.Tracks[0].Name = "canary" }},
		{"uid mismatch", "S5", func(a *AckCheck) { a.Tracks[0].PlanUID = "uid-2" }},
		{"generation mismatch", "S6", func(a *AckCheck) { a.Tracks[0].ObservedGeneration = 3 }},
		{"epoch mismatch", "S6", func(a *AckCheck) { a.Tracks[0].ObservedEpoch = 1 }},
		{"rollout mismatch", "S6", func(a *AckCheck) { a.Tracks[0].Rollout = "r-1" }},
		{"revision mismatch", "S7", func(a *AckCheck) { a.Tracks[0].Revision = "rev-zzz" }},
		{"canary loser no spec revision", "S7", func(a *AckCheck) {
			a.Loser = "canary"
			a.Tracks[0].Name = "canary"
			a.Tracks[0].Revision = "rev-b"
			a.Plan.Spec.Tracks.Canary = nil
		}},
		{"nil lease", "S8", func(a *AckCheck) { a.LoserLease = nil }},
		{"nil session", "S8", func(a *AckCheck) { a.Tracks[0].Session = nil }},
		{"holder changed", "S8", func(a *AckCheck) { a.Tracks[0].Session.Holder = "pod-9" }},
		{"transitions changed", "S8", func(a *AckCheck) { a.Tracks[0].Session.LeaseTransitions = 4 }},
		{"not released flag", "S9", func(a *AckCheck) { a.Tracks[0].Released = false }},
		{"draining phase", "S9", func(a *AckCheck) { a.Tracks[0].Phase = v1alpha1.PhaseDraining }},
		{"acquired phase", "S9", func(a *AckCheck) { a.Tracks[0].Phase = v1alpha1.PhaseAcquired }},
	}
	for _, c := range cases {
		a := validCheck()
		c.mute(&a)
		err := ValidRelease(a)
		if err == nil {
			t.Errorf("%s: nil error, want %s", c.name, c.rule)
			continue
		}
		if !strings.Contains(err.Error(), c.rule) {
			t.Errorf("%s: error %q lacks %s", c.name, err, c.rule)
		}
	}
	t.Run("canary loser binds canary revision", func(t *testing.T) {
		a := validCheck()
		a.Loser = "canary"
		a.Tracks[0].Name = "canary"
		a.Tracks[0].Revision = "rev-b"
		if err := ValidRelease(a); err != nil {
			t.Fatalf("valid canary ack: %v", err)
		}
		a.Tracks[0].Revision = "rev-a"
		if err := ValidRelease(a); err == nil {
			t.Fatal("canary ack with stable revision: nil error, want S7")
		}
	})
}

// publishFixture builds a fake client with plan uid-1 (epoch 2) and
// both track leases held.
func publishFixture(t *testing.T) client.Client {
	t.Helper()
	plan := baseOffPlan()
	plan.Spec.Epoch = 2
	plan.Spec.Canary.Mode = v1alpha1.ModeActive
	plan.Spec.Canary.WeightPerMille = 1000
	holder, transitions := "pod-0", int32(1)
	mkLease := func(name string) *coordinationv1.Lease {
		return &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: planKey.Namespace, Name: name},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &transitions},
		}
	}
	return fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(plan, mkLease("widget-operator-stable"), mkLease("widget-operator-canary")).
		Build()
}

// TestPublishOwnEntry_Bindings pins S1-S4: one call writes exactly
// the caller's entry with bindings from a single observed version,
// and republishing is idempotent.
func TestPublishOwnEntry_Bindings(t *testing.T) {
	ctx := context.Background()
	fc := publishFixture(t)
	opts := AckOptions{
		Client: fc, APIReader: fc, PlanKey: planKey,
		Track: "stable", Revision: "rev-a",
		Version: PlanVersion{UID: "uid-1", Generation: 0, Epoch: 2, Rollout: "r-1"},
		Phase:   v1alpha1.PhaseReleased, Released: true, OwnedNamespaces: 0,
		ReleasedWrites: map[string]string{"widgets.shardkit.dev": "42"},
		LeaseName:      "widget-operator-stable", LeaseNamespace: planKey.Namespace,
	}
	if err := PublishOwnEntry(ctx, opts); err != nil {
		t.Fatal(err)
	}
	// Idempotent republish.
	if err := PublishOwnEntry(ctx, opts); err != nil {
		t.Fatalf("republish: %v", err)
	}
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	if len(live.Status.Tracks) != 1 {
		t.Fatalf("tracks = %d, want exactly the stable entry", len(live.Status.Tracks))
	}
	e := live.Status.Tracks[0]
	if e.Name != "stable" || e.Revision != "rev-a" || e.PlanUID != "uid-1" ||
		e.ObservedEpoch != 2 || e.Rollout != "r-1" ||
		e.Phase != v1alpha1.PhaseReleased || !e.Released {
		t.Fatalf("entry = %+v, want exact S3 bindings", e)
	}
	if e.ReleasedWrites["widgets.shardkit.dev"] != "42" {
		t.Fatalf("releasedWrites = %v", e.ReleasedWrites)
	}
	if e.Session == nil || e.Session.Holder != "pod-0" || e.Session.LeaseTransitions != 1 {
		t.Fatalf("session = %+v", e.Session)
	}
	// The other track's entry is untouched: publish canary, stable stays.
	opts.Track, opts.Revision = "canary", "rev-b"
	opts.Phase, opts.OwnedNamespaces = v1alpha1.PhaseAcquired, 21
	opts.LeaseName = "widget-operator-canary"
	if err := PublishOwnEntry(ctx, opts); err != nil {
		t.Fatal(err)
	}
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	if len(live.Status.Tracks) != 2 || live.Status.Tracks[0].Name != "stable" {
		t.Fatalf("tracks = %+v, want stable entry preserved first", live.Status.Tracks)
	}
}

// TestPublishOwnEntry_RequiresLease pins the M1 S8 rule: no session
// without a live lease, and nothing is written on failure.
func TestPublishOwnEntry_RequiresLease(t *testing.T) {
	ctx := context.Background()
	fc := publishFixture(t)
	opts := AckOptions{
		Client: fc, APIReader: fc, PlanKey: planKey,
		Track: "stable", Revision: "rev-a",
		Version: PlanVersion{UID: "uid-1", Generation: 0, Epoch: 2, Rollout: "r-1"},
		Phase:   v1alpha1.PhaseReleased, Released: true,
		LeaseName: "widget-operator-missing", LeaseNamespace: planKey.Namespace,
	}
	if err := PublishOwnEntry(ctx, opts); err == nil {
		t.Fatal("nil error, want lease-required failure")
	} else if !strings.Contains(err.Error(), "S8") {
		t.Fatalf("error %q lacks S8", err)
	}
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	if len(live.Status.Tracks) != 0 {
		t.Fatalf("tracks written despite failure: %+v", live.Status.Tracks)
	}
}

// TestPublishOwnEntry_Concurrent pins S2-style conflict handling:
// two tracks publishing at once both land with final values.
func TestPublishOwnEntry_Concurrent(t *testing.T) {
	ctx := context.Background()
	fc := publishFixture(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, track := range []string{"stable", "canary"} {
		wg.Add(1)
		go func(track string) {
			defer wg.Done()
			rev := "rev-a"
			if track == "canary" {
				rev = "rev-b"
			}
			for i := 0; i < 10; i++ {
				if err := PublishOwnEntry(ctx, AckOptions{
					Client: fc, APIReader: fc, PlanKey: planKey,
					Track: track, Revision: rev,
					Version: PlanVersion{UID: "uid-1", Generation: 0, Epoch: 2, Rollout: "r-1"},
					Phase:   v1alpha1.PhaseAcquired, Released: true,
					OwnedNamespaces: int32(i),
					LeaseName:       "widget-operator-" + track, LeaseNamespace: planKey.Namespace,
				}); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}(track)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx2, planKey, &live); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int32{}
	for _, e := range live.Status.Tracks {
		seen[e.Name] = e.OwnedNamespaces
	}
	if seen["stable"] != 9 || seen["canary"] != 9 {
		t.Fatalf("owned = %v, want both 9 (lost update)", seen)
	}
}

// TestClusterQualifiedSession pins M4 multi-cluster keys: a cluster
// name qualifies the published holder, the same qualifier passes
// ValidRelease, and an unqualified or foreign-cluster checker voids
// on S8 instead of trusting a same-named pod elsewhere.
func TestClusterQualifiedSession(t *testing.T) {
	ctx := context.Background()
	fc := publishFixture(t)
	opts := AckOptions{
		Client: fc, APIReader: fc, PlanKey: planKey,
		Track: "stable", Revision: "rev-a",
		Version: PlanVersion{UID: "uid-1", Generation: 0, Epoch: 2, Rollout: "r-1"},
		Phase:   v1alpha1.PhaseReleased, Released: true,
		LeaseName: "widget-operator-stable", LeaseNamespace: planKey.Namespace,
		ClusterName: "prod-east",
	}
	if err := PublishOwnEntry(ctx, opts); err != nil {
		t.Fatal(err)
	}
	var live v1alpha1.ShardPlan
	if err := fc.Get(ctx, planKey, &live); err != nil {
		t.Fatal(err)
	}
	e := live.Status.Tracks[0]
	if e.Session == nil || e.Session.Holder != "prod-east/pod-0" {
		t.Fatalf("session = %+v, want qualified holder", e.Session)
	}
	qualified := validCheck()
	qualified.ClusterName = "prod-east"
	qualified.Tracks[0].Session.Holder = "prod-east/pod-0"
	if err := ValidRelease(qualified); err != nil {
		t.Fatalf("qualified ack: %v", err)
	}
	unqualified := validCheck()
	unqualified.Tracks[0].Session.Holder = "prod-east/pod-0"
	if err := ValidRelease(unqualified); err == nil ||
		!strings.Contains(err.Error(), "S8") {
		t.Fatalf("unqualified check of qualified ack: %v, want S8", err)
	}
	foreign := validCheck()
	foreign.ClusterName = "prod-west"
	foreign.Tracks[0].Session.Holder = "prod-east/pod-0"
	if err := ValidRelease(foreign); err == nil ||
		!strings.Contains(err.Error(), "S8") {
		t.Fatalf("foreign-cluster check: %v, want S8", err)
	}
	if err := ValidateClusterName("east/west"); err == nil {
		t.Fatal("slashed cluster: nil error, want explicit failure")
	}
	if err := ValidateClusterName(""); err != nil {
		t.Fatalf("empty cluster: %v, want unqualified allowed", err)
	}
}
