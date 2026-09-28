// Package envtest exercises the ShardPlan API against a real
// API server (envtest 1.36.x): server-side CEL validation and
// concurrent per-track status publishing with conflict retries.
//
// Hermetic unit runs (`make test`) skip this package when
// KUBEBUILDER_ASSETS is unset; `make test-envtest` runs it with
// binaries from `setup-envtest use 1.36.x`.
package envtest_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

var (
	env       *envtest.Environment
	testCfg   *rest.Config
	k8sClient client.Client
	testNS    = "envtest-shardkit"
)

// TestMain boots one shared envtest control plane for the package,
// installing the generated CRDs from config/crd.
func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("SKIP: KUBEBUILDER_ASSETS unset (run `make test-envtest`)")
		os.Exit(0)
	}
	env = &envtest.Environment{
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths: []string{"../../config/crd"},
		},
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Println("envtest start:", err)
		os.Exit(1)
	}
	testCfg = cfg
	if err := v1alpha1.AddToScheme(scheme.Scheme); err != nil {
		fmt.Println("scheme:", err)
		os.Exit(1)
	}
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		fmt.Println("client:", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS}}
	if err := k8sClient.Create(ctx, ns); err != nil {
		fmt.Println("namespace:", err)
		os.Exit(1)
	}
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Println("envtest stop:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

// basePlan returns a valid plan keyed by name.
func basePlan(name string) *v1alpha1.ShardPlan {
	return &v1alpha1.ShardPlan{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
		Spec: v1alpha1.ShardPlanSpec{
			Key: "namespace", Rollout: "r-001", Epoch: 1, Seed: "9a1f2e",
			Tracks: v1alpha1.TrackSet{
				Stable: v1alpha1.TrackRevision{Revision: "rev-a"},
				Canary: &v1alpha1.TrackRevision{Revision: "rev-b"},
			},
			Canary: v1alpha1.CanarySpec{
				Mode: "Active", WeightPerMille: 10,
				Include: v1alpha1.IncludeSpec{Namespaces: []string{"sandbox-a"}},
			},
			SingletonOwner: "stable",
		},
	}
}

// requireCreate creates the plan or fails the test.
func requireCreate(t *testing.T, p *v1alpha1.ShardPlan) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create %s: %v", p.Name, err)
	}
}

// requireGet fetches the live plan or fails the test.
func requireGet(t *testing.T, name string) *v1alpha1.ShardPlan {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &live); err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return &live
}

// TestPlan_ServerValidation pins the generated CRD's server-side
// enforcement: the valid base creates, each invalid shape is rejected
// (structural rules and V-message CEL rules), and epochs never move
// backward while forward updates pass.
func TestPlan_ServerValidation(t *testing.T) {
	valid := basePlan("valid")
	requireCreate(t, valid)
	got := requireGet(t, "valid")
	if got.Spec.Epoch != 1 || got.Spec.Canary.WeightPerMille != 10 {
		t.Fatalf("round trip changed spec: %+v", got.Spec)
	}

	invalid := []struct {
		name   string
		want   string
		mutate func(*v1alpha1.ShardPlan)
	}{
		{"bad-key", "key", func(p *v1alpha1.ShardPlan) { p.Spec.Key = "cluster" }},
		{"empty-seed", "seed", func(p *v1alpha1.ShardPlan) { p.Spec.Seed = "" }},
		{"bad-mode", "mode", func(p *v1alpha1.ShardPlan) { p.Spec.Canary.Mode = "Warp" }},
		{"weight-over", "1000", func(p *v1alpha1.ShardPlan) { p.Spec.Canary.WeightPerMille = 1001 }},
		{"off-with-weight", "V4", func(p *v1alpha1.ShardPlan) {
			p.Spec.Canary.Mode = "Off"
			p.Spec.Canary.WeightPerMille = 10
		}},
		{"bad-stable-rev", "revision", func(p *v1alpha1.ShardPlan) { p.Spec.Tracks.Stable.Revision = "Bad!" }},
		{"active-no-canary", "V6", func(p *v1alpha1.ShardPlan) { p.Spec.Tracks.Canary = nil }},
		{"bad-singleton", "singletonOwner", func(p *v1alpha1.ShardPlan) { p.Spec.SingletonOwner = "both" }},
		{"singleton-canary-no-rev", "V7", func(p *v1alpha1.ShardPlan) {
			p.Spec.Canary.Mode = "Off"
			p.Spec.Canary.WeightPerMille = 0
			p.Spec.Tracks.Canary = nil
			p.Spec.SingletonOwner = "canary"
		}},
		{"match-expressions", "V8", func(p *v1alpha1.ShardPlan) {
			p.Spec.Canary.Exclude.Selector = &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "tier", Operator: "In"}},
			}
		}},
	}
	for _, c := range invalid {
		p := basePlan(c.name)
		c.mutate(p)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := k8sClient.Create(ctx, p)
		cancel()
		if err == nil {
			t.Errorf("%s: create accepted, want rejection (%s)", c.name, c.want)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q lacks %q", c.name, err, c.want)
		}
	}

	// Epoch never decreases, forward updates pass (status untouched).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	live := requireGet(t, "valid")
	live.Spec.Epoch = 0
	if err := k8sClient.Update(ctx, live); err == nil {
		t.Error("backward epoch accepted, want V9 rejection")
	} else if !strings.Contains(err.Error(), "V9") {
		t.Errorf("backward epoch error %q lacks V9", err)
	}
	live = requireGet(t, "valid")
	live.Spec.Epoch = 2
	live.Spec.Canary.WeightPerMille = 500
	if err := k8sClient.Update(ctx, live); err != nil {
		t.Errorf("forward update: %v", err)
	}
	if got := requireGet(t, "valid"); got.Spec.Epoch != 2 || got.Spec.Canary.WeightPerMille != 500 {
		t.Errorf("forward update lost: %+v", got.Spec)
	}
}

// TestStatus_ConcurrentPublish proves two tracks can publish status
// concurrently through the production helper: after 20 rounds each,
// both entries hold their final values (no lost update). The helper
// requires live leases for the S8 session binding, created here.
func TestStatus_ConcurrentPublish(t *testing.T) {
	requireCreate(t, basePlan("concurrent"))
	key := types.NamespacedName{Namespace: testNS, Name: "concurrent"}
	planVer := shardkit.VersionOf(requireGet(t, "concurrent"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	holder, transitions := "envtest-holder", int32(0)
	for _, track := range []string{"stable", "canary"} {
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "concurrent-" + track},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &transitions},
		}
		if err := k8sClient.Create(ctx, lease); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan string, 2)
	for _, track := range []string{"stable", "canary"} {
		go func(track string) {
			rev := "rev-a"
			if track == "canary" {
				rev = "rev-b"
			}
			pctx, pcancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer pcancel()
			for i := 0; i < 20; i++ {
				if err := shardkit.PublishOwnEntry(pctx, shardkit.AckOptions{
					Client: k8sClient, APIReader: k8sClient, PlanKey: key,
					Track: track, Revision: rev,
					Version: planVer,
					Phase:   v1alpha1.PhaseAcquired, Released: true,
					OwnedNamespaces: int32(i),
					LeaseName:       "concurrent-" + track, LeaseNamespace: testNS,
				}); err != nil {
					done <- fmt.Sprintf("%s round %d: %v", track, i, err)
					return
				}
			}
			done <- ""
		}(track)
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != "" {
			t.Fatalf("publisher: %s", err)
		}
	}
	final := requireGet(t, "concurrent")
	seen := map[string]int32{}
	for _, e := range final.Status.Tracks {
		seen[e.Name] = e.OwnedNamespaces
	}
	for _, track := range []string{"stable", "canary"} {
		if seen[track] != 19 {
			t.Errorf("track %s owned=%d, want 19 (lost update)", track, seen[track])
		}
	}
}
