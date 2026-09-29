// Real-server gate benchmarks (review round 1, item 1): the m4-scale
// "11.5 µs, no caching layer needed" figure came from a fake client.
// These measure Gate.Owned against the envtest API server — one
// serially (the true per-call cost: two live GETs) and one as a
// MaxConcurrentReconciles-style storm (the singleflight sharing).
//
// Run with assets, e.g.:
//
//	eval $(setup-envtest use -p env 1.37.x)
//	go test ./test/envtest/ -run=NONE -bench 'BenchmarkGateOwned' -benchtime=20x
package envtest_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

// countClient counts ShardPlan Gets while delegating everything.
type countClient struct {
	client.Client
	plans atomic.Int64
}

func (c *countClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1alpha1.ShardPlan); ok {
		c.plans.Add(1)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// benchSeq disambiguates fixtures across calibration reruns and
// -count=N repeats sharing one server.
var benchSeq atomic.Int64

// benchGate builds an Off plan plus one namespace and attaches a
// stable gate whose reads go through the counting client.
func benchGate(b *testing.B, name string) (*shardkit.Gate, *countClient, string) {
	b.Helper()
	ctx := context.Background()
	tag := fmt.Sprintf("%s-%d", name, benchSeq.Add(1))
	nsName := "bench-ns-" + tag
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}
	if err := k8sClient.Create(ctx, ns); err != nil {
		b.Fatal(err)
	}
	plan := basePlan("bench-" + tag)
	plan.Spec.Epoch = 1
	plan.Spec.Canary.Mode = v1alpha1.ModeOff
	plan.Spec.Canary.WeightPerMille = 0
	requireCreate(b, plan)
	cc := &countClient{Client: k8sClient}
	g, err := shardkit.Attach(ctx, cc,
		types.NamespacedName{Namespace: testNS, Name: "bench-" + tag}, "stable", "rev-a")
	if err != nil {
		b.Fatal(err)
	}
	return g, cc, nsName
}

// BenchmarkGateOwned_Serial measures one Owned call against the real
// API server: two live GETs (plan + namespace) plus validation and
// adoption. This is the number the fake-client bench understated.
func BenchmarkGateOwned_Serial(b *testing.B) {
	g, _, ns := benchGate(b, "serial")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obs, err := g.Owned(ctx, ns)
		if err != nil {
			b.Fatal(err)
		}
		if !obs.Owned {
			b.Fatal("stable should own bench namespace Off")
		}
	}
}

// BenchmarkGateOwned_Storm runs each op as a 100-call storm across
// 50 workers (the MaxConcurrentReconciles>1 shape: an acquire wave
// re-checking one namespace at once) and reports plan GETs per op.
// Sharing is in-flight only, so storms collapse toward one read.
func BenchmarkGateOwned_Storm(b *testing.B) {
	g, cc, ns := benchGate(b, "storm")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := cc.plans.Load()
		var wg sync.WaitGroup
		errs := make(chan error, 100)
		for w := 0; w < 50; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 2; j++ {
					obs, err := g.Owned(ctx, ns)
					if err != nil {
						errs <- err
						return
					}
					if !obs.Owned {
						errs <- fmt.Errorf("stable should own bench namespace Off")
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			b.Fatal(err)
		}
		reads := cc.plans.Load() - before
		b.ReportMetric(float64(reads), "plan-gets/op")
		if reads > 50 {
			b.Fatalf("storm plan reads = %d for 100 calls, want <= 50 (sharing broken?)", reads)
		}
	}
}
