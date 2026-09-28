package shardkit

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// BenchmarkGate_Owned measures the per-namespace gate check
// (plan fetch + freshness + ownership) against a fake client.
func BenchmarkGate_Owned(b *testing.B) {
	g, err := Attach(context.Background(), fixture(b, nil), planKey, v1alpha1.TrackStable, "rev-a")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.Owned(ctx, "demo-87"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkValidateCreate measures full plan validation
// (V1–V11) for a representative Off plan.
func BenchmarkValidateCreate(b *testing.B) {
	plan := baseOffPlan()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := plan.ValidateCreate(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConfirmDelete_Charge measures the check-inside-RV-loop
// budget charge plus the UID-preconditioned delete for one
// object: the per-eviction cost a mass-delete cleanup pays.
func BenchmarkConfirmDelete_Charge(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		gc, _ := budgetScaleFixture(b, 1<<20)
		name := fmt.Sprintf("bench-cm-%d", i)
		obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: name}}
		if err := gc.Create(context.Background(), obj); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := gc.ConfirmDelete(context.Background(), planKey, obj); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkShadowed_Delete measures one shadowed (dry-run) delete
// decision: guard + DryRunAll write + recorder append.
func BenchmarkShadowed_Delete(b *testing.B) {
	gc, _ := budgetScaleFixture(b, 1<<20)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := &ShadowRecorder{}
		obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "demo-87", Name: "cm-shadow"}}
		if err := gc.Shadowed(rec).Delete(ctx, obj); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkListAssign measures the API half of a fleet sweep at
// 1k/10k namespaces: one List plus per-namespace partition
// assignment, the shape a status sweeper or CLI simulate pays.
func BenchmarkListAssign(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("namespaces=%d", n), func(b *testing.B) {
			fc := scaleNamespaceFixture(b, n)
			spec, err := PartitionSpec(baseOffPlan())
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var list corev1.NamespaceList
				if err := fc.List(ctx, &list); err != nil {
					b.Fatal(err)
				}
				for j := range list.Items {
					if _, err := spec.Owner(list.Items[j].Name, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// budgetScaleFixture builds a GuardedClient over an Off plan with
// a generous delete cap for single-charge benchmarks.
func budgetScaleFixture(b *testing.B, cap int32) (*GuardedClient, client.Client) {
	b.Helper()
	plan := baseOffPlan()
	n := cap
	plan.Spec.Budget = &v1alpha1.BudgetSpec{MaxDeletions: &n}
	fc := fake.NewClientBuilder().WithScheme(testScheme(b)).
		WithRESTMapper(testRestMapper()).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).
		WithObjects(
			plan,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
		).Build()
	return newGuardedRev(b, v1alpha1.TrackStable, "rev-a", fc)
}

// scaleNamespaceFixture builds a fake client holding n namespaces
// plus the Off plan for list+assign benchmarks.
func scaleNamespaceFixture(b *testing.B, n int) client.Client {
	b.Helper()
	objs := make([]client.Object, 0, n+1)
	objs = append(objs, baseOffPlan())
	for i := 0; i < n; i++ {
		objs = append(objs, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("tenant-%05d", i),
		}})
	}
	return fake.NewClientBuilder().WithScheme(testScheme(b)).
		WithObjects(objs...).Build()
}
