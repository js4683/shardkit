package partition

import (
	"fmt"
	"testing"
)

// scaleNames returns n deterministic namespace names for
// 1k/10k handoff benchmarks.
func scaleNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("tenant-%05d", i)
	}
	return names
}

func scaleSpec() Spec {
	return Spec{Mode: ModeActive, WeightPerMille: 250, Seed: "m4-scale"}
}

// BenchmarkSpec_Owner measures per-namespace handoff assignment
// at 1k/10k fleet sizes. Owner is pure CPU (hash + window
// check), so this bounds the partition half of reconcile cost.
func BenchmarkSpec_Owner(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("namespaces=%d", n), func(b *testing.B) {
			spec := scaleSpec()
			names := scaleNames(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, ns := range names {
					if _, err := spec.Owner(ns, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkSpec_Explain measures the explain path (ownership
// plus reason) at fleet sizes; the CLI simulate command pays
// this per namespace.
func BenchmarkSpec_Explain(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("namespaces=%d", n), func(b *testing.B) {
			spec := scaleSpec()
			names := scaleNames(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, ns := range names {
					if _, err := spec.Explain(ns, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
