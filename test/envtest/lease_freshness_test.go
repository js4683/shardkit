package envtest_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/js4683/shardkit/pkg/shardkit"
)

// TestBarrier_RealServer pins the freshness barrier against live
// LIST semantics: recorded versions pass, future versions time out,
// unlisted types and non-decimal versions fail closed immediately.
func TestBarrier_RealServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "barrier-1"},
		Data:       map[string]string{"k": "v"},
	}
	if err := k8sClient.Create(ctx, cm); err != nil {
		t.Fatal(err)
	}
	cmGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
	opts := shardkit.BarrierOptions{
		Reader: k8sClient, Mapper: k8sClient.RESTMapper(),
		Types:  []schema.GroupVersionKind{cmGVK},
		Writes: map[string]string{"configmaps": cm.ResourceVersion},
	}
	if err := shardkit.WaitFresh(ctx, opts); err != nil {
		t.Errorf("recorded version: %v", err)
	}
	future := opts
	future.Writes = map[string]string{"configmaps": "999999999999"}
	fctx, fcancel := context.WithTimeout(ctx, 2*time.Second)
	defer fcancel()
	if err := shardkit.WaitFresh(fctx, future); err == nil {
		t.Error("future version: nil error, want timeout")
	}
	unlisted := opts
	unlisted.Writes = map[string]string{"deployments.apps": "42"}
	if err := shardkit.WaitFresh(ctx, unlisted); err == nil {
		t.Error("unlisted type: nil error, want fail-closed")
	}
	nan := opts
	nan.Writes = map[string]string{"configmaps": "abc"}
	if err := shardkit.WaitFresh(ctx, nan); err == nil {
		t.Error("non-decimal version: nil error, want fail-closed")
	}
	empty := opts
	empty.Writes = map[string]string{}
	if err := shardkit.WaitFresh(ctx, empty); err != nil {
		t.Errorf("no writes: %v, want immediate pass", err)
	}
}

// TestLegacyMutex_Handover pins staged-migration fencing with real
// lease timing: the first contender holds while the second blocks,
// and cancellation hands over within seconds.
func TestLegacyMutex_Handover(t *testing.T) {
	cs, err := clientset.NewForConfig(testCfg)
	if err != nil {
		t.Fatal(err)
	}
	d := shardkit.LeaseDurations{
		LeaseDuration: 10 * time.Second,
		RenewDeadline: 5 * time.Second,
		RetryPeriod:   200 * time.Millisecond,
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	mutexA := shardkit.NewLegacyMutex(cs, testNS, "legacy-mutex", "holder-a", d)
	errA := make(chan error, 1)
	go func() { errA <- mutexA.Run(ctxA) }()
	deadline := time.Now().Add(15 * time.Second)
	for !mutexA.Holds() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !mutexA.Holds() {
		t.Fatal("first mutex never acquired")
	}
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	mutexB := shardkit.NewLegacyMutex(cs, testNS, "legacy-mutex", "holder-b", d)
	errB := make(chan error, 1)
	go func() { errB <- mutexB.Run(ctxB) }()
	time.Sleep(1500 * time.Millisecond)
	if mutexB.Holds() {
		t.Fatal("second mutex holds while first lives: dual leadership")
	}
	cancelA()
	if err := <-errA; err != context.Canceled {
		t.Fatalf("first run returned %v, want context.Canceled", err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for !mutexB.Holds() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !mutexB.Holds() {
		t.Fatal("second mutex never acquired after release")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	holder, _, err := shardkit.ReadSession(ctx, k8sClient,
		types.NamespacedName{Namespace: testNS, Name: "legacy-mutex"})
	if err != nil {
		t.Fatal(err)
	}
	if holder != "holder-b" {
		t.Fatalf("live holder = %q, want holder-b", holder)
	}
	cancelB()
	<-errB
}

// TestLeaseName pins the deterministic per-track convention both
// tracks use to derive each other's lease for the S8 check.
func TestLeaseName(t *testing.T) {
	if got := shardkit.LeaseName("widget-operator", "stable"); got != "widget-operator-stable" {
		t.Fatalf("lease name = %q", got)
	}
	if got := shardkit.LeaseName("widget-operator", "canary"); got != "widget-operator-canary" {
		t.Fatalf("lease name = %q", got)
	}
}
