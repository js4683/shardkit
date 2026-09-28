package shardkit

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeaseName derives the deterministic per-track lease name from the
// operator base name: "<base>-<track>". Both tracks derive each
// other's lease name from this convention for the S8 session check,
// so names must never be deployment-configured.
func LeaseName(base, track string) string {
	return base + "-" + track
}

// LeaseDurations tunes a legacy-migration elector. Zero values take
// the controller-runtime defaults (15s hold, 10s renew deadline,
// 2s retry).
type LeaseDurations struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

func (d LeaseDurations) withDefaults() (time.Duration, time.Duration, time.Duration) {
	hold, deadline, retry := d.LeaseDuration, d.RenewDeadline, d.RetryPeriod
	if hold <= 0 {
		hold = 15 * time.Second
	}
	if deadline <= 0 {
		deadline = 10 * time.Second
	}
	if retry <= 0 {
		retry = 2 * time.Second
	}
	return hold, deadline, retry
}

// LegacyMutex contends a pre-shardkit leader lease during staged
// migration (decision D6): the migrating track acts only while it
// holds BOTH its track lease (via the manager elector) and the
// legacy lease (via this mutex). The old binary holds the legacy
// lease as long as it runs, so the new track blocks instead of
// dual-writing; once the old binary is gone the mutex is acquired
// and the operator removes MigrateFromLease at cutover. The legacy
// hold is purely local: it gates the holder, never the observer,
// and never appears in acks.
type LegacyMutex struct {
	clientset clientset.Interface
	namespace string
	leaseName string
	identity  string
	hold      time.Duration
	deadline  time.Duration
	retry     time.Duration

	holds atomic.Bool
}

// NewLegacyMutex builds the migration mutex for the legacy lease
// name; identity should be pod-unique (e.g. pod name + UID). Call
// Run as a manager runnable (or plain goroutine) and consult Holds.
func NewLegacyMutex(cs clientset.Interface, namespace, leaseName, identity string, d LeaseDurations) *LegacyMutex {
	hold, deadline, retry := d.withDefaults()
	return &LegacyMutex{
		clientset: cs, namespace: namespace, leaseName: leaseName,
		identity: identity, hold: hold, deadline: deadline, retry: retry,
	}
}

// Holds reports whether this process currently leads the legacy lease.
func (m *LegacyMutex) Holds() bool {
	return m.holds.Load()
}

// Run contends the legacy lease until ctx ends. It returns ctx.Err()
// (usually context.Canceled) and clears Holds on exit.
func (m *LegacyMutex) Run(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Namespace: m.namespace, Name: m.leaseName},
		Client:    m.clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: m.identity,
		},
	}
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: m.hold,
		RenewDeadline: m.deadline,
		RetryPeriod:   m.retry,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) { m.holds.Store(true) },
			OnStoppedLeading: func() { m.holds.Store(false) },
		},
		ReleaseOnCancel: true,
		Name:            m.leaseName,
	})
	if err != nil {
		return fmt.Errorf("shardkit: legacy mutex %s/%s: %w", m.namespace, m.leaseName, err)
	}
	elector.Run(ctx)
	return ctx.Err()
}

// ReadSession fetches the live S8 session binding (holder identity
// plus lease transitions) for a track lease with a direct read. A
// missing lease or HolderIdentity is an error: ack sessions must be
// positive evidence of leadership, never a default.
func ReadSession(ctx context.Context, reader client.Reader, key types.NamespacedName) (holder string, transitions int32, err error) {
	var lease coordinationv1.Lease
	if err := reader.Get(ctx, key, &lease); err != nil {
		return "", 0, fmt.Errorf("shardkit: session for %s: %w", key, err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return "", 0, fmt.Errorf("shardkit: session for %s: lease unheld", key)
	}
	var n int32
	if lease.Spec.LeaseTransitions != nil {
		n = *lease.Spec.LeaseTransitions
	}
	return *lease.Spec.HolderIdentity, n, nil
}
