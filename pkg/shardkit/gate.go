package shardkit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/partition"
)

// Gate is the M1 ownership gate. It observes one ShardPlan, validates
// every version it acts on (spec V1-V10), and evaluates the pure
// partition function per namespace. Observations serialize on an
// internal mutex; reconcile throughput here is plan reads, not object
// writes, so a single mutex is plenty (M4 may shard it).
//
// Fail-closed rules (spec B1-B4): missing, deleted, recreated, or
// malformed plans close the gate; stale reads reuse the last good
// version; unreadable namespace labels retain the last evaluated
// owner and report degraded.
type Gate struct {
	client   client.Reader
	planKey  types.NamespacedName
	track    string
	revision string // reported identity for status acks (leases/item 4)

	mu            sync.Mutex
	lastUID       string
	lastEpoch     int64
	lastGen       int64
	metrics       *Metrics
	lastGood      partition.Spec
	lastMode      string
	lastWeight    int32
	lastSingleton string
	lastStableRev string
	lastCanaryRev string
	// lastSpec is the last adopted spec, deep-copied: same-UID
	// fresh versions must pass ValidateTransition against it
	// (read-time V9), so direct API edits that bypass the writer
	// contract are refused instead of adopted.
	lastSpec           v1alpha1.ShardPlanSpec
	lastOwner          map[string]partition.Owner
	degraded           string
	externalHold       func() bool
	draining           map[string]bool
	drainingSingleton  bool
	revokeCtx          context.Context
	revoke             context.CancelFunc
	inflightReconciles int64
}

// Observation is one evaluated plan version for one namespace.
type Observation struct {
	// Owned authorizes work in the namespace right now.
	Owned bool
	// Epoch, Mode, Weight identify the evaluated plan version.
	Epoch  int64
	Mode   string
	Weight int32
	// SpecRevision is the spec revision for this track at the
	// evaluated version (retained on stale reads). The guarded
	// client fences writes whose gate revision differs (S7).
	SpecRevision string
	// Stale marks evaluation from retained state (a stale plan read
	// or unreadable labels), not a fresh version.
	Stale bool
	// Degraded carries the gate reason while unhealthy, else "".
	Degraded string
}

// trackRevision returns the spec revision for a track, or "" when
// the plan carries none (Off plans may omit the canary revision).
func trackRevision(spec v1alpha1.ShardPlanSpec, track string) string {
	if track == v1alpha1.TrackCanary {
		if spec.Tracks.Canary == nil {
			return ""
		}
		return spec.Tracks.Canary.Revision
	}
	return spec.Tracks.Stable.Revision
}

// Attach binds a gate to an existing valid plan (spec B1, as refined
// in M1: any valid mode adopts, since mid-rollout restarts are
// inevitable and the observer's first-transition protocol — drain
// the complement, refresh evidence, ack, hold the grant —
// establishes safety without an Off round-trip). A missing or
// malformed plan fails with an explicit error; there is no implicit
// "run as stable without a plan".
// The reader must be direct (mgr.GetAPIReader): the gate evaluates
// the live plan on every call, and attach runs before the manager
// cache starts.
func Attach(ctx context.Context, c client.Reader, planKey types.NamespacedName, track, revision string) (*Gate, error) {
	if track != v1alpha1.TrackStable && track != v1alpha1.TrackCanary {
		return nil, fmt.Errorf("shardkit: attach: track %q, want stable or canary", track)
	}
	var plan v1alpha1.ShardPlan
	if err := c.Get(ctx, planKey, &plan); err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("shardkit: attach: plan %s missing (B1: create the plan first)", planKey)
		}
		return nil, fmt.Errorf("shardkit: attach: plan %s unreadable: %w", planKey, err)
	}
	if err := plan.ValidateCreate(); err != nil {
		return nil, fmt.Errorf("shardkit: attach: plan %s malformed: %w", planKey, err)
	}
	spec, err := PartitionSpec(&plan)
	if err != nil {
		return nil, fmt.Errorf("shardkit: attach: plan %s: %w", planKey, err)
	}
	revokeCtx, revoke := context.WithCancel(context.Background())
	return &Gate{
		client:        c,
		planKey:       planKey,
		track:         track,
		revision:      revision,
		lastUID:       string(plan.UID),
		lastEpoch:     plan.Spec.Epoch,
		lastGen:       plan.Generation,
		lastGood:      spec,
		lastMode:      plan.Spec.Canary.Mode,
		lastWeight:    plan.Spec.Canary.WeightPerMille,
		lastSingleton: plan.Spec.SingletonOwner,
		lastStableRev: plan.Spec.Tracks.Stable.Revision,
		lastCanaryRev: trackRevision(plan.Spec, v1alpha1.TrackCanary),
		lastSpec:      *plan.Spec.DeepCopy(),
		lastOwner:     map[string]partition.Owner{},
		revokeCtx:     revokeCtx,
		revoke:        revoke,
	}, nil
}

// Owned evaluates whether this track owns namespace right now
// (observer steps T1-T2). It never authorizes from a version it has
// not validated, and never flips ownership on a transient read error.
// SetMetrics attaches the Prometheus recorder; nil detaches. Call
// before Client so guarded clients inherit it. Safe for concurrent
// use.
func (g *Gate) SetMetrics(m *Metrics) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.metrics = m
}

func (g *Gate) Owned(ctx context.Context, namespace string) (obs Observation, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	defer func() {
		decision, reason := gateOutcome(obs.Owned, err)
		g.metrics.ObserveGateDecision(g.track, g.revision, "namespace", decision, reason)
	}()

	if g.externalHold != nil && !g.externalHold() {
		g.degraded = ReasonExternalHold
		return obs, &ClosedError{Reason: ReasonExternalHold,
			Msg: "external hold unsatisfied (legacy migration mutex or maintenance lock)"}
	}
	var plan v1alpha1.ShardPlan
	if err := g.client.Get(ctx, g.planKey, &plan); err != nil {
		if errors.IsNotFound(err) {
			g.degraded = ReasonPlanDeleted
			return obs, &ClosedError{Reason: ReasonPlanDeleted,
				Msg: fmt.Sprintf("plan %s deleted after attach (B2)", g.planKey)}
		}
		return obs, &ClosedError{Reason: ReasonPlanUnreadable,
			Msg: fmt.Sprintf("plan %s unreadable: %v", g.planKey, err)}
	}
	if err := plan.ValidateCreate(); err != nil {
		g.degraded = ReasonPlanMalformed
		return obs, &ClosedError{Reason: ReasonPlanMalformed,
			Msg: fmt.Sprintf("plan %s malformed, gate holds last state (B4): %v", g.planKey, err)}
	}
	spec, err := PartitionSpec(&plan)
	if err != nil {
		g.degraded = ReasonPlanMalformed
		return obs, &ClosedError{Reason: ReasonPlanMalformed, Msg: err.Error()}
	}

	eval := g.lastGood
	epoch, mode, weight := g.lastEpoch, g.lastMode, g.lastWeight
	specRev := g.retainedRevision()
	if plan.StaleFor(g.lastUID, g.lastEpoch, g.lastGen) {
		obs.Stale = true // V10: ignore transport staleness, keep last state
	} else if string(plan.UID) != g.lastUID {
		if plan.Spec.Canary.Mode != v1alpha1.ModeOff {
			g.degraded = ReasonPlanRecreated
			return obs, &ClosedError{Reason: ReasonPlanRecreated,
				Msg: fmt.Sprintf("plan %s recreated outside Off; drive it through Off (B3)", g.planKey)}
		}
		g.resetBaselines(string(plan.UID), plan.Spec.Epoch, plan.Generation, spec, plan.Spec)
		eval, epoch, mode, weight = spec, plan.Spec.Epoch, plan.Spec.Canary.Mode,
			plan.Spec.Canary.WeightPerMille
		specRev = trackRevision(plan.Spec, g.track)
	} else {
		// Fresh same-UID version: enforce the writer contract
		// before adopting (read-time V9). Refusals hold every
		// baseline, so a tampered version changes nothing.
		if err := v1alpha1.ValidateTransition(&g.lastSpec, &plan.Spec); err != nil {
			g.degraded = ReasonPlanMalformed
			return obs, &ClosedError{Reason: ReasonPlanMalformed,
				Msg: fmt.Sprintf("plan %s violates writer contract, gate holds last state: %v", g.planKey, err)}
		}
		g.adoptLocked(&plan, spec)
		eval, epoch, mode, weight = spec, plan.Spec.Epoch, plan.Spec.Canary.Mode,
			plan.Spec.Canary.WeightPerMille
		specRev = trackRevision(plan.Spec, g.track)
	}
	if g.draining[namespace] {
		// Draining namespaces read as foreign with the live
		// version attached: no error, just no authorization.
		return Observation{Owned: false, Epoch: epoch,
			Mode: mode, Weight: weight, SpecRevision: specRev}, nil
	}

	var ns corev1.Namespace
	if err := g.client.Get(ctx, types.NamespacedName{Name: namespace}, &ns); err != nil {
		if last, ok := g.lastOwner[namespace]; ok {
			g.degraded = ReasonLabelsUnreadable
			return Observation{Owned: last == g.want(), Epoch: epoch,
				Mode: mode, Weight: weight, SpecRevision: specRev, Stale: true,
				Degraded: ReasonLabelsUnreadable}, nil
		}
		return obs, &ClosedError{Reason: ReasonPlanUnreadable,
			Msg: fmt.Sprintf("namespace %q unreadable with no retained owner: %v", namespace, err)}
	}
	owner, err := eval.Owner(namespace, ns.Labels)
	if err != nil {
		return obs, &ClosedError{Reason: ReasonPlanMalformed, Msg: err.Error()}
	}
	g.lastOwner[namespace] = owner
	g.degraded = ""
	return Observation{Owned: owner == g.want(), Epoch: epoch,
		Mode: mode, Weight: weight, SpecRevision: specRev, Stale: obs.Stale}, nil
}

// retainedRevision returns the spec revision for this track at the
// retained baseline (stale reads). Callers hold g.mu.
func (g *Gate) retainedRevision() string {
	if g.track == v1alpha1.TrackCanary {
		return g.lastCanaryRev
	}
	return g.lastStableRev
}

// SingletonOwned reports whether this track currently owns singleton
// duty (cluster-scoped resources, background loops). It follows the
// same observation rules as Owned: validated versions only, stale
// reads reuse retained state, anything else closes.
func (g *Gate) SingletonOwned(ctx context.Context) (owned bool, err error) {
	owned, _, err = g.singletonOwned(ctx)
	return owned, err
}

// singletonOwned evaluates singleton duty like SingletonOwned and
// additionally reports the spec revision for this track at the
// evaluated version, so the guarded client can fence
// cross-revision writes (S7).
func (g *Gate) singletonOwned(ctx context.Context) (owned bool, specRev string, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	defer func() {
		decision, reason := gateOutcome(owned, err)
		g.metrics.ObserveGateDecision(g.track, g.revision, "singleton", decision, reason)
	}()

	if g.externalHold != nil && !g.externalHold() {
		g.degraded = ReasonExternalHold
		return false, "", &ClosedError{Reason: ReasonExternalHold,
			Msg: "external hold unsatisfied (legacy migration mutex or maintenance lock)"}
	}
	if g.drainingSingleton {
		return false, "", nil
	}
	var plan v1alpha1.ShardPlan
	if err := g.client.Get(ctx, g.planKey, &plan); err != nil {
		if errors.IsNotFound(err) {
			return false, "", &ClosedError{Reason: ReasonPlanDeleted,
				Msg: fmt.Sprintf("plan %s deleted after attach (B2)", g.planKey)}
		}
		return false, "", &ClosedError{Reason: ReasonPlanUnreadable,
			Msg: fmt.Sprintf("plan %s unreadable: %v", g.planKey, err)}
	}
	if err := plan.ValidateCreate(); err != nil {
		return false, "", &ClosedError{Reason: ReasonPlanMalformed,
			Msg: fmt.Sprintf("plan %s malformed (B4): %v", g.planKey, err)}
	}
	spec, err := PartitionSpec(&plan)
	if err != nil {
		return false, "", &ClosedError{Reason: ReasonPlanMalformed, Msg: err.Error()}
	}
	if plan.StaleFor(g.lastUID, g.lastEpoch, g.lastGen) {
		return g.lastSingleton == g.track, g.retainedRevision(), nil
	}
	if string(plan.UID) != g.lastUID {
		if plan.Spec.Canary.Mode != v1alpha1.ModeOff {
			g.degraded = ReasonPlanRecreated
			return false, "", &ClosedError{Reason: ReasonPlanRecreated,
				Msg: fmt.Sprintf("plan %s recreated outside Off (B3)", g.planKey)}
		}
		g.resetBaselines(string(plan.UID), plan.Spec.Epoch, plan.Generation, spec, plan.Spec)
		return plan.Spec.SingletonOwner == g.track, trackRevision(plan.Spec, g.track), nil
	}
	// Fresh same-UID version: read-time V9 and shared-baseline
	// adoption, mirroring Owned, so singleton duty never follows a
	// version the namespace path refuses, and the two paths never
	// disagree about what is stale.
	if err := v1alpha1.ValidateTransition(&g.lastSpec, &plan.Spec); err != nil {
		g.degraded = ReasonPlanMalformed
		return false, "", &ClosedError{Reason: ReasonPlanMalformed,
			Msg: fmt.Sprintf("plan %s violates writer contract, gate holds last state: %v", g.planKey, err)}
	}
	g.adoptLocked(&plan, spec)
	return plan.Spec.SingletonOwner == g.track, trackRevision(plan.Spec, g.track), nil
}

// adoptLocked records a fresh same-UID version as the new baseline
// (read-time V9 already passed). Owned and singletonOwned share it
// so both paths adopt the same versions and agree about staleness.
// Callers hold g.mu.
func (g *Gate) adoptLocked(plan *v1alpha1.ShardPlan, spec partition.Spec) {
	g.lastEpoch, g.lastGen, g.lastGood = plan.Spec.Epoch, plan.Generation, spec
	g.lastMode, g.lastWeight = plan.Spec.Canary.Mode, plan.Spec.Canary.WeightPerMille
	g.lastSingleton = plan.Spec.SingletonOwner
	g.lastStableRev = plan.Spec.Tracks.Stable.Revision
	g.lastCanaryRev = trackRevision(plan.Spec, v1alpha1.TrackCanary)
	g.lastSpec = *plan.Spec.DeepCopy()
	g.degraded = ""
}

// resetBaselines adopts a recreated Off plan: epoch, ownership, and
// per-namespace memory restart with stable owning everything (B3).
// Callers hold g.mu.
func (g *Gate) resetBaselines(uid string, epoch, gen int64, spec partition.Spec, src v1alpha1.ShardPlanSpec) {
	g.lastUID = uid
	g.lastEpoch = epoch
	g.lastGen = gen
	g.lastGood = spec
	g.lastMode = src.Canary.Mode
	g.lastWeight = src.Canary.WeightPerMille
	g.lastSingleton = src.SingletonOwner
	g.lastStableRev = src.Tracks.Stable.Revision
	g.lastCanaryRev = trackRevision(src, v1alpha1.TrackCanary)
	g.lastSpec = *src.DeepCopy()
	g.lastOwner = map[string]partition.Owner{}
	g.degraded = ""
}

// want maps this gate's track to the partition owner it seeks.
func (g *Gate) want() partition.Owner {
	if g.track == v1alpha1.TrackCanary {
		return partition.Canary
	}
	return partition.Stable
}

// SetExternalHold installs an extra acting condition consulted on
// every observation: while set and false, all ownership reads fail
// closed with ReasonExternalHold. The func must be cheap and pure
// (an atomic load); it wires the legacy-migration mutex or a
// maintenance lock into enforcement. Pass nil to clear.
func (g *Gate) SetExternalHold(hold func() bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.externalHold = hold
}

// SetDraining replaces the relinquishing set for the transition the
// observer is draining: those namespaces read as foreign (no error)
// so no new work starts while in-flight work bleeds out. The
// observer replaces the set on every new transition and clears it
// with nil once the plan itself denies the namespaces again.
func (g *Gate) SetDraining(namespaces map[string]bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.draining = namespaces
}

// SetDrainingSingleton marks singleton duty as relinquishing (or
// not): while set, cluster-scoped work reads as foreign.
func (g *Gate) SetDrainingSingleton(draining bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.drainingSingleton = draining
}

// DrainingSet returns a copy of the draining namespace set.
func (g *Gate) DrainingSet() map[string]bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]bool, len(g.draining))
	for ns := range g.draining {
		out[ns] = true
	}
	return out
}

// DrainingSingleton reports whether singleton duty is relinquishing.
func (g *Gate) DrainingSingleton() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.drainingSingleton
}

// Degraded reports the gate's current degraded reason, or "" when
// healthy. Writers surface it in status conditions.
func (g *Gate) Degraded() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.degraded
}

// Track returns this gate's track name.
func (g *Gate) Track() string { return g.track }

// Revision returns this gate's reported revision identity.
func (g *Gate) Revision() string { return g.revision }

// PlanKey returns the observed plan's key.
func (g *Gate) PlanKey() types.NamespacedName { return g.planKey }

// Adopted snapshots the gate's adoption baseline: the version and
// spec of the last plan version the gate acted on. The observer
// drives adoption through the gate and compares against this
// snapshot, so both advance exactly the same versions (read-time V9
// lockstep). Callers get a copy.
func (g *Gate) Adopted() (PlanVersion, v1alpha1.ShardPlanSpec) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return PlanVersion{
		UID:        g.lastUID,
		Generation: g.lastGen,
		Epoch:      g.lastEpoch,
		Rollout:    g.lastSpec.Rollout,
	}, *g.lastSpec.DeepCopy()
}

// Scoped derives a context that cancels when the parent does or when
// Revoke fires (plan flip draining, item 4). The caller must call the
// returned cancel func.
func (g *Gate) Scoped(ctx context.Context) (context.Context, context.CancelFunc) {
	out, cancel := context.WithCancel(ctx)
	g.mu.Lock()
	rev := g.revokeCtx
	g.mu.Unlock()
	go func() {
		select {
		case <-rev.Done():
			cancel()
		case <-out.Done():
		}
	}()
	return out, cancel
}

// Revoke cancels every outstanding Scoped context. The drain
// controller (item 4) calls it when the track starts relinquishing
// work; in-flight reconciles and API calls observe ctx.Done.
func (g *Gate) Revoke() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.revoke()
	g.revokeCtx, g.revoke = context.WithCancel(context.Background())
}

// InflightReconciles counts reconciles currently inside the Wrap
// entry check. The drain controller waits for zero.
func (g *Gate) InflightReconciles() int64 {
	return atomic.LoadInt64(&g.inflightReconciles)
}

// WaitReconcilesIdle polls until no reconcile is in flight or ctx
// ends. Item 4's drain uses it after Revoke.
func (g *Gate) WaitReconcilesIdle(ctx context.Context) error {
	for g.InflightReconciles() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return nil
}

// Wrap returns a Reconciler that runs inner only for owned work: the
// entry check skips foreign namespaces and non-singleton
// cluster-scoped requests, then runs inner under a Scoped context
// with in-flight accounting. Closed-gate and foreign work return
// success without requeue; the plan observer (item 4) re-triggers
// when a valid version lands.
func (g *Gate) Wrap(inner reconcile.Reconciler) reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		if req.Namespace == "" {
			ok, err := g.SingletonOwned(ctx)
			if err != nil || !ok {
				return reconcile.Result{}, nil
			}
		} else {
			obs, err := g.Owned(ctx, req.Namespace)
			if err != nil || !obs.Owned {
				return reconcile.Result{}, nil
			}
		}
		scoped, cancel := g.Scoped(ctx)
		defer cancel()
		atomic.AddInt64(&g.inflightReconciles, 1)
		defer atomic.AddInt64(&g.inflightReconciles, -1)
		return inner.Reconcile(scoped, req)
	})
}

// PartitionSpec converts one validated plan version to the pure
// ownership model. Callers validate first; the mode switch is
// unreachable on invalid input but stays total regardless.
// Exported so the CLI simulates and explains with the exact model
// the gate enforces.
func PartitionSpec(plan *v1alpha1.ShardPlan) (partition.Spec, error) {
	var out partition.Spec
	var mode partition.Mode
	switch plan.Spec.Canary.Mode {
	case v1alpha1.ModeOff:
		mode = partition.ModeOff
	case v1alpha1.ModeShadow:
		mode = partition.ModeShadow
	case v1alpha1.ModeActive:
		mode = partition.ModeActive
	default:
		return out, fmt.Errorf("malformed plan: mode %q", plan.Spec.Canary.Mode)
	}
	var excludes map[string]string
	if sel := plan.Spec.Canary.Exclude.Selector; sel != nil {
		excludes = sel.MatchLabels
	}
	out = partition.Spec{
		Mode:           mode,
		Seed:           plan.Spec.Seed,
		WeightPerMille: int(plan.Spec.Canary.WeightPerMille),
		Include:        plan.Spec.Canary.Include.Namespaces,
		ExcludeLabels:  excludes,
	}
	if err := out.Validate(); err != nil {
		return partition.Spec{}, fmt.Errorf("malformed plan: %w", err)
	}
	return out, nil
}
