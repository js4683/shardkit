package shardkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/partition"
)

// ObserverOptions configures one track's handshake observer (spec
// T1-T5): it watches the plan, drains relinquished work, publishes
// release acks, acquires gained work behind the loser's ack plus the
// freshness barrier, and enqueues gained objects for the
// reconciler. One observer runs per track process.
type ObserverOptions struct {
	// Client serves cached reads; APIReader serves fresh reads
	// (plan, status, leases, barrier and handoff lists).
	Client    client.Client
	APIReader client.Reader
	// Gate enforces ownership; Guarded supplies write evidence.
	Gate    *Gate
	Guarded *GuardedClient
	// PlanKey locates the plan; Track names this track.
	PlanKey types.NamespacedName
	Track   string
	// LeaseBase derives per-track lease names (LeaseName); leases
	// live in the plan's namespace.
	LeaseBase string
	// ClusterName qualifies session holders as cluster/holder
	// (M4 multi-cluster keys) on every ack this observer writes
	// and every session it compares. Empty keeps the M1
	// unqualified shape.
	ClusterName string
	// Types are the integrator's watched types: barrier coverage
	// and handoff enumeration span exactly these.
	Types []schema.GroupVersionKind
	// HandoffQPS bounds gained-object enqueue; <=0 means 100.
	HandoffQPS int
	// PollInterval spaces transition evaluations; <=0 means 1s.
	PollInterval time.Duration
	// DrainTimeout bounds one drain (revoke + idle + evidence);
	// <=0 means 30s. Expiry retries next poll, never acks.
	DrainTimeout time.Duration
	// AcquireTimeout bounds one acquire wait before reporting
	// degraded; <=0 means 5m. Expiry keeps waiting, degraded.
	AcquireTimeout time.Duration
	// StatusTimeout bounds one status publish (S2); <=0 means 10s.
	StatusTimeout time.Duration
	// Metrics records handshake events and held ownership; nil
	// records nothing.
	Metrics *Metrics
}

// Observer drives one track's side of the handoff handshake. It is
// single-threaded: Run polls Step, and Step is deterministic given
// the live objects, which is also how tests drive it.
type Observer struct {
	apiReader  client.Reader
	mapper     meta.RESTMapper
	gate       *Gate
	guarded    *GuardedClient
	client     client.Client
	planKey    types.NamespacedName
	track      string
	loser      string
	leaseBase  string
	cluster    string
	types      []schema.GroupVersionKind
	handoffQPS int
	poll       time.Duration
	drain      time.Duration
	acquireTO  time.Duration
	statusTO   time.Duration
	metrics    *Metrics

	mu        sync.Mutex
	hasBase   bool
	lastUID   string
	lastEpoch int64
	lastGen   int64
	held      map[string]bool
	heldSing  bool

	events chan event.GenericEvent
}

// NewObserver validates options and returns the observer. Attaching
// the gate is the caller's job (Attach first, then NewObserver).
func NewObserver(o ObserverOptions) (*Observer, error) {
	if o.Client == nil || o.APIReader == nil || o.Gate == nil || o.Guarded == nil {
		return nil, fmt.Errorf("shardkit: observer: client, apiReader, gate, and guarded client are required")
	}
	if o.Track != v1alpha1.TrackStable && o.Track != v1alpha1.TrackCanary {
		return nil, fmt.Errorf("shardkit: observer: track %q, want stable or canary", o.Track)
	}
	if o.LeaseBase == "" {
		return nil, fmt.Errorf("shardkit: observer: lease base is required (S8 sessions)")
	}
	if len(o.Types) == 0 {
		return nil, fmt.Errorf("shardkit: observer: at least one watched type is required")
	}
	if o.Gate.Track() != o.Track {
		return nil, fmt.Errorf("shardkit: observer: gate track %q != observer track %q",
			o.Gate.Track(), o.Track)
	}
	if err := ValidateClusterName(o.ClusterName); err != nil {
		return nil, err
	}
	loser := v1alpha1.TrackCanary
	if o.Track == v1alpha1.TrackCanary {
		loser = v1alpha1.TrackStable
	}
	qps, poll, drain, acquire, statusTO :=
		o.HandoffQPS, o.PollInterval, o.DrainTimeout, o.AcquireTimeout, o.StatusTimeout
	if qps <= 0 {
		qps = 100
	}
	if poll <= 0 {
		poll = time.Second
	}
	if drain <= 0 {
		drain = 30 * time.Second
	}
	if acquire <= 0 {
		acquire = 5 * time.Minute
	}
	if statusTO <= 0 {
		statusTO = 10 * time.Second
	}
	return &Observer{
		client: o.Client, apiReader: o.APIReader,
		mapper: o.Client.RESTMapper(),
		gate:   o.Gate, guarded: o.Guarded,
		planKey: o.PlanKey, track: o.Track, loser: loser,
		leaseBase: o.LeaseBase, types: o.Types, cluster: o.ClusterName,
		handoffQPS: qps, poll: poll, drain: drain,
		acquireTO: acquire, statusTO: statusTO, metrics: o.Metrics,
		held:   map[string]bool{},
		events: make(chan event.GenericEvent, 1024),
	}, nil
}

// Events exposes the handoff source: one GenericEvent per object in
// newly gained namespaces, at the configured rate. Wire it with
// source.Channel into the manager.
func (o *Observer) Events() <-chan event.GenericEvent {
	return o.events
}

// Run polls Step until ctx ends, returning ctx.Err().
func (o *Observer) Run(ctx context.Context) error {
	for {
		if err := o.Step(ctx); err != nil {
			logf.FromContext(ctx).Info("shardkit: observer step failed; retrying next poll",
				"track", o.track, "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.poll):
		}
	}
}

// Step evaluates the live plan once against this track's baselines
// and advances the handshake: the first poll adopts any valid
// version (resume protocol below), new versions drain/ack/acquire,
// and steady polls keep the S8 session fresh.
func (o *Observer) Step(ctx context.Context) error {
	var plan v1alpha1.ShardPlan
	if err := o.apiReader.Get(ctx, o.planKey, &plan); err != nil {
		if errors.IsNotFound(err) {
			return nil // gate closed (B2); nothing to handshake
		}
		return fmt.Errorf("plan read: %w", err)
	}
	if err := plan.ValidateCreate(); err != nil {
		return nil // malformed: gate closed (B4); never ack it
	}
	spec, err := PartitionSpec(&plan)
	if err != nil {
		return nil // unreachable post-validation; treat as malformed
	}

	o.mu.Lock()
	hasBase, lastUID, lastEpoch, lastGen := o.hasBase, o.lastUID, o.lastEpoch, o.lastGen
	o.mu.Unlock()

	if !hasBase {
		return o.resume(ctx, &plan, spec)
	}
	if string(plan.UID) != lastUID {
		// True plan recreation under a live process: strict B3.
		if plan.Spec.Canary.Mode != v1alpha1.ModeOff {
			o.setDegraded(ctx, &plan, "PlanRecreated",
				"plan recreated outside Off; drive it through Off (B3)")
			return nil
		}
		return o.adopt(ctx, &plan, spec)
	}
	// Versions order by (epoch, generation) per spec section 7: one
	// transition is one (UID, generation, epoch). Strictly older
	// reads are transport staleness; the equal version runs steady
	// (session refresh, S8); anything newer is a transition
	// candidate — but transition runs only for versions the gate
	// adopts (read-time V9), so same-epoch bumps from writers
	// that bypassed the strict-epoch contract are held, not
	// handshaked.
	if plan.Spec.Epoch < lastEpoch ||
		(plan.Spec.Epoch == lastEpoch && plan.Generation < lastGen) {
		return nil
	}
	if plan.Spec.Epoch == lastEpoch && plan.Generation == lastGen {
		return o.steady(ctx, &plan)
	}
	return o.transition(ctx, &plan, spec)
}

// gateAdopted drives adoption of plan through the gate and reports
// whether the gate now acts on exactly this version. The gate
// adopts (or refuses) the live version as a side effect of
// evaluation, so probing it keeps observer and gate verdicts
// identical by construction: read-time V9 refusals, external holds,
// and B3 recreations all pin the observer without a second,
// possibly divergent rule check. The probe namespace is the plan's
// own, which exists whenever the plan does; only the adoption side
// effect matters, never the ownership verdict. A held advance waits
// for the next contracted version, whose transition then carries
// correct deltas from the held baselines.
func (o *Observer) gateAdopted(ctx context.Context, plan *v1alpha1.ShardPlan) bool {
	_, probeErr := o.gate.Owned(ctx, o.planKey.Namespace)
	ver, _ := o.gate.Adopted()
	if ver.UID == string(plan.UID) && ver.Epoch == plan.Spec.Epoch && ver.Generation == plan.Generation {
		return true
	}
	logf.FromContext(ctx).V(1).Info("shardkit: observer holds advance, gate has not adopted version",
		"track", o.track, "epoch", plan.Spec.Epoch, "generation", plan.Generation, "probeErr", probeErr)
	return false
}

// resume adopts the live version on process start (any valid mode)
// and runs the first-transition protocol: drain everything the
// version does not grant, publish the release ack with refreshed
// evidence, and hold the granted set without an acquire wait (fresh
// caches populate from the initial list, and nothing held needs
// re-fencing). This doubles as the migration bootstrap:
// prior-operator writes are fenced by the evidence refresh, and the
// legacy mutex (when configured) serializes against the old binary.
// Resume advances only gate-adopted versions (gateAdopted): the
// first poll may already see a tampered version, and handshaking it
// while the gate refuses it strands ownership the same way a
// mid-run tamper does.
func (o *Observer) resume(ctx context.Context, plan *v1alpha1.ShardPlan, spec partition.Spec) error {
	if !o.gateAdopted(ctx, plan) {
		o.metrics.ObserveTransition(o.track, o.gate.Revision(), "advance_refused")
		return nil
	}
	grant, single, err := o.evaluate(ctx, plan, spec)
	if err != nil {
		return err
	}
	o.setBaselines(string(plan.UID), plan.Spec.Epoch, plan.Generation, grant, single)
	relinquished, err := o.complement(ctx, grant)
	if err != nil {
		return err
	}
	o.gate.SetDraining(relinquished)
	o.gate.SetDrainingSingleton(!single)
	if err := o.drainWork(ctx); err != nil {
		return fmt.Errorf("resume drain: %w", err)
	}
	evidence, err := o.snapshotEvidence(ctx)
	if err != nil {
		return fmt.Errorf("resume evidence: %w", err)
	}
	return o.publishCounted(ctx, VersionOf(plan), v1alpha1.PhaseReleased, true,
		int32(len(grant)), evidence, nil, "resume")
}

// adopt takes a recreated Off plan as the new baseline (strict B3)
// and publishes a steady entry. Only the acquire event marks
// Acquired; every other attestation stays Released so S9 trusts it.
// Like every other advance, adoption waits for the gate (an
// external hold, for example, must pin the observer too).
func (o *Observer) adopt(ctx context.Context, plan *v1alpha1.ShardPlan, spec partition.Spec) error {
	if !o.gateAdopted(ctx, plan) {
		o.metrics.ObserveTransition(o.track, o.gate.Revision(), "advance_refused")
		return nil
	}
	grant, single, err := o.evaluate(ctx, plan, spec)
	if err != nil {
		return err
	}
	o.setBaselines(string(plan.UID), plan.Spec.Epoch, plan.Generation, grant, single)
	o.gate.SetDraining(nil)
	o.gate.SetDrainingSingleton(false)
	evidence, err := o.snapshotEvidence(ctx)
	if err != nil {
		return fmt.Errorf("adopt evidence: %w", err)
	}
	return o.publishCounted(ctx, VersionOf(plan), v1alpha1.PhaseReleased, true,
		int32(len(grant)), evidence, nil, "adopt")
}

// steady republishes when our entry is missing or its session went
// stale (leader change), so a future acquire never voids on S8.
// The republish keeps Released phase: steady-state entries attest
// the complement is released, and S9 only trusts Released-phase
// acks, so an Acquired republish would void our own release ack.
func (o *Observer) steady(ctx context.Context, plan *v1alpha1.ShardPlan) error {
	holder, transitions, err := ReadSession(ctx, o.apiReader, types.NamespacedName{
		Namespace: o.planKey.Namespace, Name: LeaseName(o.leaseBase, o.track)})
	if err != nil {
		return nil // lease unreadable: gate degrades via its own reads; retry next poll
	}
	for _, e := range plan.Status.Tracks {
		if e.Name == o.track && e.PlanUID == string(plan.UID) &&
			e.ObservedEpoch == plan.Spec.Epoch && e.Session != nil &&
			e.Session.Holder == qualifyHolder(o.cluster, holder) && e.Session.LeaseTransitions == transitions {
			return nil // entry current with a fresh session
		}
	}
	o.mu.Lock()
	owned := int32(len(o.held))
	o.mu.Unlock()
	evidence, err := o.snapshotEvidence(ctx)
	if err != nil {
		return fmt.Errorf("steady evidence: %w", err)
	}
	return o.publishCounted(ctx, VersionOf(plan), v1alpha1.PhaseReleased, true, owned, evidence, nil, "steady_refresh")
}

// transition advances one new plan version: drain relinquished work
// and ack, then acquire gained work behind the loser's ack plus the
// freshness barrier. Versions that change no membership republish a
// steady entry. The transition runs only for gate-adopted versions
// (gateAdopted): handshaking a version whose writes the gate denies
// advances baselines and acks past ownership that never moves, and
// later membership-steady versions then converge the acks while the
// stranded namespaces never re-enqueue (live 2026-09-28: a tampered
// seed plus a racing Off write stranded 5 canary widgets under an
// Off plan both tracks had acked).
func (o *Observer) transition(ctx context.Context, plan *v1alpha1.ShardPlan, spec partition.Spec) error {
	if !o.gateAdopted(ctx, plan) {
		o.metrics.ObserveTransition(o.track, o.gate.Revision(), "advance_refused")
		return nil
	}
	ver := VersionOf(plan)
	grant, single, err := o.evaluate(ctx, plan, spec)
	if err != nil {
		return err
	}
	o.mu.Lock()
	held, heldSing := copyHeld(o.held), o.heldSing
	o.mu.Unlock()

	var relinquished []string
	for ns := range held {
		if !grant[ns] {
			relinquished = append(relinquished, ns)
		}
	}
	var gained []string
	for ns := range grant {
		if !held[ns] {
			gained = append(gained, ns)
		}
	}
	relinquishSing := heldSing && !single
	gainSing := single && !heldSing

	if len(relinquished) > 0 || relinquishSing {
		if err := o.release(ctx, ver, relinquished, relinquishSing, int32(len(grant))); err != nil {
			return err
		}
	}
	if len(gained) > 0 || gainSing {
		acquired, err := o.acquire(ctx, ver, gained, gainSing, int32(len(grant)))
		if err != nil {
			return err
		}
		if !acquired {
			// Abandoned: the plan moved on mid-acquire. The
			// release phase above already ran (and published),
			// so the abandoned version still relinquishes:
			// narrow held to its grant and defer the gains.
			// Without this the next version's delta is
			// computed against the pre-abandon held set and
			// never enqueues namespaces that changed only in
			// the skipped version — they stay stamped by the
			// loser under converged acks (live 2026-09-28: 4
			// widgets stranded under Off). Baselines stay, so
			// the version is never acked; the next version
			// acquires the union behind the loser's fresh
			// release, whose evidence still fences the
			// abandoned version's writes.
			o.mu.Lock()
			for ns := range o.held {
				if !grant[ns] {
					delete(o.held, ns)
				}
			}
			if !single {
				o.heldSing = false
			}
			narrowed, sing := len(o.held), o.heldSing
			o.mu.Unlock()
			o.metrics.SetHeld(o.track, o.gate.Revision(), narrowed, sing)
			return nil
		}
	}
	o.setBaselines(ver.UID, ver.Epoch, ver.Generation, grant, single)
	o.gate.SetDraining(nil)
	o.gate.SetDrainingSingleton(false)
	// Membership-steady versions (and post-acquire) republish steady
	// so the entry tracks the version.
	if len(relinquished) == 0 && !relinquishSing && len(gained) == 0 && !gainSing {
		evidence, err := o.snapshotEvidence(ctx)
		if err != nil {
			return fmt.Errorf("steady evidence: %w", err)
		}
		return o.publish(ctx, ver, v1alpha1.PhaseAcquired, true, int32(len(grant)), evidence, nil)
	}
	return nil
}

// release drains relinquished work and publishes the release ack for
// ver. Drain expiry retries next poll; it never acks.
func (o *Observer) release(ctx context.Context, ver PlanVersion, relinquished []string, relinquishSing bool, owned int32) error {
	set := o.gate.DrainingSet()
	for _, ns := range relinquished {
		set[ns] = true
	}
	o.gate.SetDraining(set)
	o.gate.SetDrainingSingleton(o.gate.DrainingSingleton() || relinquishSing)
	if err := o.drainWork(ctx); err != nil {
		o.setDegradedVersion(ctx, ver, owned, "DrainTimeout", err.Error())
		return fmt.Errorf("drain: %w", err)
	}
	evidence, err := o.snapshotEvidence(ctx)
	if err != nil {
		return fmt.Errorf("release evidence: %w", err)
	}
	return o.publishCounted(ctx, ver, v1alpha1.PhaseReleased, true, owned, evidence, nil, "release")
}

// acquire waits for the loser's release ack for ver, runs the
// freshness barrier over the ack's releasedWrites, then takes the
// gained set: it undrains, sets baselines via the caller, publishes
// the acquired entry, and enqueues gained objects. It returns
// acquired=false (no error) when the plan moves on mid-wait; the
// next poll re-evaluates. It never acquires unilaterally: past the
// deadline it reports degraded and keeps waiting.
func (o *Observer) acquire(ctx context.Context, ver PlanVersion, gained []string, gainSing bool, owned int32) (bool, error) {
	deadline := time.Now().Add(o.acquireTO)
	degraded := false
	for {
		var live v1alpha1.ShardPlan
		if err := o.apiReader.Get(ctx, o.planKey, &live); err != nil {
			return false, fmt.Errorf("acquire plan read: %w", err)
		}
		if string(live.UID) != ver.UID || live.Spec.Epoch != ver.Epoch ||
			live.Generation != ver.Generation {
			o.metrics.ObserveTransition(o.track, o.gate.Revision(), "acquire_abandoned")
			return false, nil // plan moved on; next poll re-evaluates
		}
		var loserLease coordinationv1.Lease
		leaseKey := types.NamespacedName{
			Namespace: o.planKey.Namespace, Name: LeaseName(o.leaseBase, o.loser)}
		var leasePtr *coordinationv1.Lease
		if err := o.apiReader.Get(ctx, leaseKey, &loserLease); err == nil {
			leasePtr = &loserLease
		}
		var entry *v1alpha1.TrackStatus
		for i := range live.Status.Tracks {
			if live.Status.Tracks[i].Name == o.loser {
				entry = &live.Status.Tracks[i]
				break
			}
		}
		checkErr := ValidRelease(AckCheck{Plan: &live, Tracks: live.Status.Tracks,
			Loser: o.loser, LoserLease: leasePtr, ClusterName: o.cluster})
		if checkErr == nil && entry != nil {
			if err := WaitFresh(ctx, BarrierOptions{
				Reader: o.apiReader, Mapper: o.mapper,
				Types: o.types, Writes: entry.ReleasedWrites,
				Track: o.track, Metrics: o.metrics,
			}); err == nil {
				break
			} else {
				checkErr = err
			}
		}
		if time.Now().After(deadline) && !degraded {
			degraded = true
			o.mu.Lock()
			owned := int32(len(o.held))
			o.mu.Unlock()
			o.setDegradedVersion(ctx, ver, owned, "AcquireWaiting", checkErr.Error())
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(o.poll):
		}
	}
	// Take the gained set: undrain first so the enqueue wave the
	// reconciler runs is already authorized. Baselines stay with the
	// caller (transition adopts after acquire succeeds).
	set := o.gate.DrainingSet()
	for _, ns := range gained {
		delete(set, ns)
	}
	o.gate.SetDraining(set)
	if gainSing {
		o.gate.SetDrainingSingleton(false)
	}
	evidence, err := o.snapshotEvidence(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire evidence: %w", err)
	}
	if err := o.publishCounted(ctx, ver, v1alpha1.PhaseAcquired, true, owned, evidence, nil, "acquire"); err != nil {
		return false, fmt.Errorf("acquire ack: %w", err)
	}
	return true, o.enqueueGained(ctx, gained, gainSing)
}

// evaluate computes this track's grant under spec: namespaces owned
// plus singleton duty. Namespace objects come from direct reads:
// transition deltas must not rest on cache staleness.
func (o *Observer) evaluate(ctx context.Context, plan *v1alpha1.ShardPlan, spec partition.Spec) (map[string]bool, bool, error) {
	var list corev1.NamespaceList
	if err := o.apiReader.List(ctx, &list); err != nil {
		return nil, false, fmt.Errorf("namespace list: %w", err)
	}
	want := partition.Stable
	if o.track == v1alpha1.TrackCanary {
		want = partition.Canary
	}
	grant := map[string]bool{}
	for i := range list.Items {
		ns := &list.Items[i]
		owner, err := spec.Owner(ns.Name, ns.Labels)
		if err != nil {
			return nil, false, fmt.Errorf("ownership of %q: %w", ns.Name, err)
		}
		if owner == want {
			grant[ns.Name] = true
		}
	}
	return grant, plan.Spec.SingletonOwner == o.track, nil
}

// complement lists live namespaces outside the grant (drain set for
// resume: everything this version does not grant).
func (o *Observer) complement(ctx context.Context, grant map[string]bool) (map[string]bool, error) {
	var list corev1.NamespaceList
	if err := o.apiReader.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("namespace list: %w", err)
	}
	out := map[string]bool{}
	for i := range list.Items {
		if !grant[list.Items[i].Name] {
			out[list.Items[i].Name] = true
		}
	}
	return out, nil
}

// setBaselines adopts one version's ownership as the new baseline.
func (o *Observer) setBaselines(uid string, epoch, gen int64, grant map[string]bool, single bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.hasBase = true
	o.lastUID = uid
	o.lastEpoch = epoch
	o.lastGen = gen
	o.held = grant
	o.heldSing = single
	o.metrics.SetHeld(o.track, o.gate.Revision(), len(grant), single)
}

func copyHeld(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// drainWork revokes scoped contexts and waits for reconciles and
// writes to bleed out within the drain timeout.
func (o *Observer) drainWork(ctx context.Context) error {
	o.gate.Revoke()
	dctx, cancel := context.WithTimeout(ctx, o.drain)
	defer cancel()
	if err := o.gate.WaitReconcilesIdle(dctx); err != nil {
		return fmt.Errorf("reconciles still in flight: %w", err)
	}
	if err := o.guarded.WaitWritesIdle(dctx); err != nil {
		return fmt.Errorf("writes still in flight: %w", err)
	}
	return nil
}

// snapshotEvidence merges client-recorded write versions with fresh
// direct collection versions per watched type (taking the max): the
// refresh heals transient evidence gaps and crash-wiped memory, so
// release acks always fence at least the drain-time state.
func (o *Observer) snapshotEvidence(ctx context.Context) (map[string]string, error) {
	out := o.guarded.LastWritten()
	for _, gvk := range o.types {
		mapping, err := o.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("evidence: type %s unmappable: %w", gvk, err)
		}
		key := mapping.Resource.GroupResource().String()
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{
			Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List",
		})
		if err := o.apiReader.List(ctx, list); err != nil {
			return nil, fmt.Errorf("evidence: list %s: %w", key, err)
		}
		// Empty collection versions (the fake client) contribute
		// nothing; real servers always version collections.
		if rv := list.GetResourceVersion(); rv != "" {
			out[key] = maxRV(out[key], rv)
		}
	}
	return out, nil
}

// publishCounted publishes like publish and counts the handshake
// event on success.
func (o *Observer) publishCounted(ctx context.Context, ver PlanVersion, phase string, released bool, owned int32, evidence map[string]string, conds []metav1.Condition, event string) error {
	if err := o.publish(ctx, ver, phase, released, owned, evidence, conds); err != nil {
		return err
	}
	o.metrics.ObserveTransition(o.track, o.gate.Revision(), event)
	return nil
}

// publish writes this track's entry for ver with a bounded context.
func (o *Observer) publish(ctx context.Context, ver PlanVersion, phase string, released bool, owned int32, evidence map[string]string, conds []metav1.Condition) error {
	pctx, cancel := context.WithTimeout(ctx, o.statusTO)
	defer cancel()
	return PublishOwnEntry(pctx, AckOptions{
		Client: o.client, APIReader: o.apiReader, PlanKey: o.planKey,
		Track: o.track, Revision: o.gate.Revision(), Version: ver,
		Phase: phase, Released: released, OwnedNamespaces: owned,
		ReleasedWrites: evidence,
		LeaseName:      LeaseName(o.leaseBase, o.track), LeaseNamespace: o.planKey.Namespace,
		ClusterName: o.cluster,
		Conditions:  conds,
	})
}

// setDegraded publishes a best-effort Draining entry with a Degraded
// condition for the live version (used pre-baseline).
func (o *Observer) setDegraded(ctx context.Context, plan *v1alpha1.ShardPlan, reason, msg string) {
	o.metrics.ObserveTransition(o.track, o.gate.Revision(), "degraded")
	_ = o.publish(ctx, VersionOf(plan), v1alpha1.PhaseDraining, false, 0, nil,
		[]metav1.Condition{{
			Type: "Degraded", Status: metav1.ConditionTrue, Reason: reason, Message: msg,
			LastTransitionTime: metav1.Now(),
		}})
}

// setDegradedVersion publishes a best-effort Draining entry with a
// Degraded condition for an explicit version (mid-transition).
func (o *Observer) setDegradedVersion(ctx context.Context, ver PlanVersion, owned int32, reason, msg string) {
	o.metrics.ObserveTransition(o.track, o.gate.Revision(), "degraded")
	_ = o.publish(ctx, ver, v1alpha1.PhaseDraining, false, owned, nil,
		[]metav1.Condition{{
			Type: "Degraded", Status: metav1.ConditionTrue, Reason: reason, Message: msg,
			LastTransitionTime: metav1.Now(),
		}})
}

// enqueueGained emits one event per object in the gained namespaces
// for every namespaced watched type, plus one pass over
// cluster-scoped watched types when singleton duty was gained. All
// enumeration uses direct lists (a cached list could miss objects
// created during the transition) and is paced at the configured
// rate. The send blocks on a full channel: the manager must consume
// Events, since a dropped handoff is a missed reconcile.
func (o *Observer) enqueueGained(ctx context.Context, gained []string, gainSing bool) error {
	interval := time.Second / time.Duration(o.handoffQPS)
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	emit := func(list *unstructured.UnstructuredList) error {
		for i := range list.Items {
			obj := &list.Items[i]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case o.events <- event.GenericEvent{Object: obj}:
			}
		}
		return nil
	}
	for _, ns := range gained {
		for _, gvk := range o.types {
			if o.clusterScoped(gvk) {
				continue
			}
			list, err := o.listType(ctx, gvk, ns)
			if err != nil {
				return fmt.Errorf("handoff list %s in %s: %w", gvk.Kind, ns, err)
			}
			if err := emit(list); err != nil {
				return err
			}
		}
	}
	if gainSing {
		for _, gvk := range o.types {
			if !o.clusterScoped(gvk) {
				continue
			}
			list, err := o.listType(ctx, gvk, "")
			if err != nil {
				return fmt.Errorf("handoff list %s: %w", gvk.Kind, err)
			}
			if err := emit(list); err != nil {
				return err
			}
		}
	}
	return nil
}

// listType directly lists one watched type, namespace-scoped when ns
// is set.
func (o *Observer) listType(ctx context.Context, gvk schema.GroupVersionKind, ns string) (*unstructured.UnstructuredList, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List",
	})
	var opts []client.ListOption
	if ns != "" {
		opts = append(opts, client.InNamespace(ns))
	}
	if err := o.apiReader.List(ctx, list, opts...); err != nil {
		return nil, err
	}
	return list, nil
}

// clusterScoped reports whether a watched type is cluster-scoped.
// Unmappable types report false; the subsequent list then fails
// loudly through the client's own mapping, which fails the acquire
// instead of silently skipping the type.
func (o *Observer) clusterScoped(gvk schema.GroupVersionKind) bool {
	mapping, err := o.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return false
	}
	return mapping.Scope.Name() != meta.RESTScopeNamespace.Name()
}
