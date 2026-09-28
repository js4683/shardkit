// Randomized two-manager audit for safety invariants I1, I2, I4,
// and I7 (docs/safety-model.md): two independent track drivers
// (separate gates, observers, and guarded clients sharing only the
// API server, like two pods) step against random plan bumps, writes,
// namespace churn, lease rotations, and crashes. After every action
// the driver audits: never both-owned (I1), acquire-after-release
// per version plus fenced handoff events (I2), never
// both-singleton (I4), and monotone ownership with only upward
// weight moves (I7). Back-to-back steps assert no redundant writes.
// A deterministic finale (crash both, abort to Off, re-roll to
// Active/1000) then forces one fully-fenced handoff of every live
// namespace, so I2 never converges vacuously.
//
// Seed: SHARDKIT_TEST_SEED, or time (logged). Steps and namespaces
// stay small so the suite runs in about a minute.
package envtest_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

type auditTrack struct {
	name    string
	rev     string
	gate    *shardkit.Gate
	obs     *shardkit.Observer
	guarded *shardkit.GuardedClient
}

type auditEvent struct {
	track string
	ns    string
	name  string
	v     int
	step  int
}

type auditWrite struct {
	ns   string
	name string
	v    int
	step int
}

type auditor struct {
	t            *testing.T
	rng          *rand.Rand
	ctx          context.Context
	planKey      types.NamespacedName
	stable       *auditTrack
	canary       *auditTrack
	weights      []int32
	weightIdx    int
	epoch        int64
	namespaces   []string
	churnCounter int
	leaseGen     int32
	history      map[string][][2]bool // ns -> per-step (stableOwned, canaryOwned)
	firstSeen    map[string]int       // track/phase/epoch/gen -> first step
	planSnaps    []v1alpha1.ShardPlan
	entrySnaps   [][]v1alpha1.TrackStatus
	stableLeases []coordinationv1.Lease
	events       []auditEvent
	heroWrites   []auditWrite
	planRVs      []string
	actions      []string
	steps        int
	metrics      *shardkit.Metrics
	ownedSnaps   []map[string]bool // canary-owned namespace set per snapshot
	// abortHistLen scopes I7 to the monotone prefix: driveOff sends
	// namespaces back to stable by design, so per-namespace history
	// is only checked up to the recorded length. Nil when no abort
	// happened (zero random bumps), in which case I7 covers all.
	abortHistLen map[string]int
}

func TestRandomized_TwoManagerAudit(t *testing.T) {
	seed := time.Now().UnixNano()
	if raw := os.Getenv("SHARDKIT_TEST_SEED"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			seed = n
		} else {
			t.Fatalf("bad SHARDKIT_TEST_SEED %q: %v", raw, err)
		}
	}
	t.Logf("audit seed: %d", seed)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	a := &auditor{
		t:         t,
		rng:       rand.New(rand.NewSource(seed)),
		ctx:       ctx,
		planKey:   types.NamespacedName{Namespace: testNS, Name: "audit"},
		weights:   []int32{250, 500, 750, 1000},
		epoch:     1,
		history:   map[string][][2]bool{},
		firstSeen: map[string]int{},
		leaseGen:  0,
		// One registry for the whole audit: re-attached observers
		// share it (labels distinguish tracks), and the transition
		// counters diagnose stuck handshakes in auditFinal.
		metrics: shardkit.NewMetrics(prometheus.NewRegistry()),
	}
	for i := 0; i < 8; i++ {
		ns := fmt.Sprintf("aud-%02d", i)
		a.namespaces = append(a.namespaces, ns)
		if err := k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	holder, transitions := "pod-0", int32(0)
	for _, track := range []string{"stable", "canary"} {
		if err := k8sClient.Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "audit-" + track},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &holder, LeaseTransitions: &transitions},
		}); err != nil {
			t.Fatalf("lease %s: %v", track, err)
		}
	}
	plan := basePlan("audit")
	plan.Spec.Canary.Mode = v1alpha1.ModeOff
	plan.Spec.Canary.WeightPerMille = 0
	plan.Spec.Canary.Include = v1alpha1.IncludeSpec{}
	requireCreate(t, plan)

	a.stable = a.attach("stable", "rev-a")
	a.canary = a.attach("canary", "rev-b")
	a.stepTrack(a.stable, true) // resumes must succeed
	a.stepTrack(a.canary, true)
	a.snapshot("resume")
	// Deterministic pre-bump heroes: stable owns everything Off, so
	// every namespace gets a fenced write that every later acquire
	// must carry into its handoff events (I2b starts non-vacuous).
	for _, ns := range a.namespaces {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "hero"},
			Data:       map[string]string{"v": "-1"},
		}
		if err := a.stable.guarded.Create(ctx, cm); err != nil {
			t.Fatalf("setup hero %s: %v", ns, err)
		}
		a.heroWrites = append(a.heroWrites, auditWrite{ns: ns, name: "hero", v: -1, step: -1})
	}

	for a.steps = 0; a.steps < 60; a.steps++ {
		switch a.rng.Intn(10) {
		case 0:
			a.bumpWeight()
		case 1, 2:
			a.stepTrack(a.stable, false)
		case 3, 4:
			a.stepTrack(a.canary, false)
		case 5, 6:
			a.write()
		case 7:
			a.churnNamespace()
		case 8:
			a.rotateLease()
		case 9:
			a.crash()
		}
		a.drainEvents()
		a.snapshot(fmt.Sprintf("step-%d", a.steps))
		a.auditInvariants()
	}
	// Deterministic finale: crash both tracks (fresh baselines),
	// abort to Off (canary holds nothing), then re-roll to
	// Active/1000 so canary gains every live namespace in one
	// fully-fenced handoff. Without this, crashes can absorb every
	// version into silent resume-adoptions and the audit converges
	// vacuously.
	a.crashBoth()
	a.driveOff()
	a.stepTrack(a.stable, true) // resumes must succeed
	a.stepTrack(a.canary, true)
	a.drainEvents()
	a.snapshot("pre-final-resume")
	a.auditInvariants()
	a.finalBump()
	a.quiesce()
	a.auditFinal()
}

func (a *auditor) attach(track, rev string) *auditTrack {
	gate, err := shardkit.Attach(a.ctx, k8sClient, a.planKey, track, rev)
	if err != nil {
		a.t.Fatal(err)
	}
	obs, err := shardkit.NewObserver(shardkit.ObserverOptions{
		Client: k8sClient, APIReader: k8sClient,
		Gate: gate, Guarded: gate.Client(k8sClient, k8sClient),
		PlanKey: a.planKey, Track: track, LeaseBase: "audit",
		Types:          []schema.GroupVersionKind{cmGVK},
		PollInterval:   10 * time.Millisecond,
		DrainTimeout:   10 * time.Second,
		AcquireTimeout: 30 * time.Second,
		Metrics:        a.metrics,
	})
	if err != nil {
		a.t.Fatal(err)
	}
	return &auditTrack{name: track, rev: rev, gate: gate, obs: obs,
		guarded: gate.Client(k8sClient, k8sClient)}
}

// stepTrack runs one observer step. Waiting (deadline) is a legal
// outcome when must is false; any other error fails the test, since
// releases and resumes never legitimately wait. A client-side rate
// limiter refusal that would exceed the step deadline is the same
// class: the acquire loop polls faster than the shared limiter
// refills while it waits on the other track, so the budget — not
// the handshake — gave out.
func (a *auditor) stepTrack(tr *auditTrack, must bool) {
	a.actions = append(a.actions, "step-"+tr.name)
	ctx, cancel := context.WithTimeout(a.ctx, 2*time.Second)
	defer cancel()
	if err := tr.obs.Step(ctx); err != nil {
		if !must && (ctx.Err() != nil ||
			strings.Contains(err.Error(), "would exceed context deadline")) {
			return // still waiting on the other track; allowed
		}
		a.t.Fatalf("step %s: %v", tr.name, err)
	}
}

func (a *auditor) bumpWeight() {
	if a.weightIdx >= len(a.weights) {
		a.stepTrack(a.stable, false)
		return
	}
	a.actions = append(a.actions, "bump")
	a.epoch++
	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(a.ctx, a.planKey, &live); err != nil {
		a.t.Fatal(err)
	}
	live.Spec.Epoch = a.epoch
	live.Spec.Canary.Mode = v1alpha1.ModeActive
	live.Spec.Canary.WeightPerMille = a.weights[a.weightIdx]
	a.weightIdx++
	if err := k8sClient.Update(a.ctx, &live); err != nil {
		a.t.Fatal(err)
	}
}

// driveOff aborts the rollout back to Off/0 (epoch+1), the
// library's first-class abort path: Off is always reachable
// (partition.ValidNext). It records per-namespace history lengths
// so I7 stays scoped to the monotone prefix; a no-op when already
// Off/0 (zero random bumps), in which case I7 covers everything.
// The resumes that follow adopt Off, so canary holds nothing and
// the final bump re-rolls the full cohort in one handoff.
func (a *auditor) driveOff() {
	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(a.ctx, a.planKey, &live); err != nil {
		a.t.Fatal(err)
	}
	if live.Spec.Canary.Mode == v1alpha1.ModeOff && live.Spec.Canary.WeightPerMille == 0 {
		return
	}
	a.abortHistLen = map[string]int{}
	for ns, h := range a.history {
		a.abortHistLen[ns] = len(h)
	}
	a.actions = append(a.actions, "drive-off")
	a.epoch++
	live.Spec.Epoch = a.epoch
	live.Spec.Canary.Mode = v1alpha1.ModeOff
	live.Spec.Canary.WeightPerMille = 0
	if err := k8sClient.Update(a.ctx, &live); err != nil {
		a.t.Fatal(err)
	}
}

// finalBump drives the plan to Active/1000 (unless already there)
// so quiesce converges a version where canary gains every live
// namespace: every stable hero write then has a handoff, which
// keeps the I2b fencing check non-vacuous. Weights only rise, so
// I7 monotonicity is preserved.
func (a *auditor) finalBump() {
	var live v1alpha1.ShardPlan
	if err := k8sClient.Get(a.ctx, a.planKey, &live); err != nil {
		a.t.Fatal(err)
	}
	if live.Spec.Canary.Mode == v1alpha1.ModeActive && live.Spec.Canary.WeightPerMille == 1000 {
		return
	}
	a.actions = append(a.actions, "final-bump")
	a.epoch++
	live.Spec.Epoch = a.epoch
	live.Spec.Canary.Mode = v1alpha1.ModeActive
	live.Spec.Canary.WeightPerMille = 1000
	if err := k8sClient.Update(a.ctx, &live); err != nil {
		a.t.Fatal(err)
	}
	a.drainEvents()
	a.snapshot("final-bump")
	a.auditInvariants()
}

// write attempts one guarded write and pins the I1 write boundary:
// owned namespaces admit, foreign namespaces deny, for both tracks.
// The target namespace is usually one the track owns (so admitted
// writes flow through the whole run) and sometimes random (so
// denials stay covered).
func (a *auditor) write() {
	tr := a.stable
	name := "hero"
	if a.rng.Intn(2) == 0 {
		tr = a.canary
		name = "cw"
	}
	ns := ""
	if owned := a.ownedNamespaces(tr); len(owned) > 0 && a.rng.Intn(4) != 0 {
		ns = owned[a.rng.Intn(len(owned))]
	} else {
		ns = a.namespaces[a.rng.Intn(len(a.namespaces))]
	}
	a.actions = append(a.actions, "write-"+tr.name)
	obs, err := tr.gate.Owned(a.ctx, ns)
	if err != nil {
		a.t.Fatalf("owned check: %v", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Data:       map[string]string{"v": strconv.Itoa(a.steps)},
	}
	err = tr.guarded.Create(a.ctx, cm)
	if apierrors.IsAlreadyExists(err) {
		var live corev1.ConfigMap
		if gerr := k8sClient.Get(a.ctx, types.NamespacedName{Namespace: ns, Name: name}, &live); gerr != nil {
			a.t.Fatal(gerr)
		}
		live.Data = map[string]string{"v": strconv.Itoa(a.steps)}
		err = tr.guarded.Update(a.ctx, &live)
	}
	if obs.Owned && err != nil {
		a.t.Fatalf("%s write to owned %s/%s: %v", tr.name, ns, name, err)
	}
	if !obs.Owned {
		// This driver never deletes the plan or breaks it, so a
		// foreign write denies NotOwned exactly (never GateClosed).
		if denied, ok := shardkit.AsDenied(err); !ok || denied.Reason != shardkit.ReasonNotOwned {
			a.t.Fatalf("%s write to foreign %s/%s: err = %v, want NotOwned", tr.name, ns, name, err)
		}
		return
	}
	if tr.name == "stable" && name == "hero" {
		a.heroWrites = append(a.heroWrites, auditWrite{ns: ns, name: name, v: a.steps, step: a.steps})
	}
}

// ownedNamespaces lists live namespaces the track's gate owns now.
func (a *auditor) ownedNamespaces(tr *auditTrack) []string {
	var out []string
	for _, ns := range a.liveNamespaces() {
		obs, err := tr.gate.Owned(a.ctx, ns)
		if err != nil {
			a.t.Fatalf("owned check: %v", err)
		}
		if obs.Owned {
			out = append(out, ns)
		}
	}
	return out
}

func (a *auditor) churnNamespace() {
	a.actions = append(a.actions, "churn")
	live := a.liveNamespaces()
	if len(live) <= 4 || a.rng.Intn(2) == 0 {
		ns := fmt.Sprintf("aud-x%d", a.churnCounter)
		a.churnCounter++
		a.namespaces = append(a.namespaces, ns)
		if err := k8sClient.Create(a.ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			a.t.Fatal(err)
		}
		return
	}
	ns := live[a.rng.Intn(len(live))]
	if err := k8sClient.Delete(a.ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		a.t.Fatal(err)
	}
	for i, n := range a.namespaces {
		if n == ns {
			a.namespaces = append(a.namespaces[:i], a.namespaces[i+1:]...)
			break
		}
	}
}

func (a *auditor) liveNamespaces() []string {
	var list corev1.NamespaceList
	if err := k8sClient.List(a.ctx, &list); err != nil {
		a.t.Fatal(err)
	}
	keep := map[string]bool{}
	for _, n := range a.namespaces {
		keep[n] = true
	}
	var out []string
	for i := range list.Items {
		if keep[list.Items[i].Name] {
			out = append(out, list.Items[i].Name)
		}
	}
	return out
}

func (a *auditor) rotateLease() {
	tr := "stable"
	if a.rng.Intn(2) == 0 {
		tr = "canary"
	}
	a.actions = append(a.actions, "lease-"+tr)
	a.leaseGen++
	var lease coordinationv1.Lease
	key := types.NamespacedName{Namespace: testNS, Name: "audit-" + tr}
	if err := k8sClient.Get(a.ctx, key, &lease); err != nil {
		a.t.Fatal(err)
	}
	holder := fmt.Sprintf("pod-%d", a.leaseGen)
	lease.Spec.HolderIdentity, lease.Spec.LeaseTransitions = &holder, &a.leaseGen
	if err := k8sClient.Update(a.ctx, &lease); err != nil {
		a.t.Fatal(err)
	}
}

// crash drops one track's gate and observer; the next step for that
// track resumes from scratch (first-transition protocol).
func (a *auditor) crash() {
	if a.rng.Intn(2) == 0 {
		a.actions = append(a.actions, "crash-stable")
		a.stable = a.attach("stable", "rev-a")
	} else {
		a.actions = append(a.actions, "crash-canary")
		a.canary = a.attach("canary", "rev-b")
	}
}

// crashBoth drops both tracks' gates and observers; the next step
// for each resumes from scratch onto the same live version.
func (a *auditor) crashBoth() {
	a.actions = append(a.actions, "crash-both")
	a.stable = a.attach("stable", "rev-a")
	a.canary = a.attach("canary", "rev-b")
}

func (a *auditor) drainEvents() {
	for _, tr := range []*auditTrack{a.stable, a.canary} {
		for {
			select {
			case ev := <-tr.obs.Events():
				v := -1
				if u, ok := ev.Object.(*unstructured.Unstructured); ok {
					if s, _, _ := unstructured.NestedString(u.Object, "data", "v"); s != "" {
						v, _ = strconv.Atoi(s)
					}
				}
				a.events = append(a.events, auditEvent{
					track: tr.name, ns: ev.Object.GetNamespace(),
					name: ev.Object.GetName(), v: v, step: a.steps,
				})
			default:
				goto next
			}
		}
	next:
	}
}

// snapshot records the plan, entries, per-namespace ownership, and
// the plan RV for the redundant-write check.
func (a *auditor) snapshot(label string) {
	_ = label
	var plan v1alpha1.ShardPlan
	if err := k8sClient.Get(a.ctx, a.planKey, &plan); err != nil {
		a.t.Fatal(err)
	}
	a.planSnaps = append(a.planSnaps, plan)
	a.entrySnaps = append(a.entrySnaps, append([]v1alpha1.TrackStatus{}, plan.Status.Tracks...))
	a.planRVs = append(a.planRVs, plan.ResourceVersion)
	var stableLease coordinationv1.Lease
	if err := k8sClient.Get(a.ctx, types.NamespacedName{
		Namespace: testNS, Name: "audit-stable"}, &stableLease); err != nil {
		a.t.Fatal(err)
	}
	a.stableLeases = append(a.stableLeases, stableLease)
	for _, e := range plan.Status.Tracks {
		key := fmt.Sprintf("%s/%s/%d/%d", e.Name, e.Phase, e.ObservedEpoch, e.ObservedGeneration)
		if _, ok := a.firstSeen[key]; !ok {
			a.firstSeen[key] = len(a.planSnaps) - 1
		}
	}
	owned := map[string]bool{}
	for _, ns := range a.liveNamespaces() {
		obsS, err := a.stable.gate.Owned(a.ctx, ns)
		if err != nil {
			a.t.Fatalf("stable owned %s: %v", ns, err)
		}
		obsC, err := a.canary.gate.Owned(a.ctx, ns)
		if err != nil {
			a.t.Fatalf("canary owned %s: %v", ns, err)
		}
		a.history[ns] = append(a.history[ns], [2]bool{obsS.Owned, obsC.Owned})
		if obsC.Owned {
			owned[ns] = true
		}
	}
	a.ownedSnaps = append(a.ownedSnaps, owned)
	// Back-to-back identical steps with nothing between must not
	// write (duplicate status is free).
	n := len(a.actions)
	if n >= 2 && a.actions[n-1] == a.actions[n-2] &&
		(a.actions[n-1] == "step-stable" || a.actions[n-1] == "step-canary") {
		if m := len(a.planRVs); m >= 2 && a.planRVs[m-1] != a.planRVs[m-2] {
			a.t.Fatalf("repeat %s wrote (RV %s -> %s)",
				a.actions[n-1], a.planRVs[m-2], a.planRVs[m-1])
		}
	}
}

// auditInvariants checks I1 (never both-owned) and I4 (never
// both-singleton) on the live state.
func (a *auditor) auditInvariants() {
	for _, ns := range a.liveNamespaces() {
		h := a.history[ns]
		last := h[len(h)-1]
		if last[0] && last[1] {
			a.t.Fatalf("I1 violated at step %d: %s owned by both tracks", a.steps, ns)
		}
	}
	singS, err := a.stable.gate.SingletonOwned(a.ctx)
	if err != nil {
		a.t.Fatalf("stable singleton: %v", err)
	}
	singC, err := a.canary.gate.SingletonOwned(a.ctx)
	if err != nil {
		a.t.Fatalf("canary singleton: %v", err)
	}
	if singS && singC {
		a.t.Fatalf("I4 violated at step %d: singleton held by both tracks", a.steps)
	}
}

// quiesce converges the live version's handshake: it steps both
// tracks until both attest the live (UID, epoch, generation),
// then asserts three more rounds write nothing (idle RV).
// Snapshots follow every step (not every round) so a release and
// its acquire land in different snapshots and firstSeen keeps
// release-before-acquire order. Silence is not convergence: two
// stalled rounds agree byte-for-byte, so exhaustion fails instead
// of passing an unconverged handoff.
func (a *auditor) quiesce() {
	for i := 0; i < 30; i++ {
		a.stepTrack(a.stable, false)
		a.snapshot(fmt.Sprintf("quiesce-%d-stable", i))
		a.stepTrack(a.canary, false)
		a.snapshot(fmt.Sprintf("quiesce-%d-canary", i))
		a.drainEvents()
		if a.handshakeComplete() {
			break
		}
		if i == 29 {
			var plan v1alpha1.ShardPlan
			if err := k8sClient.Get(a.ctx, a.planKey, &plan); err != nil {
				a.t.Fatal(err)
			}
			a.t.Fatalf("quiesce: live version %d/%d never attested by both tracks (status=%v)",
				plan.Spec.Epoch, plan.Generation, plan.Status.Tracks)
		}
	}
	var plan v1alpha1.ShardPlan
	if err := k8sClient.Get(a.ctx, a.planKey, &plan); err != nil {
		a.t.Fatal(err)
	}
	rv := plan.ResourceVersion
	for i := 0; i < 3; i++ {
		a.stepTrack(a.stable, false)
		a.stepTrack(a.canary, false)
	}
	if err := k8sClient.Get(a.ctx, a.planKey, &plan); err != nil {
		a.t.Fatal(err)
	}
	if plan.ResourceVersion != rv {
		a.t.Fatalf("idle steps wrote (RV %s -> %s)", rv, plan.ResourceVersion)
	}
	a.snapshot("final")
}

// handshakeComplete reports whether both tracks attest the live
// plan version (UID, epoch, generation) in their status entries.
// Phases are checked by I2a/I2b; any attestation counts here, so
// membership-steady versions (both Acquired, no release) converge.
func (a *auditor) handshakeComplete() bool {
	var plan v1alpha1.ShardPlan
	if err := k8sClient.Get(a.ctx, a.planKey, &plan); err != nil {
		a.t.Fatal(err)
	}
	var stab, can bool
	for _, e := range plan.Status.Tracks {
		if e.PlanUID != string(plan.UID) || e.ObservedEpoch != plan.Spec.Epoch ||
			e.ObservedGeneration != plan.Generation {
			continue
		}
		if e.Name == a.stable.name {
			stab = true
		}
		if e.Name == a.canary.name {
			can = true
		}
	}
	return stab && can
}

// splitSeenKey parses a firstSeen key into track/phase/epoch/gen.
// Keys are built with Sprintf, so Split (not Sscanf: fmt has no
// scanset verb) plus ParseInt is the exact inverse.
func splitSeenKey(key string) (string, string, int64, int64) {
	parts := strings.Split(key, "/")
	if len(parts) != 4 {
		return "", "", -1, -1
	}
	epoch, eerr := strconv.ParseInt(parts[2], 10, 64)
	gen, gerr := strconv.ParseInt(parts[3], 10, 64)
	if eerr != nil || gerr != nil {
		return "", "", -1, -1
	}
	return parts[0], parts[1], epoch, gen
}

// diagnose logs the action mix, the live plan version, and the
// per-track handshake transition counters: on a stuck-handoff
// failure this shows whether a track never saw the version (no
// release/acquire events) or saw it and waited (abandoned/degraded
// events, barrier observations).
func (a *auditor) diagnose() {
	hist := map[string]int{}
	for _, act := range a.actions {
		hist[act]++
	}
	var live v1alpha1.ShardPlan
	ver := "unreadable"
	if err := k8sClient.Get(a.ctx, a.planKey, &live); err == nil {
		ver = fmt.Sprintf("mode=%s weight=%d epoch=%d gen=%d",
			live.Spec.Canary.Mode, live.Spec.Canary.WeightPerMille,
			live.Spec.Epoch, live.Generation)
	}
	for _, tr := range []*auditTrack{a.stable, a.canary} {
		var parts []string
		for _, ev := range []string{"resume", "adopt", "release", "acquire",
			"acquire_abandoned", "degraded", "steady_refresh"} {
			parts = append(parts, fmt.Sprintf("%s=%.0f", ev,
				testutil.ToFloat64(a.metrics.Transitions(tr.name, tr.rev, ev))))
		}
		a.t.Logf("diagnose %s: %s | %s", tr.name, ver, strings.Join(parts, " "))
	}
	a.t.Logf("diagnose actions: %v", hist)
}

// gainedAtVersion reports whether canary's owned set grew while
// the live plan was version (epoch, gen): a namespace canary held
// at some snapshot of V that it did not hold at the previous
// snapshot. Growth implies the acquire path ran at V (only
// acquires grow the held set within a version; resumes publish
// Released, never Acquired), so the loser's ack was owed.
func (a *auditor) gainedAtVersion(epoch, gen int64) bool {
	for i := range a.planSnaps {
		p := a.planSnaps[i]
		if p.Spec.Epoch != epoch || p.Generation != gen {
			continue
		}
		if i == 0 {
			if len(a.ownedSnaps[i]) > 0 {
				return true
			}
			continue
		}
		for ns := range a.ownedSnaps[i] {
			if !a.ownedSnaps[i-1][ns] {
				return true
			}
		}
	}
	return false
}

// auditFinal checks I2 ordering plus barrier fencing, and I7
// monotonicity over the recorded histories.
func (a *auditor) auditFinal() {
	a.diagnose()
	// I2a: every canary Acquired for V that actually gained
	// namespaces follows a stable Released for V. The runtime only
	// demands the loser's ack on the acquire path (gains); a
	// membership-steady version republishes Acquired with no gain
	// and no ack, so those attestations are exempt here. The
	// single-threaded driver snapshots the exact state the acquire
	// validated (nothing runs between), so the full S5-S11 check
	// re-runs green against the snapshot.
	acquired, steady := 0, 0
	for key, step := range a.firstSeen {
		track, phase, epoch, gen := splitSeenKey(key)
		if track != "canary" || phase != v1alpha1.PhaseAcquired {
			continue
		}
		if !a.gainedAtVersion(epoch, gen) {
			steady++ // no handoff at V: nothing to fence, no ack owed
			continue
		}
		acquired++
		rel, ok := a.firstSeen[fmt.Sprintf("stable/%s/%d/%d", v1alpha1.PhaseReleased, epoch, gen)]
		if !ok || rel >= step {
			a.t.Fatalf("I2 violated: canary Acquired %d/%d at snap %d without a prior stable release (rel snap %d, seen=%v)",
				epoch, gen, step, rel, ok)
		}
		plan := a.planSnaps[step]
		if plan.Spec.Epoch != epoch || plan.Generation != gen {
			a.t.Fatalf("I2 violated: canary acquired %d/%d but snap-%d plan is %d/%d",
				epoch, gen, step, plan.Spec.Epoch, plan.Generation)
		}
		lease := a.stableLeases[step]
		if err := shardkit.ValidRelease(shardkit.AckCheck{
			Plan: &plan, Tracks: a.entrySnaps[step],
			Loser: "stable", LoserLease: &lease,
		}); err != nil {
			a.t.Fatalf("I2 violated: stable ack for %d/%d fails re-validation at snap %d: %v",
				epoch, gen, step, err)
		}
	}
	if acquired == 0 {
		a.t.Fatal("I2 vacuous: canary never acquired (quiesce must converge the final version)")
	}
	a.t.Logf("I2a: %d gaining acquires ordered after release, %d steady attestations exempt",
		acquired, steady)
	// I2b: every admitted stable hero write precedes the final
	// release of that namespace (admission needs stable ownership,
	// and the driver writes only in the random phase, before the
	// abort), so the final acquire's barrier fenced it and the
	// handoff events carry it — across the abort too, since the
	// release evidence carries collection RVs that survive the
	// crash-wipe. Deleted namespaces are exempt (their objects are
	// gone), as are namespaces canary never gained: fencing
	// attaches to handoff. The abort-plus-final-bump makes canary
	// gain every live namespace, so the exemption is normally
	// empty.
	live := map[string]bool{}
	for _, ns := range a.liveNamespaces() {
		live[ns] = true
	}
	gained := map[string]bool{}
	for _, ev := range a.events {
		if ev.track == "canary" {
			gained[ev.ns] = true
		}
	}
	checked, exemptDead, exemptUngained := 0, 0, 0
	for _, w := range a.heroWrites {
		if !live[w.ns] {
			exemptDead++
			continue
		}
		if !gained[w.ns] {
			exemptUngained++
			continue
		}
		checked++
		covered := false
		for _, ev := range a.events {
			if ev.track == "canary" && ev.ns == w.ns && ev.name == w.name && ev.v >= w.v {
				covered = true
				break
			}
		}
		if !covered {
			a.t.Fatalf("I2 violated: hero write %s/%s v=%d (step %d) never fenced into a canary event",
				w.ns, w.name, w.v, w.step)
		}
	}
	if checked == 0 {
		a.t.Fatal("I2 vacuous: no hero write had a handoff to fence (final bump must gain live namespaces)")
	}
	a.t.Logf("I2b: %d hero writes fenced, %d exempt (dead ns), %d exempt (never gained)",
		checked, exemptDead, exemptUngained)
	// I7: while weights only rise, stable ownership never returns
	// and canary ownership never leaves, per namespace. The abort
	// reset drives everything back to stable by design, so history
	// is checked only up to the abort cutoff (full length when no
	// abort happened).
	for ns, hist := range a.history {
		end := len(hist)
		if a.abortHistLen != nil {
			if cut, ok := a.abortHistLen[ns]; ok && cut < end {
				end = cut
			}
		}
		seenFalseS, seenTrueC := false, false
		for _, h := range hist[:end] {
			if !h[0] {
				seenFalseS = true
			}
			if h[0] && seenFalseS {
				a.t.Fatalf("I7 violated: %s returned to stable", ns)
			}
			if h[1] {
				seenTrueC = true
			}
			if !h[1] && seenTrueC {
				a.t.Fatalf("I7 violated: %s left canary", ns)
			}
		}
	}
	a.t.Logf("audit ok: %d steps, %d hero writes, %d events, %d versions",
		a.steps, len(a.heroWrites), len(a.events), len(a.firstSeen))
}
