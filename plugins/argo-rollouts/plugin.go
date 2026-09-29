package main

import (
	"context"
	"fmt"
	"log"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rolloutv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/rollout/trafficrouting/plugin/rpc"
	pluginTypes "github.com/argoproj/argo-rollouts/utils/plugin/types"
	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// version is the release identity, stamped by the image recipe
// (plugins/argo-rollouts/Dockerfile) and reported at startup so a
// stale baked binary is visible in the controller log.
var version = "v0.2.0-dev"

// Plugin implements rpc.TrafficRouterPlugin by writing ShardPlan
// specs. Every method derives its write from the call arguments
// plus one live plan read and keeps no state: net/rpc does not
// persist the struct between calls, so retries and duplicate
// deliveries converge. The client must be direct (non-cached):
// VerifyWeight reads live acks.
//
// Weight scale: Argo percent (0-100) maps to plan per mille by
// exactly 10. Rollout binding: the ShardPlan whose opaque
// spec.rollout equals the calling Rollout's UID, in the Rollout's
// namespace.
type Plugin struct {
	Client client.Client
}

var _ rpc.TrafficRouterPlugin = &Plugin{}

// Type returns the plugin type Argo matches against the rollout's
// trafficRouting configuration.
func (p *Plugin) Type() string { return "shardkit" }

// InitPlugin probes ShardPlan API reachability. A failure names the
// RBAC/CRD fix: the plugin inherits the Rollouts controller
// ServiceAccount, which the install manifests must grant ShardPlan
// read/write.
func (p *Plugin) InitPlugin() pluginTypes.RpcError {
	var list v1alpha1.ShardPlanList
	if err := p.Client.List(context.Background(), &list, client.Limit(1)); err != nil {
		return rpcErrorf("shardkit: InitPlugin: cannot list ShardPlans: %v "+
			"(grant the Argo Rollouts ServiceAccount ShardPlan RBAC and install config/crd/)", err)
	}
	// Startup identity: a stale baked binary is otherwise
	// invisible (the plugin has no other version surface).
	log.Printf("shardkit-plugin %s serving %d ShardPlans", version, len(list.Items))
	return pluginTypes.RpcError{}
}

// UpdateHash binds the canary pod-template hash to the plan canary
// revision (S7: acks void when the running revision differs from
// spec). A hash change is a rollout transition, so it takes a new
// epoch with mode and weight preserved; an equal hash is a no-op.
// The stable hash is validated non-empty but otherwise ignored:
// the stable track is externally managed, and stableHash names
// Argo's own stable ReplicaSet, not the stable operator.
func (p *Plugin) UpdateHash(rollout *rolloutv1alpha1.Rollout, canaryHash, stableHash string,
	additionalDestinations []rolloutv1alpha1.WeightDestination) pluginTypes.RpcError {
	if len(additionalDestinations) > 0 {
		return rpcErrorf("shardkit: UpdateHash: additional destinations (%d) have no object-routing meaning; manage cohorts with the CLI",
			len(additionalDestinations))
	}
	if canaryHash == "" || stableHash == "" {
		return rpcErrorf("shardkit: UpdateHash: empty canary (%q) or stable (%q) hash",
			canaryHash, stableHash)
	}
	plan, rerr := p.resolvePlan(context.Background(), rollout)
	if rerr.HasError() {
		return rerr
	}
	if plan.Spec.Tracks.Canary == nil {
		return rpcErrorf("shardkit: UpdateHash: ShardPlan %s/%s has no canary track revision",
			plan.Namespace, plan.Name)
	}
	if plan.Spec.Tracks.Canary.Revision == canaryHash {
		return pluginTypes.RpcError{} // hash already bound
	}
	if verr := validLive(plan); verr.HasError() {
		return verr
	}
	next := plan.DeepCopy()
	// Canary side only: the stable track is externally managed (its
	// revision belongs to the plain Deployment / CLI flow), while
	// stableHash names Argo's own stable ReplicaSet — writing it
	// into spec.tracks.stable would void the real stable operator
	// (S7) and stall every handoff.
	next.Spec.Tracks.Canary.Revision = canaryHash
	next.Spec.Epoch = plan.Spec.Epoch + 1
	if err := next.ValidateUpdate(plan); err != nil {
		return rpcErrorf("shardkit: UpdateHash: update rejected: %v", err)
	}
	if err := p.updateWithRetry(context.Background(), next,
		fmt.Sprintf("UpdateHash(canary=%s)", canaryHash)); err != nil {
		return rpcErrorf("shardkit: UpdateHash: %v", err)
	}
	return pluginTypes.RpcError{}
}

// trafficScale reads the rollout's maxTrafficWeight (default 100):
// desired weights count in units of (1000/max) per mille, so the
// default moves in whole percents while maxTrafficWeight 1000
// allows 0.1% steps. Granularities that do not divide 1000 evenly
// fail closed — the plan cannot represent them exactly.
func trafficScale(rollout *rolloutv1alpha1.Rollout) (int32, pluginTypes.RpcError) {
	maxWeight := int32(100)
	if canary := rollout.Spec.Strategy.Canary; canary != nil &&
		canary.TrafficRouting != nil && canary.TrafficRouting.MaxTrafficWeight != nil {
		maxWeight = *canary.TrafficRouting.MaxTrafficWeight
	}
	if maxWeight <= 0 || 1000%maxWeight != 0 {
		return 0, rpcErrorf("shardkit: maxTrafficWeight %d cannot scale exactly to per mille (want a positive divisor of 1000)",
			maxWeight)
	}
	return maxWeight, pluginTypes.RpcError{}
}

// SetWeight moves the bound plan to weight w as a new epoch:
// Active at w*(1000/maxTrafficWeight) per mille for w > 0, Off at
// 0 for w == 0. An already-matching plan is a no-op, not a new
// epoch. Only the weight is driven; header/mirror routes and
// additional destinations fail closed (manage cohorts with the
// CLI).
//
// The zero normalization is the quiescence rule. Argo calls
// RemoveManagedRoutes on every sync of a fully-promoted rollout
// and follows it with SetWeight(0); writing Active/0 there would
// flap Off<->Active forever (each call defeats the other's no-op
// guard). Zero weight means no canary, which is Off: both calls
// then agree and every steady state is a no-op. Argo only sends
// SetWeight(0) on abort or steady-state cleanup, both of which
// want stable to reclaim everything.
func (p *Plugin) SetWeight(rollout *rolloutv1alpha1.Rollout, desiredWeight int32,
	additionalDestinations []rolloutv1alpha1.WeightDestination) pluginTypes.RpcError {
	maxWeight, rerr := trafficScale(rollout)
	if rerr.HasError() {
		return rerr
	}
	if desiredWeight < 0 || desiredWeight > maxWeight {
		return rpcErrorf("shardkit: SetWeight: desired weight %d out of range 0-%d",
			desiredWeight, maxWeight)
	}
	if len(additionalDestinations) > 0 {
		return rpcErrorf("shardkit: SetWeight: additional destinations (%d) have no object-routing meaning; manage cohorts with the CLI",
			len(additionalDestinations))
	}
	plan, rerr := p.resolvePlan(context.Background(), rollout)
	if rerr.HasError() {
		return rerr
	}
	if verr := validLive(plan); verr.HasError() {
		return verr
	}
	target := desiredWeight * (1000 / maxWeight)
	wantMode := v1alpha1.ModeActive
	if desiredWeight == 0 {
		wantMode = v1alpha1.ModeOff
	}
	if plan.Spec.Canary.Mode == wantMode &&
		plan.Spec.Canary.WeightPerMille == target {
		return pluginTypes.RpcError{} // desired state already holds
	}
	next := plan.DeepCopy()
	next.Spec.Canary.Mode = wantMode
	next.Spec.Canary.WeightPerMille = target
	next.Spec.Epoch = plan.Spec.Epoch + 1
	if err := next.ValidateUpdate(plan); err != nil {
		return rpcErrorf("shardkit: SetWeight: update rejected: %v", err)
	}
	if err := p.updateWithRetry(context.Background(), next,
		fmt.Sprintf("SetWeight(%d)", desiredWeight)); err != nil {
		return rpcErrorf("shardkit: SetWeight: %v", err)
	}
	return pluginTypes.RpcError{}
}

// VerifyWeight reports Verified only when the live spec carries
// the desired weight in Active mode AND the canary entry acks the
// live epoch, generation, and revision. Anything stale reports
// NotVerified (never true); read and binding failures are RpcError
// (fail closed: Argo surfaces them instead of advancing blind).
func (p *Plugin) VerifyWeight(rollout *rolloutv1alpha1.Rollout, desiredWeight int32,
	additionalDestinations []rolloutv1alpha1.WeightDestination) (pluginTypes.RpcVerified, pluginTypes.RpcError) {
	if len(additionalDestinations) > 0 {
		return pluginTypes.NotVerified,
			rpcErrorf("shardkit: VerifyWeight: additional destinations (%d) have no object-routing meaning; manage cohorts with the CLI",
				len(additionalDestinations))
	}
	plan, rerr := p.resolvePlan(context.Background(), rollout)
	if rerr.HasError() {
		return pluginTypes.NotVerified, rerr
	}
	if verr := validLive(plan); verr.HasError() {
		return pluginTypes.NotVerified, verr
	}
	maxWeight, rerr := trafficScale(rollout)
	if rerr.HasError() {
		return pluginTypes.NotVerified, rerr
	}
	if plan.Spec.Canary.Mode != v1alpha1.ModeActive ||
		plan.Spec.Canary.WeightPerMille != desiredWeight*(1000/maxWeight) {
		return pluginTypes.NotVerified, pluginTypes.RpcError{}
	}
	if plan.Spec.Tracks.Canary == nil {
		return pluginTypes.NotVerified,
			rpcErrorf("shardkit: VerifyWeight: ShardPlan %s/%s has no canary track revision",
				plan.Namespace, plan.Name)
	}
	for i := range plan.Status.Tracks {
		e := &plan.Status.Tracks[i]
		if e.Name != v1alpha1.TrackCanary {
			continue
		}
		if e.PlanUID == string(plan.UID) &&
			e.ObservedEpoch == plan.Spec.Epoch &&
			e.ObservedGeneration == plan.Generation &&
			e.Revision == plan.Spec.Tracks.Canary.Revision {
			return pluginTypes.Verified, pluginTypes.RpcError{}
		}
		return pluginTypes.NotVerified, pluginTypes.RpcError{}
	}
	return pluginTypes.NotVerified, pluginTypes.RpcError{}
}

// SetHeaderRoute has no object-routing meaning; succeeding would
// lie to the rollout, so it fails closed and loud.
func (p *Plugin) SetHeaderRoute(_ *rolloutv1alpha1.Rollout,
	_ *rolloutv1alpha1.SetHeaderRoute) pluginTypes.RpcError {
	return rpcErrorf("shardkit: SetHeaderRoute: header routes have no object-routing meaning")
}

// SetMirrorRoute has no object-routing meaning; succeeding would
// lie to the rollout, so it fails closed and loud.
func (p *Plugin) SetMirrorRoute(_ *rolloutv1alpha1.Rollout,
	_ *rolloutv1alpha1.SetMirrorRoute) pluginTypes.RpcError {
	return rpcErrorf("shardkit: SetMirrorRoute: mirror routes have no object-routing meaning")
}

// RemoveManagedRoutes aborts the bound plan to Off/weight 0 (new
// epoch; no-op when already there). Stable reclaims every
// namespace and the CLI can take over from the rest state, so a
// lost plugin never strands the rollout.
func (p *Plugin) RemoveManagedRoutes(ro *rolloutv1alpha1.Rollout) pluginTypes.RpcError {
	plan, rerr := p.resolvePlan(context.Background(), ro)
	if rerr.HasError() {
		return rerr
	}
	if verr := validLive(plan); verr.HasError() {
		return verr
	}
	if plan.Spec.Canary.Mode == v1alpha1.ModeOff &&
		plan.Spec.Canary.WeightPerMille == 0 {
		return pluginTypes.RpcError{} // already aborted
	}
	next := plan.DeepCopy()
	next.Spec.Canary.Mode = v1alpha1.ModeOff
	next.Spec.Canary.WeightPerMille = 0
	next.Spec.Epoch = plan.Spec.Epoch + 1
	if err := next.ValidateUpdate(plan); err != nil {
		return rpcErrorf("shardkit: RemoveManagedRoutes: update rejected: %v", err)
	}
	if err := p.updateWithRetry(context.Background(), next, "RemoveManagedRoutes"); err != nil {
		return rpcErrorf("shardkit: RemoveManagedRoutes: %v", err)
	}
	return pluginTypes.RpcError{}
}

// resolvePlan binds one Rollout to its ShardPlan via the opaque
// spec.rollout UID in the Rollout's namespace. Zero or multiple
// matches are errors that name the UID and the count — never a
// guess.
func (p *Plugin) resolvePlan(ctx context.Context,
	rollout *rolloutv1alpha1.Rollout) (*v1alpha1.ShardPlan, pluginTypes.RpcError) {
	if rollout == nil {
		return nil, rpcErrorf("shardkit: nil Rollout")
	}
	var list v1alpha1.ShardPlanList
	if err := p.Client.List(ctx, &list, client.InNamespace(rollout.Namespace)); err != nil {
		return nil, rpcErrorf("shardkit: cannot list ShardPlans in namespace %q: %v",
			rollout.Namespace, err)
	}
	var match *v1alpha1.ShardPlan
	count := 0
	for i := range list.Items {
		if list.Items[i].Spec.Rollout == string(rollout.UID) {
			count++
			match = &list.Items[i]
		}
	}
	if count != 1 {
		return nil, rpcErrorf(
			"shardkit: want exactly 1 ShardPlan with spec.rollout=%q in namespace %q, found %d",
			string(rollout.UID), rollout.Namespace, count)
	}
	return match, pluginTypes.RpcError{}
}

// updateWithRetry writes one spec update, retrying RV conflicts.
// Callers validate before calling; a plan that changed underneath
// is re-driven by the controller's next call (stateless retries
// converge). Every attempt is logged: the plugin is the only spec
// writer besides the CLI, so this line is the audit trail that
// attributes each epoch.
func (p *Plugin) updateWithRetry(ctx context.Context, next *v1alpha1.ShardPlan, why string) error {
	for attempt := 1; ; attempt++ {
		err := p.Client.Update(ctx, next)
		log.Printf("shardkit-plugin write op=%s plan=%s/%s epoch=%d mode=%s weight=%d canaryRev=%s stableRev=%s attempt=%d err=%v",
			why, next.Namespace, next.Name, next.Spec.Epoch, next.Spec.Canary.Mode,
			next.Spec.Canary.WeightPerMille, next.Spec.Tracks.Canary.Revision,
			next.Spec.Tracks.Stable.Revision, attempt, err)
		if err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) || attempt >= 3 {
			return err
		}
		var live v1alpha1.ShardPlan
		if gerr := p.Client.Get(ctx, client.ObjectKeyFromObject(next), &live); gerr != nil {
			return gerr
		}
		next.SetResourceVersion(live.GetResourceVersion())
	}
}

func rpcErrorf(format string, args ...any) pluginTypes.RpcError {
	return pluginTypes.RpcError{ErrorString: fmt.Sprintf(format, args...)}
}

// validLive refuses to drive an invalid bound plan, mirroring the
// CLI's pre-write validation.
func validLive(plan *v1alpha1.ShardPlan) pluginTypes.RpcError {
	if err := plan.ValidateCreate(); err != nil {
		return rpcErrorf("shardkit: bound ShardPlan %s/%s is invalid: %v; fix the spec before driving it",
			plan.Namespace, plan.Name, err)
	}
	return pluginTypes.RpcError{}
}
