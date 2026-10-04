package main

import (
	"context"
	"fmt"
	"log"
	"time"

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
// plus a live plan read and keeps no state: net/rpc does not
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
	ctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
	defer cancel()
	var list v1alpha1.ShardPlanList
	if err := p.Client.List(ctx, &list, client.Limit(1)); err != nil {
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
	return p.mutatePlan(rollout, fmt.Sprintf("UpdateHash(canary=%s)", canaryHash), func(next *v1alpha1.ShardPlan) (bool, error) {
		if next.Spec.Tracks.Canary == nil {
			return false, fmt.Errorf("ShardPlan %s/%s has no canary track revision", next.Namespace, next.Name)
		}
		if next.Spec.Tracks.Canary.Revision == canaryHash {
			return false, nil
		}
		next.Spec.Tracks.Canary.Revision = canaryHash
		return true, nil
	})
}

// trafficScale reads the rollout's maxTrafficWeight (default 100):
// desired weights count in units of (1000/max) per mille, so the
// default moves in whole percents while maxTrafficWeight 1000
// allows 0.1% steps. Granularities that do not divide 1000 evenly
// fail closed — the plan cannot represent them exactly.
func trafficScale(rollout *rolloutv1alpha1.Rollout) (int32, pluginTypes.RpcError) {
	if rollout == nil {
		return 0, rpcErrorf("shardkit: nil Rollout")
	}
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
	target := desiredWeight * (1000 / maxWeight)
	wantMode := v1alpha1.ModeActive
	if desiredWeight == 0 {
		wantMode = v1alpha1.ModeOff
	}
	return p.mutatePlan(rollout, fmt.Sprintf("SetWeight(%d)", desiredWeight), setWeight(target, wantMode))
}

// VerifyWeight reports Verified only when the live spec carries
// the desired weight in Active mode AND the canary entry acks the
// live epoch, generation, and revision. Anything stale reports
// NotVerified (never true); out-of-range weights, read, and binding
// failures are RpcError (fail closed: Argo surfaces them instead
// of advancing blind).
func (p *Plugin) VerifyWeight(rollout *rolloutv1alpha1.Rollout, desiredWeight int32,
	additionalDestinations []rolloutv1alpha1.WeightDestination) (pluginTypes.RpcVerified, pluginTypes.RpcError) {
	if len(additionalDestinations) > 0 {
		return pluginTypes.NotVerified,
			rpcErrorf("shardkit: VerifyWeight: additional destinations (%d) have no object-routing meaning; manage cohorts with the CLI",
				len(additionalDestinations))
	}
	ctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
	defer cancel()
	plan, rerr := p.resolvePlan(ctx, rollout)
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
	if desiredWeight < 0 || desiredWeight > maxWeight {
		return pluginTypes.NotVerified, rpcErrorf("shardkit: VerifyWeight: desired weight %d out of range 0-%d", desiredWeight, maxWeight)
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
	if canaryAckMatches(plan) {
		return pluginTypes.Verified, pluginTypes.RpcError{}
	}
	return pluginTypes.NotVerified, pluginTypes.RpcError{}
}

func canaryAckMatches(plan *v1alpha1.ShardPlan) bool {
	for _, entry := range plan.Status.Tracks {
		if entry.Name != v1alpha1.TrackCanary {
			continue
		}
		return entry.PlanUID == string(plan.UID) &&
			entry.ObservedEpoch == plan.Spec.Epoch &&
			entry.ObservedGeneration == plan.Generation &&
			entry.Revision == plan.Spec.Tracks.Canary.Revision
	}
	return false
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
	return p.mutatePlan(ro, "RemoveManagedRoutes", setWeight(0, v1alpha1.ModeOff))
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

// apiTimeout bounds a plugin call even when the Kubernetes API stops responding.
const apiTimeout = 30 * time.Second

// setWeight changes only routing fields; an already-matching plan is a no-op.
func setWeight(weight int32, mode string) func(*v1alpha1.ShardPlan) (bool, error) {
	return func(next *v1alpha1.ShardPlan) (bool, error) {
		if next.Spec.Canary.Mode == mode && next.Spec.Canary.WeightPerMille == weight {
			return false, nil
		}
		next.Spec.Canary.Mode, next.Spec.Canary.WeightPerMille = mode, weight
		return true, nil
	}
}

// mutatePlan re-resolves the binding and derives each attempt from the live
// plan. Refreshing only resourceVersion would overwrite concurrent spec or
// metadata changes. Validation and no-op detection also run on every retry.
func (p *Plugin) mutatePlan(rollout *rolloutv1alpha1.Rollout, why string, mutate func(*v1alpha1.ShardPlan) (bool, error)) pluginTypes.RpcError {
	ctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
	defer cancel()
	for attempt := 1; ; attempt++ {
		live, rerr := p.resolvePlan(ctx, rollout)
		if rerr.HasError() {
			return rerr
		}
		if verr := validLive(live); verr.HasError() {
			return verr
		}
		next := live.DeepCopy()
		changed, err := mutate(next)
		if err != nil {
			return rpcErrorf("shardkit: %s: %v", why, err)
		}
		if !changed {
			return pluginTypes.RpcError{}
		}
		next.Spec.Epoch = live.Spec.Epoch + 1
		if err := next.ValidateUpdate(live); err != nil {
			return rpcErrorf("shardkit: %s: update rejected: %v", why, err)
		}
		err = p.Client.Update(ctx, next)
		logPlanWrite(next, why, attempt, err)
		if err == nil {
			return pluginTypes.RpcError{}
		}
		if !apierrors.IsConflict(err) || attempt >= 3 {
			return rpcErrorf("shardkit: %s: %v", why, err)
		}
	}
}

func logPlanWrite(plan *v1alpha1.ShardPlan, why string, attempt int, err error) {
	canaryRevision := ""
	if plan.Spec.Tracks.Canary != nil {
		canaryRevision = plan.Spec.Tracks.Canary.Revision
	}
	log.Printf("shardkit-plugin write op=%s plan=%s/%s epoch=%d mode=%s weight=%d canaryRev=%s stableRev=%s attempt=%d err=%v",
		why, plan.Namespace, plan.Name, plan.Spec.Epoch, plan.Spec.Canary.Mode,
		plan.Spec.Canary.WeightPerMille, canaryRevision,
		plan.Spec.Tracks.Stable.Revision, attempt, err)
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
