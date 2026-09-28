package shardkit

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/partition"
)

// Confirmed deletes (I5): GuardedClient.ConfirmDelete deletes one
// object only after a direct read confirms it exists, V11 budget
// caps admit one more deletion for this track in this epoch, the
// charge is persisted against the track's own status entry, and
// the delete carries a UID precondition (FA5.4) so a
// delete-versus-recreate race fails closed with Conflict instead
// of deleting the replacement.
//
// Order and failure direction (all fail closed):
//  1. authorize (ownership + S7 fence): DeniedError, no call.
//  2. direct GET: NotFound is idempotent success (already gone, no
//     charge); any other read failure is a plain error — requeue,
//     do not proceed from cache absence (FA5.3). The reader must
//     be direct (the same APIReader the gate uses).
//  3. live plan read + ValidateCreate: malformed plans deny.
//  4. budget check against the V11 caps (nil caps skip checks and
//     charges entirely — plans without budgets behave as before):
//     absolute, then percentage over the track-owned namespace
//     count from a direct list (zero/unknown denominator denies,
//     FA5.2). Over-cap denies with ReasonBudgetExceeded; a new
//     epoch resets the window implicitly (usage keys on epoch).
//  5. charge: read-modify-write of ONLY the own entry's budget
//     stanza (S1: single writer per entry + RV retries, FA5.1),
//     preserving every other field; ack publishes preserve the
//     stanza back (see PublishOwnEntry).
//  6. delete with UID precondition, re-authorized at the API
//     boundary through guardDelete (the M1 re-check pattern).
//
// Shadow mode: reads stay real; the charge and the delete both
// dry-run and record (verbs "budget-charge" and "delete").
// Denials before any call record "confirm-delete" explicitly.
func (c *GuardedClient) ConfirmDelete(ctx context.Context, planKey types.NamespacedName, obj client.Object, opts ...client.DeleteOption) error {
	key, kerr := c.resourceKey(obj)
	if kerr != nil {
		return c.confirmDenied("confirm-delete", "unknown", shadowName(obj.GetNamespace(), obj.GetName()),
			&DeniedError{Reason: ReasonUnsupportedWrite,
				Msg: fmt.Sprintf("confirm-delete %T: unmappable type: %v", obj, kerr)})
	}
	namespaced, nerr := c.Client.IsObjectNamespaced(obj)
	if nerr != nil {
		return c.confirmDenied("confirm-delete", key, shadowName(obj.GetNamespace(), obj.GetName()),
			&DeniedError{Reason: ReasonUnsupportedWrite,
				Msg: fmt.Sprintf("confirm-delete %T: cannot determine scope: %v", obj, nerr)})
	}
	namespace, name := obj.GetNamespace(), obj.GetName()
	if err := c.authorize(ctx, namespace, name, namespaced, "confirm-delete"); err != nil {
		return c.confirmDenied("confirm-delete", key, shadowName(namespace, name), err)
	}

	fresh, err := c.confirmGet(ctx, obj)
	if err != nil {
		return err
	}
	if fresh == nil {
		return nil // already gone: idempotent success, no charge
	}

	plan, err := c.confirmPlan(ctx, planKey)
	if err != nil {
		return err
	}
	if plan.Spec.Budget == nil {
		return c.deleteUID(ctx, key, namespace, name, obj, string(fresh.GetUID()), opts)
	}
	// Check-and-charge run INSIDE the RV retry loop below: the
	// check must re-derive from a fresh read every attempt, or
	// concurrent reconciles (MaxConcurrentReconciles > 1) all pass
	// one stale check and over-delete — the FA5.1 race, observed
	// live (5 deletes against a cap of 3). RV conflicts serialize
	// the racers; each retry re-checks before writing.
	if err := c.checkAndCharge(ctx, planKey, key, namespace, name); err != nil {
		return err
	}
	return c.deleteUID(ctx, key, namespace, name, obj, string(fresh.GetUID()), opts)
}

// checkAndCharge admits one deletion against the V11 caps and
// persists the charge, retrying RV conflicts with a fresh
// read-check-write each attempt. Denials are final for this epoch
// (a new epoch resets the window); conflicts and transient errors
// propagate for requeue.
func (c *GuardedClient) checkAndCharge(ctx context.Context, planKey types.NamespacedName, key, namespace, name string) error {
	// Eight attempts matches PublishOwnEntry's conflict budget.
	// Three starves under lockstep waves: six simultaneous
	// chargers against a cap of 3 align into waves where the
	// slowest three burn every attempt on conflicts and return
	// raw errors instead of converging to BudgetExceeded. The
	// linear backoff desynchronizes the waves so losers re-read
	// a settled counter and deny instead.
	for attempt := 1; attempt <= 8; attempt++ {
		var live v1alpha1.ShardPlan
		if err := c.reader.Get(ctx, planKey, &live); err != nil {
			return fmt.Errorf("confirm-delete: charge read failed: %w", err)
		}
		used := c.budgetUsed(&live)
		caps := live.Spec.Budget
		if caps == nil {
			return fmt.Errorf("confirm-delete: budget removed mid-flight, refusing stale charge")
		}
		if caps.MaxDeletions != nil && used+1 > *caps.MaxDeletions {
			return c.confirmDenied("confirm-delete", key, shadowName(namespace, name),
				&DeniedError{Reason: ReasonBudgetExceeded,
					Msg: fmt.Sprintf("confirm-delete %s: %d deletions used, max %d for epoch %d",
						shadowName(namespace, name), used, *caps.MaxDeletions, live.Spec.Epoch)})
		}
		if caps.MaxDeletionPercent != nil {
			denied, derr := c.checkPercent(ctx, &live, key, namespace, name, used)
			if derr != nil || denied {
				return derr
			}
		}
		cerr := c.writeCharge(ctx, planKey, &live, used+1)
		if cerr == nil {
			return nil
		}
		if !apierrors.IsConflict(cerr) || attempt == 8 {
			return cerr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 10 * time.Millisecond):
		}
	}
	return fmt.Errorf("confirm-delete: charge conflicts did not converge")
}

// checkPercent enforces the percentage cap over the directly
// listed track-owned denominator. It returns (denied=true, nil)
// on a cap trip, (false, nil) when admitted, or (false, err) on
// transient failures and zero-denominator denials (FA5.2).
func (c *GuardedClient) checkPercent(ctx context.Context, live *v1alpha1.ShardPlan, key, namespace, name string, used int32) (bool, error) {
	denom, derr := c.ownedNamespaceCount(ctx, live)
	if derr != nil {
		if _, ok := AsDenied(derr); ok {
			return false, c.confirmDenied("confirm-delete", key, shadowName(namespace, name), derr)
		}
		return false, derr
	}
	if denom == 0 {
		return false, c.confirmDenied("confirm-delete", key, shadowName(namespace, name),
			&DeniedError{Reason: ReasonBudgetExceeded,
				Msg: fmt.Sprintf("confirm-delete %s: track owns no namespaces, zero denominator denies (FA5.2)",
					shadowName(namespace, name))})
	}
	pct := *live.Spec.Budget.MaxDeletionPercent
	if (int64(used)+1)*100 > int64(pct)*int64(denom) {
		return true, c.confirmDenied("confirm-delete", key, shadowName(namespace, name),
			&DeniedError{Reason: ReasonBudgetExceeded,
				Msg: fmt.Sprintf("confirm-delete %s: %d deletions used, max %d%% of %d owned namespaces for epoch %d",
					shadowName(namespace, name), used, pct, denom, live.Spec.Epoch)})
	}
	return false, nil
}

// confirmDenied records a pre-call denial when shadowing and
// returns the error unchanged.
func (c *GuardedClient) confirmDenied(verb, key, name string, err error) error {
	if c.shadowing() {
		decision, reason := writeOutcome(err)
		c.noteShadow(verb, key, name, decision, reason)
	}
	return err
}

// confirmGet direct-reads the object: nil (nil error) means
// already deleted; any other read failure is transient.
func (c *GuardedClient) confirmGet(ctx context.Context, obj client.Object) (client.Object, error) {
	fresh, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return nil, &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("confirm-delete %T: cannot deepcopy for confirm read", obj)}
	}
	fresh.SetResourceVersion("")
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(obj), fresh); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil // already gone
		}
		return nil, fmt.Errorf("confirm-delete %s/%s: direct read failed, refusing: %w",
			obj.GetNamespace(), obj.GetName(), err)
	}
	return fresh, nil
}

// confirmPlan reads and validates the live plan.
func (c *GuardedClient) confirmPlan(ctx context.Context, planKey types.NamespacedName) (*v1alpha1.ShardPlan, error) {
	var plan v1alpha1.ShardPlan
	if err := c.reader.Get(ctx, planKey, &plan); err != nil {
		return nil, fmt.Errorf("confirm-delete: plan %s unreadable, refusing: %w", planKey, err)
	}
	if err := plan.ValidateCreate(); err != nil {
		return nil, &DeniedError{Reason: ReasonPlanMalformed,
			Msg: fmt.Sprintf("confirm-delete: live plan invalid: %v", err)}
	}
	return &plan, nil
}

// budgetUsed reads this track's charged deletions for the live
// epoch; any other epoch (or a missing entry/stanza) is zero.
func (c *GuardedClient) budgetUsed(plan *v1alpha1.ShardPlan) int32 {
	for i := range plan.Status.Tracks {
		e := &plan.Status.Tracks[i]
		if e.Name != c.gate.track {
			continue
		}
		if e.Budget != nil && e.Budget.Epoch == plan.Spec.Epoch {
			return e.Budget.DeletionsUsed
		}
		return 0
	}
	return 0
}

// ownedNamespaceCount counts namespaces whose evaluated owner is
// this track, from a direct list (never cache: FA5.2).
func (c *GuardedClient) ownedNamespaceCount(ctx context.Context, plan *v1alpha1.ShardPlan) (int, error) {
	pspec, err := PartitionSpec(plan)
	if err != nil {
		return 0, &DeniedError{Reason: ReasonPlanMalformed,
			Msg: fmt.Sprintf("confirm-delete: partition spec: %v", err)}
	}
	var list corev1.NamespaceList
	if err := c.reader.List(ctx, &list); err != nil {
		return 0, fmt.Errorf("confirm-delete: namespace list failed, refusing: %w", err)
	}
	want := partition.Stable
	if c.gate.track == v1alpha1.TrackCanary {
		want = partition.Canary
	}
	n := 0
	for i := range list.Items {
		ns := &list.Items[i]
		owner, err := pspec.Owner(ns.Name, ns.Labels)
		if err != nil {
			return 0, &DeniedError{Reason: ReasonPlanMalformed,
				Msg: fmt.Sprintf("confirm-delete: owner(%s): %v", ns.Name, err)}
		}
		if owner == want {
			n++
		}
	}
	return n, nil
}

// chargeBudget persists used+1 deletions for this epoch into the
// track's own entry, preserving every other field and retrying RV
// conflicts (single writer per entry, FA5.1).
// writeCharge persists used deletions for the live object's epoch
// into the track's own entry, preserving every other field. Only
// the RV conflict signal returns; callers decide retries.
func (c *GuardedClient) writeCharge(ctx context.Context, planKey types.NamespacedName, live *v1alpha1.ShardPlan, used int32) error {
	entry := trackEntry(live, c.gate.track)
	entry.Budget = &v1alpha1.BudgetUsage{Epoch: live.Spec.Epoch, DeletionsUsed: used}
	var sopts []client.SubResourceUpdateOption
	if c.shadowing() {
		sopts = append(sopts, client.DryRunAll)
	}
	err := c.Client.Status().Update(ctx, live, sopts...)
	if c.shadowing() {
		decision, reason := writeOutcome(err)
		c.noteShadow("budget-charge", "shardplans", planKey.Namespace+"/"+planKey.Name, decision, reason)
	}
	if err == nil {
		return nil
	}
	if apierrors.IsConflict(err) {
		return err
	}
	return fmt.Errorf("confirm-delete: charge failed: %w", err)
}

// trackEntry returns the track's status entry, appending a
// budget-only entry when none exists yet. A bare entry binds
// nothing (empty planUID never validates), so later ack publishes
// adopt it via the budget preservation rule.
func trackEntry(plan *v1alpha1.ShardPlan, track string) *v1alpha1.TrackStatus {
	for i := range plan.Status.Tracks {
		if plan.Status.Tracks[i].Name == track {
			return &plan.Status.Tracks[i]
		}
	}
	plan.Status.Tracks = append(plan.Status.Tracks, v1alpha1.TrackStatus{Name: track})
	return &plan.Status.Tracks[len(plan.Status.Tracks)-1]
}

// deleteUID deletes with a UID precondition through the guarded
// path (re-authorized at the API boundary).
func (c *GuardedClient) deleteUID(ctx context.Context, key, namespace, name string, obj client.Object, uid string, opts []client.DeleteOption) error {
	do := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(do)
	}
	u := types.UID(uid)
	do.Preconditions = &metav1.Preconditions{UID: &u}
	if c.shadowing() {
		do.DryRun = []string{metav1.DryRunAll}
	}
	return c.guardDelete(ctx, obj, namespace, name, "delete", func() error {
		return c.Client.Delete(ctx, obj, do)
	})
}
