package main

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

// WidgetReconciler marks owned widgets with the writing track. Write
// path: entry check, optional delay (to straddle a handoff for the
// D3 experiment), optional guard re-check, then a status write only
// if the stamp would change. Every decision logs one line for the
// trace audit.
//
// All writes go through the guarded client, which re-checks
// ownership at the API boundary: a reconcile that straddles a
// handoff is denied rather than fenced by cancellation. Reconciles
// stay unscoped on purpose — the D3 delayed-write experiment needs
// its sleep to survive the drain — so WRITE_DELAY still straddles
// flips exactly as in M0.
type WidgetReconciler struct {
	Client      *shardkit.GuardedClient
	Gate        *shardkit.Gate
	Track       string
	Revision    string
	WriteDelay  time.Duration
	GuardWrites bool
	// Chaos, when "mass-delete", makes Reconcile delete the widget
	// and report an error instead of stamping: the M2 demo's
	// injected bug (trips the error-rate analysis, which aborts
	// the rollout). Empty disables it. Example-only: never set on
	// a real track.
	Chaos string
	// PlanKey locates the ShardPlan for confirmed deletes (M3
	// budgets): the chaos path charges V11 caps before deleting.
	PlanKey types.NamespacedName
}

// Reconcile implements reconcile.Reconciler.
func (r *WidgetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("track", r.Track, "widget", req.String())
	var w v1alpha1.Widget
	if err := r.Client.Get(ctx, req.NamespacedName, &w); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if r.Chaos == "mass-delete" {
		l.Info("CHAOS mass-delete", "widget", req.String())
		// Confirmed delete (M3): direct-read confirmation, V11
		// budget charge, UID precondition. A budget trip denies
		// here with BudgetExceeded while the error below still
		// trips the error-rate analysis — budgets bound the
		// blast radius, they don't silence the alarm.
		if err := r.Client.ConfirmDelete(ctx, r.PlanKey, &w); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		return ctrl.Result{}, fmt.Errorf("chaos mass-delete of %s", req.String())
	}
	obs, err := r.Gate.Owned(ctx, w.Namespace)
	if err != nil {
		l.Info("SKIP gate-closed", "reason", err.Error())
		return ctrl.Result{}, nil
	}
	if !obs.Owned {
		l.Info("SKIP not-owned", "epoch", obs.Epoch, "mode", obs.Mode)
		return ctrl.Result{}, nil
	}
	if r.WriteDelay > 0 {
		select {
		case <-ctx.Done():
			return ctrl.Result{}, ctx.Err()
		case <-time.After(r.WriteDelay):
		}
	}
	if r.GuardWrites {
		obs2, err := r.Gate.Owned(ctx, w.Namespace)
		if err != nil || !obs2.Owned {
			l.Info("SKIP guarded-after-delay", "epoch", obs.Epoch)
			return ctrl.Result{}, nil
		}
		obs = obs2
	}
	wrote, err := r.markOwned(ctx, &w)
	if err != nil {
		l.Error(err, "write failed")
		return ctrl.Result{}, err
	}
	if !wrote {
		l.Info("SKIP already-stamped", "epoch", obs.Epoch, "mode", obs.Mode)
		return ctrl.Result{}, nil
	}
	l.Info("WRITE", "epoch", obs.Epoch, "mode", obs.Mode,
		"revision", r.Revision)
	return ctrl.Result{}, nil
}

// markOwned stamps the widget status with this track's identity, but
// only when the stamp would change: an already-correct stamp is left
// untouched. The controller watches widgets, so every status write
// re-triggers a reconcile; an unconditional write is a
// self-sustaining hot loop (first M0-06 trace: >400k writes per
// widget, 10 MB logs per phase, conflict backoff starving ownership
// convergence). Returns whether a write was performed, retrying
// conflicts after a fresh read.
func (r *WidgetReconciler) markOwned(ctx context.Context, w *v1alpha1.Widget) (bool, error) {
	for attempt := 0; attempt < 3; attempt++ {
		var fresh v1alpha1.Widget
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(w), &fresh); err != nil {
			return false, err
		}
		if fresh.Status.OwnerTrack == r.Track && fresh.Status.OwnerRevision == r.Revision {
			return false, nil
		}
		fresh.Status.OwnerTrack = r.Track
		fresh.Status.OwnerRevision = r.Revision
		fresh.Status.Writes = 1
		if err := r.Client.Status().Update(ctx, &fresh); err != nil {
			if errors.IsConflict(err) {
				continue
			}
			return false, err
		}
		return true, nil
	}
	return false, errors.NewConflict(v1alpha1.GroupVersion.WithResource("widgets").GroupResource(),
		w.Name, nil)
}
