package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

const statusUsage = `status: show plan versions, per-track acks, and the handshake verdict.

Usage:
  kubectl shardplan status PLAN [-n NS]
`

// getPlan fetches the named plan or reports an actionable error.
func getPlan(ctx context.Context, c client.Client, namespace, name string, stderr io.Writer) (*v1alpha1.ShardPlan, bool) {
	var plan v1alpha1.ShardPlan
	key := types.NamespacedName{Namespace: namespace, Name: name}
	if err := c.Get(ctx, key, &plan); err != nil {
		if errors.IsNotFound(err) {
			fmt.Fprintf(stderr, "error: ShardPlan %q not found in namespace %q; create it first or check -n/--namespace\n", name, namespace)
		} else {
			fmt.Fprintf(stderr, "error: cannot read ShardPlan %q in namespace %q: %v\n", name, namespace, err)
		}
		return nil, false
	}
	if err := plan.ValidateCreate(); err != nil {
		fmt.Fprintf(stderr, "error: ShardPlan %q in namespace %q is invalid: %v; fix the spec (gates stay closed until a valid version lands)\n", name, namespace, err)
		return nil, false
	}
	return &plan, true
}

func parseStatus(args []string, stdout, stderr io.Writer) (execFunc, bool) {
	if wantHelp(args) {
		fmt.Fprint(stdout, statusUsage)
		return okExec, true
	}
	pos, _, _, ok := parseFlags("status", args, stderr, nil, nil)
	if !ok {
		return nil, false
	}
	if len(pos) != 1 {
		fmt.Fprintln(stderr, "error: status requires exactly one PLAN name")
		fmt.Fprint(stderr, statusUsage)
		return nil, false
	}
	name := pos[0]
	return func(ctx context.Context, c client.Client, namespace string, stdout, stderr io.Writer) int {
		return execStatus(ctx, c, namespace, name, stdout, stderr)
	}, true
}

func execStatus(ctx context.Context, c client.Client, namespace, name string, stdout, stderr io.Writer) int {
	plan, ok := getPlan(ctx, c, namespace, name, stderr)
	if !ok {
		return 1
	}
	printPlanHeader(stdout, namespace, name, plan)
	printTrackTable(stdout, plan)
	verdicts := verdictsFor(plan)
	if len(verdicts) == 0 {
		fmt.Fprintf(stdout, "verdict: in sync: both tracks ack epoch %d generation %d\n",
			plan.Spec.Epoch, plan.Generation)
		return 0
	}
	for _, v := range verdicts {
		fmt.Fprintf(stdout, "verdict: %s\n", v)
	}
	return 0
}

func printPlanHeader(stdout io.Writer, namespace, name string, plan *v1alpha1.ShardPlan) {
	fmt.Fprintf(stdout, "ShardPlan %s/%s\n", namespace, name)
	fmt.Fprintf(stdout, "  uid: %s\n", plan.UID)
	fmt.Fprintf(stdout, "  epoch: %d  generation: %d  rollout: %s  seed: %q  key: %s\n",
		plan.Spec.Epoch, plan.Generation, plan.Spec.Rollout, plan.Spec.Seed, plan.Spec.Key)
	fmt.Fprintf(stdout, "  mode: %s  weight: %d  singleton: %s\n",
		plan.Spec.Canary.Mode, plan.Spec.Canary.WeightPerMille, plan.Spec.SingletonOwner)
	fmt.Fprintf(stdout, "  include: %s  exclude: %s\n",
		formatIncludes(plan.Spec.Canary.Include.Namespaces),
		formatExcludes(plan))
}

func formatIncludes(ns []string) string {
	if len(ns) == 0 {
		return "none"
	}
	return "[" + strings.Join(ns, ",") + "]"
}

func formatExcludes(plan *v1alpha1.ShardPlan) string {
	sel := plan.Spec.Canary.Exclude.Selector
	if sel == nil {
		return "none"
	}
	return formatLabels(sel.MatchLabels)
}

func printTrackTable(stdout io.Writer, plan *v1alpha1.ShardPlan) {
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TRACK\tSPEC/ACKED REV\tEPOCH\tGEN\tPHASE\tRELEASED\tOWNED\tSESSION")
	tracks := []string{v1alpha1.TrackStable}
	if plan.Spec.Tracks.Canary != nil {
		tracks = append(tracks, v1alpha1.TrackCanary)
	}
	for _, track := range tracks {
		specRev := plan.Spec.Tracks.Stable.Revision
		if track == v1alpha1.TrackCanary {
			specRev = plan.Spec.Tracks.Canary.Revision
		}
		entry := trackEntry(plan, track)
		if entry == nil {
			fmt.Fprintf(tw, "%s\t%s/-\t-\t-\t-\t-\t-\t-\n", track, specRev)
			continue
		}
		rev := specRev + "/" + entry.Revision
		if entry.Revision != specRev {
			rev += "*"
		}
		session := "-"
		if entry.Session != nil {
			session = fmt.Sprintf("%s/%d", entry.Session.Holder, entry.Session.LeaseTransitions)
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%v\t%d\t%s\n",
			track, rev, entry.ObservedEpoch, entry.ObservedGeneration,
			entry.Phase, entry.Released, entry.OwnedNamespaces, session)
	}
	_ = tw.Flush()
	if plan.Spec.Tracks.Canary == nil {
		fmt.Fprintln(stdout, "canary: not deployed (spec.tracks.canary absent)")
	}
}

func trackEntry(plan *v1alpha1.ShardPlan, track string) *v1alpha1.TrackStatus {
	for i := range plan.Status.Tracks {
		if plan.Status.Tracks[i].Name == track {
			return &plan.Status.Tracks[i]
		}
	}
	return nil
}

// verdictsFor reports one actionable line per track problem, in
// evaluation order: missing, UID, epoch-ahead (incoherent), behind,
// revision, degraded, mid-transition. Empty means in sync. The canary
// side only binds when the spec deploys a canary revision.
func verdictsFor(plan *v1alpha1.ShardPlan) []string {
	var out []string
	tracks := []string{v1alpha1.TrackStable}
	if plan.Spec.Tracks.Canary != nil {
		tracks = append(tracks, v1alpha1.TrackCanary)
	}
	for _, track := range tracks {
		if v := verdictForTrack(plan, track); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func verdictForTrack(plan *v1alpha1.ShardPlan, track string) string {
	entry := trackEntry(plan, track)
	if entry == nil {
		return fmt.Sprintf("waiting: %s has no entry for epoch %d; check the %s Deployment's logs, then re-run status",
			track, plan.Spec.Epoch, track)
	}
	if entry.PlanUID != string(plan.UID) {
		if plan.Spec.Canary.Mode == v1alpha1.ModeOff {
			return fmt.Sprintf("waiting: %s still binds the previous plan UID; it adopts on its next poll, then re-run status", track)
		}
		return fmt.Sprintf("stale: %s binds plan UID %q, live plan is %q; drive the plan through Off (abort) so tracks adopt the recreated plan (B3)",
			track, entry.PlanUID, plan.UID)
	}
	if entry.ObservedEpoch > plan.Spec.Epoch {
		return fmt.Sprintf("incoherent: %s acked epoch %d, newer than plan epoch %d; epochs never decrease — investigate before writing",
			track, entry.ObservedEpoch, plan.Spec.Epoch)
	}
	if entry.ObservedEpoch < plan.Spec.Epoch || entry.ObservedGeneration != plan.Generation {
		return fmt.Sprintf("waiting: %s at epoch %d generation %d, plan at epoch %d generation %d; re-run status to watch (if stuck, check the %s operator's logs and leadership lease)",
			track, entry.ObservedEpoch, entry.ObservedGeneration,
			plan.Spec.Epoch, plan.Generation, track)
	}
	specRev := plan.Spec.Tracks.Stable.Revision
	if track == v1alpha1.TrackCanary && plan.Spec.Tracks.Canary != nil {
		specRev = plan.Spec.Tracks.Canary.Revision
	}
	if entry.Revision != specRev {
		return fmt.Sprintf("stale: %s runs revision %q, spec wants %q; the S7 binding voids its acks until the specified revision runs",
			track, entry.Revision, specRev)
	}
	for _, cond := range entry.Conditions {
		if cond.Type == "Degraded" && cond.Status == "True" {
			return fmt.Sprintf("degraded: %s: %s: %s; check the %s operator's logs, then re-run status",
				track, cond.Reason, cond.Message, track)
		}
	}
	if entry.Phase != v1alpha1.PhaseReleased && entry.Phase != v1alpha1.PhaseAcquired {
		return fmt.Sprintf("in progress: %s is %s on epoch %d; re-run status to watch",
			track, entry.Phase, plan.Spec.Epoch)
	}
	return ""
}
