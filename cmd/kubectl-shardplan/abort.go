package main

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

const abortUsage = `abort: return everything to stable (mode Off, weight 0) as a new epoch.

Usage:
  kubectl shardplan abort PLAN [--epoch E] [-n NS]

Flags:
  --epoch E   explicit epoch, must exceed the live epoch (default: live + 1)

Aborting an already-aborted plan is a no-op, so recovery scripts can
run abort unconditionally. A mid-flight abort is also a no-op: the
transition already running is the abort.
Examples:
  kubectl shardplan abort widget-operator -n widget-system
`

type abortOpts struct {
	plan     string
	epoch    int64
	epochSet bool
}

func parseAbort(args []string, stdout, stderr io.Writer) (execFunc, bool) {
	if wantHelp(args) {
		fmt.Fprint(stdout, abortUsage)
		return okExec, true
	}
	pos, vals, _, ok := parseFlags("abort", args, stderr, []string{"--epoch"}, nil)
	if !ok {
		return nil, false
	}
	if len(pos) != 1 {
		fmt.Fprintln(stderr, "error: abort requires exactly one PLAN name")
		fmt.Fprint(stderr, abortUsage)
		return nil, false
	}
	var o abortOpts
	o.plan = pos[0]
	if raw, set := vals["--epoch"]; set {
		e, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || e < 0 {
			fmt.Fprintf(stderr, "error: --epoch %q is not a non-negative number\n", raw)
			fmt.Fprint(stderr, abortUsage)
			return nil, false
		}
		o.epoch, o.epochSet = e, true
	}
	return func(ctx context.Context, c client.Client, namespace string, stdout, stderr io.Writer) int {
		return execAbort(ctx, c, namespace, o, stdout, stderr)
	}, true
}

func execAbort(ctx context.Context, c client.Client, namespace string, o abortOpts, stdout, stderr io.Writer) int {
	key := types.NamespacedName{Namespace: namespace, Name: o.plan}
	m := specMutation{weight: 0, mode: v1alpha1.ModeOff, epoch: o.epoch, epochAuto: !o.epochSet}
	old, next, ok := applySpecUpdate(ctx, c, key, m, o.plan, namespace, stderr)
	if !ok {
		return 1
	}
	if old == nil {
		fmt.Fprintf(stdout, "abort %s/%s: already mode Off weight 0 (no-op)\n", namespace, o.plan)
		return 0
	}
	fmt.Fprintf(stdout, "aborted %s/%s: mode Off, weight 0, epoch %d; stable reclaims all namespaces\n",
		namespace, o.plan, next.Spec.Epoch)
	fmt.Fprintf(stdout, "watch: kubectl shardplan status %s -n %s\n", o.plan, namespace)
	return 0
}
