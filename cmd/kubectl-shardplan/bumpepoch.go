package main

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const bumpEpochUsage = `bump-epoch: assign a new epoch with the identical spec, so the
handshake re-evaluates the current world.

Usage:
  kubectl shardplan bump-epoch PLAN [--epoch E] [-n NS]

Flags:
  --epoch E     explicit epoch, must exceed the live epoch (default: live + 1)

A lone epoch bump is a contracted no-op write (V9): no ownership
moves by itself, but every track re-lists, re-evaluates, and
re-acknowledges. Use it to recover LabelsDrifted/NotAcquired
denials after a routine relabel: the new version carries the
current labels through drain, ack, and barrier, which reopens the
namespace on the owning track. The update is read-modify-write
with conflict retries.
Example:
  kubectl shardplan bump-epoch widget-operator -n widget-system
`

type bumpEpochOpts struct {
	plan     string
	epoch    int64
	epochSet bool
}

func parseBumpEpoch(args []string, stdout, stderr io.Writer) (execFunc, bool) {
	if wantHelp(args) {
		fmt.Fprint(stdout, bumpEpochUsage)
		return okExec, true
	}
	pos, vals, _, ok := parseFlags("bump-epoch", args, stderr,
		[]string{"--epoch"}, nil)
	if !ok {
		return nil, false
	}
	if len(pos) != 1 {
		fmt.Fprintln(stderr, "error: bump-epoch requires exactly one PLAN name")
		fmt.Fprint(stderr, bumpEpochUsage)
		return nil, false
	}
	var o bumpEpochOpts
	o.plan = pos[0]
	if raw, set := vals["--epoch"]; set {
		e, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || e < 0 {
			fmt.Fprintf(stderr, "error: --epoch %q is not a non-negative number\n", raw)
			fmt.Fprint(stderr, bumpEpochUsage)
			return nil, false
		}
		o.epoch, o.epochSet = e, true
	}
	return func(ctx context.Context, c client.Client, namespace string, stdout, stderr io.Writer) int {
		return execBumpEpoch(ctx, c, namespace, o, stdout, stderr)
	}, true
}

func execBumpEpoch(ctx context.Context, c client.Client, namespace string, o bumpEpochOpts, stdout, stderr io.Writer) int {
	key := types.NamespacedName{Namespace: namespace, Name: o.plan}
	m := specMutation{epoch: o.epoch, epochAuto: !o.epochSet, keepSpec: true}
	old, newPlan, ok := applySpecUpdate(ctx, c, key, m, o.plan, namespace, stderr)
	if !ok {
		return 1
	}
	fmt.Fprintf(stdout, "set %s/%s: epoch %d -> %d (spec unchanged; namespaces re-evaluate through the handshake)\n",
		namespace, o.plan, old.Spec.Epoch, newPlan.Spec.Epoch)
	fmt.Fprintf(stdout, "watch: kubectl shardplan status %s -n %s\n", o.plan, namespace)
	return 0
}
