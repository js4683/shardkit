package main

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/partition"
)

const setWeightUsage = `set-weight: move the plan to a new canary weight as a new epoch.

Usage:
  kubectl shardplan set-weight PLAN N [--mode M] [--epoch E] [--rollout R] [-n NS]

Flags:
  --mode M      Off, Shadow, or Active (default: keep the live mode)
  --epoch E     explicit epoch, must exceed the live epoch (default: live + 1)
  --rollout R   explicit rollout ID (default: keep the live rollout)

N is 0..1000. The update is read-modify-write with conflict retries;
a concurrent writer loses nothing and the CLI says to re-run.
Setting the live weight and mode is a no-op, not a new epoch.
Examples:
  kubectl shardplan set-weight widget-operator 100 --mode Active -n widget-system
  kubectl shardplan set-weight widget-operator 0 -n widget-system
`

// updateAttempts bounds optimistic-concurrency retries.
const updateAttempts = 5

type setWeightOpts struct {
	plan     string
	weight   int32
	mode     string
	epoch    int64
	epochSet bool
	rollout  string
}

func parseSetWeight(args []string, stdout, stderr io.Writer) (execFunc, bool) {
	if wantHelp(args) {
		fmt.Fprint(stdout, setWeightUsage)
		return okExec, true
	}
	pos, vals, _, ok := parseFlags("set-weight", args, stderr,
		[]string{"--mode", "--epoch", "--rollout"}, nil)
	if !ok {
		return nil, false
	}
	if len(pos) != 2 {
		fmt.Fprintln(stderr, "error: set-weight requires a PLAN name and a weight N (0..1000)")
		fmt.Fprint(stderr, setWeightUsage)
		return nil, false
	}
	var o setWeightOpts
	o.plan = pos[0]
	n, err := strconv.Atoi(pos[1])
	if err != nil || n < 0 || n > partition.MaxWeight {
		fmt.Fprintf(stderr, "error: weight %q is not a number 0..%d\n", pos[1], partition.MaxWeight)
		fmt.Fprint(stderr, setWeightUsage)
		return nil, false
	}
	o.weight = int32(n)
	o.mode, o.rollout = vals["--mode"], vals["--rollout"]
	if raw, set := vals["--epoch"]; set {
		e, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || e < 0 {
			fmt.Fprintf(stderr, "error: --epoch %q is not a non-negative number\n", raw)
			fmt.Fprint(stderr, setWeightUsage)
			return nil, false
		}
		o.epoch, o.epochSet = e, true
	}
	if o.mode != "" && o.mode != v1alpha1.ModeOff && o.mode != v1alpha1.ModeShadow && o.mode != v1alpha1.ModeActive {
		fmt.Fprintf(stderr, "error: --mode %q invalid: Off, Shadow, or Active\n", o.mode)
		fmt.Fprint(stderr, setWeightUsage)
		return nil, false
	}
	return func(ctx context.Context, c client.Client, namespace string, stdout, stderr io.Writer) int {
		return execSetWeight(ctx, c, namespace, o, stdout, stderr)
	}, true
}

func execSetWeight(ctx context.Context, c client.Client, namespace string, o setWeightOpts, stdout, stderr io.Writer) int {
	key := types.NamespacedName{Namespace: namespace, Name: o.plan}
	m := specMutation{weight: o.weight, mode: o.mode, epoch: o.epoch, epochAuto: !o.epochSet, rollout: o.rollout}
	old, newPlan, ok := applySpecUpdate(ctx, c, key, m, o.plan, namespace, stderr)
	if !ok {
		return 1
	}
	if old == nil {
		fmt.Fprintf(stdout, "set %s/%s: already weight %d mode %s (no-op)\n",
			namespace, o.plan, o.weight, modeOrLive(o.mode, newPlan))
		return 0
	}
	fmt.Fprintf(stdout, "set %s/%s: weight %d -> %d, mode %s, epoch %d\n",
		namespace, o.plan, old.Spec.Canary.WeightPerMille, o.weight,
		newPlan.Spec.Canary.Mode, newPlan.Spec.Epoch)
	if o.weight == 0 && len(newPlan.Spec.Canary.Include.Namespaces) > 0 {
		fmt.Fprintf(stderr, "warning: include list still routes %s to canary; use abort to return everything to stable\n",
			formatIncludes(newPlan.Spec.Canary.Include.Namespaces))
	}
	fmt.Fprintf(stdout, "watch: kubectl shardplan status %s -n %s\n", o.plan, namespace)
	return 0
}

func modeOrLive(mode string, plan *v1alpha1.ShardPlan) string {
	if mode != "" {
		return mode
	}
	return plan.Spec.Canary.Mode
}

// specMutation describes one spec transition. Zero epoch with
// epochAuto assigns live+1; explicit epochs must exceed live.
// keepSpec writes live+epoch only (bump-epoch): it skips the no-op
// check and never clobbers a concurrently written spec, because the
// write derives from the freshly read live object.
type specMutation struct {
	weight    int32
	mode      string
	epoch     int64
	epochAuto bool
	rollout   string
	keepSpec  bool
}

// applySpecUpdate runs one optimistic-concurrency spec update: read,
// validate client-side, write, retry on RV conflict. Success returns
// the old and new plans; a no-op (spec already matches) returns a
// nil old plan with the live plan. Any failure reports the action
// and returns ok=false.
func applySpecUpdate(ctx context.Context, c client.Client, key types.NamespacedName, m specMutation, planName, namespace string, stderr io.Writer) (old, newPlan *v1alpha1.ShardPlan, ok bool) {
	for attempt := 1; ; attempt++ {
		var live v1alpha1.ShardPlan
		if err := c.Get(ctx, key, &live); err != nil {
			if errors.IsNotFound(err) {
				fmt.Fprintf(stderr, "error: ShardPlan %q not found in namespace %q; create it first or check -n/--namespace\n", planName, namespace)
			} else {
				fmt.Fprintf(stderr, "error: cannot read ShardPlan %q: %v\n", planName, err)
			}
			return nil, nil, false
		}
		if err := live.ValidateCreate(); err != nil {
			fmt.Fprintf(stderr, "error: ShardPlan %q is invalid: %v; fix the spec before writing\n", planName, err)
			return nil, nil, false
		}
		mode := live.Spec.Canary.Mode
		if m.mode != "" {
			mode = m.mode
		}
		epoch := m.epoch
		if m.epochAuto {
			epoch = live.Spec.Epoch + 1
		}
		if epoch <= live.Spec.Epoch {
			fmt.Fprintf(stderr, "error: epoch %d must exceed live epoch %d (epochs strictly increase); re-run without --epoch to auto-assign %d\n",
				epoch, live.Spec.Epoch, live.Spec.Epoch+1)
			return nil, nil, false
		}
		rollout := live.Spec.Rollout
		if m.rollout != "" {
			rollout = m.rollout
		}
		if mode != v1alpha1.ModeActive && m.weight > 0 {
			fmt.Fprintf(stderr, "error: weight %d needs mode Active (plan mode is %s); pass --mode Active to start a rollout\n", m.weight, live.Spec.Canary.Mode)
			return nil, nil, false
		}
		if mode == live.Spec.Canary.Mode && m.weight == live.Spec.Canary.WeightPerMille && rollout == live.Spec.Rollout {
			if !m.keepSpec {
				return nil, &live, true // desired state already holds
			}
		}
		next := live.DeepCopy()
		if m.keepSpec {
			next.Spec.Epoch = epoch // lone bump: spec untouched
		} else {
			next.Spec.Canary.Mode = mode
			next.Spec.Canary.WeightPerMille = m.weight
			next.Spec.Epoch = epoch
			next.Spec.Rollout = rollout
		}
		if err := next.ValidateUpdate(&live); err != nil {
			fmt.Fprintf(stderr, "error: update rejected: %v; fix and re-run\n", err)
			return nil, nil, false
		}
		if err := c.Update(ctx, next); err != nil {
			if errors.IsConflict(err) && attempt < updateAttempts {
				continue
			}
			if errors.IsConflict(err) {
				fmt.Fprintf(stderr, "error: plan changed concurrently (%d attempts); re-run: kubectl shardplan set-weight %s %d -n %s%s%s\n",
					updateAttempts, planName, m.weight, namespace, modeFlag(m.mode), rolloutFlag(m.rollout))
				return nil, nil, false
			}
			fmt.Fprintf(stderr, "error: update rejected by the server: %v; fix and re-run\n", err)
			return nil, nil, false
		}
		return &live, next, true
	}
}

func modeFlag(mode string) string {
	if mode == "" {
		return ""
	}
	return " --mode " + mode
}

func rolloutFlag(rollout string) string {
	if rollout == "" {
		return ""
	}
	return " --rollout " + rollout
}
