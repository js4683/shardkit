package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/partition"
	"github.com/js4683/shardkit/pkg/shardkit"
)

const simulateUsage = `simulate: report the ownership distribution for the live plan, or for
a hypothetical mode/weight/seed, without writing anything to the cluster.

Usage:
  kubectl shardplan simulate PLAN [--weight N] [--mode M] [--seed S]
      [--namespaces a,b] [-q] [-n NS]

Flags:
  --weight N           hypothetical weight 0..1000 (default: live weight)
  --mode M             hypothetical mode Off, Shadow, or Active (default: live mode)
  --seed S             hypothetical seed (default: live seed)
  --namespaces a,b     restrict to these namespaces (default: all namespaces)
  -q, --quiet          counts only, no per-namespace table

Cohort rules (include/exclude) always come from the live plan.
Examples:
  kubectl shardplan simulate widget-operator -n widget-system
  kubectl shardplan simulate widget-operator --weight 100 -n widget-system
  kubectl shardplan simulate widget-operator --weight 1000 -q -n widget-system
`

// maxSimRows caps the per-namespace table; counts always cover the
// full set and the output says how many rows were cut.
const maxSimRows = 100

type simulateOpts struct {
	plan       string
	weight     int
	mode       string
	seed       string
	namespaces []string
	quiet      bool
}

func parseSimulate(args []string, stdout, stderr io.Writer) (execFunc, bool) {
	if wantHelp(args) {
		fmt.Fprint(stdout, simulateUsage)
		return okExec, true
	}
	pos, vals, bools, ok := parseFlags("simulate", args, stderr,
		[]string{"--weight", "--mode", "--seed", "--namespaces"}, []string{"-q", "--quiet"})
	if !ok {
		return nil, false
	}
	if len(pos) != 1 {
		fmt.Fprintln(stderr, "error: simulate requires exactly one PLAN name")
		fmt.Fprint(stderr, simulateUsage)
		return nil, false
	}
	var o simulateOpts
	o.plan = pos[0]
	o.weight = -1 // -1 keeps the live weight
	if raw, set := vals["--weight"]; set {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > partition.MaxWeight {
			fmt.Fprintf(stderr, "error: --weight %q is not a number 0..%d\n", raw, partition.MaxWeight)
			fmt.Fprint(stderr, simulateUsage)
			return nil, false
		}
		o.weight = n
	}
	o.mode, o.seed = vals["--mode"], vals["--seed"]
	if o.mode != "" && o.mode != v1alpha1.ModeOff && o.mode != v1alpha1.ModeShadow && o.mode != v1alpha1.ModeActive {
		fmt.Fprintf(stderr, "error: --mode %q invalid: Off, Shadow, or Active\n", o.mode)
		fmt.Fprint(stderr, simulateUsage)
		return nil, false
	}
	if raw := vals["--namespaces"]; raw != "" {
		for _, n := range strings.Split(raw, ",") {
			if n = strings.TrimSpace(n); n != "" {
				o.namespaces = append(o.namespaces, n)
			}
		}
		if len(o.namespaces) == 0 {
			fmt.Fprintln(stderr, "error: --namespaces names nothing; drop the flag for all namespaces")
			return nil, false
		}
	}
	o.quiet = bools["-q"] || bools["--quiet"]
	return func(ctx context.Context, c client.Client, namespace string, stdout, stderr io.Writer) int {
		return execSimulate(ctx, c, namespace, o, stdout, stderr)
	}, true
}

func execSimulate(ctx context.Context, c client.Client, namespace string, o simulateOpts, stdout, stderr io.Writer) int {
	plan, ok := getPlan(ctx, c, namespace, o.plan, stderr)
	if !ok {
		return 1
	}
	live, err := shardkit.PartitionSpec(plan)
	if err != nil {
		fmt.Fprintf(stderr, "error: plan %q does not map to an ownership model: %v\n", o.plan, err)
		return 1
	}
	hypo := live
	if o.mode != "" {
		hypo.Mode = partitionMode(o.mode)
	}
	if o.weight >= 0 {
		hypo.WeightPerMille = o.weight
	}
	if o.seed != "" {
		hypo.Seed = o.seed
	}
	if err := hypo.Validate(); err != nil {
		hint := ""
		if o.mode == "" && o.weight >= 0 && live.Mode != partition.ModeActive {
			hint = "; pass --mode Active to preview an active rollout"
		} else if o.weight < 0 && o.mode != "" && hypo.Mode != partition.ModeActive && live.WeightPerMille > 0 {
			hint = "; pass --weight 0 to preview a non-active mode"
		}
		fmt.Fprintf(stderr, "error: hypothetical spec invalid: %v%s\n", err, hint)
		return 1
	}
	items, ok := simNamespaces(ctx, c, o.namespaces, stderr)
	if !ok {
		return 1
	}
	type row struct {
		name  string
		owner partition.Owner
		rule  string
	}
	var rows []row
	counts := map[partition.Owner]int{}
	for _, ns := range items {
		owner, rule, err := namespaceOwner(hypo, ns.Name, ns.Labels)
		if err != nil {
			fmt.Fprintf(stderr, "error: cannot evaluate %q: %v\n", ns.Name, err)
			return 1
		}
		rows = append(rows, row{ns.Name, owner, rule})
		counts[owner]++
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	fmt.Fprintf(stdout, "simulate %s/%s: mode %s weight %d seed %q\n",
		namespace, o.plan, partitionModeName(hypo.Mode), hypo.WeightPerMille, hypo.Seed)
	if len(rows) == 0 {
		fmt.Fprintln(stdout, "no namespaces found (nothing to assign); no writes made")
		return 0
	}
	fmt.Fprintf(stdout, "stable: %d  canary: %d  (%d namespaces, no writes made)\n",
		counts[partition.Stable], counts[partition.Canary], len(rows))
	if o.quiet {
		return 0
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAMESPACE\tOWNER\tRULE")
	shown := rows
	truncated := 0
	if len(rows) > maxSimRows {
		shown, truncated = rows[:maxSimRows], len(rows)-maxSimRows
	}
	for _, r := range shown {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", r.name, r.owner, r.rule)
	}
	_ = tw.Flush()
	if truncated > 0 {
		fmt.Fprintf(stdout, "(%d more rows cut; re-run with -q for counts only)\n", truncated)
	}
	return 0
}

// simNamespaces resolves the simulation set with reads only: named
// namespaces by GET, or the whole cluster by LIST.
func simNamespaces(ctx context.Context, c client.Client, names []string, stderr io.Writer) ([]corev1.Namespace, bool) {
	if len(names) > 0 {
		var out []corev1.Namespace
		for _, n := range names {
			var ns corev1.Namespace
			if err := c.Get(ctx, types.NamespacedName{Name: n}, &ns); err != nil {
				fmt.Fprintf(stderr, "error: namespace %q not found\n", n)
				return nil, false
			}
			out = append(out, ns)
		}
		return out, true
	}
	var list corev1.NamespaceList
	if err := c.List(ctx, &list); err != nil {
		fmt.Fprintf(stderr, "error: cannot list namespaces: %v\n", err)
		return nil, false
	}
	return list.Items, true
}

func partitionMode(mode string) partition.Mode {
	switch mode {
	case v1alpha1.ModeShadow:
		return partition.ModeShadow
	case v1alpha1.ModeActive:
		return partition.ModeActive
	default:
		return partition.ModeOff
	}
}

func partitionModeName(m partition.Mode) string {
	switch m {
	case partition.ModeShadow:
		return v1alpha1.ModeShadow
	case partition.ModeActive:
		return v1alpha1.ModeActive
	default:
		return v1alpha1.ModeOff
	}
}
