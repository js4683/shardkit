package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/js4683/shardkit/pkg/partition"
	"github.com/js4683/shardkit/pkg/shardkit"
)

const explainUsage = `explain: show why NAMESPACE is owned by its track.

Usage:
  kubectl shardplan explain PLAN NAMESPACE [-n NS]
`

func parseExplain(args []string, stdout, stderr io.Writer) (execFunc, bool) {
	if wantHelp(args) {
		fmt.Fprint(stdout, explainUsage)
		return okExec, true
	}
	pos, _, _, ok := parseFlags("explain", args, stderr, nil, nil)
	if !ok {
		return nil, false
	}
	if len(pos) != 2 {
		fmt.Fprintln(stderr, "error: explain requires a PLAN and a NAMESPACE name")
		fmt.Fprint(stderr, explainUsage)
		return nil, false
	}
	plan, ns := pos[0], pos[1]
	return func(ctx context.Context, c client.Client, namespace string, stdout, stderr io.Writer) int {
		return execExplain(ctx, c, namespace, plan, ns, stdout, stderr)
	}, true
}

func execExplain(ctx context.Context, c client.Client, namespace, planName, nsName string, stdout, stderr io.Writer) int {
	plan, ok := getPlan(ctx, c, namespace, planName, stderr)
	if !ok {
		return 1
	}
	ns, ok := getNamespace(ctx, c, nsName, stderr)
	if !ok {
		return 1
	}
	spec, err := shardkit.PartitionSpec(plan)
	if err != nil {
		fmt.Fprintf(stderr, "error: plan %q does not map to an ownership model: %v\n", planName, err)
		return 1
	}
	exp, err := spec.Explain(ns.Name, ns.Labels)
	if err != nil {
		fmt.Fprintf(stderr, "error: cannot evaluate %q: %v\n", nsName, err)
		return 1
	}
	fmt.Fprintf(stdout, "%s -> %s (rule: %s)\n", nsName, exp.Owner, exp.Rule)
	fmt.Fprintf(stdout, "  mode %s, seed %q (offset %d), weight %d\n",
		plan.Spec.Canary.Mode, plan.Spec.Seed, exp.Offset, plan.Spec.Canary.WeightPerMille)
	fmt.Fprintf(stdout, "  labels: %s\n", formatLabels(ns.Labels))
	switch exp.Rule {
	case "mode":
		fmt.Fprintf(stdout, "  mode %s assigns every namespace to stable\n", plan.Spec.Canary.Mode)
	case "exclude":
		fmt.Fprintf(stdout, "  matches exclude matchLabels %s\n", formatExcludes(plan))
	case "include":
		fmt.Fprintf(stdout, "  explicitly included in the canary cohort\n")
	case "window":
		fmt.Fprintf(stdout, "  bucket(%s) = %d, window %s %s\n", nsName, exp.Bucket,
			formatWindow(exp.Offset, int(plan.Spec.Canary.WeightPerMille)),
			windowVerdict(exp, int(plan.Spec.Canary.WeightPerMille)))
	}
	return 0
}

// getNamespace preserves API errors so missing objects and read failures differ.
func getNamespace(ctx context.Context, c client.Client, nsName string, stderr io.Writer) (*corev1.Namespace, bool) {
	var ns corev1.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: nsName}, &ns); err != nil {
		if errors.IsNotFound(err) {
			fmt.Fprintf(stderr, "error: namespace %q not found\n", nsName)
		} else {
			fmt.Fprintf(stderr, "error: cannot read namespace %q: %v\n", nsName, err)
		}
		return nil, false
	}
	return &ns, true
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, k+"="+labels[k])
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

// formatWindow renders the ownership window [offset, offset+w) modulo
// the bucket count, splitting wraparound ranges.
func formatWindow(offset, w int) string {
	if w <= 0 {
		return "empty (weight 0)"
	}
	end := offset + w
	if end <= partition.Buckets {
		return fmt.Sprintf("[%d, %d)", offset, end)
	}
	return fmt.Sprintf("[%d, %d) U [0, %d)", offset, partition.Buckets, end-partition.Buckets)
}

func windowVerdict(exp partition.Explanation, w int) string {
	if w <= 0 {
		return "covers nothing"
	}
	end := exp.Offset + w
	hit := exp.Bucket >= exp.Offset && exp.Bucket < end ||
		end > partition.Buckets && exp.Bucket < end-partition.Buckets
	if hit {
		return "covers it"
	}
	return "misses it"
}

// namespaceOwner is the pure simulation kernel: hypothetical spec
// plus namespace names/labels in, owners out. No cluster contact.
func namespaceOwner(spec partition.Spec, name string, labels map[string]string) (partition.Owner, string, error) {
	exp, err := spec.Explain(name, labels)
	if err != nil {
		return partition.Stable, "", err
	}
	return exp.Owner, exp.Rule, nil
}
