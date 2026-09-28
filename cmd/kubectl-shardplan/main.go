// Command kubectl-shardplan is the manual CLI for ShardPlans: status,
// explain, simulate, set-weight, and abort. It works without Argo,
// so a plugin-API change degrades to manual steps rather than an
// outage. Installed on PATH as kubectl-shardplan, it runs as
// `kubectl shardplan`.
//
// Exit codes: 0 reports coherent truth (even mid-handoff, and
// including no-op mutations); 1 means the CLI failed, the plan is
// invalid, or the report is incoherent; 2 means the invocation
// itself was wrong (usage). Every failure names the resource and
// the next action.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// version is the release identity printed by the version command,
// stamped by make release; the -dev default means an unstamped
// local build.
var version = "v0.2.0-dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type globals struct {
	kubeconfig string
	context    string
	namespace  string
}

const usageText = `kubectl shardplan: manual ShardPlan operations.

Usage:
  kubectl shardplan [globals] <command> [args] [flags]

Commands:
  status PLAN                     plan versions, per-track acks, handshake verdict
  explain PLAN NAMESPACE          why NAMESPACE is owned by its track
  simulate PLAN [flags]           ownership distribution without cluster writes
  set-weight PLAN N [flags]       new canary weight (optimistic concurrency)
  abort PLAN [flags]              return everything to stable (mode Off, weight 0)

Globals (before or after the command):
  --kubeconfig PATH   kubeconfig file (default: standard resolution)
  --context NAME      kubeconfig context (default: current)
  -n, --namespace NS  plan namespace (default: context namespace)

Run 'kubectl shardplan <command> --help' for command flags.
`

// run dispatches one invocation and returns the process exit code.
// Argument errors surface before any cluster contact, so usage
// mistakes never read as connection failures.
func run(args []string, stdout, stderr io.Writer) int {
	g, rest := splitGlobals(args)
	if len(rest) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	cmd, cargs := rest[0], rest[1:]
	switch cmd {
	case "help", "-h", "--help", "-help":
		fmt.Fprint(stdout, usageText)
		return 0
	case "-v", "-V", "--version", "version":
		fmt.Fprintln(stdout, "kubectl-shardplan "+version)
		return 0
	case "status", "explain", "simulate", "set-weight", "abort":
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usageText)
		return 2
	}
	exec, ok := parseArgs(cmd, cargs, stdout, stderr)
	if !ok {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, ns, err := buildClient(g)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return exec(ctx, c, ns, stdout, stderr)
}

// execFunc runs a parsed command against the cluster.
type execFunc func(ctx context.Context, c client.Client, namespace string, stdout, stderr io.Writer) int

// parseArgs validates one command's arguments without touching the
// cluster. Parse failures report usage on stderr and return ok=false.
func parseArgs(cmd string, args []string, stdout, stderr io.Writer) (execFunc, bool) {
	switch cmd {
	case "status":
		return parseStatus(args, stdout, stderr)
	case "explain":
		return parseExplain(args, stdout, stderr)
	case "simulate":
		return parseSimulate(args, stdout, stderr)
	case "set-weight":
		return parseSetWeight(args, stdout, stderr)
	case "abort":
		return parseAbort(args, stdout, stderr)
	default:
		return nil, false
	}
}

// okExec is the no-op execution for --help: usage is already printed.
func okExec(ctx context.Context, c client.Client, namespace string, stdout, stderr io.Writer) int {
	return 0
}

// wantHelp reports whether args ask for command help.
func wantHelp(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "--help" || a == "-help" {
			return true
		}
	}
	return false
}

// splitGlobals pulls global flags out of args in any position,
// leaving the command and its args. Both "--flag value" and
// "--flag=value" forms work; "--" stops scanning.
func splitGlobals(args []string) (globals, []string) {
	var g globals
	var rest []string
	glob := map[string]bool{
		"--kubeconfig": true, "--context": true, "--namespace": true, "-n": true,
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			rest = append(rest, args[i+1:]...)
			break
		}
		name, value, joined := a, "", false
		if j := strings.IndexByte(a, '='); strings.HasPrefix(a, "-") && j > 0 {
			name, value, joined = a[:j], a[j+1:], true
		}
		if !glob[name] {
			rest = append(rest, a)
			continue
		}
		if !joined {
			i++
			if i >= len(args) {
				rest = append(rest, a)
				break
			}
			value = args[i]
		}
		switch name {
		case "--kubeconfig":
			g.kubeconfig = value
		case "--context":
			g.context = value
		case "--namespace", "-n":
			g.namespace = value
		}
	}
	return g, rest
}

// parseFlags splits args into positionals and flags, allowing flags
// anywhere (kubectl style). values names value flags (--flag v or
// --flag=v); singles names bool flags. A value flag consumes the
// next token literally, even when dash-led. Unknown flags, missing
// values, and malformed bools fail with the valid set inline so the
// caller self-corrects without a help round-trip.
func parseFlags(cmd string, args []string, stderr io.Writer, values, singles []string) (pos []string, vals map[string]string, bools map[string]bool, ok bool) {
	vals, bools = map[string]string{}, map[string]bool{}
	isValue, isSingle := map[string]bool{}, map[string]bool{}
	for _, v := range values {
		isValue[v] = true
	}
	for _, s := range singles {
		isSingle[s] = true
	}
	valid := strings.Join(append(append([]string{}, values...), singles...), ", ")
	fail := func(format string, a ...any) ([]string, map[string]string, map[string]bool, bool) {
		fmt.Fprintf(stderr, "error: "+format+"\n", a...)
		fmt.Fprintf(stderr, "valid flags for `%s`: %s (--help always allowed)\n", cmd, valid)
		return nil, nil, nil, false
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pos = append(pos, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			pos = append(pos, a)
			continue
		}
		name, value, joined := a, "", false
		if j := strings.IndexByte(a, '='); j > 0 {
			name, value, joined = a[:j], a[j+1:], true
		}
		if isValue[name] {
			if !joined {
				i++
				if i >= len(args) {
					return fail("%s needs a value", name)
				}
				value = args[i]
			}
			vals[name] = value
			continue
		}
		if isSingle[name] {
			if !joined {
				bools[name] = true
				continue
			}
			switch strings.ToLower(value) {
			case "true", "1":
				bools[name] = true
			case "false", "0":
				bools[name] = false
			default:
				return fail("%s takes true or false, got %q", name, value)
			}
			continue
		}
		return fail("unknown flag %s for `%s`", name, cmd)
	}
	return pos, vals, bools, true
}
