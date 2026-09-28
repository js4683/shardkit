// Command argo-shardplan-plugin is the M2 Argo Rollouts
// traffic-router plugin: it serves the pinned TrafficRouterPlugin
// interface (github.com/argoproj/argo-rollouts v1.10.0) by writing
// ShardPlan specs, so analysis-gated weight steps and aborts flow
// through the acknowledged handoff. Method-by-method contract:
// docs/argo-plugin.md.
package main

import (
	"flag"
	"fmt"
	"os"

	goPlugin "github.com/hashicorp/go-plugin"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rolloutsPlugin "github.com/argoproj/argo-rollouts/rollout/trafficrouting/plugin/rpc"
	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// handshakeConfig mirrors the upstream plugin sample: UX guard,
// not a security feature.
var handshakeConfig = goPlugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "ARGO_ROLLOUTS_RPC_PLUGIN",
	MagicCookieValue: "trafficrouter",
}

func main() {
	var kubeconfig string
	flag.StringVar(&kubeconfig, "kubeconfig", "",
		"Path to a kubeconfig (default: standard resolution, then in-cluster).")
	flag.Parse()

	// Direct (non-cached) client: VerifyWeight must read live acks,
	// never a stale cache.
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		exit(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		exit(err)
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loading.ExplicitPath = kubeconfig
	}
	rest, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loading, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		exit(fmt.Errorf("kubeconfig: %w", err))
	}
	k8s, err := client.New(rest, client.Options{Scheme: scheme})
	if err != nil {
		exit(err)
	}
	impl := &Plugin{Client: k8s}
	var pluginMap = map[string]goPlugin.Plugin{
		"RpcTrafficRouterPlugin": &rolloutsPlugin.RpcTrafficRouterPlugin{Impl: impl},
	}
	goPlugin.Serve(&goPlugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, "argo-shardplan-plugin:", err)
	os.Exit(1)
}
