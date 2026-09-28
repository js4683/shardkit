package main

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// buildClient loads kubeconfig (explicit path, KUBECONFIG, default,
// or in-cluster), resolves the plan namespace (explicit -n wins over
// the context namespace), and returns a client for core plus
// ShardPlan types.
func buildClient(g globals) (client.Client, string, error) {
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	if g.kubeconfig != "" {
		loading.ExplicitPath = g.kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if g.context != "" {
		overrides.CurrentContext = g.context
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides)
	restCfg, err := loader.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("cannot load kubeconfig: %w (use --kubeconfig/--context or check KUBECONFIG)", err)
	}
	ns := g.namespace
	if ns == "" {
		ns, _, err = loader.Namespace()
		if err != nil {
			return nil, "", fmt.Errorf("cannot resolve namespace: %w (pass -n/--namespace)", err)
		}
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, "", fmt.Errorf("core scheme: %w", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return nil, "", fmt.Errorf("shardkit scheme: %w", err)
	}
	c, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, "", fmt.Errorf("cannot build client: %w", err)
	}
	return c, ns, nil
}
