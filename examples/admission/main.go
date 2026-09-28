// Command admission is the M4 admission example: a minimal manager
// that serves pkg/shardkit.ShardPlanValidator on the webhook
// server. It enforces exactly what CEL cannot (V9 frozen-tuple /
// epoch-bump comparison); everything else is already server-side.
// See docs/webhook.md for the limits that keep this optional and
// examples/admission/README.md for certs and install.
//
// It runs nothing else: no reconcilers, no leader election. Serve
// it behind the Service in config/admission.yaml.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

func main() {
	var certDir string
	flag.StringVar(&certDir, "webhook-cert-dir",
		envOr("WEBHOOK_CERT_DIR", "/tmp/k8s-webhook-server/serving-certs"),
		"Directory holding tls.crt and tls.key for the webhook server.")
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		exit(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		exit(err)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    9443,
			CertDir: certDir,
		}),
		LeaderElection: false,
	})
	if err != nil {
		exit(err)
	}

	mgr.GetWebhookServer().Register("/validate-shardkit-dev-v1alpha1-shardplan",
		admission.WithValidator(scheme, &shardkit.ShardPlanValidator{}))

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		exit(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func exit(err error) {
	ctrl.Log.WithName("admission").Error(err, "fatal")
	os.Exit(1)
}
