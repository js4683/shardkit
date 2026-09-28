// Command widget-operator is the shardkit sample workload: a tiny
// controller-runtime operator whose two tracks (stable, canary) hand
// widget namespaces back and forth under a ShardPlan. It is the
// reference library integration: Attach a gate, wrap writes in a
// guarded client, run one observer per track leader, and feed the
// observer's handoff events into the controller. See docs/cli.md for
// driving it and docs/traces/m1-widget-rewire.md for the M0->M1
// rewire notes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	"github.com/js4683/shardkit/pkg/shardkit"
)

// Revision is baked per image (docker --build-arg REVISION / ENV).
var Revision = "dev"

func main() {
	var metricsAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		"The address the metric endpoint binds to.")
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	track := mustEnv("TRACK")
	revision := envOr("REVISION", Revision)
	planKey := types.NamespacedName{
		Namespace: envOr("PLAN_NAMESPACE", "widget-system"),
		Name:      envOr("PLAN_NAME", "widget-operator"),
	}
	writeDelay := envDuration("WRITE_DELAY", 0)
	guardWrites := envBool("GUARD_WRITES", true)

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		exit(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		exit(err)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		LeaderElection:          true,
		LeaderElectionID:        "widget-operator-" + track,
		LeaderElectionNamespace: planKey.Namespace,
		Metrics:                 server.Options{BindAddress: metricsAddr},
	})
	if err != nil {
		exit(err)
	}

	// Library integration: attach the gate, record metrics, guard
	// every write, and run the handshake observer on the leader.
	// The instruments register on controller-runtime's registry
	// (the one the metrics server scrapes), not the Prometheus
	// default: registering anywhere else exposes nothing.
	metrics := shardkit.NewMetrics(crmetrics.Registry)
	attachCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	gate, err := shardkit.Attach(attachCtx, mgr.GetAPIReader(), planKey, track, revision)
	cancel()
	if err != nil {
		exit(err)
	}
	gate.SetMetrics(metrics)
	guarded := gate.Client(mgr.GetClient(), mgr.GetAPIReader())
	obs, err := shardkit.NewObserver(shardkit.ObserverOptions{
		Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(),
		Gate: gate, Guarded: guarded,
		PlanKey: planKey, Track: track, LeaseBase: "widget-operator",
		Types:        []schema.GroupVersionKind{v1alpha1.GroupVersion.WithKind("Widget")},
		PollInterval: envDuration("OBSERVER_POLL", time.Second),
		Metrics:      metrics,
	})
	if err != nil {
		exit(err)
	}
	if err := mgr.Add(&observerRunner{obs: obs}); err != nil {
		exit(err)
	}

	rec := &WidgetReconciler{
		Client:      guarded,
		Gate:        gate,
		Track:       track,
		Revision:    revision,
		WriteDelay:  writeDelay,
		GuardWrites: guardWrites,
		Chaos:       envOr("CHAOS", ""),
		PlanKey:     planKey,
	}
	// Per-track controller name: controller-runtime metrics carry
	// only the controller dimension, so both tracks stamping as
	// "widget" would merge canary errors into stable's. The shardkit
	// metrics already label track and revision.
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Widget{}).
		Named(track + "-widget").
		WatchesRawSource(source.Channel(obs.Events(), &handler.EnqueueRequestForObject{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5}).
		Complete(rec); err != nil {
		exit(err)
	}

	setupLog := ctrl.Log.WithName("setup")
	setupLog.Info("starting widget-operator",
		"track", track, "revision", revision,
		"plan", planKey.String(),
		"writeDelay", writeDelay.String(), "guardWrites", guardWrites)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		exit(err)
	}
}

// observerRunner adapts the shardkit observer to a leader-elected
// manager runnable. Shutdown cancellation is clean (nil), matching
// every other controller-runtime runnable.
type observerRunner struct {
	obs *shardkit.Observer
}

// NeedLeaderElection implements manager.LeaderElectionRunnable: only
// the track leader handshakes.
func (r *observerRunner) NeedLeaderElection() bool { return true }

// Start implements manager.Runnable.
func (r *observerRunner) Start(ctx context.Context) error {
	if err := r.obs.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func mustEnv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		exit(fmt.Errorf("missing required env %s", key))
	}
	return v
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		exit(fmt.Errorf("bad duration %s=%q: %w", key, v, err))
	}
	return d
}

func envBool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		exit(fmt.Errorf("bad bool %s=%q: %w", key, v, err))
	}
	return b
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, "widget-operator:", err)
	os.Exit(1)
}
