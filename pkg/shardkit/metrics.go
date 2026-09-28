package shardkit

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Metric names are stable API: dashboards and the M2 Argo analysis
// templates read them.
const (
	metricGateDecisions = "shardkit_gate_decisions_total"
	metricClientWrites  = "shardkit_client_writes_total"
	metricTransitions   = "shardkit_observer_transitions_total"
	metricBarrier       = "shardkit_barrier_duration_seconds"
	metricHeldNS        = "shardkit_held_namespaces"
	metricHeldSingleton = "shardkit_held_singleton"
)

// Metrics carries the library's Prometheus instruments. A nil
// *Metrics is a valid no-op recorder, so integrators that do not
// call NewMetrics pay nothing and change nothing.
//
// Each instrument answers one operational question:
//   - gateDecisions: is a track being denied, and why (stuck rollout)?
//   - clientWrites: are writes flowing or fenced, per verb/resource?
//   - transitions: is the handshake progressing, waiting, or degraded?
//   - barrier: how long does freshness fencing take (stuck acquires)?
//   - held: what does each track currently own (dashboard state)?
//
// All labels are bounded enums (reasons, decisions, verbs); error
// text stays in logs, never in a dimension.
type Metrics struct {
	gateDecisions *prometheus.CounterVec
	clientWrites  *prometheus.CounterVec
	transitions   *prometheus.CounterVec
	barrier       *prometheus.HistogramVec
	heldNS        *prometheus.GaugeVec
	heldSingleton *prometheus.GaugeVec
}

// NewMetrics registers the shardkit instruments on reg and returns
// the recorder. It panics on duplicate registration: call once per
// registry (once per process in practice).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		gateDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricGateDecisions,
			Help: "Gate ownership evaluations by track, scope, decision, and reason.",
		}, []string{"track", "revision", "scope", "decision", "reason"}),
		clientWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricClientWrites,
			Help: "Guarded client writes by track, verb, resource, decision, and reason.",
		}, []string{"track", "revision", "verb", "resource", "decision", "reason"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricTransitions,
			Help: "Observer handshake events by track and event.",
		}, []string{"track", "revision", "event"}),
		barrier: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: metricBarrier,
			Help: "Freshness barrier wait duration by track and outcome.",
		}, []string{"track", "outcome"}),
		heldNS: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricHeldNS,
			Help: "Namespaces held by this track at its adopted version.",
		}, []string{"track", "revision"}),
		heldSingleton: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricHeldSingleton,
			Help: "Singleton duty held by this track (1) or not (0).",
		}, []string{"track", "revision"}),
	}
	reg.MustRegister(m.gateDecisions, m.clientWrites, m.transitions,
		m.barrier, m.heldNS, m.heldSingleton)
	return m
}

// ObserveGateDecision records one Owned (scope "namespace") or
// SingletonOwned (scope "singleton") evaluation. Decision is owned,
// foreign, or denied; reason is "none" unless denied.
func (m *Metrics) ObserveGateDecision(track, revision, scope, decision, reason string) {
	if m == nil {
		return
	}
	m.gateDecisions.WithLabelValues(track, revision, scope, decision, reason).Inc()
}

// ObserveClientWrite records one guarded write. Decision is allowed,
// denied, or error; reason is "none" when allowed, a denial reason
// when denied, or a server-error class on error.
func (m *Metrics) ObserveClientWrite(track, revision, verb, resource, decision, reason string) {
	if m == nil {
		return
	}
	m.clientWrites.WithLabelValues(track, revision, verb, resource, decision, reason).Inc()
}

// ObserveTransition records one observer handshake event: resume,
// adopt, steady_refresh, release, acquire, acquire_abandoned,
// advance_refused (observer held for a version the gate has not
// adopted), or degraded.
func (m *Metrics) ObserveTransition(track, revision, event string) {
	if m == nil {
		return
	}
	m.transitions.WithLabelValues(track, revision, event).Inc()
}

// ObserveBarrier records one freshness-barrier wait. Outcome is
// success or error.
func (m *Metrics) ObserveBarrier(track string, d time.Duration, outcome string) {
	if m == nil {
		return
	}
	m.barrier.WithLabelValues(track, outcome).Observe(d.Seconds())
}

// SetHeld records the adopted ownership: namespace count plus
// singleton duty (1 held, 0 not).
func (m *Metrics) SetHeld(track, revision string, namespaces int, singleton bool) {
	if m == nil {
		return
	}
	m.heldNS.WithLabelValues(track, revision).Set(float64(namespaces))
	sing := 0.0
	if singleton {
		sing = 1.0
	}
	m.heldSingleton.WithLabelValues(track, revision).Set(sing)
}

// Child accessors expose single series for integrator self-tests
// (drive the library against a private registry, then assert with
// testutil). Production code reads metrics by scraping, not here.

// GateDecisions returns one gate-decision counter series.
func (m *Metrics) GateDecisions(track, revision, scope, decision, reason string) prometheus.Counter {
	return m.gateDecisions.WithLabelValues(track, revision, scope, decision, reason)
}

// ClientWrites returns one guarded-write counter series.
func (m *Metrics) ClientWrites(track, revision, verb, resource, decision, reason string) prometheus.Counter {
	return m.clientWrites.WithLabelValues(track, revision, verb, resource, decision, reason)
}

// Transitions returns one handshake-event counter series.
func (m *Metrics) Transitions(track, revision, event string) prometheus.Counter {
	return m.transitions.WithLabelValues(track, revision, event)
}

// Barrier returns one barrier-duration observer series.
func (m *Metrics) Barrier(track, outcome string) prometheus.Observer {
	obs, _ := m.barrier.GetMetricWithLabelValues(track, outcome)
	return obs
}

// HeldNamespaces returns one held-namespaces gauge series.
func (m *Metrics) HeldNamespaces(track, revision string) prometheus.Gauge {
	return m.heldNS.WithLabelValues(track, revision)
}

// HeldSingleton returns one held-singleton gauge series.
func (m *Metrics) HeldSingleton(track, revision string) prometheus.Gauge {
	return m.heldSingleton.WithLabelValues(track, revision)
}

// gateOutcome maps an Owned result to decision/reason labels.
func gateOutcome(owned bool, err error) (decision, reason string) {
	if err == nil {
		if owned {
			return "owned", "none"
		}
		return "foreign", "none"
	}
	if closed, ok := AsClosed(err); ok {
		return "denied", closed.Reason
	}
	return "denied", "Unknown"
}

// writeOutcome maps a guarded-write result to decision/reason labels.
// Server errors classify coarsely: conflicts, forbiddens, and
// not-founds are operator-actionable; the rest is ServerError.
func writeOutcome(err error) (decision, reason string) {
	if err == nil {
		return "allowed", "none"
	}
	if denied, ok := AsDenied(err); ok {
		return "denied", denied.Reason
	}
	switch {
	case apierrors.IsConflict(err):
		return "error", "Conflict"
	case apierrors.IsForbidden(err):
		return "error", "Forbidden"
	case apierrors.IsNotFound(err):
		return "error", "NotFound"
	default:
		return "error", "ServerError"
	}
}
