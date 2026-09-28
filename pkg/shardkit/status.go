package shardkit

import (
	"context"
	"fmt"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// AckOptions carries one status publication: this track's entry for
// one observed plan version (spec S1-S4).
type AckOptions struct {
	// Client performs the status write; APIReader does fresh reads.
	Client    client.Client
	APIReader client.Reader
	// PlanKey locates the plan; Track names this track's entry.
	PlanKey types.NamespacedName
	Track   string
	// Version pins the acked transition explicitly. Live reads
	// supply only the object RV for the update; bindings never
	// come from a re-read, so a plan that moves mid-drain cannot
	// redirect an ack onto an undrained version.
	Version PlanVersion
	// Revision is this track's self-reported revision identity.
	Revision string
	// Phase and Released describe this track's handshake state.
	Phase    string
	Released bool
	// OwnedNamespaces counts namespaces this track holds now.
	OwnedNamespaces int32
	// ReleasedWrites maps resource.group to this track's last
	// written resourceVersion per type (D4 barrier input).
	ReleasedWrites map[string]string
	// LeaseName and LeaseNamespace locate this track's lease for
	// the S8 session binding (direct read at ack time).
	LeaseName      string
	LeaseNamespace string
	// ClusterName qualifies the session holder as cluster/holder
	// (M4 multi-cluster keys): pod names repeat across clusters,
	// so a shared reader of several clusters needs the qualifier
	// to attribute sessions. Empty means unqualified (M1 shape).
	ClusterName string
	// Conditions appends to the entry (Degraded etc.).
	Conditions []metav1.Condition
	// MaxAttempts bounds conflict retries; <=0 means 8.
	MaxAttempts int
}

// PlanVersion pins one plan transition for ack bindings.
type PlanVersion struct {
	UID        string
	Generation int64
	Epoch      int64
	Rollout    string
}

// VersionOf snapshots a plan's version identity.
func VersionOf(plan *v1alpha1.ShardPlan) PlanVersion {
	return PlanVersion{
		UID:        string(plan.UID),
		Generation: plan.Generation,
		Epoch:      plan.Spec.Epoch,
		Rollout:    plan.Spec.Rollout,
	}
}

// PublishOwnEntry writes exactly this track's status entry (S1):
// bindings come from the caller's explicit Version plus the live
// lease read at ack time (S3), and conflict retries reuse the same
// payload (S4). A missing or unreadable own lease fails the publish:
// M1 requires the S8 session, never a nil one. The caller bounds
// total time with ctx (S2 status timeout).
func PublishOwnEntry(ctx context.Context, o AckOptions) error {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 8
	}
	if o.Version.UID == "" {
		return fmt.Errorf("shardkit: publish %s: explicit version is required", o.Track)
	}
	var lease coordinationv1.Lease
	if err := o.APIReader.Get(ctx, types.NamespacedName{
		Namespace: o.LeaseNamespace, Name: o.LeaseName,
	}, &lease); err != nil {
		return fmt.Errorf("shardkit: publish %s: own lease %s/%s unreadable, refusing session-less ack (S8): %w",
			o.Track, o.LeaseNamespace, o.LeaseName, err)
	}
	entry := v1alpha1.TrackStatus{
		Name:               o.Track,
		Revision:           o.Revision,
		PlanUID:            o.Version.UID,
		ObservedGeneration: o.Version.Generation,
		ObservedEpoch:      o.Version.Epoch,
		Rollout:            o.Version.Rollout,
		Phase:              o.Phase,
		Released:           o.Released,
		ReleasedWrites:     o.ReleasedWrites,
		Session: &v1alpha1.LeaderSession{
			Holder:           qualifyHolder(o.ClusterName, leaseHolder(&lease)),
			LeaseTransitions: leaseTransitions(&lease),
		},
		OwnedNamespaces: o.OwnedNamespaces,
		Conditions:      o.Conditions,
	}
	backoff := wait.Backoff{Duration: 50 * time.Millisecond, Factor: 2, Jitter: 0.2, Steps: o.MaxAttempts}
	return wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		var live v1alpha1.ShardPlan
		if err := o.APIReader.Get(ctx, o.PlanKey, &live); err != nil {
			return false, err
		}
		found := false
		for i := range live.Status.Tracks {
			if live.Status.Tracks[i].Name == o.Track {
				// Budget charges (M3) live in the same entry but
				// are written by a separate read-modify-write;
				// carry them forward so an ack never wipes a
				// charge (or vice versa — the charge preserves
				// every other field the same way).
				entry.Budget = live.Status.Tracks[i].Budget
				live.Status.Tracks[i] = entry
				found = true
				break
			}
		}
		if !found {
			live.Status.Tracks = append(live.Status.Tracks, entry)
		}
		if err := o.Client.Status().Update(ctx, &live); err != nil {
			if errors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
}

func leaseHolder(lease *coordinationv1.Lease) string {
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

// qualifyHolder prefixes a lease holder with its cluster for
// multi-cluster readers. Empty cluster keeps the M1 unqualified
// shape; "/" separates because ValidateClusterName forbids it in
// cluster names, so qualification is unambiguous and reversible.
func qualifyHolder(cluster, holder string) string {
	if cluster == "" {
		return holder
	}
	return cluster + "/" + holder
}

// ValidateClusterName rejects names that would make qualified
// holders ambiguous. Empty is allowed (means unqualified).
func ValidateClusterName(cluster string) error {
	if cluster == "" {
		return nil
	}
	if strings.Contains(cluster, "/") {
		return fmt.Errorf("shardkit: cluster name %q must not contain %q", cluster, "/")
	}
	return nil
}

func leaseTransitions(lease *coordinationv1.Lease) int32 {
	if lease.Spec.LeaseTransitions == nil {
		return 0
	}
	return *lease.Spec.LeaseTransitions
}

// AckCheck bundles the live inputs for one acquisition decision
// (spec S5-S11): the transition being acquired, the live status, and
// a direct read of the loser's lease.
type AckCheck struct {
	// Plan is the live plan carrying the transition to acquire.
	Plan *v1alpha1.ShardPlan
	// Tracks is the live status track list.
	Tracks []v1alpha1.TrackStatus
	// Loser is the track name that must have released.
	Loser string
	// LoserLease is the loser's live lease (direct read, not cache).
	// A nil lease voids the ack (S8).
	LoserLease *coordinationv1.Lease
	// ClusterName qualifies the expected session holder the same
	// way PublishOwnEntry wrote it; a checker in another cluster
	// (or without the qualifier) voids on S8 instead of trusting
	// a same-named pod elsewhere.
	ClusterName string
}

// ValidRelease reports whether the loser has validly released the
// transition in Plan. Any failure returns a void reason naming its S
// rule; void means "not released", never "released". Timeouts on top
// of this predicate must never become acknowledgements.
func ValidRelease(a AckCheck) error {
	var entry *v1alpha1.TrackStatus
	for i := range a.Tracks {
		if a.Tracks[i].Name == a.Loser {
			entry = &a.Tracks[i]
			break
		}
	}
	if entry == nil { // S11
		return fmt.Errorf("S11: no status entry for %q: not released", a.Loser)
	}
	if entry.PlanUID != string(a.Plan.UID) { // S5
		return fmt.Errorf("S5: ack planUID %q != live %q: void (plan recreated)", entry.PlanUID, a.Plan.UID)
	}
	if entry.ObservedGeneration != a.Plan.Generation ||
		entry.ObservedEpoch != a.Plan.Spec.Epoch ||
		entry.Rollout != a.Plan.Spec.Rollout { // S6
		return fmt.Errorf("S6: ack (gen %d epoch %d rollout %q) != transition (gen %d epoch %d rollout %q): void",
			entry.ObservedGeneration, entry.ObservedEpoch, entry.Rollout,
			a.Plan.Generation, a.Plan.Spec.Epoch, a.Plan.Spec.Rollout)
	}
	wantRev := a.Plan.Spec.Tracks.Stable.Revision
	if a.Loser == v1alpha1.TrackCanary {
		if a.Plan.Spec.Tracks.Canary == nil {
			return fmt.Errorf("S7: loser canary has no spec revision: void")
		}
		wantRev = a.Plan.Spec.Tracks.Canary.Revision
	}
	if entry.Revision != wantRev { // S7
		return fmt.Errorf("S7: ack revision %q != spec %q: void (foreign pod)", entry.Revision, wantRev)
	}
	if a.LoserLease == nil { // S8
		return fmt.Errorf("S8: loser lease missing: void")
	}
	wantHolder := qualifyHolder(a.ClusterName, leaseHolder(a.LoserLease))
	if entry.Session == nil ||
		entry.Session.Holder != wantHolder ||
		entry.Session.LeaseTransitions != leaseTransitions(a.LoserLease) { // S8
		return fmt.Errorf("S8: ack session %+v != live lease (%q, %d): void (leadership changed)",
			entry.Session, wantHolder, leaseTransitions(a.LoserLease))
	}
	if !entry.Released || entry.Phase != v1alpha1.PhaseReleased { // S9
		return fmt.Errorf("S9: phase %q released=%v: not released", entry.Phase, entry.Released)
	}
	return nil
}
