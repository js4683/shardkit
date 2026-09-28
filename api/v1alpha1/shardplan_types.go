package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ShardPlan mode values.
const (
	ModeOff    = "Off"
	ModeShadow = "Shadow"
	ModeActive = "Active"
)

// ShardPlan phase values for per-track handshake state.
const (
	PhaseDraining  = "Draining"
	PhaseReleased  = "Released"
	PhaseAcquiring = "Acquiring"
	PhaseAcquired  = "Acquired"
)

// Track names.
const (
	TrackStable = "stable"
	TrackCanary = "canary"
)

// ShardPlanSpec defines the desired rollout state. Writers own this;
// the library only reads it. See docs/shardplan-spec.md sections 1-3.
// CEL rules mirror library validation V1-V9 plus V11 range rules as
// defense in depth; library validation stays authoritative.
//
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.epoch) || self.epoch >= oldSelf.epoch",message="V9: epoch must never decrease"
// +kubebuilder:validation:XValidation:rule="self.canary.mode == 'Off' || has(self.tracks.canary)",message="V6: Active and Shadow modes require tracks.canary.revision"
// +kubebuilder:validation:XValidation:rule="self.singletonOwner == 'stable' || has(self.tracks.canary)",message="V7: singletonOwner canary requires tracks.canary.revision"
type ShardPlanSpec struct {
	// Key selects the shard identity. M1: only "namespace".
	// +kubebuilder:validation:Enum=namespace
	Key string `json:"key"`
	// Rollout is the opaque orchestrator-set rollout ID. A fresh
	// rollout (changed frozen tuple) must mint a fresh ID.
	Rollout string `json:"rollout"`
	// Epoch is writer-assigned and strictly increasing per plan UID.
	Epoch int64 `json:"epoch"`
	// Seed fixes the hash window for the rollout. Non-empty.
	// +kubebuilder:validation:MinLength=1
	Seed string `json:"seed"`
	// Tracks maps track names to revisions.
	Tracks TrackSet `json:"tracks"`
	// Canary carries mode, weight, and cohort rules.
	Canary CanarySpec `json:"canary"`
	// SingletonOwner selects which track runs singleton work.
	// +kubebuilder:validation:Enum=stable;canary
	SingletonOwner string `json:"singletonOwner"`
	// Budget caps destructive deletes per track per epoch (M3).
	// Absent (nil) means unbounded; existing plans without budgets
	// behave exactly as before.
	// +optional
	Budget *BudgetSpec `json:"budget,omitempty"`
}

// BudgetSpec caps destructive deletes (V11): each track may delete
// at most MaxDeletions objects AND at most MaxDeletionPercent of
// the namespaces it owns, per spec epoch. The two caps apply
// concurrently; a nil cap is unbounded. Counters live in each
// track's own status entry (BudgetUsage) so S1 single-writer
// ownership needs no new writer, and the epoch window resets them
// without an explicit reset write.
type BudgetSpec struct {
	// MaxDeletions caps deletes per track per epoch.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxDeletions *int32 `json:"maxDeletions,omitempty"`
	// MaxDeletionPercent caps deletes per track per epoch as a
	// percentage of the namespaces the track owns (0..100).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MaxDeletionPercent *int32 `json:"maxDeletionPercent,omitempty"`
}

// TrackSet holds the stable and optional canary revisions.
type TrackSet struct {
	Stable TrackRevision `json:"stable"`
	// Canary is absent when no canary revision is deployed.
	// +optional
	Canary *TrackRevision `json:"canary,omitempty"`
}

// TrackRevision identifies one deployed revision: a non-empty
// DNS-1123 label (V5/V6).
type TrackRevision struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Revision string `json:"revision"`
}

// CanarySpec carries the canary mode, weight, and cohorts.
//
// +kubebuilder:validation:XValidation:rule="self.mode == 'Active' || self.weightPerMille == 0",message="V4: Off and Shadow modes require weight 0"
// +kubebuilder:validation:XValidation:rule="!has(self.exclude) || !has(self.exclude.selector) || !has(self.exclude.selector.matchExpressions) || size(self.exclude.selector.matchExpressions) == 0",message="V8: exclude.selector.matchExpressions is unsupported in M1"
type CanarySpec struct {
	// Mode is Off, Shadow, or Active.
	// +kubebuilder:validation:Enum=Off;Shadow;Active
	Mode string `json:"mode"`
	// WeightPerMille is 0..1000; 0 when Off/Shadow.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	WeightPerMille int32 `json:"weightPerMille"`
	// +optional
	Include IncludeSpec `json:"include,omitempty"`
	// +optional
	Exclude ExcludeSpec `json:"exclude,omitempty"`
}

// IncludeSpec lists explicitly canaried namespaces.
type IncludeSpec struct {
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`
}

// ExcludeSpec selects namespaces pinned to stable. M1 supports
// matchLabels only; matchExpressions is rejected until implemented.
type ExcludeSpec struct {
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
}

// ShardPlanStatus carries per-track handshake state. Each track's
// leader owns exactly its own entry. See spec sections 4-5.
type ShardPlanStatus struct {
	// +optional
	Tracks []TrackStatus `json:"tracks,omitempty"`
}

// TrackStatus is one track's acknowledgement of one plan version.
type TrackStatus struct {
	Name string `json:"name"`
	// Revision is self-reported; acks count only when it matches
	// the spec revision for this track name.
	Revision string `json:"revision"`
	// PlanUID binds the ack to one plan object identity.
	PlanUID            string `json:"planUID"`
	ObservedGeneration int64  `json:"observedGeneration"`
	ObservedEpoch      int64  `json:"observedEpoch"`
	Rollout            string `json:"rollout"`
	// Phase is Draining, Released, Acquiring, or Acquired.
	Phase    string `json:"phase"`
	Released bool   `json:"released"`
	// ReleasedWrites maps resource.group to the decimal
	// resourceVersion of this track's last write of the type.
	// +optional
	ReleasedWrites map[string]string `json:"releasedWrites,omitempty"`
	// Session copies the track lease identity at ack time.
	// +optional
	Session *LeaderSession `json:"session,omitempty"`
	// +optional
	OwnedNamespaces int32 `json:"ownedNamespaces,omitempty"`
	// Budget charges destructive deletes against V11 caps. Only the
	// owning track writes its own entry's usage (S1); ack publishes
	// preserve it.
	// +optional
	Budget *BudgetUsage `json:"budget,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// BudgetUsage charges one epoch's destructive deletes. A usage
// counts only when Epoch equals the live spec epoch; any other
// value reads as zero and is overwritten on the next charge, so
// new epochs reset without a reset write.
type BudgetUsage struct {
	Epoch int64 `json:"epoch"`
	// +optional
	DeletionsUsed int32 `json:"deletionsUsed,omitempty"`
}

// LeaderSession binds an ack to the acker's leadership term.
type LeaderSession struct {
	Holder           string `json:"holder"`
	LeaseTransitions int32  `json:"leaseTransitions"`
}

// ShardPlan is the Schema for the shardplans API.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=sp
type ShardPlan struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ShardPlanSpec   `json:"spec,omitempty"`
	Status ShardPlanStatus `json:"status,omitempty"`
}

// ShardPlanList contains a list of ShardPlan.
//
// +kubebuilder:object:root=true
type ShardPlanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShardPlan `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ShardPlan{}, &ShardPlanList{})
}
