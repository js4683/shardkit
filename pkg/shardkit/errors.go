// Package shardkit is the M1 progressive-delivery library for
// Kubernetes controllers: a cooperative ownership gate plus a guarded
// write client. A new controller version (track) owns a slice of
// namespaces described by a ShardPlan; the gate decides ownership per
// namespace and the client enforces it on every write path. On any
// uncertainty the gate closes: affected writes stop with a typed
// denial, never a silent pass.
package shardkit

import (
	"errors"
	"fmt"
)

// Denial reasons for writes the guarded client refuses.
const (
	// ReasonNotOwned: the object's namespace belongs to the other track.
	ReasonNotOwned = "NotOwned"
	// ReasonSingletonElsewhere: a cluster-scoped write while another
	// track owns singleton duty.
	ReasonSingletonElsewhere = "SingletonElsewhere"
	// ReasonGateClosed: the gate cannot authorize (deleted, malformed,
	// or unreadable plan); see ClosedError for the underlying reason.
	ReasonGateClosed = "GateClosed"
	// ReasonUnsupportedWrite: the write shape carries no attributable
	// namespace (multi-namespace DeleteAllOf, exotic apply config).
	ReasonUnsupportedWrite = "UnsupportedWrite"
	// ReasonRevisionMismatch: the writer's revision differs from the
	// spec revision for its track (S7). A superseded process —
	// previous image, previous Argo ReplicaSet — may still read a
	// namespace as owned, but it must not write: the current
	// revision's stamps would ping-pong against its own.
	ReasonRevisionMismatch = "RevisionMismatch"
	// ReasonBudgetExceeded: a confirmed delete would exceed the
	// track's V11 destructive budget for this epoch (I5). The
	// delete is refused; a new epoch resets the window.
	ReasonBudgetExceeded = "BudgetExceeded"
)

// Gate-closed reasons (spec B2-B4 plus local read failures).
const (
	// ReasonPlanDeleted: the plan vanished after attachment (B2).
	ReasonPlanDeleted = "PlanDeleted"
	// ReasonPlanRecreated: same name, new UID, not through Off (B3).
	ReasonPlanRecreated = "PlanRecreated"
	// ReasonPlanMalformed: spec fails V1-V9 or V11 (B4).
	ReasonPlanMalformed = "PlanMalformed"
	// ReasonPlanUnreadable: the plan or a namespace could not be read
	// (transient API failure or first-seen namespace with no retained
	// owner); fail closed until a good read lands.
	ReasonPlanUnreadable = "PlanUnreadable"
	// ReasonLabelsUnreadable: namespace labels unreadable; the gate
	// retains the last evaluated owner and reports degraded.
	ReasonLabelsUnreadable = "LabelsUnreadable"
	// ReasonExternalHold: a configured external hold (legacy-lease
	// migration mutex, maintenance lock) is not satisfied; the
	// track must not act even where the plan grants ownership.
	ReasonExternalHold = "ExternalHold"
)

// DeniedError refuses one write. Denials are final for the rejected
// state: retrying the same write against the same plan fails again.
type DeniedError struct {
	Reason string
	Msg    string
}

func (e *DeniedError) Error() string {
	return fmt.Sprintf("shardkit: write denied (%s): %s", e.Reason, e.Msg)
}

// AsDenied reports whether err is (or wraps) a *DeniedError.
func AsDenied(err error) (*DeniedError, bool) {
	var denied *DeniedError
	if errors.As(err, &denied) {
		return denied, true
	}
	return nil, false
}

// ClosedError reports that the gate cannot authorize: the plan is
// deleted, recreated, malformed, or unreadable. Callers stop
// authorizing new writes; in-flight work drains under its deadline.
type ClosedError struct {
	Reason string
	Msg    string
}

func (e *ClosedError) Error() string {
	return fmt.Sprintf("shardkit: gate closed (%s): %s", e.Reason, e.Msg)
}

// AsClosed reports whether err is (or wraps) a *ClosedError.
func AsClosed(err error) (*ClosedError, bool) {
	var closed *ClosedError
	if errors.As(err, &closed) {
		return closed, true
	}
	return nil, false
}
