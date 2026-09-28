package shardkit

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// ShardPlanValidator adapts library validation (V1-V9, V11) to an
// admission webhook. It is an experiment (M3), not wired into any
// manager: see docs/webhook.md for what it adds over CEL and why
// it stays optional.
//
// Mapping: create and update run the same validators writers and
// the gate run, so the server rejects malformed specs at the
// boundary with identical rule IDs. Delete always allows:
// absence handling (recreate detection, bootstrap) is
// controller-side protocol (spec section 6), never an admission
// decision. V10 staleness is not enforceable here — it needs the
// controller's acted-epoch memory, which admission does not have.
type ShardPlanValidator struct{}

// Compile-time proof this satisfies the webhook interface.
var _ admission.Validator[*v1alpha1.ShardPlan] = (*ShardPlanValidator)(nil)

// ValidateCreate implements admission.Validator.
func (*ShardPlanValidator) ValidateCreate(_ context.Context, obj *v1alpha1.ShardPlan) (admission.Warnings, error) {
	if obj == nil {
		return nil, fmt.Errorf("shardkit: webhook: nil ShardPlan on create")
	}
	if err := obj.ValidateCreate(); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements admission.Validator.
func (*ShardPlanValidator) ValidateUpdate(_ context.Context, oldObj, newObj *v1alpha1.ShardPlan) (admission.Warnings, error) {
	if oldObj == nil || newObj == nil {
		return nil, fmt.Errorf("shardkit: webhook: nil ShardPlan on update (old=%v new=%v)", oldObj == nil, newObj == nil)
	}
	if err := newObj.ValidateUpdate(oldObj); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements admission.Validator: deletions carry
// no spec to validate.
func (*ShardPlanValidator) ValidateDelete(_ context.Context, obj *v1alpha1.ShardPlan) (admission.Warnings, error) {
	if obj == nil {
		return nil, fmt.Errorf("shardkit: webhook: nil ShardPlan on delete")
	}
	return nil, nil
}
