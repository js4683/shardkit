package shardkit

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

// webhookPlan returns a valid plan pair (old, new) one epoch apart.
func webhookPlan() (old, new *v1alpha1.ShardPlan) {
	mk := func(epoch int64) *v1alpha1.ShardPlan {
		return &v1alpha1.ShardPlan{
			ObjectMeta: metav1.ObjectMeta{Namespace: "widget-system", Name: "widget-operator"},
			Spec: v1alpha1.ShardPlanSpec{
				Key: "namespace", Rollout: "r-1", Epoch: epoch, Seed: "9a1f2e",
				Tracks: v1alpha1.TrackSet{
					Stable: v1alpha1.TrackRevision{Revision: "rev-a"},
					Canary: &v1alpha1.TrackRevision{Revision: "rev-b"},
				},
				Canary:         v1alpha1.CanarySpec{Mode: "Active", WeightPerMille: 10},
				SingletonOwner: "stable",
			},
		}
	}
	return mk(7), mk(8)
}

// TestWebhook_Mapping pins the experiment's contract: the webhook
// accepts what the library accepts and rejects with the same rule
// IDs, on create and on update.
func TestWebhook_Mapping(t *testing.T) {
	ctx := context.Background()
	v := &ShardPlanValidator{}
	old, new := webhookPlan()

	if _, err := v.ValidateCreate(ctx, new); err != nil {
		t.Fatalf("create valid: %v", err)
	}
	if _, err := v.ValidateUpdate(ctx, old, new); err != nil {
		t.Fatalf("update valid: %v", err)
	}
	if _, err := v.ValidateDelete(ctx, new); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, badNew := webhookPlan()
	badNew.Spec.Canary.WeightPerMille = 1001
	if _, err := v.ValidateCreate(ctx, badNew); err == nil || !strings.Contains(err.Error(), "V4") {
		t.Fatalf("create weight 1001: err = %v, want V4", err)
	}

	// V9 is the webhook's marginal value over CEL (see
	// docs/webhook.md): a frozen-tuple change under the same
	// rollout ID must fail here.
	_, frozen := webhookPlan()
	frozen.Spec.Seed = "other"
	if _, err := v.ValidateUpdate(ctx, old, frozen); err == nil || !strings.Contains(err.Error(), "V9") {
		t.Fatalf("frozen tuple same rollout: err = %v, want V9", err)
	}

	// V11 ranges ride along.
	_, budgeted := webhookPlan()
	pct := int32(101)
	budgeted.Spec.Budget = &v1alpha1.BudgetSpec{MaxDeletionPercent: &pct}
	if _, err := v.ValidateUpdate(ctx, old, budgeted); err == nil || !strings.Contains(err.Error(), "V11") {
		t.Fatalf("percent 101: err = %v, want V11", err)
	}

	if _, err := v.ValidateCreate(ctx, nil); err == nil {
		t.Fatal("nil create: nil error, want failure")
	}
	if _, err := v.ValidateUpdate(ctx, nil, new); err == nil {
		t.Fatal("nil old: nil error, want failure")
	}
}
