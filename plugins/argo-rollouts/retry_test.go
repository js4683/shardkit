package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pluginTypes "github.com/argoproj/argo-rollouts/utils/plugin/types"
	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type concurrentPlanClient struct {
	client.Client
	change         func(*v1alpha1.ShardPlan)
	updates        int
	alwaysConflict bool
}

func (c *concurrentPlanClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.updates++
	if c.alwaysConflict {
		return apierrors.NewConflict(schema.GroupResource{Resource: "shardplans"}, obj.GetName(), fmt.Errorf("concurrent update"))
	}
	if c.updates == 1 {
		var live v1alpha1.ShardPlan
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(obj), &live); err != nil {
			return err
		}
		c.change(&live)
		if err := c.Client.Update(ctx, &live); err != nil {
			return err
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

func TestPlugin_ConcurrentChangesPreserved(t *testing.T) {
	calls := []struct {
		name string
		call func(*Plugin) pluginTypes.RpcError
	}{
		{"weight", func(p *Plugin) pluginTypes.RpcError { return p.SetWeight(mkRollout(), 25, nil) }},
		{"hash", func(p *Plugin) pluginTypes.RpcError { return p.UpdateHash(mkRollout(), "rev-c", "rev-a", nil) }},
		{"abort", func(p *Plugin) pluginTypes.RpcError { return p.RemoveManagedRoutes(mkRollout()) }},
	}
	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			p := pluginFixture(t, mkBoundPlan(nil))
			c := &concurrentPlanClient{Client: p.Client, change: func(plan *v1alpha1.ShardPlan) {
				plan.Spec.Epoch++
				plan.Spec.Tracks.Stable.Revision = "concurrent-stable"
				plan.Labels = map[string]string{"concurrent": "preserved"}
			}}
			p.Client = c
			if err := tc.call(p); err.HasError() {
				t.Fatal(err)
			}
			got := getPlan(t, p)
			if got.Spec.Epoch != 5 || got.Spec.Tracks.Stable.Revision != "concurrent-stable" || got.Labels["concurrent"] != "preserved" {
				t.Fatalf("concurrent update overwritten: %+v", got)
			}
		})
	}
}

func TestPlugin_RetryRecognizesDesiredState(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	c := &concurrentPlanClient{Client: p.Client, change: func(plan *v1alpha1.ShardPlan) {
		plan.Spec.Epoch++
		plan.Spec.Canary.WeightPerMille = 250
	}}
	p.Client = c
	if err := p.SetWeight(mkRollout(), 25, nil); err.HasError() {
		t.Fatal(err)
	}
	if got := getPlan(t, p); got.Spec.Epoch != 4 || c.updates != 1 {
		t.Fatalf("redundant write: epoch=%d updates=%d", got.Spec.Epoch, c.updates)
	}
}

func TestPlugin_RetryBindingChange(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	p.Client = &concurrentPlanClient{Client: p.Client, change: func(plan *v1alpha1.ShardPlan) { plan.Spec.Epoch++; plan.Spec.Rollout = "another-rollout" }}
	if err := p.SetWeight(mkRollout(), 25, nil); !err.HasError() {
		t.Fatal("changed binding must fail")
	}
	if got := getPlan(t, p); got.Spec.Rollout != "another-rollout" {
		t.Fatal("binding overwritten")
	}
}

func TestPlugin_RetryBounded(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	c := &concurrentPlanClient{Client: p.Client, alwaysConflict: true}
	p.Client = c
	if err := p.SetWeight(mkRollout(), 25, nil); !err.HasError() {
		t.Fatal("expected conflict")
	}
	if c.updates != 3 {
		t.Fatalf("got %d attempts, want 3", c.updates)
	}
}

func TestPlugin_NilRollout(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	if err := p.SetWeight(nil, 10, nil); !err.HasError() {
		t.Fatal("nil rollout must fail")
	}
}

func TestPlugin_StableOnlyAbort(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(func(plan *v1alpha1.ShardPlan) {
		plan.Spec.Tracks.Canary = nil
		plan.Spec.Canary.Mode = v1alpha1.ModeOff
		plan.Spec.Canary.WeightPerMille = 0
	}))
	if err := p.RemoveManagedRoutes(mkRollout()); err.HasError() {
		t.Fatal(err)
	}
	if got := getPlan(t, p); got.Spec.Canary.Mode != v1alpha1.ModeOff {
		t.Fatal("not aborted")
	}
}

func TestPlugin_UpdateHashValidatesNoop(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(func(plan *v1alpha1.ShardPlan) { plan.Spec.Seed = "" }))
	if err := p.UpdateHash(mkRollout(), "rev-b", "rev-a", nil); !err.HasError() {
		t.Fatal("invalid plan accepted")
	}
}

func TestPlugin_VerifyWeightRange(t *testing.T) {
	p := pluginFixture(t, mkBoundPlan(nil))
	for _, weight := range []int32{-1, 101} {
		verified, err := p.VerifyWeight(mkRollout(), weight, nil)
		if !err.HasError() || verified != pluginTypes.NotVerified {
			t.Fatalf("weight %d: verified=%v error=%v", weight, verified, err)
		}
	}
}

type deadlineClient struct {
	client.Client
	t *testing.T
}

func (c deadlineClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > apiTimeout {
		c.t.Fatal("API request lacks bounded deadline")
	}
	return context.DeadlineExceeded
}

func TestPlugin_APICallsHaveDeadlines(t *testing.T) {
	p := pluginFixture(t)
	p.Client = deadlineClient{Client: p.Client, t: t}
	for _, call := range []func() pluginTypes.RpcError{
		p.InitPlugin,
		func() pluginTypes.RpcError { return p.SetWeight(mkRollout(), 25, nil) },
		func() pluginTypes.RpcError { _, err := p.VerifyWeight(mkRollout(), 25, nil); return err },
		func() pluginTypes.RpcError { return p.UpdateHash(mkRollout(), "rev-c", "rev-a", nil) },
		func() pluginTypes.RpcError { return p.RemoveManagedRoutes(mkRollout()) },
	} {
		if err := call(); !err.HasError() || !strings.Contains(err.ErrorString, context.DeadlineExceeded.Error()) {
			t.Fatalf("timeout not propagated: %v", err)
		}
	}
}
