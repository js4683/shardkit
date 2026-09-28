package shardkit

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// BarrierOptions carries one freshness-barrier evaluation (decision
// D4, option A): prove this process's direct reads reflect the
// released writes before acquiring.
type BarrierOptions struct {
	// Reader performs the barrier lists and must be direct (a cache
	// would certify its own staleness).
	Reader client.Reader
	// Mapper resolves watched types to resources.
	Mapper meta.RESTMapper
	// Types are the integrator's watched types: every recorded
	// write must resolve to one of them.
	Types []schema.GroupVersionKind
	// Writes maps resource.group to the released resourceVersion
	// (a release ack's releasedWrites).
	Writes map[string]string
	// PollInterval spaces barrier lists; <=0 means 200ms.
	PollInterval time.Duration
	// Track labels the barrier wait; Metrics records it. A nil
	// Metrics records nothing.
	Track   string
	Metrics *Metrics
}

// WaitFresh blocks until every recorded type's live collection
// version reaches its released version, or ctx ends. Each type is
// evaluated with a direct quorum LIST (no RV precondition: the read
// is fresh by construction, the question is only whether it is fresh
// enough). A recorded type missing from Types, an unparseable
// version, or an unlistable type fails closed with an explicit
// error; ctx bounds the total wait (the observer passes its acquire
// timeout).
func WaitFresh(ctx context.Context, o BarrierOptions) (err error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		o.Metrics.ObserveBarrier(o.Track, time.Since(start), outcome)
	}()
	if o.PollInterval <= 0 {
		o.PollInterval = 200 * time.Millisecond
	}
	index := make(map[string]schema.GroupVersionKind, len(o.Types))
	for _, gvk := range o.Types {
		mapping, err := o.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return fmt.Errorf("shardkit: barrier: watched type %s unmappable: %w", gvk, err)
		}
		index[mapping.Resource.GroupResource().String()] = gvk
	}
	for key, want := range o.Writes {
		gvk, ok := index[key]
		if !ok {
			return fmt.Errorf("shardkit: barrier: written type %q not in watched list: "+
				"add it or stop writing it", key)
		}
		if _, ok := parseRV(want); !ok {
			return fmt.Errorf("shardkit: barrier: type %q version %q is not decimal: "+
				"extension-server versioning is unsupported in M1 (D4)", key, want)
		}
		if err := waitTypeFresh(ctx, o.Reader, gvk, want, o.PollInterval); err != nil {
			return fmt.Errorf("shardkit: barrier: type %q: %w", key, err)
		}
	}
	return nil
}

// waitTypeFresh polls one type's live collection version to want.
func waitTypeFresh(ctx context.Context, reader client.Reader, gvk schema.GroupVersionKind, want string, poll time.Duration) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List",
	})
	for {
		// Fresh list object per attempt: List appends otherwise.
		attempt := &unstructured.UnstructuredList{}
		attempt.SetGroupVersionKind(list.GroupVersionKind())
		if err := reader.List(ctx, attempt); err != nil {
			return fmt.Errorf("barrier list failed: %w", err)
		}
		if got := attempt.GetResourceVersion(); reached(got, want) {
			return nil
		} else {
			select {
			case <-ctx.Done():
				return fmt.Errorf("collection at %q, want %q: %w", got, want, ctx.Err())
			case <-time.After(poll):
			}
		}
	}
}
