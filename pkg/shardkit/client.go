package shardkit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// GuardedClient wraps a controller-runtime client and authorizes
// every mutating verb through the gate before delegating. Reads pass
// through untouched. Each admitted write is counted in flight until
// the API call returns, and its resourceVersion is folded into the
// per-type fencing evidence (LastWritten) that release acks carry.
//
// Coverage: Create, Update, Patch, Delete, DeleteAllOf, Apply, and
// the Status/SubResource writers (Create, Update, Patch, Apply).
// Anything unattributable to one namespace or to singleton duty is
// denied with a *DeniedError (fail closed).
//
// Evidence failures (a post-write read that cannot complete) never
// fail the write itself: they log, and the drain-time refresh
// re-lists every watched type directly, so a transient gap heals
// before any ack is published. A systematic gap (RBAC, unmappable
// type) fails the drain loudly instead.
type GuardedClient struct {
	client.Client
	gate    *Gate
	reader  client.Reader
	metrics *Metrics
	// shadow, when non-nil, dry-runs every mutating verb and
	// records attempts instead of persisting them (see
	// shadow.go). Reads are unaffected.
	shadow *ShadowRecorder

	writes        atomic.Int64
	lastWrittenMu sync.Mutex
	lastWritten   map[string]string
}

// Client wraps c: reads behave exactly as c, writes go through the
// gate, and post-write evidence reads use reader (which must be
// direct, e.g. mgr.GetAPIReader: cache RVs would under-fence). The
// wrapped client inherits the gate's metrics recorder.
func (g *Gate) Client(c client.Client, reader client.Reader) *GuardedClient {
	g.mu.Lock()
	defer g.mu.Unlock()
	return &GuardedClient{Client: c, gate: g, reader: reader,
		metrics: g.metrics, lastWritten: map[string]string{}}
}

// InflightWrites counts admitted writes with an API call outstanding.
func (c *GuardedClient) InflightWrites() int64 {
	return c.writes.Load()
}

// WaitWritesIdle polls until no write is in flight or ctx ends. The
// drain controller uses it after Revoke.
func (c *GuardedClient) WaitWritesIdle(ctx context.Context) error {
	for c.InflightWrites() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return nil
}

// LastWritten snapshots per-type last-written resourceVersions keyed
// by resource.group ("configmaps", "deployments.apps"). The release
// ack carries this map; the freshness barrier waits for each type's
// collection to reach its recorded version.
func (c *GuardedClient) LastWritten() map[string]string {
	c.lastWrittenMu.Lock()
	defer c.lastWrittenMu.Unlock()
	out := make(map[string]string, len(c.lastWritten))
	for k, v := range c.lastWritten {
		out[k] = v
	}
	return out
}

// record folds one written version into the evidence map.
func (c *GuardedClient) record(key, rv string) {
	if key == "" || rv == "" {
		return
	}
	c.lastWrittenMu.Lock()
	defer c.lastWrittenMu.Unlock()
	c.lastWritten[key] = maxRV(c.lastWritten[key], rv)
}

// evidenceLost logs a fencing-evidence gap the drain-time refresh
// must close. It never fails the write itself.
func evidenceLost(ctx context.Context, msg string, keysAndValues ...any) {
	args := append([]any{"fencing", "evidence-lost"}, keysAndValues...)
	logf.FromContext(ctx).Info("shardkit: "+msg, args...)
}

// resourceKey resolves obj to its resource.group evidence key,
// failing closed on unknown types.
func (c *GuardedClient) resourceKey(obj runtime.Object) (string, error) {
	gvk, err := apiutil.GVKForObject(obj, c.Client.Scheme())
	if err != nil {
		return "", err
	}
	mapping, err := c.Client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return "", err
	}
	return mapping.Resource.GroupResource().String(), nil
}

// guard authorizes one single-object write with a response revision
// (Create, Update, Patch, and the subresource writers): mapping,
// then ownership, then the call, then evidence from the response.
func (c *GuardedClient) guard(ctx context.Context, obj client.Object, verb string, run func() error) (err error) {
	key := "unknown"
	defer func() {
		decision, reason := writeOutcome(err)
		if c.shadowing() {
			c.noteShadow(verb, key, shadowName(obj.GetNamespace(), obj.GetName()), decision, reason)
		} else {
			c.metrics.ObserveClientWrite(c.gate.track, c.gate.revision, verb, key, decision, reason)
		}
	}()
	k, kerr := c.resourceKey(obj)
	if kerr != nil {
		return &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("%s %T: unmappable type: %v", verb, obj, kerr)}
	}
	key = k
	namespaced, nerr := c.Client.IsObjectNamespaced(obj)
	if nerr != nil {
		return &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("%s %T: cannot determine scope: %v", verb, obj, nerr)}
	}
	if err := c.authorize(ctx, obj.GetNamespace(), obj.GetName(), namespaced, verb); err != nil {
		return err
	}
	c.writes.Add(1)
	defer c.writes.Add(-1)
	if err := run(); err != nil {
		return err
	}
	if !c.shadowing() {
		// Dry-run responses carry resourceVersions for nothing
		// persisted; citing them in acks would fence on phantoms.
		c.record(key, obj.GetResourceVersion())
	}
	return nil
}

// authorize applies the ownership rule to one namespace/name pair,
// then the revision fence (S7): a writer whose revision differs
// from the spec revision for its track is superseded (previous
// image, previous Argo ReplicaSet) and must not write, even where
// the namespace still reads as owned — otherwise its stamps
// ping-pong against the current revision's. Ownership is checked
// first so foreign writes keep their existing denial reasons.
func (c *GuardedClient) authorize(ctx context.Context, namespace, name string, namespaced bool, verb string) error {
	if namespaced {
		obs, err := c.gate.Owned(ctx, namespace)
		if err != nil {
			return &DeniedError{Reason: ReasonGateClosed,
				Msg: fmt.Sprintf("%s %s/%s: %v", verb, namespace, name, err)}
		}
		if !obs.Owned {
			return &DeniedError{Reason: ReasonNotOwned,
				Msg: fmt.Sprintf("%s %s/%s: namespace owned by the other track (epoch %d)",
					verb, namespace, name, obs.Epoch)}
		}
		return c.fenceRevision(verb, namespace+"/"+name, obs.SpecRevision)
	}
	ok, specRev, err := c.gate.singletonOwned(ctx)
	if err != nil {
		return &DeniedError{Reason: ReasonGateClosed,
			Msg: fmt.Sprintf("%s %s (cluster-scoped): %v", verb, name, err)}
	}
	if !ok {
		return &DeniedError{Reason: ReasonSingletonElsewhere,
			Msg: fmt.Sprintf("%s %s: singleton duty owned by the other track", verb, name)}
	}
	return c.fenceRevision(verb, name+" (cluster-scoped)", specRev)
}

// fenceRevision denies writes from a superseded revision. A missing
// spec revision (Off plans may omit the canary revision) cannot
// bind: writers fail closed rather than writing unattributed.
func (c *GuardedClient) fenceRevision(verb, what, specRev string) error {
	if c.gate.revision != specRev {
		return &DeniedError{Reason: ReasonRevisionMismatch,
			Msg: fmt.Sprintf("%s %s: writer revision %q != spec revision %q for track %s",
				verb, what, c.gate.revision, specRev, c.gate.track)}
	}
	return nil
}

// applyMeta matches the identity accessors every generated apply
// configuration carries. A nil namespace means cluster scope.
type applyMeta interface {
	GetNamespace() *string
	GetName() *string
	GetKind() *string
	GetAPIVersion() *string
}

// guardApply authorizes one server-side apply. The apply shape must
// carry complete identity (type, name, and namespace-or-cluster);
// evidence comes from a direct post-apply GET because apply configs
// expose no resourceVersion.
func (c *GuardedClient) guardApply(ctx context.Context, obj runtime.ApplyConfiguration, verb string, run func() error) (err error) {
	key := "unknown"
	name := ""
	defer func() {
		decision, reason := writeOutcome(err)
		if c.shadowing() {
			c.noteShadow(verb, key, name, decision, reason)
		} else {
			c.metrics.ObserveClientWrite(c.gate.track, c.gate.revision, verb, key, decision, reason)
		}
	}()
	meta, ok := obj.(applyMeta)
	if !ok {
		return &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("%s %T: no identity accessors, unattributable", verb, obj)}
	}
	if meta.GetKind() == nil || meta.GetAPIVersion() == nil || meta.GetName() == nil {
		return &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("%s: apply without complete type identity is unattributable", verb)}
	}
	gv, err := schema.ParseGroupVersion(*meta.GetAPIVersion())
	if err != nil {
		return &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("%s: bad apiVersion %q: %v", verb, *meta.GetAPIVersion(), err)}
	}
	gvk := gv.WithKind(*meta.GetKind())
	mapping, err := c.Client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("%s %s: unmappable type: %v", verb, gvk, err)}
	}
	key = mapping.Resource.GroupResource().String()
	ns := meta.GetNamespace()
	name = shadowName(deref(ns), *meta.GetName())
	if err := c.authorize(ctx, deref(ns), *meta.GetName(), ns != nil, verb); err != nil {
		return err
	}
	c.writes.Add(1)
	defer c.writes.Add(-1)
	if err := run(); err != nil {
		return err
	}
	if c.shadowing() {
		// No evidence: a dry-run apply persists nothing to cite.
		return nil
	}
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	if err := c.reader.Get(ctx, client.ObjectKey{Namespace: deref(ns), Name: *meta.GetName()}, got); err != nil {
		evidenceLost(ctx, "apply evidence read failed; drain refresh must close the gap",
			"type", key, "name", *meta.GetName(), "error", err)
		return nil
	}
	c.record(key, got.GetResourceVersion())
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// guardDelete authorizes one Delete or DeleteAllOf. Deletes carry no
// response revision, so evidence comes from a direct post-delete
// LIST whose collection version fences the deletion.
func (c *GuardedClient) guardDelete(ctx context.Context, obj client.Object, namespace, name, verb string, run func() error) (err error) {
	key := "unknown"
	defer func() {
		decision, reason := writeOutcome(err)
		if c.shadowing() {
			c.noteShadow(verb, key, shadowName(namespace, name), decision, reason)
		} else {
			c.metrics.ObserveClientWrite(c.gate.track, c.gate.revision, verb, key, decision, reason)
		}
	}()
	k, kerr := c.resourceKey(obj)
	if kerr != nil {
		return &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("%s %T: unmappable type: %v", verb, obj, kerr)}
	}
	key = k
	namespaced, nerr := c.Client.IsObjectNamespaced(obj)
	if nerr != nil {
		return &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: fmt.Sprintf("%s %T: cannot determine scope: %v", verb, obj, nerr)}
	}
	if err := c.authorize(ctx, namespace, name, namespaced, verb); err != nil {
		return err
	}
	c.writes.Add(1)
	defer c.writes.Add(-1)
	if err := run(); err != nil {
		return err
	}
	gvk, err := apiutil.GVKForObject(obj, c.Client.Scheme())
	if err != nil {
		evidenceLost(ctx, "delete evidence lost: GVK unresolvable; drain refresh must close the gap",
			"type", key, "error", err)
		return nil
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List",
	})
	var opts []client.ListOption
	if namespaced {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := c.reader.List(ctx, list, opts...); err != nil {
		evidenceLost(ctx, "delete evidence list failed; drain refresh must close the gap",
			"type", key, "error", err)
		return nil
	}
	if !c.shadowing() {
		c.record(key, list.GetResourceVersion())
	}
	return nil
}

// Create implements client.Client.
func (c *GuardedClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if c.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return c.guard(ctx, obj, "create", func() error {
		return c.Client.Create(ctx, obj, opts...)
	})
}

// Delete implements client.Client.
func (c *GuardedClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if c.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return c.guardDelete(ctx, obj, obj.GetNamespace(), obj.GetName(), "delete", func() error {
		return c.Client.Delete(ctx, obj, opts...)
	})
}

// Update implements client.Client.
func (c *GuardedClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if c.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return c.guard(ctx, obj, "update", func() error {
		return c.Client.Update(ctx, obj, opts...)
	})
}

// Patch implements client.Client.
func (c *GuardedClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return c.guard(ctx, obj, "patch", func() error {
		return c.Client.Patch(ctx, obj, patch, opts...)
	})
}

// Apply implements client.Client.
func (c *GuardedClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	if c.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return c.guardApply(ctx, obj, "apply", func() error {
		return c.Client.Apply(ctx, obj, opts...)
	})
}

// DeleteAllOf implements client.Client. Only a single-namespace
// selection is attributable; anything broader is denied.
func (c *GuardedClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	o := &client.DeleteAllOfOptions{}
	for _, opt := range opts {
		opt.ApplyToDeleteAllOf(o)
	}
	if o.Namespace == "" {
		err := &DeniedError{Reason: ReasonUnsupportedWrite,
			Msg: "deletecollection across namespaces is unattributable; scope to one namespace"}
		if c.shadowing() {
			decision, reason := writeOutcome(err)
			c.noteShadow("deletecollection", "unknown", "<collection>", decision, reason)
		}
		return err
	}
	if c.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return c.guardDelete(ctx, obj, o.Namespace, "<collection>", "deletecollection", func() error {
		return c.Client.DeleteAllOf(ctx, obj, opts...)
	})
}

// Status implements client.Client.
func (c *GuardedClient) Status() client.SubResourceWriter {
	return &guardedStatusWriter{parent: c, sub: c.Client.Status()}
}

// SubResource implements client.Client.
func (c *GuardedClient) SubResource(name string) client.SubResourceClient {
	return &guardedSubResourceClient{parent: c, sub: c.Client.SubResource(name)}
}

// guardedStatusWriter guards the status writer's mutating verbs.
type guardedStatusWriter struct {
	parent *GuardedClient
	sub    client.SubResourceWriter
}

// Create implements client.SubResourceWriter.
func (s *guardedStatusWriter) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	if s.parent.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return s.parent.guard(ctx, obj, "status-create", func() error {
		return s.sub.Create(ctx, obj, sub, opts...)
	})
}

// Update implements client.SubResourceWriter.
func (s *guardedStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if s.parent.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return s.parent.guard(ctx, obj, "status-update", func() error {
		return s.sub.Update(ctx, obj, opts...)
	})
}

// Patch implements client.SubResourceWriter.
func (s *guardedStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if s.parent.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return s.parent.guard(ctx, obj, "status-patch", func() error {
		return s.sub.Patch(ctx, obj, patch, opts...)
	})
}

// Apply implements client.SubResourceWriter.
func (s *guardedStatusWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	if s.parent.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return s.parent.guardApply(ctx, obj, "status-apply", func() error {
		return s.sub.Apply(ctx, obj, opts...)
	})
}

// guardedSubResourceClient guards one named subresource; reads pass through.
type guardedSubResourceClient struct {
	parent *GuardedClient
	sub    client.SubResourceClient
}

// Get implements client.SubResourceReader (read-only, unguarded).
func (s *guardedSubResourceClient) Get(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceGetOption) error {
	return s.sub.Get(ctx, obj, sub, opts...)
}

// Create implements client.SubResourceWriter.
func (s *guardedSubResourceClient) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	if s.parent.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return s.parent.guard(ctx, obj, "subresource-create", func() error {
		return s.sub.Create(ctx, obj, sub, opts...)
	})
}

// Update implements client.SubResourceWriter.
func (s *guardedSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if s.parent.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return s.parent.guard(ctx, obj, "subresource-update", func() error {
		return s.sub.Update(ctx, obj, opts...)
	})
}

// Patch implements client.SubResourceWriter.
func (s *guardedSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if s.parent.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return s.parent.guard(ctx, obj, "subresource-patch", func() error {
		return s.sub.Patch(ctx, obj, patch, opts...)
	})
}

// Apply implements client.SubResourceWriter.
func (s *guardedSubResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	if s.parent.shadowing() {
		opts = append(opts, client.DryRunAll)
	}
	return s.parent.guardApply(ctx, obj, "subresource-apply", func() error {
		return s.sub.Apply(ctx, obj, opts...)
	})
}
