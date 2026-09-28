package main

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/js4683/shardkit/api/v1alpha1"
)

const cliNS = "cli-test"

// cliFixture builds a fake cluster with the given objects.
func cliFixture(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("Namespace"), meta.RESTScopeRoot)
	mapper.Add(corev1.SchemeGroupVersion.WithKind("NamespaceList"), meta.RESTScopeRoot)
	mapper.Add(v1alpha1.GroupVersion.WithKind("ShardPlan"), meta.RESTScopeNamespace)
	return fake.NewClientBuilder().WithScheme(s).WithRESTMapper(mapper).
		WithStatusSubresource(&v1alpha1.ShardPlan{}).WithObjects(objs...).Build()
}

// mkPlan returns a valid Active plan; mutate adjusts it before use.
func mkPlan(mutate func(*v1alpha1.ShardPlan)) *v1alpha1.ShardPlan {
	p := &v1alpha1.ShardPlan{
		ObjectMeta: metav1.ObjectMeta{Namespace: cliNS, Name: "demo", UID: "uid-9", Generation: 5},
		Spec: v1alpha1.ShardPlanSpec{
			Key: "namespace", Rollout: "r-7", Epoch: 2, Seed: "9a1f2e",
			Tracks: v1alpha1.TrackSet{
				Stable: v1alpha1.TrackRevision{Revision: "rev-a"},
				Canary: &v1alpha1.TrackRevision{Revision: "rev-b"},
			},
			Canary: v1alpha1.CanarySpec{
				Mode: "Active", WeightPerMille: 1000,
				Exclude: v1alpha1.ExcludeSpec{Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"tier": "critical"},
				}},
			},
			SingletonOwner: "stable",
		},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

// setEntries replaces the plan's status entries.
func setEntries(t *testing.T, c client.Client, entries ...v1alpha1.TrackStatus) {
	t.Helper()
	ctx := context.Background()
	var live v1alpha1.ShardPlan
	if err := c.Get(ctx, types.NamespacedName{Namespace: cliNS, Name: "demo"}, &live); err != nil {
		t.Fatal(err)
	}
	live.Status.Tracks = entries
	if err := c.Status().Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
}

func inSyncEntries() []v1alpha1.TrackStatus {
	sess := &v1alpha1.LeaderSession{Holder: "pod-0", LeaseTransitions: 0}
	return []v1alpha1.TrackStatus{
		{Name: "stable", Revision: "rev-a", PlanUID: "uid-9",
			ObservedGeneration: 5, ObservedEpoch: 2, Rollout: "r-7",
			Phase: "Released", Released: true, OwnedNamespaces: 1, Session: sess},
		{Name: "canary", Revision: "rev-b", PlanUID: "uid-9",
			ObservedGeneration: 5, ObservedEpoch: 2, Rollout: "r-7",
			Phase: "Acquired", Released: true, OwnedNamespaces: 1, Session: sess},
	}
}

func cliNamespaces() []client.Object {
	return []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-87"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "critical-db",
			Labels: map[string]string{"tier": "critical"}}},
	}
}

// runExec executes a parsed command against the fake client.
func runExec(exec execFunc, c client.Client) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := exec(context.Background(), c, cliNS, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func mustParse(t *testing.T, parse func([]string, *bytes.Buffer, *bytes.Buffer) (execFunc, bool), args ...string) execFunc {
	t.Helper()
	var stdout, stderr bytes.Buffer
	exec, ok := parse(args, &stdout, &stderr)
	if !ok {
		t.Fatalf("parse %v failed: %s", args, stderr.String())
	}
	return exec
}

func TestStatus_InSync(t *testing.T) {
	objs := append([]client.Object{mkPlan(nil)}, cliNamespaces()...)
	c := cliFixture(t, objs...)
	setEntries(t, c, inSyncEntries()...)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseStatus(a, o, e)
	}, "demo")
	code, stdout, _ := runExec(exec, c)
	if code != 0 {
		t.Fatalf("exit %d, stdout:\n%s", code, stdout)
	}
	for _, want := range []string{"epoch: 2", "mode: Active", "in sync", "Acquired", "pod-0/0"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout lacks %q:\n%s", want, stdout)
		}
	}
}

func TestStatus_Verdicts(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*v1alpha1.ShardPlan)
		entries func() []v1alpha1.TrackStatus
		want    string
	}{
		{"waiting behind", nil, func() []v1alpha1.TrackStatus {
			e := inSyncEntries()
			e[1].ObservedEpoch = 1
			return e
		}, "waiting: canary at epoch 1"},
		{"missing entry", nil, func() []v1alpha1.TrackStatus {
			return inSyncEntries()[:1]
		}, "waiting: canary has no entry"},
		{"degraded", nil, func() []v1alpha1.TrackStatus {
			e := inSyncEntries()
			e[1].Conditions = []metav1.Condition{{
				Type: "Degraded", Status: "True", Reason: "AcquireWaiting",
				Message: "S11: no status entry",
			}}
			e[1].Phase, e[1].Released = "Draining", false
			return e
		}, "degraded: canary: AcquireWaiting"},
		{"revision drift", nil, func() []v1alpha1.TrackStatus {
			e := inSyncEntries()
			e[0].Revision = "rev-zzz"
			return e
		}, "stale: stable runs revision"},
		{"recreated plan", func(p *v1alpha1.ShardPlan) {
			p.UID = "uid-new"
		}, inSyncEntries, "drive the plan through Off (abort)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := append([]client.Object{mkPlan(tc.mutate)}, cliNamespaces()...)
			c := cliFixture(t, objs...)
			setEntries(t, c, tc.entries()...)
			exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
				return parseStatus(a, o, e)
			}, "demo")
			code, stdout, _ := runExec(exec, c)
			if code != 0 {
				t.Fatalf("exit %d, stdout:\n%s", code, stdout)
			}
			if !strings.Contains(stdout, tc.want) {
				t.Errorf("stdout lacks %q:\n%s", tc.want, stdout)
			}
		})
	}
}

func TestStatus_Errors(t *testing.T) {
	c := cliFixture(t, cliNamespaces()...)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseStatus(a, o, e)
	}, "demo")
	if code, _, stderr := runExec(exec, c); code != 1 || !strings.Contains(stderr, "not found") {
		t.Errorf("missing plan: exit %d stderr %q, want 1 + not found", code, stderr)
	}
	bad := mkPlan(func(p *v1alpha1.ShardPlan) { p.Spec.Canary.WeightPerMille = 1001 })
	c2 := cliFixture(t, bad)
	if code, _, stderr := runExec(exec, c2); code != 1 || !strings.Contains(stderr, "invalid") {
		t.Errorf("malformed plan: exit %d stderr %q, want 1 + invalid", code, stderr)
	}
}

func TestStatus_NoCanaryDeployed(t *testing.T) {
	p := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Tracks.Canary = nil
		p.Spec.Canary.Mode = "Off"
		p.Spec.Canary.WeightPerMille = 0
	})
	c := cliFixture(t, p)
	setEntries(t, c, inSyncEntries()[:1]...)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseStatus(a, o, e)
	}, "demo")
	code, stdout, _ := runExec(exec, c)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, stdout)
	}
	if !strings.Contains(stdout, "canary: not deployed") {
		t.Errorf("stdout lacks not-deployed line:\n%s", stdout)
	}
}

func TestExplain_Rules(t *testing.T) {
	off := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Canary.Mode = "Off"
		p.Spec.Canary.WeightPerMille = 0
	})
	inc := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Canary.WeightPerMille = 0
		p.Spec.Canary.Include = v1alpha1.IncludeSpec{Namespaces: []string{"demo-87"}}
	})
	inc.Name = "inc"
	off.Name = "off"
	objs := append([]client.Object{mkPlan(nil), off, inc}, cliNamespaces()...)
	c := cliFixture(t, objs...)
	cases := []struct {
		plan string
		ns   string
		want []string
	}{
		{"demo", "demo-87", []string{"demo-87 -> canary (rule: window)", "covers it"}},
		{"demo", "critical-db", []string{"critical-db -> stable (rule: exclude)", "tier=critical"}},
		{"off", "demo-87", []string{"demo-87 -> stable (rule: mode)"}},
		{"inc", "demo-87", []string{"demo-87 -> canary (rule: include)"}},
	}
	for _, tc := range cases {
		exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
			return parseExplain(a, o, e)
		}, tc.plan, tc.ns)
		code, stdout, stderr := runExec(exec, c)
		if code != 0 {
			t.Fatalf("%s/%s: exit %d stderr %q", tc.plan, tc.ns, code, stderr)
		}
		for _, w := range tc.want {
			if !strings.Contains(stdout, w) {
				t.Errorf("%s/%s: stdout lacks %q:\n%s", tc.plan, tc.ns, w, stdout)
			}
		}
	}
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseExplain(a, o, e)
	}, "demo", "ghost")
	if code, _, stderr := runExec(exec, c); code != 1 || !strings.Contains(stderr, "not found") {
		t.Errorf("ghost ns: exit %d stderr %q", code, stderr)
	}
}

func TestSimulate_Distribution(t *testing.T) {
	objs := append([]client.Object{mkPlan(nil)}, cliNamespaces()...)
	c := cliFixture(t, objs...)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSimulate(a, o, e)
	}, "demo")
	code, stdout, _ := runExec(exec, c)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, stdout)
	}
	// Full weight routes demo-87 to canary; the tier=critical
	// namespace stays stable via the exclude selector.
	for _, want := range []string{"stable: 1", "canary: 1", "no writes made", "demo-87", "critical-db"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout lacks %q:\n%s", want, stdout)
		}
	}
}

func TestSimulate_OverridesAndQuiet(t *testing.T) {
	objs := append([]client.Object{mkPlan(nil)}, cliNamespaces()...)
	c := cliFixture(t, objs...)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSimulate(a, o, e)
	}, "demo", "--weight", "0", "-q")
	code, stdout, _ := runExec(exec, c)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, stdout)
	}
	if !strings.Contains(stdout, "stable: 2") || strings.Contains(stdout, "demo-87") {
		t.Errorf("quiet weight-0 output wrong:\n%s", stdout)
	}
	exec = mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSimulate(a, o, e)
	}, "demo", "--namespaces", "demo-87")
	code, stdout, _ = runExec(exec, c)
	if code != 0 || !strings.Contains(stdout, "canary: 1") || !strings.Contains(stdout, "(1 namespaces") {
		t.Errorf("scoped output wrong (exit %d):\n%s", code, stdout)
	}
}

func TestSimulate_ModeWeightHint(t *testing.T) {
	off := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Canary.Mode = "Off"
		p.Spec.Canary.WeightPerMille = 0
	})
	c := cliFixture(t, off)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSimulate(a, o, e)
	}, "demo", "--weight", "100")
	if code, _, stderr := runExec(exec, c); code != 1 || !strings.Contains(stderr, "--mode Active") {
		t.Errorf("exit %d stderr %q, want --mode Active hint", code, stderr)
	}
}

func TestSimulate_NoWrites(t *testing.T) {
	objs := append([]client.Object{mkPlan(nil)}, cliNamespaces()...)
	c := cliFixture(t, objs...)
	setEntries(t, c, inSyncEntries()...)
	ctx := context.Background()
	var before v1alpha1.ShardPlan
	if err := c.Get(ctx, types.NamespacedName{Namespace: cliNS, Name: "demo"}, &before); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"demo"}, {"demo", "--weight", "500"}, {"demo", "-q"}} {
		exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
			return parseSimulate(a, o, e)
		}, args...)
		if code, _, stderr := runExec(exec, c); code != 0 {
			t.Fatalf("%v: exit %d stderr %q", args, code, stderr)
		}
	}
	var after v1alpha1.ShardPlan
	if err := c.Get(ctx, types.NamespacedName{Namespace: cliNS, Name: "demo"}, &after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("simulate mutated the plan:\nbefore: %+v\nafter: %+v", before, after)
	}
}

func TestSetWeight_HappyPath(t *testing.T) {
	p := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Epoch = 1
		p.Spec.Canary.Mode = "Off"
		p.Spec.Canary.WeightPerMille = 0
	})
	c := cliFixture(t, p)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSetWeight(a, o, e)
	}, "demo", "100", "--mode", "Active")
	code, stdout, _ := runExec(exec, c)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, stdout)
	}
	if !strings.Contains(stdout, "weight 0 -> 100") || !strings.Contains(stdout, "epoch 2") {
		t.Errorf("stdout lacks transition line:\n%s", stdout)
	}
	if !strings.Contains(stdout, "watch: kubectl shardplan status demo -n cli-test") {
		t.Errorf("stdout lacks watch hint:\n%s", stdout)
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: cliNS, Name: "demo"}, &live); err != nil {
		t.Fatal(err)
	}
	if live.Spec.Canary.Mode != "Active" || live.Spec.Canary.WeightPerMille != 100 || live.Spec.Epoch != 2 {
		t.Fatalf("live spec = %+v, want Active/100/epoch 2", live.Spec)
	}
}

func TestSetWeight_ErrorsAndNoOp(t *testing.T) {
	off := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Epoch = 1
		p.Spec.Canary.Mode = "Off"
		p.Spec.Canary.WeightPerMille = 0
	})
	c := cliFixture(t, off)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSetWeight(a, o, e)
	}, "demo", "100")
	if code, _, stderr := runExec(exec, c); code != 1 || !strings.Contains(stderr, "--mode Active") {
		t.Errorf("Off+weight: exit %d stderr %q, want --mode Active hint", code, stderr)
	}
	active := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Canary.WeightPerMille = 100
	})
	c2 := cliFixture(t, active)
	exec2 := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSetWeight(a, o, e)
	}, "demo", "100")
	code, stdout, _ := runExec(exec2, c2)
	if code != 0 || !strings.Contains(stdout, "no-op") {
		t.Errorf("same weight: exit %d stdout %q, want no-op", code, stdout)
	}
	var live v1alpha1.ShardPlan
	if err := c2.Get(context.Background(), types.NamespacedName{Namespace: cliNS, Name: "demo"}, &live); err != nil {
		t.Fatal(err)
	}
	if live.Spec.Epoch != 2 {
		t.Errorf("no-op bumped epoch to %d", live.Spec.Epoch)
	}
	exec3 := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSetWeight(a, o, e)
	}, "demo", "200", "--epoch", "1")
	if code, _, stderr := runExec(exec3, c2); code != 1 || !strings.Contains(stderr, "must exceed") {
		t.Errorf("epoch regression: exit %d stderr %q", code, stderr)
	}
}

func TestSetWeight_ZeroWarnsOnIncludes(t *testing.T) {
	p := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Canary.Include = v1alpha1.IncludeSpec{Namespaces: []string{"demo-87"}}
	})
	c := cliFixture(t, p)
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSetWeight(a, o, e)
	}, "demo", "0")
	code, _, stderr := runExec(exec, c)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(stderr, "warning") || !strings.Contains(stderr, "abort") {
		t.Errorf("stderr lacks include warning: %q", stderr)
	}
}

func TestAbort_FlipAndNoOp(t *testing.T) {
	c := cliFixture(t, mkPlan(nil))
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseAbort(a, o, e)
	}, "demo")
	code, stdout, _ := runExec(exec, c)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, stdout)
	}
	if !strings.Contains(stdout, "aborted") || !strings.Contains(stdout, "epoch 3") {
		t.Errorf("stdout lacks abort line:\n%s", stdout)
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: cliNS, Name: "demo"}, &live); err != nil {
		t.Fatal(err)
	}
	if live.Spec.Canary.Mode != "Off" || live.Spec.Canary.WeightPerMille != 0 || live.Spec.Epoch != 3 {
		t.Fatalf("live spec = %+v, want Off/0/epoch 3", live.Spec)
	}
	off := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Canary.Mode = "Off"
		p.Spec.Canary.WeightPerMille = 0
	})
	c2 := cliFixture(t, off)
	code, stdout, _ = runExec(exec, c2)
	if code != 0 || !strings.Contains(stdout, "no-op") {
		t.Errorf("re-abort: exit %d stdout %q, want no-op", code, stdout)
	}
}

// flakyUpdateClient fails the first failsLeft Updates with a
// Conflict, then delegates; it pins optimistic-concurrency retries.
type flakyUpdateClient struct {
	client.Client
	failsLeft int
	calls     int
}

func (f *flakyUpdateClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	f.calls++
	if f.failsLeft > 0 {
		f.failsLeft--
		return apierrors.NewConflict(
			schema.GroupResource{Group: "shardkit.dev", Resource: "shardplans"},
			obj.GetName(), fmt.Errorf("injected conflict"))
	}
	return f.Client.Update(ctx, obj, opts...)
}

func TestSetWeight_ConflictRetry(t *testing.T) {
	off := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Epoch = 1
		p.Spec.Canary.Mode = "Off"
		p.Spec.Canary.WeightPerMille = 0
	})
	c := &flakyUpdateClient{Client: cliFixture(t, off), failsLeft: 2}
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSetWeight(a, o, e)
	}, "demo", "100", "--mode", "Active")
	code, stdout, _ := runExec(exec, c)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, stdout)
	}
	if c.calls != 3 {
		t.Errorf("update calls = %d, want 3 (2 conflicts + success)", c.calls)
	}
	var live v1alpha1.ShardPlan
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: cliNS, Name: "demo"}, &live); err != nil {
		t.Fatal(err)
	}
	if live.Spec.Epoch != 2 || live.Spec.Canary.WeightPerMille != 100 {
		t.Fatalf("live spec = %+v, want epoch 2 weight 100", live.Spec)
	}
}

func TestSetWeight_ConflictExhausted(t *testing.T) {
	off := mkPlan(func(p *v1alpha1.ShardPlan) {
		p.Spec.Epoch = 1
		p.Spec.Canary.Mode = "Off"
		p.Spec.Canary.WeightPerMille = 0
	})
	c := &flakyUpdateClient{Client: cliFixture(t, off), failsLeft: 100}
	exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
		return parseSetWeight(a, o, e)
	}, "demo", "100", "--mode", "Active")
	code, _, stderr := runExec(exec, c)
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(stderr, "changed concurrently") || !strings.Contains(stderr, "re-run: kubectl shardplan set-weight demo 100") {
		t.Errorf("stderr lacks actionable retry:\n%s", stderr)
	}
}

func TestRun_Dispatch(t *testing.T) {
	cases := []struct {
		name string
		args []string
		code int
		want string
	}{
		{"no args", nil, 2, "Usage:"},
		{"unknown command", []string{"warp"}, 2, "unknown command"},
		{"help", []string{"--help"}, 0, "Usage:"},
		{"version", []string{"--version"}, 0, "kubectl-shardplan"},
		{"status help", []string{"status", "--help"}, 0, "status:"},
		{"status missing plan", []string{"status"}, 2, "exactly one PLAN"},
		{"explain missing ns", []string{"explain", "demo"}, 2, "PLAN and a NAMESPACE"},
		{"bad weight", []string{"set-weight", "demo", "abc"}, 2, "not a number"},
		{"weight range", []string{"set-weight", "demo", "1001"}, 2, "not a number"},
		{"bad mode", []string{"simulate", "demo", "--mode", "Warp"}, 2, "invalid"},
		{"unknown flag", []string{"status", "demo", "--bogus"}, 2, "unknown flag --bogus"},
		{"missing value", []string{"simulate", "demo", "--weight"}, 2, "--weight needs a value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != tc.code {
				t.Errorf("exit %d, want %d (stdout %q stderr %q)",
					code, tc.code, stdout.String(), stderr.String())
			}
			if combined := stdout.String() + stderr.String(); !strings.Contains(combined, tc.want) {
				t.Errorf("output lacks %q (stdout %q stderr %q)",
					tc.want, stdout.String(), stderr.String())
			}
		})
	}
}

func TestParse_InterspersedFlags(t *testing.T) {
	// Flags work before, between, and after positionals, in both
	// --flag value and --flag=value forms.
	objs := append([]client.Object{mkPlan(nil)}, cliNamespaces()...)
	c := cliFixture(t, objs...)
	argSets := [][]string{
		{"demo", "--weight", "0", "-q"},
		{"--weight", "0", "demo", "-q"},
		{"demo", "--weight=0", "--quiet"},
	}
	for _, args := range argSets {
		exec := mustParse(t, func(a []string, o, e *bytes.Buffer) (execFunc, bool) {
			return parseSimulate(a, o, e)
		}, args...)
		code, stdout, _ := runExec(exec, c)
		if code != 0 || !strings.Contains(stdout, "stable: 2") || strings.Contains(stdout, "demo-87") {
			t.Errorf("%v: exit %d stdout:\n%s", args, code, stdout)
		}
	}
}

func TestRun_KubeconfigError(t *testing.T) {
	// Unknown cluster paths fail before any network use, with the
	// fix inline, whether globals come before or after the command.
	for _, args := range [][]string{
		{"--kubeconfig", "/nonexistent-xyz", "status", "demo"},
		{"status", "demo", "--kubeconfig", "/nonexistent-xyz"},
	} {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		if code != 1 || !strings.Contains(stderr.String(), "cannot load kubeconfig") {
			t.Errorf("%v: exit %d stderr %q", args, code, stderr.String())
		}
	}
}
