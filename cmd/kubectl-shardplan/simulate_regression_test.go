package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSimulate_UniqueNamespaces(t *testing.T) {
	c := cliFixture(t, append([]client.Object{mkPlan(nil)}, cliNamespaces()...)...)
	var out, errOut bytes.Buffer
	exec, ok := parseSimulate([]string{"demo", "--namespaces", "demo-87, critical-db, demo-87,,critical-db"}, &out, &errOut)
	if !ok {
		t.Fatal(errOut.String())
	}
	code, stdout, stderr := runExec(exec, c)
	if code != 0 || !strings.Contains(stdout, "stable: 1  canary: 1  (2 namespaces") {
		t.Fatalf("exit %d stdout %q stderr %q", code, stdout, stderr)
	}
}

func TestSimulate_EmptyNamespaceSelection(t *testing.T) {
	for _, raw := range []string{"", " ", ", ,"} {
		t.Run(fmt.Sprintf("%q", raw), func(t *testing.T) {
			var out, errOut bytes.Buffer
			_, ok := parseSimulate([]string{"demo", "--namespaces=" + raw}, &out, &errOut)
			if ok || !strings.Contains(errOut.String(), "names nothing") {
				t.Fatalf("accepted empty selection: ok=%v error=%q", ok, errOut.String())
			}
		})
	}
}

type namespaceReadErrorClient struct {
	client.Client
	err error
}

func (c namespaceReadErrorClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Namespace); ok {
		return c.err
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestSimulate_NamespaceReadErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"missing", apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, "demo-87"), "not found"},
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "demo-87", fmt.Errorf("access denied")), "forbidden"},
		{"timeout", context.DeadlineExceeded, "context deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := namespaceReadErrorClient{Client: cliFixture(t, mkPlan(nil)), err: tc.err}
			var out, errOut bytes.Buffer
			exec, ok := parseSimulate([]string{"demo", "--namespaces", "demo-87"}, &out, &errOut)
			if !ok {
				t.Fatal(errOut.String())
			}
			code, stdout, stderr := runExec(exec, c)
			if code != 1 || stdout != "" || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit %d stdout %q stderr %q", code, stdout, stderr)
			}
		})
	}
}

func TestSimulate_TableLimitAndQuietCounts(t *testing.T) {
	objects := []client.Object{mkPlan(nil)}
	for i := 104; i >= 0; i-- {
		objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ns-%03d", i)}})
	}
	c := cliFixture(t, objects...)
	for _, quiet := range []bool{false, true} {
		var stdout, stderr bytes.Buffer
		code := execSimulate(context.Background(), c, cliNS, simulateOpts{plan: "demo", weight: -1, quiet: quiet}, &stdout, &stderr)
		output := stdout.String()
		if code != 0 || !strings.Contains(output, "canary: 105  (105 namespaces") {
			t.Fatalf("exit %d stdout %q stderr %q", code, output, stderr.String())
		}
		if quiet {
			if strings.Contains(output, "ns-") || strings.Contains(output, "more rows") {
				t.Fatalf("quiet output includes table: %s", output)
			}
			continue
		}
		if !strings.Contains(output, "5 more rows cut") || strings.Contains(output, "ns-100") {
			t.Fatalf("table not capped: %s", output)
		}
		previous := -1
		for i := 0; i < 100; i++ {
			position := strings.Index(output, fmt.Sprintf("ns-%03d", i))
			if position <= previous {
				t.Fatalf("missing or unsorted row %d", i)
			}
			previous = position
		}
	}
}
