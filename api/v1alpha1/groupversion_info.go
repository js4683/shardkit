// Package v1alpha1 holds the ShardPlan and Widget API types. Field
// names and semantics follow docs/shardplan-spec.md. Validation
// lives in validation.go; DeepCopy methods are controller-gen
// output (make generate); CRD manifests come from the same
// generator (make manifests).
//
// +kubebuilder:object:generate=true
// +groupName=shardkit.dev
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the prototype API group and version.
	GroupVersion = schema.GroupVersion{Group: "shardkit.dev", Version: "v1alpha1"}

	// SchemeBuilder registers the prototype types.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the prototype types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
