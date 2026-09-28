package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WidgetSpec is the prototype workload: a value the owning track
// copies into status alongside its identity.
type WidgetSpec struct {
	// +optional
	Value string `json:"value,omitempty"`
}

// WidgetStatus records which track last wrote the object. The M0
// trace reads this as the write audit.
type WidgetStatus struct {
	// +optional
	OwnerTrack string `json:"ownerTrack,omitempty"`
	// +optional
	OwnerRevision string `json:"ownerRevision,omitempty"`
	// Writes counts status writes by the current owner since it
	// acquired the widget. The prototype stamps once per
	// acquisition and skips already-correct stamps, so this is 1
	// at rest; a larger value means repeated ownership flips.
	// +optional
	Writes int64 `json:"writes,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Widget is the Schema for the prototype workload API.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=w
type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WidgetSpec   `json:"spec,omitempty"`
	Status WidgetStatus `json:"status,omitempty"`
}

// WidgetList contains a list of Widget.
//
// +kubebuilder:object:root=true
type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Widget `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Widget{}, &WidgetList{})
}
