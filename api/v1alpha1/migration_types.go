package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Migration is the controller-backed durable offline PVC migration resource.
// Its API contract is independent from the internal session state machine;
// the CRD session store performs the explicit adapter conversion.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=mig
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Migration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MigrationSpec   `json:"spec"`
	Status MigrationStatus `json:"status,omitempty"`
}

// MigrationList contains Migration resources.
// +kubebuilder:object:root=true
type MigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Migration `json:"items"`
}
