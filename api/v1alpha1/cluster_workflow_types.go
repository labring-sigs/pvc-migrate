package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Cluster workflow specs and statuses are distinct API types. They currently
// expose the same operation capabilities as their namespaced counterparts,
// while the cluster scope permits explicit cross-namespace references. The
// separate types leave room for cluster-only fields without weakening the
// namespaced schemas.
type (
	ClusterMigrationSpec    MigrationSpec
	ClusterPodMigrationSpec PodMigrationSpec
	ClusterReservationSpec  ReservationSpec
	ClusterCopySpec         CopySpec
	ClusterRenameSpec       RenameSpec
	ClusterMoveSpec         MoveSpec
)

type (
	ClusterMigrationStatus    MigrationStatus
	ClusterPodMigrationStatus PodMigrationStatus
	ClusterReservationStatus  ReservationStatus
	ClusterCopyStatus         CopyStatus
	ClusterBackupStatus       BackupStatus
	ClusterRestoreStatus      RestoreStatus
	ClusterRenameStatus       RenameStatus
	ClusterMoveStatus         MoveStatus
)

// ClusterBackupSpec is the cluster-scoped form of BackupSpec. The source and
// repository namespaces are explicit because metadata.namespace is empty on a
// cluster resource.
type ClusterBackupSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	SourceNamespace string `json:"sourceNamespace" yaml:"sourceNamespace"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	SessionNamespace string          `json:"sessionNamespace"    yaml:"sessionNamespace"`
	CreatedBy        string          `json:"createdBy,omitempty" yaml:"createdBy,omitempty"`
	SourcePVC        ObjectReference `json:"sourcePVC"           yaml:"sourcePVC"`
	SourcePV         ObjectReference `json:"sourcePV"            yaml:"sourcePV"`
	Path             string          `json:"path,omitempty"      yaml:"path,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name                   string              `json:"name"                             yaml:"name"`
	RepositoryRef          RepositoryReference `json:"repositoryRef"                    yaml:"repositoryRef"`
	Online                 bool                `json:"online,omitempty"                 yaml:"online,omitempty"`
	OpenEBSLVMEnableShared bool                `json:"openebsLvmEnableShared,omitempty" yaml:"openebsLvmEnableShared,omitempty"`
	ToolImage              string              `json:"toolImage,omitempty"              yaml:"toolImage,omitempty"`
	DeleteExtraneous       bool                `json:"deleteExtraneous,omitempty"       yaml:"deleteExtraneous,omitempty"`
}

// ClusterRestoreSpec is the cluster-scoped form of RestoreSpec. A repository
// namespace is mandatory and is resolved by the controller using its own
// cluster-level RBAC.
type ClusterRestoreSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	DestinationNamespace string `json:"destinationNamespace" yaml:"destinationNamespace"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	SessionNamespace string          `json:"sessionNamespace"    yaml:"sessionNamespace"`
	CreatedBy        string          `json:"createdBy,omitempty" yaml:"createdBy,omitempty"`
	DestinationPVC   ObjectReference `json:"destinationPVC"      yaml:"destinationPVC"`
	Path             string          `json:"path,omitempty"      yaml:"path,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name                    string              `json:"name"                              yaml:"name"`
	RepositoryRef           RepositoryReference `json:"repositoryRef"                     yaml:"repositoryRef"`
	CreatePVC               bool                `json:"createPVC,omitempty"               yaml:"createPVC,omitempty"`
	DestinationStorageClass string              `json:"destinationStorageClass,omitempty" yaml:"destinationStorageClass,omitempty"`
	DestinationAccessMode   string              `json:"destinationAccessMode,omitempty"   yaml:"destinationAccessMode,omitempty"`
	DestinationCapacity     string              `json:"destinationCapacity,omitempty"     yaml:"destinationCapacity,omitempty"`
	AllowMounted            bool                `json:"allowMounted,omitempty"            yaml:"allowMounted,omitempty"`
	TargetNode              string              `json:"targetNode,omitempty"              yaml:"targetNode,omitempty"`
	ToolImage               string              `json:"toolImage,omitempty"               yaml:"toolImage,omitempty"`
	DeleteExtraneous        bool                `json:"deleteExtraneous,omitempty"        yaml:"deleteExtraneous,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=cmig
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterMigration struct {
	metav1.TypeMeta   `                       json:",inline"`
	metav1.ObjectMeta `                       json:"metadata,omitempty"`
	Spec              ClusterMigrationSpec   `json:"spec"`
	Status            ClusterMigrationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterMigrationList struct {
	metav1.TypeMeta `                   json:",inline"`
	metav1.ListMeta `                   json:"metadata,omitempty"`
	Items           []ClusterMigration `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=cpmig
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterPodMigration struct {
	metav1.TypeMeta   `                          json:",inline"`
	metav1.ObjectMeta `                          json:"metadata,omitempty"`
	Spec              ClusterPodMigrationSpec   `json:"spec"`
	Status            ClusterPodMigrationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterPodMigrationList struct {
	metav1.TypeMeta `                      json:",inline"`
	metav1.ListMeta `                      json:"metadata,omitempty"`
	Items           []ClusterPodMigration `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=cresv
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterReservation struct {
	metav1.TypeMeta   `                         json:",inline"`
	metav1.ObjectMeta `                         json:"metadata,omitempty"`
	Spec              ClusterReservationSpec   `json:"spec"`
	Status            ClusterReservationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterReservationList struct {
	metav1.TypeMeta `                     json:",inline"`
	metav1.ListMeta `                     json:"metadata,omitempty"`
	Items           []ClusterReservation `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=ccopy
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterCopy struct {
	metav1.TypeMeta   `                  json:",inline"`
	metav1.ObjectMeta `                  json:"metadata,omitempty"`
	Spec              ClusterCopySpec   `json:"spec"`
	Status            ClusterCopyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterCopyList struct {
	metav1.TypeMeta `              json:",inline"`
	metav1.ListMeta `              json:"metadata,omitempty"`
	Items           []ClusterCopy `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=cback
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterBackup struct {
	metav1.TypeMeta   `                    json:",inline"`
	metav1.ObjectMeta `                    json:"metadata,omitempty"`
	Spec              ClusterBackupSpec   `json:"spec"`
	Status            ClusterBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterBackupList struct {
	metav1.TypeMeta `                json:",inline"`
	metav1.ListMeta `                json:"metadata,omitempty"`
	Items           []ClusterBackup `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=crest
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterRestore struct {
	metav1.TypeMeta   `                     json:",inline"`
	metav1.ObjectMeta `                     json:"metadata,omitempty"`
	Spec              ClusterRestoreSpec   `json:"spec"`
	Status            ClusterRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterRestoreList struct {
	metav1.TypeMeta `                 json:",inline"`
	metav1.ListMeta `                 json:",inline"`
	Items           []ClusterRestore `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=crename
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterRename struct {
	metav1.TypeMeta   `                    json:",inline"`
	metav1.ObjectMeta `                    json:"metadata,omitempty"`
	Spec              ClusterRenameSpec   `json:"spec"`
	Status            ClusterRenameStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterRenameList struct {
	metav1.TypeMeta `                json:",inline"`
	metav1.ListMeta `                json:",inline"`
	Items           []ClusterRename `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=cmove
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterMove struct {
	metav1.TypeMeta   `                  json:",inline"`
	metav1.ObjectMeta `                  json:"metadata,omitempty"`
	Spec              ClusterMoveSpec   `json:"spec"`
	Status            ClusterMoveStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterMoveList struct {
	metav1.TypeMeta `              json:",inline"`
	metav1.ListMeta `              json:",inline"`
	Items           []ClusterMove `json:"items"`
}
