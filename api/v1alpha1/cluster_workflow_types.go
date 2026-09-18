package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NamespaceName is a Kubernetes namespace name used by a cluster-scoped
// workflow. Namespaced workflows derive this boundary from metadata.namespace.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
type NamespaceName string

// +kubebuilder:validation:XValidation:rule="has(self.volumes) && size(self.volumes) > 0",message="volumes must contain at least one source PVC"
type ClusterMigrationPlan struct {
	// +kubebuilder:validation:Enum=Keep;Delete
	UnusedStoragePolicy  UnusedStoragePolicy `json:"unusedStoragePolicy,omitempty" yaml:"unusedStoragePolicy,omitempty"`
	SourceNamespace      NamespaceName       `json:"sourceNamespace"               yaml:"sourceNamespace"`
	TemporaryNamespace   NamespaceName       `json:"temporaryNamespace"            yaml:"temporaryNamespace"`
	DestinationNamespace NamespaceName       `json:"destinationNamespace"          yaml:"destinationNamespace"`
	SessionNamespace     NamespaceName       `json:"sessionNamespace"              yaml:"sessionNamespace"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes    []VolumeSpec `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
	SourceNode string       `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	TargetNode string       `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	ToolImage  string       `json:"toolImage,omitempty"  yaml:"toolImage,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Strategies           []string `json:"strategies,omitempty"           yaml:"strategies,omitempty"`
	VerifyChecksum       bool     `json:"verifyChecksum,omitempty"       yaml:"verifyChecksum,omitempty"`
	DeleteExtraneous     bool     `json:"deleteExtraneous,omitempty"     yaml:"deleteExtraneous,omitempty"`
	SkipSourceUsageCheck bool     `json:"skipSourceUsageCheck,omitempty" yaml:"skipSourceUsageCheck,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="has(self.volumes) && size(self.volumes) > 0",message="volumes must contain at least one source PVC"
// +kubebuilder:validation:XValidation:rule="self.workload.adapter != 'None'",message="ClusterPodMigration workload.adapter must identify a supported workload"
type ClusterPodMigrationPlan struct {
	// +kubebuilder:validation:Enum=Keep;Delete
	UnusedStoragePolicy UnusedStoragePolicy `json:"unusedStoragePolicy,omitempty" yaml:"unusedStoragePolicy,omitempty"`
	// Pod migration preserves workload and PVC identities in SourceNamespace.
	// TemporaryNamespace and SessionNamespace are the only cross-namespace roles.
	SourceNamespace    NamespaceName `json:"sourceNamespace"    yaml:"sourceNamespace"`
	TemporaryNamespace NamespaceName `json:"temporaryNamespace" yaml:"temporaryNamespace"`
	SessionNamespace   NamespaceName `json:"sessionNamespace"   yaml:"sessionNamespace"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes    []VolumeSpec `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
	SourceNode string       `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	TargetNode string       `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	ToolImage  string       `json:"toolImage,omitempty"  yaml:"toolImage,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Strategies             []string     `json:"strategies,omitempty"             yaml:"strategies,omitempty"`
	VerifyChecksum         bool         `json:"verifyChecksum,omitempty"         yaml:"verifyChecksum,omitempty"`
	DeleteExtraneous       bool         `json:"deleteExtraneous,omitempty"       yaml:"deleteExtraneous,omitempty"`
	SkipSourceUsageCheck   bool         `json:"skipSourceUsageCheck,omitempty"   yaml:"skipSourceUsageCheck,omitempty"`
	Workload               WorkloadSpec `json:"workload"                         yaml:"workload"`
	PrecopyPasses          int          `json:"precopyPasses"                    yaml:"precopyPasses"`
	OpenEBSLVMEnableShared bool         `json:"openebsLvmEnableShared,omitempty" yaml:"openebsLvmEnableShared,omitempty"`
}

type ClusterReservationPlan struct {
	ReservationPlan      `              json:",inline"              yaml:",inline"`
	SourceNamespace      NamespaceName `json:"sourceNamespace"      yaml:"sourceNamespace"`
	DestinationNamespace NamespaceName `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     NamespaceName `json:"sessionNamespace"     yaml:"sessionNamespace"`
}

type ClusterCopyPlan struct {
	CopyPlan             `              json:",inline"              yaml:",inline"`
	SourceNamespace      NamespaceName `json:"sourceNamespace"      yaml:"sourceNamespace"`
	DestinationNamespace NamespaceName `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     NamespaceName `json:"sessionNamespace"     yaml:"sessionNamespace"`
}

type MoveIdentity struct {
	SourcePVC      LocalResourceReference `json:"sourcePVC"      yaml:"sourcePVC"`
	SourcePV       LocalResourceReference `json:"sourcePV"       yaml:"sourcePV"`
	DestinationPVC LocalResourceReference `json:"destinationPVC" yaml:"destinationPVC"`
	SourceTemplate PVCSourceTemplate      `json:"sourceTemplate" yaml:"sourceTemplate"`
}

type MovePlan struct {
	SourceNamespace      NamespaceName `json:"sourceNamespace"      yaml:"sourceNamespace"`
	DestinationNamespace NamespaceName `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     NamespaceName `json:"sessionNamespace"     yaml:"sessionNamespace"`
	Identity             MoveIdentity  `json:"identity"             yaml:"identity"`
}

// Cluster status types use fully qualified references. A cluster-scoped
// workflow can span namespaces, so checkpoints remain independently auditable.
type ClusterVolumeActivationStatus struct {
	TemporaryPVCDeleted bool             `json:"temporaryPVCDeleted,omitempty" yaml:"temporaryPVCDeleted,omitempty"`
	SourcePVCDeleted    bool             `json:"sourcePVCDeleted,omitempty"    yaml:"sourcePVCDeleted,omitempty"`
	DestinationReserved bool             `json:"destinationReserved,omitempty" yaml:"destinationReserved,omitempty"`
	ActivePVC           *ObjectReference `json:"activePVC,omitempty"           yaml:"activePVC,omitempty"`
	ActivatedAt         *metav1.Time     `json:"activatedAt,omitempty"         yaml:"activatedAt,omitempty"`
	RolledBackAt        *metav1.Time     `json:"rolledBackAt,omitempty"        yaml:"rolledBackAt,omitempty"`
}

type ClusterPodMigrationWorkloadStatus struct {
	Pod          *ObjectReference  `json:"pod,omitempty"          yaml:"pod,omitempty"`
	AffectedPods []ObjectReference `json:"affectedPods,omitempty" yaml:"affectedPods,omitempty"`
	// VMCluster carries the controller's pause-probe outcomes (such as
	// whether the CRD kept the per-component paused field) so resume and
	// restore reuse the semantics recorded during the pause.
	VMCluster *VMClusterSpec `json:"vmCluster,omitempty" yaml:"vmCluster,omitempty"`
}

// ClusterVolumeReservationStatus is the storage-provisioning checkpoint shared by
// workflows that reserve a destination volume. Copy and activation progress
// remain in their owning operation types.
type ClusterVolumeReservationStatus struct {
	SourcePVCName     string           `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *ObjectReference `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *ObjectReference `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy  `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool             `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
}

type ClusterMigrationVolumeStatus struct {
	ClusterVolumeReservationStatus `                              json:",inline"    yaml:",inline"`
	Sync                           MigrationSyncStatus           `json:"sync"       yaml:"sync"`
	Activation                     ClusterVolumeActivationStatus `json:"activation" yaml:"activation"`
}

type ClusterPodMigrationVolumeStatus struct {
	ClusterVolumeReservationStatus `                              json:",inline"    yaml:",inline"`
	Sync                           PodMigrationSyncStatus        `json:"sync"       yaml:"sync"`
	Activation                     ClusterVolumeActivationStatus `json:"activation" yaml:"activation"`
}

type ClusterReservationVolumeStatus struct {
	ClusterVolumeReservationStatus `json:",inline" yaml:",inline"`
}

type ClusterCopyVolumeStatus struct {
	ClusterVolumeReservationStatus `               json:",inline" yaml:",inline"`
	Sync                           CopySyncStatus `json:"sync"    yaml:"sync"`
}

type MoveActivationStatus struct {
	ActivePVC    *ObjectReference `json:"activePVC,omitempty"    yaml:"activePVC,omitempty"`
	ActivatedAt  *metav1.Time     `json:"activatedAt,omitempty"  yaml:"activatedAt,omitempty"`
	RolledBackAt *metav1.Time     `json:"rolledBackAt,omitempty" yaml:"rolledBackAt,omitempty"`
}

type ClusterMigrationStatus struct {
	Plan           *ClusterMigrationPlan `json:"plan,omitempty"    yaml:"plan,omitempty"`
	WorkflowStatus `                               json:",inline"           yaml:",inline"`
	Volumes        []ClusterMigrationVolumeStatus `json:"volumes,omitempty" yaml:"volumes,omitempty"`
}

type ClusterPodMigrationStatus struct {
	Plan                    *ClusterPodMigrationPlan `json:"plan,omitempty"                    yaml:"plan,omitempty"`
	WorkflowStatus          `                                   json:",inline"                           yaml:",inline"`
	WarmPassesCompleted     int                                `json:"warmPassesCompleted"               yaml:"warmPassesCompleted"`
	OriginalPodSnapshotHash string                             `json:"originalPodSnapshotHash,omitempty" yaml:"originalPodSnapshotHash,omitempty"`
	Workload                *ClusterPodMigrationWorkloadStatus `json:"workload,omitempty"                yaml:"workload,omitempty"`
	Volumes                 []ClusterPodMigrationVolumeStatus  `json:"volumes,omitempty"                 yaml:"volumes,omitempty"`
	OpenEBSLVMSharedMounts  []SharedMountStatus                `json:"openebsLvmSharedMounts,omitempty"  yaml:"openebsLvmSharedMounts,omitempty"`
}

type ClusterReservationStatus struct {
	Plan           *ClusterReservationPlan `json:"plan,omitempty"    yaml:"plan,omitempty"`
	WorkflowStatus `                                 json:",inline"           yaml:",inline"`
	Volumes        []ClusterReservationVolumeStatus `json:"volumes,omitempty" yaml:"volumes,omitempty"`
}

type ClusterCopyStatus struct {
	// SourceNode checkpoints runtime placement inferred from online consumers.
	SourceNode     string           `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	Plan           *ClusterCopyPlan `json:"plan,omitempty"       yaml:"plan,omitempty"`
	WorkflowStatus `                          json:",inline"              yaml:",inline"`
	Volumes        []ClusterCopyVolumeStatus `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
}

type MoveStatus struct {
	Plan           *MovePlan `json:"plan,omitempty" yaml:"plan,omitempty"`
	WorkflowStatus `                     json:",inline"        yaml:",inline"`
	Activation     MoveActivationStatus `json:"activation"     yaml:"activation"`
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
// +kubebuilder:resource:scope=Cluster,shortName=move
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Move struct {
	metav1.TypeMeta   `           json:",inline"`
	metav1.ObjectMeta `           json:"metadata,omitempty"`
	Spec              MoveSpec   `json:"spec"`
	Status            MoveStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type MoveList struct {
	metav1.TypeMeta `       json:",inline"`
	metav1.ListMeta `       json:"metadata,omitempty"`
	Items           []Move `json:"items"`
}
