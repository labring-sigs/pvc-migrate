//nolint:golines // Durable API fields keep explicit JSON and YAML tags together.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NamespaceName is a Kubernetes namespace name used by a cluster-scoped
// workflow. Namespaced workflows derive this boundary from metadata.namespace.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
type NamespaceName string

// ClusterVolumeSpec is the cluster-workflow planning contract. PVC names are
// relative to the source and destination-storage namespace roles declared by
// the parent workflow. PV references remain cluster-scoped.
type ClusterVolumeSpec struct {
	SourcePVC      LocalResourceReference `json:"sourcePVC"      yaml:"sourcePVC"`
	SourcePV       LocalResourceReference `json:"sourcePV"       yaml:"sourcePV"`
	DestinationPVC LocalResourceReference `json:"destinationPVC" yaml:"destinationPVC"`

	SourceReclaimPolicy PVReclaimPolicy                     `json:"sourceReclaimPolicy,omitempty" yaml:"sourceReclaimPolicy,omitempty"`
	SourcePVCSpec       PVCSpec                             `json:"sourcePVCSpec,omitempty"       yaml:"sourcePVCSpec,omitempty"`
	SourcePVCMetadata   PVCMetadata                         `json:"sourcePVCMetadata,omitempty"   yaml:"sourcePVCMetadata,omitempty"`
	Capacity            string                              `json:"capacity"                      yaml:"capacity"`
	SourceCapacity      string                              `json:"sourceCapacity"                yaml:"sourceCapacity"`
	SourceUsedBytes     int64                               `json:"sourceUsedBytes,omitempty"     yaml:"sourceUsedBytes,omitempty"`
	SourceUsageKnown    bool                                `json:"sourceUsageKnown,omitempty"    yaml:"sourceUsageKnown,omitempty"`
	StorageClass        string                              `json:"storageClass"                  yaml:"storageClass"`
	AccessModes         []corev1.PersistentVolumeAccessMode `json:"accessModes"                   yaml:"accessModes"`
	VolumeMode          corev1.PersistentVolumeMode         `json:"volumeMode"                    yaml:"volumeMode"`
	ConcurrentConsumers int                                 `json:"concurrentConsumers,omitempty" yaml:"concurrentConsumers,omitempty"`
	TransferScope       *TransferScope                      `json:"transferScope,omitempty"       yaml:"transferScope,omitempty"`
}

// ClusterWorkloadSpec is the workload snapshot owned by ClusterPodMigration.
// Workload references are relative to spec.sourceNamespace.
type ClusterWorkloadSpec struct {
	Adapter          WorkloadKind            `json:"adapter"                    yaml:"adapter"`
	Pod              *LocalResourceReference `json:"pod,omitempty"              yaml:"pod,omitempty"`
	Controller       *LocalResourceReference `json:"controller,omitempty"       yaml:"controller,omitempty"`
	OriginalReplicas *int32                  `json:"originalReplicas,omitempty" yaml:"originalReplicas,omitempty"`
	Ordinal          *int32                  `json:"ordinal,omitempty"          yaml:"ordinal,omitempty"`
	// +kubebuilder:validation:MaxItems=1024
	AffectedPods   []LocalResourceReference `json:"affectedPods,omitempty"   yaml:"affectedPods,omitempty"`
	OriginalObject *apiextensionsv1.JSON    `json:"originalObject,omitempty" yaml:"originalObject,omitempty"`
	KubeBlocks     *KubeBlocksSpec          `json:"kubeBlocks,omitempty"     yaml:"kubeBlocks,omitempty"`
	VMCluster      *VMClusterSpec           `json:"vmCluster,omitempty"      yaml:"vmCluster,omitempty"`
	Grafana        *GrafanaSpec             `json:"grafana,omitempty"        yaml:"grafana,omitempty"`
}

type ClusterMigrationSpec struct {
	SourceNamespace      NamespaceName `json:"sourceNamespace"      yaml:"sourceNamespace"`
	TemporaryNamespace   NamespaceName `json:"temporaryNamespace"   yaml:"temporaryNamespace"`
	DestinationNamespace NamespaceName `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     NamespaceName `json:"sessionNamespace"     yaml:"sessionNamespace"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes    []ClusterVolumeSpec `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
	SourceNode string              `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	TargetNode string              `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	ToolImage  string              `json:"toolImage,omitempty"  yaml:"toolImage,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Strategies           []string `json:"strategies,omitempty"           yaml:"strategies,omitempty"`
	VerifyChecksum       bool     `json:"verifyChecksum,omitempty"       yaml:"verifyChecksum,omitempty"`
	DeleteExtraneous     bool     `json:"deleteExtraneous,omitempty"     yaml:"deleteExtraneous,omitempty"`
	SkipSourceUsageCheck bool     `json:"skipSourceUsageCheck,omitempty" yaml:"skipSourceUsageCheck,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.workload.adapter != 'None'",message="ClusterPodMigration workload.adapter must identify a supported workload"
type ClusterPodMigrationSpec struct {
	// Pod migration preserves workload and PVC identities in SourceNamespace.
	// TemporaryNamespace and SessionNamespace are the only cross-namespace roles.
	SourceNamespace    NamespaceName `json:"sourceNamespace"    yaml:"sourceNamespace"`
	TemporaryNamespace NamespaceName `json:"temporaryNamespace" yaml:"temporaryNamespace"`
	SessionNamespace   NamespaceName `json:"sessionNamespace"   yaml:"sessionNamespace"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes    []ClusterVolumeSpec `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
	SourceNode string              `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	TargetNode string              `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	ToolImage  string              `json:"toolImage,omitempty"  yaml:"toolImage,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Strategies             []string            `json:"strategies,omitempty"             yaml:"strategies,omitempty"`
	VerifyChecksum         bool                `json:"verifyChecksum,omitempty"         yaml:"verifyChecksum,omitempty"`
	DeleteExtraneous       bool                `json:"deleteExtraneous,omitempty"       yaml:"deleteExtraneous,omitempty"`
	SkipSourceUsageCheck   bool                `json:"skipSourceUsageCheck,omitempty"   yaml:"skipSourceUsageCheck,omitempty"`
	Workload               ClusterWorkloadSpec `json:"workload"                         yaml:"workload"`
	PrecopyPasses          int                 `json:"precopyPasses"                    yaml:"precopyPasses"`
	OpenEBSLVMEnableShared bool                `json:"openebsLvmEnableShared,omitempty" yaml:"openebsLvmEnableShared,omitempty"`
}

type ClusterReservationSpec struct {
	SourceNamespace      NamespaceName `json:"sourceNamespace"      yaml:"sourceNamespace"`
	DestinationNamespace NamespaceName `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     NamespaceName `json:"sessionNamespace"     yaml:"sessionNamespace"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes              []ClusterVolumeSpec `json:"volumes,omitempty"              yaml:"volumes,omitempty"`
	TargetNode           string              `json:"targetNode,omitempty"           yaml:"targetNode,omitempty"`
	ToolImage            string              `json:"toolImage,omitempty"            yaml:"toolImage,omitempty"`
	SkipSourceUsageCheck bool                `json:"skipSourceUsageCheck,omitempty" yaml:"skipSourceUsageCheck,omitempty"`
}

type ClusterCopySpec struct {
	SourceNamespace      NamespaceName `json:"sourceNamespace"      yaml:"sourceNamespace"`
	DestinationNamespace NamespaceName `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     NamespaceName `json:"sessionNamespace"     yaml:"sessionNamespace"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes    []ClusterVolumeSpec `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
	SourceNode string              `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	TargetNode string              `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	ToolImage  string              `json:"toolImage,omitempty"  yaml:"toolImage,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Strategies []string `json:"strategies,omitempty"           yaml:"strategies,omitempty"`
	// VerifyChecksum enables rsync checksum comparison during final sync. It
	// defaults to false when omitted.
	VerifyChecksum       bool `json:"verifyChecksum,omitempty"       yaml:"verifyChecksum,omitempty"`
	DeleteExtraneous     bool `json:"deleteExtraneous,omitempty"     yaml:"deleteExtraneous,omitempty"`
	SkipSourceUsageCheck bool `json:"skipSourceUsageCheck,omitempty" yaml:"skipSourceUsageCheck,omitempty"`
	Online               bool `json:"online,omitempty"               yaml:"online,omitempty"`
}

type MoveIdentity struct {
	SourcePVC      LocalResourceReference `json:"sourcePVC"      yaml:"sourcePVC"`
	SourcePV       LocalResourceReference `json:"sourcePV"       yaml:"sourcePV"`
	DestinationPVC LocalResourceReference `json:"destinationPVC" yaml:"destinationPVC"`
	SourceTemplate PVCSourceTemplate      `json:"sourceTemplate" yaml:"sourceTemplate"`
}

type MoveSpec struct {
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

type ClusterSharedMountStatus struct {
	// SourcePV is cluster-scoped and therefore normally has no namespace.
	SourcePV LocalResourceReference `json:"sourcePV" yaml:"sourcePV"`
	// LVMVolume is namespaced and must retain its namespace for controller
	// recovery when a cluster PodMigration spans namespaces.
	LVMVolume         ObjectReference `json:"lvmVolume"                   yaml:"lvmVolume"`
	PreviousShared    string          `json:"previousShared,omitempty"    yaml:"previousShared,omitempty"`
	PreviousSharedSet bool            `json:"previousSharedSet,omitempty" yaml:"previousSharedSet,omitempty"`
}

type ClusterPodMigrationWorkloadStatus struct {
	Pod          *ObjectReference  `json:"pod,omitempty"          yaml:"pod,omitempty"`
	AffectedPods []ObjectReference `json:"affectedPods,omitempty" yaml:"affectedPods,omitempty"`
}

type ClusterMigrationVolumeStatus struct {
	SourcePVCName     string                        `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *ObjectReference              `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *ObjectReference              `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy               `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool                          `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
	Sync              MigrationSyncStatus           `json:"sync"                               yaml:"sync"`
	Activation        ClusterVolumeActivationStatus `json:"activation"                         yaml:"activation"`
}

type ClusterPodMigrationVolumeStatus struct {
	SourcePVCName     string                        `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *ObjectReference              `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *ObjectReference              `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy               `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool                          `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
	Sync              PodMigrationSyncStatus        `json:"sync"                               yaml:"sync"`
	Activation        ClusterVolumeActivationStatus `json:"activation"                         yaml:"activation"`
}

type ClusterReservationVolumeStatus struct {
	SourcePVCName     string           `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *ObjectReference `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *ObjectReference `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy  `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool             `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
}

type ClusterCopyVolumeStatus struct {
	SourcePVCName     string           `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *ObjectReference `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *ObjectReference `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy  `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool             `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
	Sync              CopySyncStatus   `json:"sync"                               yaml:"sync"`
}

type MoveActivationStatus struct {
	ActivePVC    *ObjectReference `json:"activePVC,omitempty"    yaml:"activePVC,omitempty"`
	ActivatedAt  *metav1.Time     `json:"activatedAt,omitempty"  yaml:"activatedAt,omitempty"`
	RolledBackAt *metav1.Time     `json:"rolledBackAt,omitempty" yaml:"rolledBackAt,omitempty"`
}

type MoveVolumeStatus struct {
	SourcePVCName string               `json:"sourcePVCName" yaml:"sourcePVCName"`
	Activation    MoveActivationStatus `json:"activation"    yaml:"activation"`
}

type ClusterMigrationStatus struct {
	WorkflowStatus `                               json:",inline" yaml:",inline"`
	Volumes        []ClusterMigrationVolumeStatus `json:"volumes" yaml:"volumes"`
}

type ClusterPodMigrationStatus struct {
	WorkflowStatus          `    json:",inline"             yaml:",inline"`
	WarmPassesCompleted     int                                `json:"warmPassesCompleted" yaml:"warmPassesCompleted"`
	OriginalPodSnapshotHash string                             `json:"originalPodSnapshotHash,omitempty" yaml:"originalPodSnapshotHash,omitempty"`
	Workload                *ClusterPodMigrationWorkloadStatus `json:"workload,omitempty"                yaml:"workload,omitempty"`
	Volumes                 []ClusterPodMigrationVolumeStatus  `json:"volumes"                           yaml:"volumes"`
	OpenEBSLVMSharedMounts  []ClusterSharedMountStatus         `json:"openebsLvmSharedMounts,omitempty"  yaml:"openebsLvmSharedMounts,omitempty"`
}

type ClusterReservationStatus struct {
	WorkflowStatus `                                 json:",inline" yaml:",inline"`
	Volumes        []ClusterReservationVolumeStatus `json:"volumes" yaml:"volumes"`
}

type ClusterCopyStatus struct {
	WorkflowStatus `                          json:",inline" yaml:",inline"`
	Volumes        []ClusterCopyVolumeStatus `json:"volumes" yaml:"volumes"`
}

type MoveStatus struct {
	WorkflowStatus `                  json:",inline" yaml:",inline"`
	Volumes        []MoveVolumeStatus `json:"volumes" yaml:"volumes"`
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
	metav1.TypeMeta `        json:",inline"`
	metav1.ListMeta `        json:"metadata,omitempty"`
	Items           []Move `json:"items"`
}
