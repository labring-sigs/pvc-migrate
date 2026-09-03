//nolint:wsl_v5,golines // Durable API fields keep explicit JSON and YAML tags together.
package v1alpha1

import (
	"maps"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ObjectReference identifies a Kubernetes object involved in a workflow.
type ObjectReference struct {
	// +kubebuilder:validation:MaxLength=253
	APIVersion string `json:"apiVersion,omitempty" yaml:"apiVersion,omitempty"`
	// +kubebuilder:validation:MaxLength=253
	Kind string `json:"kind,omitempty" yaml:"kind,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Name string    `json:"name"          yaml:"name"`
	UID  types.UID `json:"uid,omitempty" yaml:"uid,omitempty"`
	// +kubebuilder:validation:MaxLength=256
	ResourceVersion string `json:"resourceVersion,omitempty" yaml:"resourceVersion,omitempty"`
}

func (in *ObjectReference) DeepCopyInto(out *ObjectReference) {
	if in == nil || out == nil {
		return
	}
	*out = *in
}

// LocalResourceReference identifies a resource whose namespace is established
// by the containing workflow, or a cluster-scoped resource such as a PV.
// Namespaced workflow APIs derive their boundary from metadata.namespace.
type LocalResourceReference struct {
	// +kubebuilder:validation:MaxLength=253
	APIVersion string `json:"apiVersion,omitempty" yaml:"apiVersion,omitempty"`
	// +kubebuilder:validation:MaxLength=253
	Kind string `json:"kind,omitempty" yaml:"kind,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Name string    `json:"name"          yaml:"name"`
	UID  types.UID `json:"uid,omitempty" yaml:"uid,omitempty"`
	// +kubebuilder:validation:MaxLength=256
	ResourceVersion string `json:"resourceVersion,omitempty" yaml:"resourceVersion,omitempty"`
}

// +kubebuilder:validation:Enum=None;StandalonePod;Deployment;StatefulSet;VictoriaLogs;KubeBlocks;VMCluster;Grafana
type WorkloadKind string

const (
	WorkloadNone         WorkloadKind = "None"
	WorkloadStandalone   WorkloadKind = "StandalonePod"
	WorkloadDeployment   WorkloadKind = "Deployment"
	WorkloadStatefulSet  WorkloadKind = "StatefulSet"
	WorkloadVictoriaLogs WorkloadKind = "VictoriaLogs"
	WorkloadKubeBlocks   WorkloadKind = "KubeBlocks"
	WorkloadVMCluster    WorkloadKind = "VMCluster"
	WorkloadGrafana      WorkloadKind = "Grafana"
)

type TransferScope struct {
	// +kubebuilder:validation:MaxLength=1024
	SourcePath string `json:"sourcePath" yaml:"sourcePath"`
	// +kubebuilder:validation:MaxLength=1024
	DestinationPath string `json:"destinationPath" yaml:"destinationPath"`
}

type (
	PVCSpec         = corev1.PersistentVolumeClaimSpec
	PVReclaimPolicy = corev1.PersistentVolumeReclaimPolicy
)

// VolumeSpec is planning output required to resume a PVC transfer. It is an
// API-owned type with only the fields needed by transfer workflows.
type VolumeSpec struct {
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

type PVCMetadata struct {
	Labels          map[string]string       `json:"labels,omitempty"          yaml:"labels,omitempty"`
	Annotations     map[string]string       `json:"annotations,omitempty"     yaml:"annotations,omitempty"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences,omitempty" yaml:"ownerReferences,omitempty"`
}

// WorkloadSpec is specific to PodMigration and records workload ownership
// and restoration data needed by that operation.
type WorkloadSpec struct {
	Adapter          WorkloadKind            `json:"adapter"                    yaml:"adapter"`
	Pod              *LocalResourceReference `json:"pod,omitempty"              yaml:"pod,omitempty"`
	Controller       *LocalResourceReference `json:"controller,omitempty"       yaml:"controller,omitempty"`
	OriginalReplicas *int32                  `json:"originalReplicas,omitempty" yaml:"originalReplicas,omitempty"`
	Ordinal          *int32                  `json:"ordinal,omitempty"          yaml:"ordinal,omitempty"`
	// +kubebuilder:validation:MaxItems=1024
	AffectedPods []LocalResourceReference `json:"affectedPods,omitempty" yaml:"affectedPods,omitempty"`
	// The serialized byte-size limit is enforced by the controller/domain
	// boundary because OpenAPI cannot express a byte limit for opaque JSON.
	OriginalObject *apiextensionsv1.JSON `json:"originalObject,omitempty" yaml:"originalObject,omitempty"`
	KubeBlocks     *KubeBlocksSpec       `json:"kubeBlocks,omitempty"     yaml:"kubeBlocks,omitempty"`
	VMCluster      *VMClusterSpec        `json:"vmCluster,omitempty"      yaml:"vmCluster,omitempty"`
	Grafana        *GrafanaSpec          `json:"grafana,omitempty"        yaml:"grafana,omitempty"`
}

type KubeBlocksSpec struct {
	Cluster                  string    `json:"cluster"                            yaml:"cluster"`
	Component                string    `json:"component"                          yaml:"component"`
	Instance                 string    `json:"instance"                           yaml:"instance"`
	Role                     string    `json:"role,omitempty"                     yaml:"role,omitempty"`
	SwitchoverCandidate      string    `json:"switchoverCandidate,omitempty"      yaml:"switchoverCandidate,omitempty"`
	SwitchoverStrategy       string    `json:"switchoverStrategy,omitempty"       yaml:"switchoverStrategy,omitempty"`
	SwitchoverContainer      string    `json:"switchoverContainer,omitempty"      yaml:"switchoverContainer,omitempty"`
	OpsAPIVersion            string    `json:"opsAPIVersion"                      yaml:"opsAPIVersion"`
	ClusterUID               types.UID `json:"clusterUID"                         yaml:"clusterUID"`
	OriginalPaused           bool      `json:"originalPaused,omitempty"           yaml:"originalPaused,omitempty"`
	OriginalPausedConfigured bool      `json:"originalPausedConfigured,omitempty" yaml:"originalPausedConfigured,omitempty"`
}

type VMClusterSpec struct {
	APIVersion                      string    `json:"apiVersion"                      yaml:"apiVersion"`
	Name                            string    `json:"name"                            yaml:"name"`
	UID                             types.UID `json:"uid,omitempty"                   yaml:"uid,omitempty"`
	Component                       string    `json:"component"                       yaml:"component"`
	OriginalPaused                  bool      `json:"originalPaused"                  yaml:"originalPaused"`
	OriginalPausedConfigured        bool      `json:"originalPausedConfigured"        yaml:"originalPausedConfigured"`
	OriginalClusterPaused           bool      `json:"originalClusterPaused"           yaml:"originalClusterPaused"`
	OriginalClusterPausedConfigured bool      `json:"originalClusterPausedConfigured" yaml:"originalClusterPausedConfigured"`
	OriginalReplicas                int32     `json:"originalReplicas"                yaml:"originalReplicas"`
	OriginalReplicasConfigured      bool      `json:"originalReplicasConfigured"      yaml:"originalReplicasConfigured"`
}

type GrafanaSpec struct {
	APIVersion                string    `json:"apiVersion"                yaml:"apiVersion"`
	Name                      string    `json:"name"                      yaml:"name"`
	UID                       types.UID `json:"uid,omitempty"             yaml:"uid,omitempty"`
	OriginalSuspend           bool      `json:"originalSuspend"           yaml:"originalSuspend"`
	OriginalSuspendConfigured bool      `json:"originalSuspendConfigured" yaml:"originalSuspendConfigured"`
	OriginalReplicas          int32     `json:"originalReplicas"          yaml:"originalReplicas"`
}

// MigrationSpec is an offline PVC migration. It has no workload controls.
type MigrationSpec struct {
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

// +kubebuilder:validation:XValidation:rule="self.workload.adapter != 'None'",message="PodMigration workload.adapter must identify a supported workload"
// PodMigrationSpec is a workload-aware migration. Workload and precopy
// controls are exclusive to this operation.
type PodMigrationSpec struct {
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

type ReservationSpec struct {
	// +kubebuilder:validation:MaxItems=1024
	Volumes              []VolumeSpec `json:"volumes,omitempty"              yaml:"volumes,omitempty"`
	TargetNode           string       `json:"targetNode,omitempty"           yaml:"targetNode,omitempty"`
	ToolImage            string       `json:"toolImage,omitempty"            yaml:"toolImage,omitempty"`
	SkipSourceUsageCheck bool         `json:"skipSourceUsageCheck,omitempty" yaml:"skipSourceUsageCheck,omitempty"`
}

type CopySpec struct {
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
	Online               bool     `json:"online,omitempty"               yaml:"online,omitempty"`
}

type BackupSpec struct {
	SourcePVC LocalResourceReference `json:"sourcePVC"           yaml:"sourcePVC"`
	SourcePV  LocalResourceReference `json:"sourcePV"            yaml:"sourcePV"`
	Path      string                 `json:"path,omitempty"      yaml:"path,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name string `json:"name" yaml:"name"`
	// RepositoryRef selects a user-owned BackupRepository in this workflow
	// namespace. The referenced object owns the complete backup location.
	RepositoryRef          LocalObjectReference `json:"repositoryRef"                    yaml:"repositoryRef"`
	Online                 bool                 `json:"online,omitempty"                 yaml:"online,omitempty"`
	OpenEBSLVMEnableShared bool                 `json:"openebsLvmEnableShared,omitempty" yaml:"openebsLvmEnableShared,omitempty"`
	ToolImage              string               `json:"toolImage,omitempty"              yaml:"toolImage,omitempty"`
	DeleteExtraneous       bool                 `json:"deleteExtraneous,omitempty"       yaml:"deleteExtraneous,omitempty"`
}

type RestoreSpec struct {
	DestinationPVC LocalResourceReference `json:"destinationPVC"      yaml:"destinationPVC"`
	Path           string                 `json:"path,omitempty"      yaml:"path,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name string `json:"name" yaml:"name"`
	// RepositoryRef selects a user-owned BackupRepository in this workflow
	// namespace. The referenced object owns the complete backup location.
	RepositoryRef           LocalObjectReference `json:"repositoryRef"                     yaml:"repositoryRef"`
	CreatePVC               bool                 `json:"createPVC,omitempty"               yaml:"createPVC,omitempty"`
	DestinationStorageClass string               `json:"destinationStorageClass,omitempty" yaml:"destinationStorageClass,omitempty"`
	DestinationAccessMode   string               `json:"destinationAccessMode,omitempty"   yaml:"destinationAccessMode,omitempty"`
	DestinationCapacity     string               `json:"destinationCapacity,omitempty"     yaml:"destinationCapacity,omitempty"`
	AllowMounted            bool                 `json:"allowMounted,omitempty"            yaml:"allowMounted,omitempty"`
	TargetNode              string               `json:"targetNode,omitempty"              yaml:"targetNode,omitempty"`
	ToolImage               string               `json:"toolImage,omitempty"               yaml:"toolImage,omitempty"`
	DeleteExtraneous        bool                 `json:"deleteExtraneous,omitempty"        yaml:"deleteExtraneous,omitempty"`
}

type PVCSourceTemplate struct {
	Spec          PVCSpec         `json:"spec"                    yaml:"spec"`
	Metadata      PVCMetadata     `json:"metadata,omitempty"      yaml:"metadata,omitempty"`
	ReclaimPolicy PVReclaimPolicy `json:"reclaimPolicy,omitempty" yaml:"reclaimPolicy,omitempty"`
}

type PVCIdentityFields struct {
	SourcePVC      LocalResourceReference `json:"sourcePVC"      yaml:"sourcePVC"`
	SourcePV       LocalResourceReference `json:"sourcePV"       yaml:"sourcePV"`
	DestinationPVC LocalResourceReference `json:"destinationPVC" yaml:"destinationPVC"`
	SourceTemplate PVCSourceTemplate      `json:"sourceTemplate" yaml:"sourceTemplate"`
}

type RenameSpec struct {
	PVCIdentityFields `       json:",inline"             yaml:",inline"`
}

// +kubebuilder:validation:Enum=Planned;Reserving;Reserved;WarmCopying;WarmCopied;Pausing;Paused;FinalSyncing;FinalSynced;Activating;Activated;Resuming;Completed;Aborting;Aborted;RollingBack;RolledBack;Renaming;Moving;Failed
// WorkflowPhase identifies one durable workflow state.
type WorkflowPhase string

type WorkflowCondition struct {
	// +kubebuilder:validation:MaxLength=64
	Type   string                 `json:"type"   yaml:"type"`
	Status metav1.ConditionStatus `json:"status" yaml:"status"`
	// +kubebuilder:validation:MaxLength=128
	Reason string `json:"reason,omitempty" yaml:"reason,omitempty"`
	// +kubebuilder:validation:MaxLength=8192
	Message            string      `json:"message,omitempty"  yaml:"message,omitempty"`
	LastTransitionTime metav1.Time `json:"lastTransitionTime" yaml:"lastTransitionTime"`
}

type WorkflowHistoryEntry struct {
	Phase WorkflowPhase `json:"phase" yaml:"phase"`
	Time  metav1.Time   `json:"time"  yaml:"time"`
	// +kubebuilder:validation:MaxLength=8192
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
}

type WorkflowStatus struct {
	Phase      WorkflowPhase `json:"phase"                yaml:"phase"`
	ResumeFrom WorkflowPhase `json:"resumeFrom,omitempty" yaml:"resumeFrom,omitempty"`
	// +kubebuilder:validation:MaxLength=8192
	FailureReason      string       `json:"failureReason,omitempty"      yaml:"failureReason,omitempty"`
	ObservedGeneration int64        `json:"observedGeneration,omitempty" yaml:"observedGeneration,omitempty"`
	StartedAt          metav1.Time  `json:"startedAt"                    yaml:"startedAt"`
	UpdatedAt          metav1.Time  `json:"updatedAt"                    yaml:"updatedAt"`
	CompletedAt        *metav1.Time `json:"completedAt,omitempty"        yaml:"completedAt,omitempty"`
	// +kubebuilder:validation:MaxLength=8192
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Conditions []WorkflowCondition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	// +kubebuilder:validation:MaxItems=256
	History []WorkflowHistoryEntry `json:"history,omitempty" yaml:"history,omitempty"`
}

// MigrationSyncStatus contains final-copy checkpoints. Offline Migration has
// no warm-copy phase, so warmCompletedAt is intentionally absent.
type MigrationSyncStatus struct {
	FinalCompletedAt *metav1.Time `json:"finalCompletedAt,omitempty" yaml:"finalCompletedAt,omitempty"`
	Attempts         int          `json:"attempts"                   yaml:"attempts"`
	BytesCopied      int64        `json:"bytesCopied,omitempty"      yaml:"bytesCopied,omitempty"`
	ChecksumVerified bool         `json:"checksumVerified,omitempty" yaml:"checksumVerified,omitempty"`
	// +kubebuilder:validation:MaxLength=8192
	LastError string `json:"lastError,omitempty" yaml:"lastError,omitempty"`
}

// PodMigrationSyncStatus tracks both warm-copy and final-copy checkpoints.
type PodMigrationSyncStatus struct {
	WarmCompletedAt  *metav1.Time `json:"warmCompletedAt,omitempty"  yaml:"warmCompletedAt,omitempty"`
	FinalCompletedAt *metav1.Time `json:"finalCompletedAt,omitempty" yaml:"finalCompletedAt,omitempty"`
	Attempts         int          `json:"attempts"                   yaml:"attempts"`
	BytesCopied      int64        `json:"bytesCopied,omitempty"      yaml:"bytesCopied,omitempty"`
	ChecksumVerified bool         `json:"checksumVerified,omitempty" yaml:"checksumVerified,omitempty"`
	// +kubebuilder:validation:MaxLength=8192
	LastError string `json:"lastError,omitempty" yaml:"lastError,omitempty"`
}

// CopySyncStatus tracks the warm-copy operation owned by Copy.
type CopySyncStatus struct {
	WarmCompletedAt *metav1.Time `json:"warmCompletedAt,omitempty" yaml:"warmCompletedAt,omitempty"`
	Attempts        int          `json:"attempts"                  yaml:"attempts"`
	BytesCopied     int64        `json:"bytesCopied,omitempty"     yaml:"bytesCopied,omitempty"`
	// +kubebuilder:validation:MaxLength=8192
	LastError string `json:"lastError,omitempty" yaml:"lastError,omitempty"`
}

type VolumeActivationStatus struct {
	TemporaryPVCDeleted bool                    `json:"temporaryPVCDeleted,omitempty" yaml:"temporaryPVCDeleted,omitempty"`
	SourcePVCDeleted    bool                    `json:"sourcePVCDeleted,omitempty"    yaml:"sourcePVCDeleted,omitempty"`
	DestinationReserved bool                    `json:"destinationReserved,omitempty" yaml:"destinationReserved,omitempty"`
	ActivePVC           *LocalResourceReference `json:"activePVC,omitempty"           yaml:"activePVC,omitempty"`
	ActivatedAt         *metav1.Time            `json:"activatedAt,omitempty"         yaml:"activatedAt,omitempty"`
	RolledBackAt        *metav1.Time            `json:"rolledBackAt,omitempty"        yaml:"rolledBackAt,omitempty"`
}

type SharedMountStatus struct {
	SourcePV          LocalResourceReference `json:"sourcePV"                    yaml:"sourcePV"`
	LVMVolume         LocalResourceReference `json:"lvmVolume"                   yaml:"lvmVolume"`
	PreviousShared    string                 `json:"previousShared,omitempty"    yaml:"previousShared,omitempty"`
	PreviousSharedSet bool                   `json:"previousSharedSet,omitempty" yaml:"previousSharedSet,omitempty"`
}

// PodMigrationWorkloadStatus tracks Pod identities recreated while pausing,
// resuming, or rolling back a workload. The original migration target and
// recovery snapshot remain immutable in spec.workload.
type PodMigrationWorkloadStatus struct {
	Pod          *LocalResourceReference  `json:"pod,omitempty"          yaml:"pod,omitempty"`
	AffectedPods []LocalResourceReference `json:"affectedPods,omitempty" yaml:"affectedPods,omitempty"`
}

// MigrationVolumeStatus is the durable checkpoint for an offline migration
// volume. It intentionally excludes workload-only progress and OpenEBS
// shared-mount state.
type MigrationVolumeStatus struct {
	SourcePVCName     string                  `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *LocalResourceReference `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *LocalResourceReference `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy         `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool                    `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
	Sync              MigrationSyncStatus     `json:"sync"                               yaml:"sync"`
	Activation        VolumeActivationStatus  `json:"activation"                         yaml:"activation"`
}

// PodMigrationVolumeStatus is the durable checkpoint for a workload-aware
// migration volume. It is a distinct API type even though its transfer and
// activation fields currently match MigrationVolumeStatus.
type PodMigrationVolumeStatus struct {
	SourcePVCName     string                  `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *LocalResourceReference `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *LocalResourceReference `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy         `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool                    `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
	Sync              PodMigrationSyncStatus  `json:"sync"                               yaml:"sync"`
	Activation        VolumeActivationStatus  `json:"activation"                         yaml:"activation"`
}

// ReservationVolumeStatus reports only destination reservation progress.
// Copying and activation checkpoints are not part of a Reservation contract.
type ReservationVolumeStatus struct {
	SourcePVCName     string                  `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *LocalResourceReference `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *LocalResourceReference `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy         `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool                    `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
}

// CopyVolumeStatus reports reservation and copy progress. PVC activation is
// owned by Migration/PodMigration and is intentionally absent here.
type CopyVolumeStatus struct {
	SourcePVCName     string                  `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *LocalResourceReference `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *LocalResourceReference `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy         `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool                    `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
	Sync              CopySyncStatus          `json:"sync"                               yaml:"sync"`
}

// PVCIdentityVolumeStatus is the namespaced Rename checkpoint. Rename changes
// PVC identity and never runs a data-copy phase.
type PVCIdentityVolumeStatus struct {
	SourcePVCName string                      `json:"sourcePVCName" yaml:"sourcePVCName"`
	Activation    PVCIdentityActivationStatus `json:"activation"    yaml:"activation"`
}

// PVCIdentityActivationStatus is the checkpoint needed to roll back a Rename.
// Temporary-volume cleanup fields do not apply to identity operations.
type PVCIdentityActivationStatus struct {
	ActivePVC    *LocalResourceReference `json:"activePVC,omitempty"    yaml:"activePVC,omitempty"`
	ActivatedAt  *metav1.Time            `json:"activatedAt,omitempty"  yaml:"activatedAt,omitempty"`
	RolledBackAt *metav1.Time            `json:"rolledBackAt,omitempty" yaml:"rolledBackAt,omitempty"`
}

type MigrationStatus struct {
	WorkflowStatus `                        json:",inline" yaml:",inline"`
	Volumes        []MigrationVolumeStatus `json:"volumes" yaml:"volumes"`
}

type PodMigrationStatus struct {
	WorkflowStatus      `    json:",inline"             yaml:",inline"`
	WarmPassesCompleted int `json:"warmPassesCompleted" yaml:"warmPassesCompleted"`
	// OriginalPodSnapshotHash is controller-owned evidence that the standalone
	// Pod snapshot was captured from the referenced live Pod before execution.
	OriginalPodSnapshotHash string                      `json:"originalPodSnapshotHash,omitempty" yaml:"originalPodSnapshotHash,omitempty"`
	Workload                *PodMigrationWorkloadStatus `json:"workload,omitempty"                yaml:"workload,omitempty"`
	Volumes                 []PodMigrationVolumeStatus  `json:"volumes"                           yaml:"volumes"`
	OpenEBSLVMSharedMounts  []SharedMountStatus         `json:"openebsLvmSharedMounts,omitempty"  yaml:"openebsLvmSharedMounts,omitempty"`
}

type ReservationStatus struct {
	WorkflowStatus `                          json:",inline" yaml:",inline"`
	Volumes        []ReservationVolumeStatus `json:"volumes" yaml:"volumes"`
}
type CopyStatus struct {
	WorkflowStatus `                   json:",inline" yaml:",inline"`
	Volumes        []CopyVolumeStatus `json:"volumes" yaml:"volumes"`
}

// +kubebuilder:validation:XValidation:rule="size(self.credentialsSecretUID) > 0",message="credentialsSecretUID must not be empty"
type S3BackupRepositoryBindingStatus struct {
	CredentialsSecretUID types.UID `json:"credentialsSecretUID" yaml:"credentialsSecretUID"`
}

// +kubebuilder:validation:XValidation:rule="size(self.claimUID) > 0",message="claimUID must not be empty"
type PVCBackupRepositoryBindingStatus struct {
	ClaimUID types.UID `json:"claimUID" yaml:"claimUID"`
}

// +kubebuilder:validation:XValidation:rule="(self.type == 's3' && has(self.s3) && !has(self.pvc)) || (self.type == 'pvc' && has(self.pvc) && !has(self.s3))",message="exactly one backend status must match type"
// +kubebuilder:validation:XValidation:rule="size(self.uid) > 0",message="uid must not be empty"
type BackupRepositoryBindingStatus struct {
	Type BackupRepositoryType `json:"type" yaml:"type"`
	UID  types.UID            `json:"uid"  yaml:"uid"`

	// +kubebuilder:validation:Minimum=1
	Generation int64 `json:"generation" yaml:"generation"`

	S3  *S3BackupRepositoryBindingStatus  `json:"s3,omitempty"          yaml:"s3,omitempty"`
	PVC *PVCBackupRepositoryBindingStatus `json:"pvc,omitempty"         yaml:"pvc,omitempty"`
}

type BackupStatus struct {
	WorkflowStatus         `json:",inline" yaml:",inline"`
	Repository             *BackupRepositoryBindingStatus `json:"repository,omitempty"              yaml:"repository,omitempty"`
	OpenEBSLVMSharedMounts []SharedMountStatus            `json:"openebsLvmSharedMounts,omitempty" yaml:"openebsLvmSharedMounts,omitempty"`
}
type RestoreStatus struct {
	WorkflowStatus `json:",inline" yaml:",inline"`
	Repository     *BackupRepositoryBindingStatus `json:"repository,omitempty"     yaml:"repository,omitempty"`
	DestinationPVC *ObjectReference               `json:"destinationPVC,omitempty" yaml:"destinationPVC,omitempty"`
	DestinationPV  *ObjectReference               `json:"destinationPV,omitempty"  yaml:"destinationPV,omitempty"`
}
type RenameStatus struct {
	WorkflowStatus `                          json:",inline" yaml:",inline"`
	Volumes        []PVCIdentityVolumeStatus `json:"volumes" yaml:"volumes"`
}

func (s MigrationSpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		SourceNode:           s.SourceNode,
		TargetNode:           s.TargetNode,
		ToolImage:            s.ToolImage,
		Strategies:           append([]string(nil), s.Strategies...),
		VerifyChecksum:       s.VerifyChecksum,
		DeleteExtraneous:     s.DeleteExtraneous,
		SkipSourceUsageCheck: s.SkipSourceUsageCheck,
	}
}

func (s PodMigrationSpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		SourceNode:           s.SourceNode,
		TargetNode:           s.TargetNode,
		ToolImage:            s.ToolImage,
		Strategies:           append([]string(nil), s.Strategies...),
		VerifyChecksum:       s.VerifyChecksum,
		DeleteExtraneous:     s.DeleteExtraneous,
		SkipSourceUsageCheck: s.SkipSourceUsageCheck,
	}
}

func (s ReservationSpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		TargetNode:           s.TargetNode,
		ToolImage:            s.ToolImage,
		SkipSourceUsageCheck: s.SkipSourceUsageCheck,
	}
}

func (s CopySpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		SourceNode:           s.SourceNode,
		TargetNode:           s.TargetNode,
		ToolImage:            s.ToolImage,
		Strategies:           append([]string(nil), s.Strategies...),
		VerifyChecksum:       s.VerifyChecksum,
		DeleteExtraneous:     s.DeleteExtraneous,
		SkipSourceUsageCheck: s.SkipSourceUsageCheck,
	}
}

func (s BackupSpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		ToolImage:        s.ToolImage,
		DeleteExtraneous: s.DeleteExtraneous,
	}
}

func (s RestoreSpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		TargetNode:       s.TargetNode,
		ToolImage:        s.ToolImage,
		DeleteExtraneous: s.DeleteExtraneous,
	}
}

func volumeSpecFromDomain(v domain.VolumeSpec) VolumeSpec {
	return VolumeSpec{
		SourcePVC:           localRefFromDomain(v.SourcePVC),
		SourcePV:            localRefFromDomain(v.SourcePV),
		SourceReclaimPolicy: v.SourceReclaimPolicy,
		SourcePVCSpec:       *v.SourcePVCSpec.DeepCopy(),
		SourcePVCMetadata:   pvcMetadataFromDomain(v.SourcePVCMetadata),
		DestinationPVC: LocalResourceReference{
			APIVersion: v.DestinationPVC.APIVersion,
			Kind:       v.DestinationPVC.Kind,
			Name:       v.DestinationPVC.Name,
		},
		Capacity:            v.Capacity,
		SourceCapacity:      v.SourceCapacity,
		SourceUsedBytes:     v.SourceUsedBytes,
		SourceUsageKnown:    v.SourceUsageKnown,
		StorageClass:        v.StorageClass,
		AccessModes:         append([]corev1.PersistentVolumeAccessMode(nil), v.AccessModes...),
		VolumeMode:          v.VolumeMode,
		ConcurrentConsumers: v.ConcurrentConsumers,
		TransferScope:       scopeFromDomain(v.TransferScope),
	}
}

func (v VolumeSpec) Domain(sourceNamespace, destinationNamespace string) domain.VolumeSpec {
	return domain.VolumeSpec{
		SourcePVC:           localRefToDomain(v.SourcePVC, sourceNamespace),
		SourcePV:            localRefToDomain(v.SourcePV, ""),
		SourceReclaimPolicy: v.SourceReclaimPolicy,
		SourcePVCSpec:       *v.SourcePVCSpec.DeepCopy(),
		SourcePVCMetadata:   pvcMetadataToDomain(v.SourcePVCMetadata),
		DestinationPVC:      localRefToDomain(v.DestinationPVC, destinationNamespace),
		Capacity:            v.Capacity,
		SourceCapacity:      v.SourceCapacity,
		SourceUsedBytes:     v.SourceUsedBytes,
		SourceUsageKnown:    v.SourceUsageKnown,
		StorageClass:        v.StorageClass,
		AccessModes:         append([]corev1.PersistentVolumeAccessMode(nil), v.AccessModes...),
		VolumeMode:          v.VolumeMode,
		ConcurrentConsumers: v.ConcurrentConsumers,
		TransferScope:       scopeToDomain(v.TransferScope),
	}
}

func volumesFromDomain(in []domain.VolumeSpec) []VolumeSpec {
	if in == nil {
		return nil
	}
	out := make([]VolumeSpec, len(in))
	for i := range in {
		out[i] = volumeSpecFromDomain(in[i])
	}
	return out
}

func volumesToDomain(
	in []VolumeSpec,
	sourceNamespace, destinationNamespace string,
) []domain.VolumeSpec {
	if in == nil {
		return nil
	}
	out := make([]domain.VolumeSpec, len(in))
	for i := range in {
		out[i] = in[i].Domain(sourceNamespace, destinationNamespace)
	}
	return out
}

func namespacedSessionCommon(
	namespace string,
	volumes []VolumeSpec,
) domain.SessionCommon {
	return domain.SessionCommon{
		SourceNamespace:      namespace,
		TemporaryNamespace:   namespace,
		DestinationNamespace: namespace,
		SessionNamespace:     namespace,
		Volumes:              volumesToDomain(volumes, namespace, namespace),
	}
}

func (s MigrationSpec) Domain(namespace string) domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: namespacedSessionCommon(namespace, s.Volumes),
		Type:          domain.SessionTypeMigrate,
		Migrate:       &domain.MigrateSessionSpec{SessionWorkflowOptions: s.workflowOptions()},
	}
}

func (s PodMigrationSpec) Domain(namespace string) domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: namespacedSessionCommon(namespace, s.Volumes),
		Type:          domain.SessionTypeMigratePod,
		MigratePod: &domain.MigratePodSessionSpec{
			SessionWorkflowOptions: s.workflowOptions(),
			Workload:               workloadToDomain(s.Workload, namespace),
			PrecopyPasses:          s.PrecopyPasses,
			OpenEBSLVMEnableShared: s.OpenEBSLVMEnableShared,
		},
	}
}

func (s ReservationSpec) Domain(namespace string) domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: namespacedSessionCommon(namespace, s.Volumes),
		Type:          domain.SessionTypeReserve,
		Reserve:       &domain.ReserveSessionSpec{SessionWorkflowOptions: s.workflowOptions()},
	}
}

func (s CopySpec) Domain(namespace string) domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: namespacedSessionCommon(namespace, s.Volumes),
		Type:          domain.SessionTypeCopy,
		Copy: &domain.CopySessionSpec{
			SessionWorkflowOptions: s.workflowOptions(),
			Online:                 s.Online,
		},
	}
}

func (s BackupSpec) Domain(namespace string) domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: domain.SessionCommon{
			SourceNamespace:  namespace,
			SessionNamespace: namespace,
		},
		Type: domain.SessionTypeBackup,
		Backup: &domain.BackupSessionSpec{
			SessionWorkflowOptions:    s.workflowOptions(),
			Online:                    s.Online,
			SourcePVC:                 localRefToDomain(s.SourcePVC, namespace),
			SourcePV:                  localRefToDomain(s.SourcePV, ""),
			Path:                      s.Path,
			Name:                      s.Name,
			BackupRepository:          repositoryName(s.RepositoryRef),
			BackupRepositoryNamespace: namespace,
			OpenEBSLVMEnableShared:    s.OpenEBSLVMEnableShared,
		},
	}
}

func (s RestoreSpec) Domain(namespace string) domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: domain.SessionCommon{
			SourceNamespace:      namespace,
			DestinationNamespace: namespace,
			SessionNamespace:     namespace,
		},
		Type: domain.SessionTypeRestore,
		Restore: &domain.RestoreSessionSpec{
			SessionWorkflowOptions:    s.workflowOptions(),
			DestinationPVC:            localRefToDomain(s.DestinationPVC, namespace),
			Path:                      s.Path,
			Name:                      s.Name,
			BackupRepository:          repositoryName(s.RepositoryRef),
			BackupRepositoryNamespace: namespace,
			CreatePVC:                 s.CreatePVC,
			DestinationStorageClass:   s.DestinationStorageClass,
			DestinationAccessMode:     s.DestinationAccessMode,
			DestinationCapacity:       s.DestinationCapacity,
			AllowMounted:              s.AllowMounted,
		},
	}
}

func repositoryName(ref LocalObjectReference) string {
	return ref.Name
}

func repositoryRef(name string) LocalObjectReference {
	return LocalObjectReference{Name: name}
}

func (s RenameSpec) Domain(namespace string) domain.SessionSpec {
	return identitySessionSpec(
		domain.SessionTypeRename,
		namespace,
		namespace,
		namespace,
		s.SourcePVC,
		s.SourcePV,
		s.DestinationPVC,
		s.SourceTemplate,
	)
}

func identitySessionSpec(
	t domain.SessionType,
	source, destination, sessionNamespace string,
	sourcePVC, sourcePV, destinationPVC LocalResourceReference,
	template PVCSourceTemplate,
) domain.SessionSpec {
	spec := domain.SessionSpec{
		SessionCommon: domain.SessionCommon{
			SourceNamespace:      source,
			TemporaryNamespace:   destination,
			DestinationNamespace: destination,
			SessionNamespace:     sessionNamespace,
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC:           localRefToDomain(sourcePVC, source),
					SourcePV:            localRefToDomain(sourcePV, ""),
					SourcePVCSpec:       *template.Spec.DeepCopy(),
					SourcePVCMetadata:   pvcMetadataToDomain(template.Metadata),
					SourceReclaimPolicy: template.ReclaimPolicy,
					DestinationPVC:      localRefToDomain(destinationPVC, destination),
				},
			},
		},
		Type: t,
	}
	if t == domain.SessionTypeRename {
		spec.Rename = &domain.RenameSessionSpec{}
	} else {
		spec.Move = &domain.MoveSessionSpec{}
	}
	return spec
}

func MigrationSpecFromDomain(s domain.SessionSpec) MigrationSpec {
	options := s.WorkflowOptions()

	return MigrationSpec{
		Volumes:              volumesFromDomain(s.Volumes),
		SourceNode:           options.SourceNode,
		TargetNode:           options.TargetNode,
		ToolImage:            options.ToolImage,
		Strategies:           append([]string(nil), options.Strategies...),
		VerifyChecksum:       options.VerifyChecksum,
		DeleteExtraneous:     options.DeleteExtraneous,
		SkipSourceUsageCheck: options.SkipSourceUsageCheck,
	}
}

func PodMigrationSpecFromDomain(s domain.SessionSpec) PodMigrationSpec {
	options := s.WorkflowOptions()

	return PodMigrationSpec{
		Volumes:                volumesFromDomain(s.Volumes),
		SourceNode:             options.SourceNode,
		TargetNode:             options.TargetNode,
		ToolImage:              options.ToolImage,
		Strategies:             append([]string(nil), options.Strategies...),
		VerifyChecksum:         options.VerifyChecksum,
		DeleteExtraneous:       options.DeleteExtraneous,
		SkipSourceUsageCheck:   options.SkipSourceUsageCheck,
		Workload:               workloadFromDomain(s.Workload()),
		PrecopyPasses:          s.PrecopyPasses(),
		OpenEBSLVMEnableShared: s.OpenEBSLVMSharedMountEnabled(),
	}
}

func ReservationSpecFromDomain(s domain.SessionSpec) ReservationSpec {
	options := s.WorkflowOptions()

	return ReservationSpec{
		Volumes:              volumesFromDomain(s.Volumes),
		TargetNode:           options.TargetNode,
		ToolImage:            options.ToolImage,
		SkipSourceUsageCheck: options.SkipSourceUsageCheck,
	}
}

func CopySpecFromDomain(s domain.SessionSpec) CopySpec {
	options := s.WorkflowOptions()

	return CopySpec{
		Volumes:              volumesFromDomain(s.Volumes),
		SourceNode:           options.SourceNode,
		TargetNode:           options.TargetNode,
		ToolImage:            options.ToolImage,
		Strategies:           append([]string(nil), options.Strategies...),
		VerifyChecksum:       options.VerifyChecksum,
		DeleteExtraneous:     options.DeleteExtraneous,
		SkipSourceUsageCheck: options.SkipSourceUsageCheck,
		Online:               s.Online(),
	}
}

func BackupSpecFromDomain(s domain.SessionSpec) BackupSpec {
	p := s.Backup
	if p == nil {
		p = &domain.BackupSessionSpec{}
	}
	return BackupSpec{
		SourcePVC:              localRefFromDomain(p.SourcePVC),
		SourcePV:               localRefFromDomain(p.SourcePV),
		Path:                   p.Path,
		Name:                   p.Name,
		RepositoryRef:          repositoryRef(p.BackupRepository),
		Online:                 p.Online,
		OpenEBSLVMEnableShared: p.OpenEBSLVMEnableShared,
		ToolImage:              p.ToolImage,
		DeleteExtraneous:       p.DeleteExtraneous,
	}
}

func RestoreSpecFromDomain(s domain.SessionSpec) RestoreSpec {
	p := s.Restore
	if p == nil {
		p = &domain.RestoreSessionSpec{}
	}
	return RestoreSpec{
		DestinationPVC: LocalResourceReference{
			APIVersion: p.DestinationPVC.APIVersion,
			Kind:       p.DestinationPVC.Kind,
			Name:       p.DestinationPVC.Name,
		},
		Path:                    p.Path,
		Name:                    p.Name,
		RepositoryRef:           repositoryRef(p.BackupRepository),
		CreatePVC:               p.CreatePVC,
		DestinationStorageClass: p.DestinationStorageClass,
		DestinationAccessMode:   p.DestinationAccessMode,
		DestinationCapacity:     p.DestinationCapacity,
		AllowMounted:            p.AllowMounted,
		TargetNode:              p.TargetNode,
		ToolImage:               p.ToolImage,
		DeleteExtraneous:        p.DeleteExtraneous,
	}
}

func RenameSpecFromDomain(s domain.SessionSpec) RenameSpec {
	return RenameSpec{
		PVCIdentityFields: identityFieldsFromVolume(firstVolume(s.Volumes)),
	}
}

func identityFieldsFromVolume(v domain.VolumeSpec) PVCIdentityFields {
	return PVCIdentityFields{
		SourcePVC:      localRefFromDomain(v.SourcePVC),
		SourcePV:       localRefFromDomain(v.SourcePV),
		DestinationPVC: localRefFromDomain(v.DestinationPVC),
		SourceTemplate: PVCSourceTemplate{
			Spec:          *v.SourcePVCSpec.DeepCopy(),
			Metadata:      pvcMetadataFromDomain(v.SourcePVCMetadata),
			ReclaimPolicy: v.SourceReclaimPolicy,
		},
	}
}

func firstVolume(in []domain.VolumeSpec) domain.VolumeSpec {
	if len(in) == 0 {
		return domain.VolumeSpec{}
	}
	return in[0]
}

func pvcMetadataToDomain(m PVCMetadata) domain.PVCMetadata {
	return domain.PVCMetadata{
		Labels:          maps.Clone(m.Labels),
		Annotations:     maps.Clone(m.Annotations),
		OwnerReferences: copyOwnerReferences(m.OwnerReferences),
	}
}

func pvcMetadataFromDomain(m domain.PVCMetadata) PVCMetadata {
	return PVCMetadata{
		Labels:          maps.Clone(m.Labels),
		Annotations:     maps.Clone(m.Annotations),
		OwnerReferences: copyOwnerReferences(m.OwnerReferences),
	}
}

func copyOwnerReferences(in []metav1.OwnerReference) []metav1.OwnerReference {
	if in == nil {
		return nil
	}
	out := make([]metav1.OwnerReference, len(in))
	for i := range in {
		in[i].DeepCopyInto(&out[i])
	}
	return out
}

func refFromDomain(in domain.ObjectReference) ObjectReference {
	return ObjectReference{
		APIVersion:      in.APIVersion,
		Kind:            in.Kind,
		Namespace:       in.Namespace,
		Name:            in.Name,
		UID:             in.UID,
		ResourceVersion: in.ResourceVersion,
	}
}

func refToDomain(in ObjectReference) domain.ObjectReference {
	return domain.ObjectReference{
		APIVersion:      in.APIVersion,
		Kind:            in.Kind,
		Namespace:       in.Namespace,
		Name:            in.Name,
		UID:             in.UID,
		ResourceVersion: in.ResourceVersion,
	}
}

func localRefFromDomain(in domain.ObjectReference) LocalResourceReference {
	return LocalResourceReference{
		APIVersion:      in.APIVersion,
		Kind:            in.Kind,
		Name:            in.Name,
		UID:             in.UID,
		ResourceVersion: in.ResourceVersion,
	}
}

func localRefToDomain(
	in LocalResourceReference,
	namespace string,
) domain.ObjectReference {
	return domain.ObjectReference{
		APIVersion:      in.APIVersion,
		Kind:            in.Kind,
		Namespace:       namespace,
		Name:            in.Name,
		UID:             in.UID,
		ResourceVersion: in.ResourceVersion,
	}
}

func optionalLocalRefFromDomain(in domain.ObjectReference) *LocalResourceReference {
	if in.Name == "" {
		return nil
	}

	out := localRefFromDomain(in)
	return &out
}

func optionalLocalRefToDomain(
	in *LocalResourceReference,
	namespace string,
) domain.ObjectReference {
	if in == nil {
		return domain.ObjectReference{}
	}

	return localRefToDomain(*in, namespace)
}

func localRefsFromDomain(in []domain.ObjectReference) []LocalResourceReference {
	if in == nil {
		return nil
	}

	out := make([]LocalResourceReference, len(in))
	for i := range in {
		out[i] = localRefFromDomain(in[i])
	}

	return out
}

func localRefsToDomain(
	in []LocalResourceReference,
	namespace string,
) []domain.ObjectReference {
	if in == nil {
		return nil
	}

	out := make([]domain.ObjectReference, len(in))
	for i := range in {
		out[i] = localRefToDomain(in[i], namespace)
	}

	return out
}

func optionalRefFromDomain(in domain.ObjectReference) *ObjectReference {
	if in.Name == "" {
		return nil
	}
	out := refFromDomain(in)
	return &out
}

func optionalRefToDomain(in *ObjectReference) domain.ObjectReference {
	if in == nil {
		return domain.ObjectReference{}
	}
	return refToDomain(*in)
}

func refsFromDomain(in []domain.ObjectReference) []ObjectReference {
	if in == nil {
		return nil
	}
	out := make([]ObjectReference, len(in))
	for i := range in {
		out[i] = refFromDomain(in[i])
	}
	return out
}

func refsToDomain(in []ObjectReference) []domain.ObjectReference {
	if in == nil {
		return nil
	}
	out := make([]domain.ObjectReference, len(in))
	for i := range in {
		out[i] = refToDomain(in[i])
	}
	return out
}

func scopeFromDomain(in *domain.TransferScope) *TransferScope {
	if in == nil {
		return nil
	}
	return &TransferScope{SourcePath: in.SourcePath, DestinationPath: in.DestinationPath}
}

func scopeToDomain(in *TransferScope) *domain.TransferScope {
	if in == nil {
		return nil
	}
	return &domain.TransferScope{SourcePath: in.SourcePath, DestinationPath: in.DestinationPath}
}

func copyInt32(in *int32) *int32 {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func copyTime(in *metav1.Time) *metav1.Time {
	if in == nil {
		return nil
	}
	return in.DeepCopy()
}

func workloadFromDomain(w domain.WorkloadSpec) WorkloadSpec {
	out := WorkloadSpec{
		Adapter:          WorkloadKind(w.Adapter),
		Pod:              optionalLocalRefFromDomain(w.Pod),
		Controller:       optionalLocalRefFromDomain(w.Controller),
		OriginalReplicas: copyInt32(w.OriginalReplicas),
		Ordinal:          copyInt32(w.Ordinal),
		AffectedPods:     localRefsFromDomain(w.AffectedPods),
	}
	if len(w.OriginalObject) > 0 {
		out.OriginalObject = &apiextensionsv1.JSON{Raw: append([]byte(nil), w.OriginalObject...)}
	}
	if w.KubeBlocks != nil {
		out.KubeBlocks = &KubeBlocksSpec{
			Cluster:                  w.KubeBlocks.Cluster,
			Component:                w.KubeBlocks.Component,
			Instance:                 w.KubeBlocks.Instance,
			Role:                     w.KubeBlocks.Role,
			SwitchoverCandidate:      w.KubeBlocks.SwitchoverCandidate,
			SwitchoverStrategy:       string(w.KubeBlocks.SwitchoverStrategy),
			SwitchoverContainer:      w.KubeBlocks.SwitchoverContainer,
			OpsAPIVersion:            w.KubeBlocks.OpsAPIVersion,
			ClusterUID:               w.KubeBlocks.ClusterUID,
			OriginalPaused:           w.KubeBlocks.OriginalPaused,
			OriginalPausedConfigured: w.KubeBlocks.OriginalPausedConfigured,
		}
	}
	if w.VMCluster != nil {
		out.VMCluster = &VMClusterSpec{
			APIVersion:                      w.VMCluster.APIVersion,
			Name:                            w.VMCluster.Name,
			UID:                             w.VMCluster.UID,
			Component:                       w.VMCluster.Component,
			OriginalPaused:                  w.VMCluster.OriginalPaused,
			OriginalPausedConfigured:        w.VMCluster.OriginalPausedConfigured,
			OriginalClusterPaused:           w.VMCluster.OriginalClusterPaused,
			OriginalClusterPausedConfigured: w.VMCluster.OriginalClusterPausedConfigured,
			OriginalReplicas:                w.VMCluster.OriginalReplicas,
			OriginalReplicasConfigured:      w.VMCluster.OriginalReplicasConfigured,
		}
	}
	if w.Grafana != nil {
		out.Grafana = &GrafanaSpec{
			APIVersion:                w.Grafana.APIVersion,
			Name:                      w.Grafana.Name,
			UID:                       w.Grafana.UID,
			OriginalSuspend:           w.Grafana.OriginalSuspend,
			OriginalSuspendConfigured: w.Grafana.OriginalSuspendConfigured,
			OriginalReplicas:          w.Grafana.OriginalReplicas,
		}
	}
	return out
}

func workloadToDomain(w WorkloadSpec, namespace string) domain.WorkloadSpec {
	out := domain.WorkloadSpec{
		Adapter:          domain.WorkloadKind(w.Adapter),
		Pod:              optionalLocalRefToDomain(w.Pod, namespace),
		Controller:       optionalLocalRefToDomain(w.Controller, namespace),
		OriginalReplicas: copyInt32(w.OriginalReplicas),
		Ordinal:          copyInt32(w.Ordinal),
		AffectedPods:     localRefsToDomain(w.AffectedPods, namespace),
	}
	if w.OriginalObject != nil {
		out.OriginalObject = append([]byte(nil), w.OriginalObject.Raw...)
	}
	if w.KubeBlocks != nil {
		out.KubeBlocks = &domain.KubeBlocksSpec{
			Cluster:             w.KubeBlocks.Cluster,
			Component:           w.KubeBlocks.Component,
			Instance:            w.KubeBlocks.Instance,
			Role:                w.KubeBlocks.Role,
			SwitchoverCandidate: w.KubeBlocks.SwitchoverCandidate,
			SwitchoverStrategy: domain.KubeBlocksSwitchoverStrategy(
				w.KubeBlocks.SwitchoverStrategy,
			),
			SwitchoverContainer:      w.KubeBlocks.SwitchoverContainer,
			OpsAPIVersion:            w.KubeBlocks.OpsAPIVersion,
			ClusterUID:               w.KubeBlocks.ClusterUID,
			OriginalPaused:           w.KubeBlocks.OriginalPaused,
			OriginalPausedConfigured: w.KubeBlocks.OriginalPausedConfigured,
		}
	}
	if w.VMCluster != nil {
		out.VMCluster = &domain.VMClusterSpec{
			APIVersion:                      w.VMCluster.APIVersion,
			Name:                            w.VMCluster.Name,
			UID:                             w.VMCluster.UID,
			Component:                       w.VMCluster.Component,
			OriginalPaused:                  w.VMCluster.OriginalPaused,
			OriginalPausedConfigured:        w.VMCluster.OriginalPausedConfigured,
			OriginalClusterPaused:           w.VMCluster.OriginalClusterPaused,
			OriginalClusterPausedConfigured: w.VMCluster.OriginalClusterPausedConfigured,
			OriginalReplicas:                w.VMCluster.OriginalReplicas,
			OriginalReplicasConfigured:      w.VMCluster.OriginalReplicasConfigured,
		}
	}
	if w.Grafana != nil {
		out.Grafana = &domain.GrafanaSpec{
			APIVersion:                w.Grafana.APIVersion,
			Name:                      w.Grafana.Name,
			UID:                       w.Grafana.UID,
			OriginalSuspend:           w.Grafana.OriginalSuspend,
			OriginalSuspendConfigured: w.Grafana.OriginalSuspendConfigured,
			OriginalReplicas:          w.Grafana.OriginalReplicas,
		}
	}
	return out
}

func workflowStatusFromDomain(s domain.SessionStatus) WorkflowStatus {
	out := WorkflowStatus{
		Phase:              WorkflowPhase(s.Phase),
		ResumeFrom:         WorkflowPhase(s.ResumeFrom),
		FailureReason:      domain.BoundWorkflowMessage(string(s.FailureReason)),
		ObservedGeneration: s.ObservedGeneration,
		StartedAt:          s.StartedAt,
		UpdatedAt:          s.UpdatedAt,
		Message:            domain.BoundWorkflowMessage(s.Message),
	}
	if s.CompletedAt != nil {
		out.CompletedAt = s.CompletedAt.DeepCopy()
	}
	conditions := s.Conditions
	if len(conditions) > domain.MaxWorkflowConditions {
		conditions = conditions[len(conditions)-domain.MaxWorkflowConditions:]
	}
	out.Conditions = make([]WorkflowCondition, len(conditions))
	for i := range conditions {
		c := conditions[i]
		out.Conditions[i] = WorkflowCondition{
			Type:               domain.BoundWorkflowConditionType(c.Type),
			Status:             c.Status,
			Reason:             domain.BoundWorkflowReason(c.Reason),
			Message:            domain.BoundWorkflowMessage(c.Message),
			LastTransitionTime: c.LastTransitionTime,
		}
	}
	history := s.History
	if len(history) > domain.MaxWorkflowHistoryEntries {
		history = history[len(history)-domain.MaxWorkflowHistoryEntries:]
	}
	out.History = make([]WorkflowHistoryEntry, len(history))
	for i := range history {
		h := history[i]
		out.History[i] = WorkflowHistoryEntry{
			Phase:   WorkflowPhase(h.Phase),
			Time:    h.Time,
			Message: domain.BoundWorkflowMessage(h.Message),
		}
	}
	return out
}

func workflowStatusToDomain(s WorkflowStatus) domain.SessionStatus {
	out := domain.SessionStatus{
		Phase:              domain.Phase(s.Phase),
		ResumeFrom:         domain.Phase(s.ResumeFrom),
		FailureReason:      domain.SessionFailureReason(s.FailureReason),
		ObservedGeneration: s.ObservedGeneration,
		StartedAt:          s.StartedAt,
		UpdatedAt:          s.UpdatedAt,
		Message:            s.Message,
	}
	if s.CompletedAt != nil {
		out.CompletedAt = s.CompletedAt.DeepCopy()
	}
	out.Conditions = make([]domain.Condition, len(s.Conditions))
	for i := range s.Conditions {
		c := s.Conditions[i]
		out.Conditions[i] = domain.Condition{
			Type:               c.Type,
			Status:             c.Status,
			Reason:             c.Reason,
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime,
		}
	}
	out.History = make([]domain.HistoryEntry, len(s.History))
	for i := range s.History {
		h := s.History[i]
		out.History[i] = domain.HistoryEntry{
			Phase:   domain.Phase(h.Phase),
			Time:    h.Time,
			Message: h.Message,
		}
	}
	return out
}

func migrationVolumeStatusFromDomain(
	v domain.VolumeStatus,
	spec domain.VolumeSpec,
) MigrationVolumeStatus {
	return MigrationVolumeStatus{
		SourcePVCName:     v.SourcePVCName,
		DestinationPVC:    optionalLocalRefFromDomain(spec.DestinationPVC),
		DestinationPV:     optionalLocalRefFromDomain(spec.DestinationPV),
		DestinationPolicy: spec.DestinationPolicy,
		Reserved:          v.Reserved,
		Sync: MigrationSyncStatus{
			FinalCompletedAt: copyTime(v.Sync.FinalCompletedAt),
			Attempts:         v.Sync.Attempts,
			BytesCopied:      v.Sync.BytesCopied,
			ChecksumVerified: v.Sync.ChecksumVerified,
			LastError:        domain.BoundWorkflowMessage(v.Sync.LastError),
		},
		Activation: VolumeActivationStatus{
			TemporaryPVCDeleted: v.Activation.TemporaryPVCDeleted,
			SourcePVCDeleted:    v.Activation.SourcePVCDeleted,
			DestinationReserved: v.Activation.DestinationReserved,
			ActivePVC:           optionalLocalRefFromDomain(v.Activation.ActivePVC),
			ActivatedAt:         copyTime(v.Activation.ActivatedAt),
			RolledBackAt:        copyTime(v.Activation.RolledBackAt),
		},
	}
}

func migrationVolumeStatusToDomain(
	v MigrationVolumeStatus,
	namespace string,
) domain.VolumeStatus {
	return domain.VolumeStatus{
		SourcePVCName: v.SourcePVCName,
		Reserved:      v.Reserved,
		Sync: domain.SyncState{
			FinalCompletedAt: copyTime(v.Sync.FinalCompletedAt),
			Attempts:         v.Sync.Attempts,
			BytesCopied:      v.Sync.BytesCopied,
			ChecksumVerified: v.Sync.ChecksumVerified,
			LastError:        domain.BoundWorkflowMessage(v.Sync.LastError),
		},
		Activation: domain.ActivationState{
			TemporaryPVCDeleted: v.Activation.TemporaryPVCDeleted,
			SourcePVCDeleted:    v.Activation.SourcePVCDeleted,
			DestinationReserved: v.Activation.DestinationReserved,
			ActivePVC:           optionalLocalRefToDomain(v.Activation.ActivePVC, namespace),
			ActivatedAt:         copyTime(v.Activation.ActivatedAt),
			RolledBackAt:        copyTime(v.Activation.RolledBackAt),
		},
	}
}

func podMigrationVolumeStatusFromDomain(
	v domain.VolumeStatus,
	spec domain.VolumeSpec,
) PodMigrationVolumeStatus {
	return PodMigrationVolumeStatus{
		SourcePVCName:     v.SourcePVCName,
		DestinationPVC:    optionalLocalRefFromDomain(spec.DestinationPVC),
		DestinationPV:     optionalLocalRefFromDomain(spec.DestinationPV),
		DestinationPolicy: spec.DestinationPolicy,
		Reserved:          v.Reserved,
		Sync: PodMigrationSyncStatus{
			WarmCompletedAt:  copyTime(v.Sync.WarmCompletedAt),
			FinalCompletedAt: copyTime(v.Sync.FinalCompletedAt),
			Attempts:         v.Sync.Attempts,
			BytesCopied:      v.Sync.BytesCopied,
			ChecksumVerified: v.Sync.ChecksumVerified,
			LastError:        domain.BoundWorkflowMessage(v.Sync.LastError),
		},
		Activation: VolumeActivationStatus{
			TemporaryPVCDeleted: v.Activation.TemporaryPVCDeleted,
			SourcePVCDeleted:    v.Activation.SourcePVCDeleted,
			DestinationReserved: v.Activation.DestinationReserved,
			ActivePVC:           optionalLocalRefFromDomain(v.Activation.ActivePVC),
			ActivatedAt:         copyTime(v.Activation.ActivatedAt),
			RolledBackAt:        copyTime(v.Activation.RolledBackAt),
		},
	}
}

func podMigrationVolumeStatusToDomain(
	v PodMigrationVolumeStatus,
	namespace string,
) domain.VolumeStatus {
	return domain.VolumeStatus{
		SourcePVCName: v.SourcePVCName,
		Reserved:      v.Reserved,
		Sync: domain.SyncState{
			WarmCompletedAt:  copyTime(v.Sync.WarmCompletedAt),
			FinalCompletedAt: copyTime(v.Sync.FinalCompletedAt),
			Attempts:         v.Sync.Attempts,
			BytesCopied:      v.Sync.BytesCopied,
			ChecksumVerified: v.Sync.ChecksumVerified,
			LastError:        v.Sync.LastError,
		},
		Activation: domain.ActivationState{
			TemporaryPVCDeleted: v.Activation.TemporaryPVCDeleted,
			SourcePVCDeleted:    v.Activation.SourcePVCDeleted,
			DestinationReserved: v.Activation.DestinationReserved,
			ActivePVC:           optionalLocalRefToDomain(v.Activation.ActivePVC, namespace),
			ActivatedAt:         copyTime(v.Activation.ActivatedAt),
			RolledBackAt:        copyTime(v.Activation.RolledBackAt),
		},
	}
}

func reservationVolumeStatusFromDomain(
	v domain.VolumeStatus,
	spec domain.VolumeSpec,
) ReservationVolumeStatus {
	return ReservationVolumeStatus{
		SourcePVCName:     v.SourcePVCName,
		DestinationPVC:    optionalLocalRefFromDomain(spec.DestinationPVC),
		DestinationPV:     optionalLocalRefFromDomain(spec.DestinationPV),
		DestinationPolicy: spec.DestinationPolicy,
		Reserved:          v.Reserved,
	}
}

func reservationVolumeStatusToDomain(v ReservationVolumeStatus) domain.VolumeStatus {
	return domain.VolumeStatus{SourcePVCName: v.SourcePVCName, Reserved: v.Reserved}
}

func copyVolumeStatusFromDomain(v domain.VolumeStatus, spec domain.VolumeSpec) CopyVolumeStatus {
	return CopyVolumeStatus{
		SourcePVCName:     v.SourcePVCName,
		DestinationPVC:    optionalLocalRefFromDomain(spec.DestinationPVC),
		DestinationPV:     optionalLocalRefFromDomain(spec.DestinationPV),
		DestinationPolicy: spec.DestinationPolicy,
		Reserved:          v.Reserved,
		Sync: CopySyncStatus{
			WarmCompletedAt: copyTime(v.Sync.WarmCompletedAt),
			Attempts:        v.Sync.Attempts,
			BytesCopied:     v.Sync.BytesCopied,
			LastError:       domain.BoundWorkflowMessage(v.Sync.LastError),
		},
	}
}

func copyVolumeStatusToDomain(v CopyVolumeStatus) domain.VolumeStatus {
	return domain.VolumeStatus{
		SourcePVCName: v.SourcePVCName,
		Reserved:      v.Reserved,
		Sync: domain.SyncState{
			WarmCompletedAt: copyTime(v.Sync.WarmCompletedAt),
			Attempts:        v.Sync.Attempts,
			BytesCopied:     v.Sync.BytesCopied,
			LastError:       v.Sync.LastError,
		},
	}
}

func volumeSpecAt(volumes []domain.VolumeSpec, index int) domain.VolumeSpec {
	if index < 0 || index >= len(volumes) {
		return domain.VolumeSpec{}
	}

	return volumes[index]
}

func applyDestinationCheckpoint(
	volume *domain.VolumeSpec,
	destinationPVC *LocalResourceReference,
	destinationPV *LocalResourceReference,
	destinationNamespace string,
	policy PVReclaimPolicy,
) {
	if volume == nil {
		return
	}

	if destinationPVC != nil {
		volume.DestinationPVC = localRefToDomain(*destinationPVC, destinationNamespace)
	}
	if destinationPV != nil {
		volume.DestinationPV = localRefToDomain(*destinationPV, "")
	}
	if policy != "" {
		volume.DestinationPolicy = policy
	}
}

func pvcIdentityVolumeStatusFromDomain(v domain.VolumeStatus) PVCIdentityVolumeStatus {
	return PVCIdentityVolumeStatus{
		SourcePVCName: v.SourcePVCName,
		Activation: PVCIdentityActivationStatus{
			ActivePVC:    optionalLocalRefFromDomain(v.Activation.ActivePVC),
			ActivatedAt:  copyTime(v.Activation.ActivatedAt),
			RolledBackAt: copyTime(v.Activation.RolledBackAt),
		},
	}
}

func pvcIdentityVolumeStatusToDomain(
	v PVCIdentityVolumeStatus,
	namespace string,
) domain.VolumeStatus {
	return domain.VolumeStatus{
		SourcePVCName: v.SourcePVCName,
		Activation: domain.ActivationState{
			ActivePVC:    optionalLocalRefToDomain(v.Activation.ActivePVC, namespace),
			ActivatedAt:  copyTime(v.Activation.ActivatedAt),
			RolledBackAt: copyTime(v.Activation.RolledBackAt),
		},
	}
}

func sharedMountFromDomain(v domain.OpenEBSLVMSharedMount) SharedMountStatus {
	return SharedMountStatus{
		SourcePV:          localRefFromDomain(v.SourcePV),
		LVMVolume:         localRefFromDomain(v.LVMVolume),
		PreviousShared:    v.PreviousShared,
		PreviousSharedSet: v.PreviousSharedSet,
	}
}

func sharedMountToDomain(v SharedMountStatus) domain.OpenEBSLVMSharedMount {
	return domain.OpenEBSLVMSharedMount{
		SourcePV:          localRefToDomain(v.SourcePV, ""),
		LVMVolume:         localRefToDomain(v.LVMVolume, ""),
		PreviousShared:    v.PreviousShared,
		PreviousSharedSet: v.PreviousSharedSet,
	}
}

func podMigrationWorkloadStatusFromDomain(spec domain.SessionSpec) *PodMigrationWorkloadStatus {
	if spec.MigratePod == nil {
		return nil
	}

	workload := spec.MigratePod.Workload

	return &PodMigrationWorkloadStatus{
		Pod:          optionalLocalRefFromDomain(workload.Pod),
		AffectedPods: localRefsFromDomain(workload.AffectedPods),
	}
}

func migrationVolumeStatusesFromDomain(
	v []domain.VolumeStatus,
	specs []domain.VolumeSpec,
) []MigrationVolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]MigrationVolumeStatus, len(v))
	for i := range v {
		out[i] = migrationVolumeStatusFromDomain(v[i], volumeSpecAt(specs, i))
	}
	return out
}

func migrationVolumeStatusesToDomain(
	v []MigrationVolumeStatus,
	namespace string,
) []domain.VolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]domain.VolumeStatus, len(v))
	for i := range v {
		out[i] = migrationVolumeStatusToDomain(v[i], namespace)
	}
	return out
}

func podMigrationVolumeStatusesFromDomain(
	v []domain.VolumeStatus,
	specs []domain.VolumeSpec,
) []PodMigrationVolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]PodMigrationVolumeStatus, len(v))
	for i := range v {
		out[i] = podMigrationVolumeStatusFromDomain(v[i], volumeSpecAt(specs, i))
	}
	return out
}

func podMigrationVolumeStatusesToDomain(
	v []PodMigrationVolumeStatus,
	namespace string,
) []domain.VolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]domain.VolumeStatus, len(v))
	for i := range v {
		out[i] = podMigrationVolumeStatusToDomain(v[i], namespace)
	}
	return out
}

func reservationVolumeStatusesFromDomain(
	v []domain.VolumeStatus,
	specs []domain.VolumeSpec,
) []ReservationVolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]ReservationVolumeStatus, len(v))
	for i := range v {
		out[i] = reservationVolumeStatusFromDomain(v[i], volumeSpecAt(specs, i))
	}
	return out
}

func reservationVolumeStatusesToDomain(v []ReservationVolumeStatus) []domain.VolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]domain.VolumeStatus, len(v))
	for i := range v {
		out[i] = reservationVolumeStatusToDomain(v[i])
	}
	return out
}

func copyVolumeStatusesFromDomain(
	v []domain.VolumeStatus,
	specs []domain.VolumeSpec,
) []CopyVolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]CopyVolumeStatus, len(v))
	for i := range v {
		out[i] = copyVolumeStatusFromDomain(v[i], volumeSpecAt(specs, i))
	}
	return out
}

func copyVolumeStatusesToDomain(v []CopyVolumeStatus) []domain.VolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]domain.VolumeStatus, len(v))
	for i := range v {
		out[i] = copyVolumeStatusToDomain(v[i])
	}
	return out
}

func pvcIdentityVolumeStatusesFromDomain(v []domain.VolumeStatus) []PVCIdentityVolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]PVCIdentityVolumeStatus, len(v))
	for i := range v {
		out[i] = pvcIdentityVolumeStatusFromDomain(v[i])
	}
	return out
}

func pvcIdentityVolumeStatusesToDomain(
	v []PVCIdentityVolumeStatus,
	namespace string,
) []domain.VolumeStatus {
	if v == nil {
		return nil
	}
	out := make([]domain.VolumeStatus, len(v))
	for i := range v {
		out[i] = pvcIdentityVolumeStatusToDomain(v[i], namespace)
	}
	return out
}

func sharedMountsFromDomain(v []domain.OpenEBSLVMSharedMount) []SharedMountStatus {
	if v == nil {
		return nil
	}
	out := make([]SharedMountStatus, len(v))
	for i := range v {
		out[i] = sharedMountFromDomain(v[i])
	}
	return out
}

func sharedMountsToDomain(v []SharedMountStatus) []domain.OpenEBSLVMSharedMount {
	if v == nil {
		return nil
	}
	out := make([]domain.OpenEBSLVMSharedMount, len(v))
	for i := range v {
		out[i] = sharedMountToDomain(v[i])
	}
	return out
}

func (s MigrationStatus) Domain(namespace string) domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)
	out.Volumes = migrationVolumeStatusesToDomain(s.Volumes, namespace)
	return out
}

// ApplyToDomainSpec restores controller-owned destination identities from
// status into the internal state-machine representation.
func (s MigrationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil {
		return
	}
	for i := range min(len(spec.Volumes), len(s.Volumes)) {
		checkpoint := s.Volumes[i]
		applyDestinationCheckpoint(
			&spec.Volumes[i],
			checkpoint.DestinationPVC,
			checkpoint.DestinationPV,
			spec.DestinationNamespace,
			checkpoint.DestinationPolicy,
		)
	}
}

func (s PodMigrationStatus) Domain(namespace string) domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)
	out.WarmPassesCompleted = s.WarmPassesCompleted
	out.OriginalPodSnapshotHash = s.OriginalPodSnapshotHash
	out.Volumes = podMigrationVolumeStatusesToDomain(s.Volumes, namespace)
	out.OpenEBSLVMSharedMounts = sharedMountsToDomain(s.OpenEBSLVMSharedMounts)
	return out
}

func (s PodMigrationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil {
		return
	}
	for i := range min(len(spec.Volumes), len(s.Volumes)) {
		checkpoint := s.Volumes[i]
		applyDestinationCheckpoint(
			&spec.Volumes[i],
			checkpoint.DestinationPVC,
			checkpoint.DestinationPV,
			spec.DestinationNamespace,
			checkpoint.DestinationPolicy,
		)
	}

	workload := spec.WorkloadPtr()
	if workload == nil || s.Workload == nil {
		return
	}
	if s.Workload.Pod != nil {
		workload.Pod = localRefToDomain(*s.Workload.Pod, spec.SourceNamespace)
	}
	if len(s.Workload.AffectedPods) > 0 {
		workload.AffectedPods = localRefsToDomain(s.Workload.AffectedPods, spec.SourceNamespace)
	}
}

func (s ReservationStatus) Domain(_ string) domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)
	out.Volumes = reservationVolumeStatusesToDomain(s.Volumes)
	return out
}

func (s ReservationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil {
		return
	}
	for i := range min(len(spec.Volumes), len(s.Volumes)) {
		checkpoint := s.Volumes[i]
		applyDestinationCheckpoint(
			&spec.Volumes[i],
			checkpoint.DestinationPVC,
			checkpoint.DestinationPV,
			spec.DestinationNamespace,
			checkpoint.DestinationPolicy,
		)
	}
}

func (s CopyStatus) Domain(_ string) domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)
	out.Volumes = copyVolumeStatusesToDomain(s.Volumes)
	return out
}

func (s CopyStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil {
		return
	}
	for i := range min(len(spec.Volumes), len(s.Volumes)) {
		checkpoint := s.Volumes[i]
		applyDestinationCheckpoint(
			&spec.Volumes[i],
			checkpoint.DestinationPVC,
			checkpoint.DestinationPV,
			spec.DestinationNamespace,
			checkpoint.DestinationPolicy,
		)
	}
}

func (s BackupStatus) Domain() domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)
	out.BackupRepository = repositoryBindingStatusToDomain(s.Repository)
	out.OpenEBSLVMSharedMounts = sharedMountsToDomain(s.OpenEBSLVMSharedMounts)
	return out
}

func (s RestoreStatus) Domain() domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)
	out.BackupRepository = repositoryBindingStatusToDomain(s.Repository)
	return out
}

// ApplyToDomainSpec restores the controller-owned destination checkpoint while
// preserving the user-selected PVC name and namespace from spec.
func (s RestoreStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil || spec.Restore == nil {
		return
	}

	if s.DestinationPVC != nil {
		checkpoint := refToDomain(*s.DestinationPVC)
		destination := spec.Restore.DestinationPVC
		if checkpoint.Name == destination.Name && checkpoint.Namespace == destination.Namespace {
			spec.Restore.DestinationPVC = checkpoint
		}
	}

	if s.DestinationPV != nil {
		spec.Restore.DestinationPV = refToDomain(*s.DestinationPV)
	}
}

func (s RenameStatus) Domain(namespace string) domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)
	out.Volumes = pvcIdentityVolumeStatusesToDomain(s.Volumes, namespace)
	return out
}

func MigrationStatusFromDomain(
	s domain.SessionStatus,
	volumes []domain.VolumeSpec,
) MigrationStatus {
	return MigrationStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Volumes:        migrationVolumeStatusesFromDomain(s.Volumes, volumes),
	}
}

func PodMigrationStatusFromDomain(
	s domain.SessionStatus,
	spec domain.SessionSpec,
) PodMigrationStatus {
	return PodMigrationStatus{
		WorkflowStatus:          workflowStatusFromDomain(s),
		WarmPassesCompleted:     s.WarmPassesCompleted,
		OriginalPodSnapshotHash: s.OriginalPodSnapshotHash,
		Workload:                podMigrationWorkloadStatusFromDomain(spec),
		Volumes:                 podMigrationVolumeStatusesFromDomain(s.Volumes, spec.Volumes),
		OpenEBSLVMSharedMounts:  sharedMountsFromDomain(s.OpenEBSLVMSharedMounts),
	}
}

func ReservationStatusFromDomain(
	s domain.SessionStatus,
	volumes []domain.VolumeSpec,
) ReservationStatus {
	return ReservationStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Volumes:        reservationVolumeStatusesFromDomain(s.Volumes, volumes),
	}
}

func CopyStatusFromDomain(s domain.SessionStatus, volumes []domain.VolumeSpec) CopyStatus {
	return CopyStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Volumes:        copyVolumeStatusesFromDomain(s.Volumes, volumes),
	}
}

func BackupStatusFromDomain(s domain.SessionStatus) BackupStatus {
	return BackupStatus{
		WorkflowStatus:         workflowStatusFromDomain(s),
		Repository:             repositoryBindingStatusFromDomain(s.BackupRepository),
		OpenEBSLVMSharedMounts: sharedMountsFromDomain(s.OpenEBSLVMSharedMounts),
	}
}

func RestoreStatusFromDomain(s domain.SessionStatus, spec domain.SessionSpec) RestoreStatus {
	status := RestoreStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Repository:     repositoryBindingStatusFromDomain(s.BackupRepository),
	}
	if spec.Restore == nil {
		return status
	}

	if spec.Restore.DestinationPVC.UID != "" {
		status.DestinationPVC = optionalRefFromDomain(spec.Restore.DestinationPVC)
	}
	if spec.Restore.DestinationPV.Name != "" && spec.Restore.DestinationPV.UID != "" {
		status.DestinationPV = optionalRefFromDomain(spec.Restore.DestinationPV)
	}

	return status
}

func repositoryBindingStatusToDomain(
	binding *BackupRepositoryBindingStatus,
) *domain.BackupRepositoryBindingStatus {
	if binding == nil {
		return nil
	}

	out := &domain.BackupRepositoryBindingStatus{
		Type:       domain.BackupRepositoryType(binding.Type),
		UID:        binding.UID,
		Generation: binding.Generation,
	}
	if binding.S3 != nil {
		out.S3 = &domain.S3BackupRepositoryBindingStatus{
			CredentialsSecretUID: binding.S3.CredentialsSecretUID,
		}
	}
	if binding.PVC != nil {
		out.PVC = &domain.PVCBackupRepositoryBindingStatus{ClaimUID: binding.PVC.ClaimUID}
	}

	return out
}

func repositoryBindingStatusFromDomain(
	binding *domain.BackupRepositoryBindingStatus,
) *BackupRepositoryBindingStatus {
	if binding == nil {
		return nil
	}

	out := &BackupRepositoryBindingStatus{
		Type:       BackupRepositoryType(binding.Type),
		UID:        binding.UID,
		Generation: binding.Generation,
	}
	if binding.S3 != nil {
		out.S3 = &S3BackupRepositoryBindingStatus{
			CredentialsSecretUID: binding.S3.CredentialsSecretUID,
		}
	}
	if binding.PVC != nil {
		out.PVC = &PVCBackupRepositoryBindingStatus{ClaimUID: binding.PVC.ClaimUID}
	}

	return out
}

func RenameStatusFromDomain(s domain.SessionStatus) RenameStatus {
	return RenameStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Volumes:        pvcIdentityVolumeStatusesFromDomain(s.Volumes),
	}
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=pmig
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type PodMigration struct {
	metav1.TypeMeta   `                   json:",inline"`
	metav1.ObjectMeta `                   json:"metadata,omitempty"`
	Spec              PodMigrationSpec   `json:"spec"`
	Status            PodMigrationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PodMigrationList struct {
	metav1.TypeMeta `               json:",inline"`
	metav1.ListMeta `               json:"metadata,omitempty"`
	Items           []PodMigration `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=resv
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Reservation struct {
	metav1.TypeMeta   `                  json:",inline"`
	metav1.ObjectMeta `                  json:"metadata,omitempty"`
	Spec              ReservationSpec   `json:"spec"`
	Status            ReservationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ReservationList struct {
	metav1.TypeMeta `              json:",inline"`
	metav1.ListMeta `              json:"metadata,omitempty"`
	Items           []Reservation `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=pcopy
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Copy struct {
	metav1.TypeMeta   `           json:",inline"`
	metav1.ObjectMeta `           json:"metadata,omitempty"`
	Spec              CopySpec   `json:"spec"`
	Status            CopyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type CopyList struct {
	metav1.TypeMeta `       json:",inline"`
	metav1.ListMeta `       json:"metadata,omitempty"`
	Items           []Copy `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=pback
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Backup struct {
	metav1.TypeMeta   `             json:",inline"`
	metav1.ObjectMeta `             json:"metadata,omitempty"`
	Spec              BackupSpec   `json:"spec"`
	Status            BackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type BackupList struct {
	metav1.TypeMeta `         json:",inline"`
	metav1.ListMeta `         json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=rest
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Restore struct {
	metav1.TypeMeta   `              json:",inline"`
	metav1.ObjectMeta `              json:"metadata,omitempty"`
	Spec              RestoreSpec   `json:"spec"`
	Status            RestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type RestoreList struct {
	metav1.TypeMeta `          json:",inline"`
	metav1.ListMeta `          json:"metadata,omitempty"`
	Items           []Restore `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=prename
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Rename struct {
	metav1.TypeMeta   `             json:",inline"`
	metav1.ObjectMeta `             json:"metadata,omitempty"`
	Spec              RenameSpec   `json:"spec"`
	Status            RenameStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type RenameList struct {
	metav1.TypeMeta `         json:",inline"`
	metav1.ListMeta `         json:"metadata,omitempty"`
	Items           []Rename `json:"items"`
}
