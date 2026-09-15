//nolint:wsl_v5 // Durable API fields keep explicit JSON and YAML tags together.
package v1alpha1

import (
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

// +kubebuilder:validation:XValidation:rule="has(self.sourcePVC.uid) && size(self.sourcePVC.uid) > 0 && has(self.sourcePV.uid) && size(self.sourcePV.uid) > 0",message="sourcePVC.uid and sourcePV.uid are required planning identities"
// VolumeSpec is planning output required to resume a PVC transfer. It is an
// API-owned type with only the fields needed by transfer workflows.
// PVC references are relative to the workflow's source and destination
// namespace roles; PV references are always cluster-scoped.
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
// +kubebuilder:validation:XValidation:rule="self.adapter == 'None' || (has(self.pod) && has(self.pod.apiVersion) && size(self.pod.apiVersion) > 0 && has(self.pod.kind) && size(self.pod.kind) > 0 && has(self.pod.name) && size(self.pod.name) > 0 && has(self.pod.uid) && size(self.pod.uid) > 0)",message="workload.pod must include apiVersion, kind, name, and uid"
// +kubebuilder:validation:XValidation:rule="self.adapter == 'None' || self.adapter == 'StandalonePod' || (has(self.controller) && has(self.controller.apiVersion) && size(self.controller.apiVersion) > 0 && has(self.controller.kind) && size(self.controller.kind) > 0 && has(self.controller.name) && size(self.controller.name) > 0 && has(self.controller.uid) && size(self.controller.uid) > 0)",message="workload.controller must include apiVersion, kind, name, and uid for managed workloads"
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

// KubeBlocksSwitchoverStrategy identifies the planned leader handoff mechanism.
type KubeBlocksSwitchoverStrategy string

const (
	KubeBlocksSwitchoverOpsRequest    KubeBlocksSwitchoverStrategy = "opsrequest"
	KubeBlocksSwitchoverMongoDBNative KubeBlocksSwitchoverStrategy = "mongodb-native"
)

type KubeBlocksSpec struct {
	Cluster                  string                       `json:"cluster"                            yaml:"cluster"`
	Component                string                       `json:"component"                          yaml:"component"`
	Instance                 string                       `json:"instance"                           yaml:"instance"`
	Role                     string                       `json:"role,omitempty"                     yaml:"role,omitempty"`
	SwitchoverCandidate      string                       `json:"switchoverCandidate,omitempty"      yaml:"switchoverCandidate,omitempty"`
	SwitchoverStrategy       KubeBlocksSwitchoverStrategy `json:"switchoverStrategy,omitempty"       yaml:"switchoverStrategy,omitempty"`
	SwitchoverContainer      string                       `json:"switchoverContainer,omitempty"      yaml:"switchoverContainer,omitempty"`
	OpsAPIVersion            string                       `json:"opsAPIVersion"                      yaml:"opsAPIVersion"`
	ClusterUID               types.UID                    `json:"clusterUID"                         yaml:"clusterUID"`
	OriginalPaused           bool                         `json:"originalPaused,omitempty"           yaml:"originalPaused,omitempty"`
	OriginalPausedConfigured bool                         `json:"originalPausedConfigured,omitempty" yaml:"originalPausedConfigured,omitempty"`
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

// +kubebuilder:validation:XValidation:rule="has(self.volumes) && size(self.volumes) > 0",message="volumes must contain at least one source PVC"
// MigrationPlan is an offline PVC migration. It has no workload controls.
type MigrationPlan struct {
	// +kubebuilder:validation:Enum=Retain;Delete
	SourcePVReclaimPolicy string `json:"sourcePVReclaimPolicy,omitempty" yaml:"sourcePVReclaimPolicy,omitempty"`
	// +kubebuilder:validation:Enum=Retain;Delete
	DestinationPVCReclaimPolicy string `json:"destinationPVCReclaimPolicy,omitempty" yaml:"destinationPVCReclaimPolicy,omitempty"`
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
// +kubebuilder:validation:XValidation:rule="self.workload.adapter != 'None'",message="PodMigration workload.adapter must identify a supported workload"
// PodMigrationPlan is a workload-aware migration. Workload and precopy
// controls are exclusive to this operation.
type PodMigrationPlan struct {
	// +kubebuilder:validation:Enum=Retain;Delete
	SourcePVReclaimPolicy string `json:"sourcePVReclaimPolicy,omitempty" yaml:"sourcePVReclaimPolicy,omitempty"`
	// +kubebuilder:validation:Enum=Retain;Delete
	DestinationPVCReclaimPolicy string `json:"destinationPVCReclaimPolicy,omitempty" yaml:"destinationPVCReclaimPolicy,omitempty"`
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

// +kubebuilder:validation:XValidation:rule="has(self.volumes) && size(self.volumes) > 0",message="volumes must contain at least one source PVC"
type ReservationPlan struct {
	// +kubebuilder:validation:Enum=Retain;Delete
	DestinationPVCReclaimPolicy string `json:"destinationPVCReclaimPolicy,omitempty" yaml:"destinationPVCReclaimPolicy,omitempty"`
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
type CopyPlan struct {
	// +kubebuilder:validation:Enum=Retain;Delete
	DestinationPVCReclaimPolicy string `json:"destinationPVCReclaimPolicy,omitempty" yaml:"destinationPVCReclaimPolicy,omitempty"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes    []VolumeSpec `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
	SourceNode string       `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	TargetNode string       `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	ToolImage  string       `json:"toolImage,omitempty"  yaml:"toolImage,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Strategies []string `json:"strategies,omitempty" yaml:"strategies,omitempty"`
	// VerifyChecksum enables rsync checksum comparison during final sync. It
	// defaults to false when omitted.
	VerifyChecksum       bool `json:"verifyChecksum,omitempty"       yaml:"verifyChecksum,omitempty"`
	DeleteExtraneous     bool `json:"deleteExtraneous,omitempty"     yaml:"deleteExtraneous,omitempty"`
	SkipSourceUsageCheck bool `json:"skipSourceUsageCheck,omitempty" yaml:"skipSourceUsageCheck,omitempty"`
	Online               bool `json:"online,omitempty"               yaml:"online,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="has(self.sourcePVC.uid) && size(self.sourcePVC.uid) > 0 && has(self.sourcePV.uid) && size(self.sourcePV.uid) > 0",message="sourcePVC.uid and sourcePV.uid are required planning identities"
type BackupPlan struct {
	SourcePVC LocalResourceReference `json:"sourcePVC"      yaml:"sourcePVC"`
	SourcePV  LocalResourceReference `json:"sourcePV"       yaml:"sourcePV"`
	Path      string                 `json:"path,omitempty" yaml:"path,omitempty"`
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
}

type RestorePlan struct {
	DestinationPVC LocalResourceReference `json:"destinationPVC" yaml:"destinationPVC"`
	Path           string                 `json:"path,omitempty" yaml:"path,omitempty"`
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

// +kubebuilder:validation:XValidation:rule="has(self.sourcePVC.uid) && size(self.sourcePVC.uid) > 0 && has(self.sourcePV.uid) && size(self.sourcePV.uid) > 0",message="sourcePVC.uid and sourcePV.uid are required planning identities"
type PVCIdentityFields struct {
	SourcePVC      LocalResourceReference `json:"sourcePVC"      yaml:"sourcePVC"`
	SourcePV       LocalResourceReference `json:"sourcePV"       yaml:"sourcePV"`
	DestinationPVC LocalResourceReference `json:"destinationPVC" yaml:"destinationPVC"`
	SourceTemplate PVCSourceTemplate      `json:"sourceTemplate" yaml:"sourceTemplate"`
}

type RenamePlan struct {
	PVCIdentityFields `json:",inline" yaml:",inline"`
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
	ExecutionIntentHash string `json:"executionIntentHash,omitempty" yaml:"executionIntentHash,omitempty"`
	// +kubebuilder:validation:Enum=validation;precondition;conflict;kubernetes;copy;timeout;internal
	ErrorCategory string        `json:"errorCategory,omitempty" yaml:"errorCategory,omitempty"`
	Phase         WorkflowPhase `json:"phase,omitempty"         yaml:"phase,omitempty"`
	ResumeFrom    WorkflowPhase `json:"resumeFrom,omitempty"    yaml:"resumeFrom,omitempty"`
	// +kubebuilder:validation:MaxLength=8192
	FailureReason      string       `json:"failureReason,omitempty"      yaml:"failureReason,omitempty"`
	ObservedGeneration int64        `json:"observedGeneration,omitempty" yaml:"observedGeneration,omitempty"`
	StartedAt          metav1.Time  `json:"startedAt,omitempty"          yaml:"startedAt,omitempty"`
	UpdatedAt          metav1.Time  `json:"updatedAt,omitempty"          yaml:"updatedAt,omitempty"`
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
	SourcePV LocalResourceReference `json:"sourcePV" yaml:"sourcePV"`
	// LVMVolume can live outside the workflow namespace.
	LVMVolume         ObjectReference `json:"lvmVolume"                   yaml:"lvmVolume"`
	PreviousShared    string          `json:"previousShared,omitempty"    yaml:"previousShared,omitempty"`
	PreviousSharedSet bool            `json:"previousSharedSet,omitempty" yaml:"previousSharedSet,omitempty"`
}

// PodMigrationWorkloadStatus tracks Pod identities recreated while pausing,
// resuming, or rolling back a workload. The original migration target and
// recovery snapshot remain immutable in status.plan.workload.
type PodMigrationWorkloadStatus struct {
	Pod          *LocalResourceReference  `json:"pod,omitempty"          yaml:"pod,omitempty"`
	AffectedPods []LocalResourceReference `json:"affectedPods,omitempty" yaml:"affectedPods,omitempty"`
}

// MigrationVolumeStatus is the durable checkpoint for an offline migration
// volume. It intentionally excludes workload-only progress and OpenEBS
// shared-mount state.
// VolumeReservationStatus is the storage-provisioning checkpoint shared by
// workflows that reserve a destination volume. Copy and activation progress
// remain in their owning operation types.
type VolumeReservationStatus struct {
	SourcePVCName     string                  `json:"sourcePVCName"                      yaml:"sourcePVCName"`
	DestinationPVC    *LocalResourceReference `json:"destinationPVC,omitempty"           yaml:"destinationPVC,omitempty"`
	DestinationPV     *LocalResourceReference `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy PVReclaimPolicy         `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Reserved          bool                    `json:"reserved,omitempty"                 yaml:"reserved,omitempty"`
}

type MigrationVolumeStatus struct {
	VolumeReservationStatus `                       json:",inline"    yaml:",inline"`
	Sync                    MigrationSyncStatus    `json:"sync"       yaml:"sync"`
	Activation              VolumeActivationStatus `json:"activation" yaml:"activation"`
}

// PodMigrationVolumeStatus is the durable checkpoint for a workload-aware
// migration volume. It is a distinct API type even though its transfer and
// activation fields currently match MigrationVolumeStatus.
type PodMigrationVolumeStatus struct {
	VolumeReservationStatus `                       json:",inline"    yaml:",inline"`
	Sync                    PodMigrationSyncStatus `json:"sync"       yaml:"sync"`
	Activation              VolumeActivationStatus `json:"activation" yaml:"activation"`
}

// ReservationVolumeStatus reports only destination reservation progress.
// Copying and activation checkpoints are not part of a Reservation contract.
type ReservationVolumeStatus struct {
	VolumeReservationStatus `json:",inline" yaml:",inline"`
}

// CopyVolumeStatus reports reservation and copy progress. PVC activation is
// owned by Migration/PodMigration and is intentionally absent here.
type CopyVolumeStatus struct {
	VolumeReservationStatus `               json:",inline" yaml:",inline"`
	Sync                    CopySyncStatus `json:"sync"    yaml:"sync"`
}

// RenameActivationStatus is the checkpoint needed to roll back a Rename.
// Temporary-volume cleanup fields do not apply to identity operations.
type RenameActivationStatus struct {
	ActivePVC    *LocalResourceReference `json:"activePVC,omitempty"    yaml:"activePVC,omitempty"`
	ActivatedAt  *metav1.Time            `json:"activatedAt,omitempty"  yaml:"activatedAt,omitempty"`
	RolledBackAt *metav1.Time            `json:"rolledBackAt,omitempty" yaml:"rolledBackAt,omitempty"`
}

type MigrationStatus struct {
	Plan           *MigrationPlan `json:"plan,omitempty"    yaml:"plan,omitempty"`
	WorkflowStatus `                        json:",inline"           yaml:",inline"`
	Volumes        []MigrationVolumeStatus `json:"volumes,omitempty" yaml:"volumes,omitempty"`
}

type PodMigrationStatus struct {
	Plan                *PodMigrationPlan `json:"plan,omitempty"      yaml:"plan,omitempty"`
	WorkflowStatus      `                  json:",inline"             yaml:",inline"`
	WarmPassesCompleted int `json:"warmPassesCompleted" yaml:"warmPassesCompleted"`
	// OriginalPodSnapshotHash is controller-owned evidence that the standalone
	// Pod snapshot was captured from the referenced live Pod before execution.
	OriginalPodSnapshotHash string                      `json:"originalPodSnapshotHash,omitempty" yaml:"originalPodSnapshotHash,omitempty"`
	Workload                *PodMigrationWorkloadStatus `json:"workload,omitempty"                yaml:"workload,omitempty"`
	Volumes                 []PodMigrationVolumeStatus  `json:"volumes,omitempty"                 yaml:"volumes,omitempty"`
	OpenEBSLVMSharedMounts  []SharedMountStatus         `json:"openebsLvmSharedMounts,omitempty"  yaml:"openebsLvmSharedMounts,omitempty"`
}

type ReservationStatus struct {
	Plan           *ReservationPlan `json:"plan,omitempty"    yaml:"plan,omitempty"`
	WorkflowStatus `                          json:",inline"           yaml:",inline"`
	Volumes        []ReservationVolumeStatus `json:"volumes,omitempty" yaml:"volumes,omitempty"`
}
type CopyStatus struct {
	// SourceNode checkpoints runtime placement inferred from online consumers.
	SourceNode     string    `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	Plan           *CopyPlan `json:"plan,omitempty"       yaml:"plan,omitempty"`
	WorkflowStatus `                   json:",inline"              yaml:",inline"`
	Volumes        []CopyVolumeStatus `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
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

	S3  *S3BackupRepositoryBindingStatus  `json:"s3,omitempty"  yaml:"s3,omitempty"`
	PVC *PVCBackupRepositoryBindingStatus `json:"pvc,omitempty" yaml:"pvc,omitempty"`
}

type BackupStatus struct {
	Plan                   *BackupPlan `json:"plan,omitempty"                   yaml:"plan,omitempty"`
	WorkflowStatus         `                               json:",inline"                          yaml:",inline"`
	Repository             *BackupRepositoryBindingStatus `json:"repository,omitempty"             yaml:"repository,omitempty"`
	OpenEBSLVMSharedMounts []SharedMountStatus            `json:"openebsLvmSharedMounts,omitempty" yaml:"openebsLvmSharedMounts,omitempty"`
}
type RestoreStatus struct {
	Plan           *RestorePlan `json:"plan,omitempty"           yaml:"plan,omitempty"`
	WorkflowStatus `                               json:",inline"                  yaml:",inline"`
	Repository     *BackupRepositoryBindingStatus `json:"repository,omitempty"     yaml:"repository,omitempty"`
	DestinationPVC *ObjectReference               `json:"destinationPVC,omitempty" yaml:"destinationPVC,omitempty"`
	DestinationPV  *ObjectReference               `json:"destinationPV,omitempty"  yaml:"destinationPV,omitempty"`
}

type RenameStatus struct {
	Plan           *RenamePlan `json:"plan,omitempty" yaml:"plan,omitempty"`
	WorkflowStatus `                       json:",inline"        yaml:",inline"`
	Activation     RenameActivationStatus `json:"activation"     yaml:"activation"`
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
