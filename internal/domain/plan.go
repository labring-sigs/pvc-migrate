package domain

import (
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

const (
	MigrationPlanKind     = "MigrationPlan"
	OrphanCleanupPlanKind = "OrphanCleanupPlan"
)

type OrphanCleanupMode string

const (
	OrphanCleanupPreActivation  OrphanCleanupMode = "PreActivation"
	OrphanCleanupPostActivation OrphanCleanupMode = "PostActivation"
)

type CheckSeverity string

const (
	SeverityInfo    CheckSeverity = "info"
	SeverityWarning CheckSeverity = "warning"
	SeverityError   CheckSeverity = "error"
)

// CheckName is a stable machine-readable planning check identifier. CLI
// guidance and external plan consumers branch on these values, while Message
// remains human-readable detail.
type CheckName string

const (
	// Request and execution checks.
	CheckNameAccessMode             CheckName = "access-mode"
	CheckNameAllowLeaderDowntime    CheckName = "allow-leader-downtime"
	CheckNameCapacity               CheckName = "capacity"
	CheckNameCapacityAwareness      CheckName = "capacity-awareness"
	CheckNameClusters               CheckName = "clusters"
	CheckNameCopyMode               CheckName = "copy-mode"
	CheckNameForceReprovision       CheckName = "force-reprovision"
	CheckNameIdentity               CheckName = "identity"
	CheckNameNamespace              CheckName = "namespace"
	CheckNameOnline                 CheckName = "online"
	CheckNameOpenEBSLVMEnableShared CheckName = "openebs-lvm-enable-shared"
	CheckNameOperation              CheckName = "operation"
	CheckNamePrecopyPasses          CheckName = "precopy-passes"
	CheckNameSessionID              CheckName = "session-id"
	CheckNameStrategy               CheckName = "strategy"
	CheckNameStrategySelection      CheckName = "strategy-selection"
	CheckNameToolImage              CheckName = "tool-image"
	CheckNameTransferPath           CheckName = "transfer-path"
	CheckNameTransferScope          CheckName = "transfer-scope"

	// Workload, node, and policy checks.
	CheckNameActivationPolicy    CheckName = "activation-policy"
	CheckNameControllerAdapter   CheckName = "controller-adapter"
	CheckNameDatabasePauseScope  CheckName = "database-pause-scope"
	CheckNameDatabaseRole        CheckName = "database-role"
	CheckNameKubeBlocksCandidate CheckName = "kubeblocks-candidate"
	CheckNameLimitRange          CheckName = "limit-range"
	CheckNameNetworkPolicy       CheckName = "network-policy"
	CheckNamePod                 CheckName = "pod"
	CheckNamePodDependencies     CheckName = "pod-dependencies"
	CheckNamePodResources        CheckName = "pod-resources"
	CheckNamePodScheduling       CheckName = "pod-scheduling"
	CheckNameRBAC                CheckName = "rbac"
	CheckNameResourceQuota       CheckName = "resource-quota"
	CheckNameSourceConsumers     CheckName = "source-consumers"
	CheckNameSourceNode          CheckName = "source-node"
	CheckNameSourceNodeInference CheckName = "source-node-inference"
	CheckNameSourcePod           CheckName = "source-pod"
	CheckNameTargetNode          CheckName = "target-node"
	CheckNameTargetNodeSelection CheckName = "target-node-selection"

	// Storage and transfer checks.
	CheckNameAvailabilityZone            CheckName = "availability-zone"
	CheckNameCSINode                     CheckName = "csi-node"
	CheckNameDestinationAccessModes      CheckName = "destination-access-modes"
	CheckNameDestinationCapacity         CheckName = "destination-capacity"
	CheckNameDestinationNamespace        CheckName = "destination-namespace"
	CheckNameDestinationPath             CheckName = "destination-path"
	CheckNameDestinationPV               CheckName = "destination-pv"
	CheckNameDestinationPVC              CheckName = "destination-pvc"
	CheckNameDestinationPVCPolicy        CheckName = "destination-pvc-policy"
	CheckNameDestinationSharedMount      CheckName = "destination-shared-mount"
	CheckNameDestinationSharedScheduling CheckName = "destination-shared-scheduling"
	CheckNameDestinationStorageClass     CheckName = "destination-storage-class"
	CheckNameMigrationNeeded             CheckName = "migration-needed"
	CheckNamePVPair                      CheckName = "pv-pair"
	CheckNamePVCConsumers                CheckName = "pvc-consumers"
	CheckNamePVCFinalizers               CheckName = "pvc-finalizers"
	CheckNameSourceBinding               CheckName = "source-binding"
	CheckNameSourcePath                  CheckName = "source-path"
	CheckNameSourcePV                    CheckName = "source-pv"
	CheckNameSourcePVC                   CheckName = "source-pvc"
	CheckNameSourcePVCs                  CheckName = "source-pvcs"
	CheckNameSourceStorageClass          CheckName = "source-storage-class"
	CheckNameSourceUsage                 CheckName = "source-usage"
	CheckNameStorageCapacity             CheckName = "storage-capacity"
	CheckNameStorageClass                CheckName = "storage-class"
	CheckNameStorageTopology             CheckName = "storage-topology"
	CheckNameVolumeMode                  CheckName = "volume-mode"
	CheckNameWarmCopyMount               CheckName = "warm-copy-mount"

	// Ownership and cleanup checks.
	CheckNameActivationState      CheckName = "activation-state"
	CheckNameCurrentClaim         CheckName = "current-claim"
	CheckNameCurrentOwnership     CheckName = "current-ownership"
	CheckNameCurrentPolicy        CheckName = "current-policy"
	CheckNameCurrentPV            CheckName = "current-pv"
	CheckNameDestinationClaim     CheckName = "destination-claim"
	CheckNameDestinationConsumers CheckName = "destination-consumers"
	CheckNameDestinationOwnership CheckName = "destination-ownership"
	CheckNameDestinationPolicy    CheckName = "destination-policy"
	CheckNameMove                 CheckName = "move"
	CheckNameOtherResources       CheckName = "other-resources"
	CheckNamePVCOwnership         CheckName = "pvc-ownership"
	CheckNameRename               CheckName = "rename"
	CheckNameRenameOffline        CheckName = "rename-offline"
	CheckNameResources            CheckName = "resources"
	CheckNameRollbackOwnership    CheckName = "rollback-ownership"
	CheckNameRollbackPolicy       CheckName = "rollback-policy"
	CheckNameRollbackPV           CheckName = "rollback-pv"
	CheckNameRollbackState        CheckName = "rollback-state"
	CheckNameSessionRecord        CheckName = "session-record"
	CheckNameSessionOwnership     CheckName = "session-ownership"
	CheckNameSourceOwnership      CheckName = "source-ownership"
)

type CapacityAwareness string

const (
	CapacityAwarenessAuto    CapacityAwareness = AutoValue
	CapacityAwarenessRequire CapacityAwareness = "require"
	CapacityAwarenessOff     CapacityAwareness = "off"
)

type StorageCapacityStatus string

const (
	StorageCapacitySufficient   StorageCapacityStatus = "sufficient"
	StorageCapacityInsufficient StorageCapacityStatus = "insufficient"
	StorageCapacityUnknown      StorageCapacityStatus = "unknown"
)

type Check struct {
	Name     CheckName     `json:"name"     yaml:"name"`
	Severity CheckSeverity `json:"severity" yaml:"severity"`
	Passed   bool          `json:"passed"   yaml:"passed"`
	Message  string        `json:"message"  yaml:"message"`
}

type ResourceEstimate struct {
	StorageRequests      string            `json:"storageRequests"                      yaml:"storageRequests"`
	PVCs                 int               `json:"persistentVolumeClaims"               yaml:"persistentVolumeClaims"`
	Pods                 int               `json:"pods"                                 yaml:"pods"`
	TerminatingPods      int               `json:"terminatingPods,omitempty"            yaml:"terminatingPods,omitempty"`
	NotTerminatingPods   int               `json:"notTerminatingPods,omitempty"         yaml:"notTerminatingPods,omitempty"`
	Jobs                 int               `json:"jobs"                                 yaml:"jobs"`
	Deployments          int               `json:"deployments"                          yaml:"deployments"`
	ReplicaSets          int               `json:"replicaSets"                          yaml:"replicaSets"`
	Services             int               `json:"services"                             yaml:"services"`
	ServiceNodePorts     int               `json:"serviceNodePorts"                     yaml:"serviceNodePorts"`
	ServiceLoadBalancers int               `json:"serviceLoadBalancers"                 yaml:"serviceLoadBalancers"`
	Endpoints            int               `json:"endpoints"                            yaml:"endpoints"`
	EndpointSlices       int               `json:"endpointSlices"                       yaml:"endpointSlices"`
	Secrets              int               `json:"secrets"                              yaml:"secrets"`
	ConfigMaps           int               `json:"configMaps"                           yaml:"configMaps"`
	ServiceAccounts      int               `json:"serviceAccounts"                      yaml:"serviceAccounts"`
	Leases               int               `json:"leases,omitempty"                     yaml:"leases,omitempty"`
	ByStorageClass       map[string]string `json:"byStorageClass"                       yaml:"byStorageClass"`
	PVCsByStorageClass   map[string]int    `json:"persistentVolumeClaimsByStorageClass" yaml:"persistentVolumeClaimsByStorageClass"`
}

type PlannedVolume struct {
	SourcePVC           ObjectReference                     `json:"sourcePVC"                     yaml:"sourcePVC"`
	SourcePV            ObjectReference                     `json:"sourcePV"                      yaml:"sourcePV"`
	DestinationPVC      ObjectReference                     `json:"destinationPVC"                yaml:"destinationPVC"`
	Capacity            string                              `json:"capacity"                      yaml:"capacity"`
	SourceCapacity      string                              `json:"sourceCapacity"                yaml:"sourceCapacity"`
	SourceUsedBytes     int64                               `json:"sourceUsedBytes,omitempty"     yaml:"sourceUsedBytes,omitempty"`
	SourceUsageKnown    bool                                `json:"sourceUsageKnown,omitempty"    yaml:"sourceUsageKnown,omitempty"`
	AccessModes         []corev1.PersistentVolumeAccessMode `json:"accessModes"                   yaml:"accessModes"`
	VolumeMode          corev1.PersistentVolumeMode         `json:"volumeMode"                    yaml:"volumeMode"`
	StorageClass        string                              `json:"storageClass"                  yaml:"storageClass"`
	BindingMode         storagev1.VolumeBindingMode         `json:"bindingMode"                   yaml:"bindingMode"`
	CSIProvisioner      string                              `json:"csiProvisioner"                yaml:"csiProvisioner"`
	ConcurrentConsumers int                                 `json:"concurrentConsumers,omitempty" yaml:"concurrentConsumers,omitempty"`
	TransferScope       *TransferScope                      `json:"transferScope,omitempty"       yaml:"transferScope,omitempty"`
}

type StorageCapacityReport struct {
	StorageClass      string                `json:"storageClass"                yaml:"storageClass"`
	CSIProvisioner    string                `json:"csiProvisioner"              yaml:"csiProvisioner"`
	TargetNode        string                `json:"targetNode"                  yaml:"targetNode"`
	RequestedCapacity string                `json:"requestedCapacity"           yaml:"requestedCapacity"`
	LargestVolume     string                `json:"largestVolume"               yaml:"largestVolume"`
	ReportedCapacity  string                `json:"reportedCapacity,omitempty"  yaml:"reportedCapacity,omitempty"`
	MaximumVolumeSize string                `json:"maximumVolumeSize,omitempty" yaml:"maximumVolumeSize,omitempty"`
	PublishedObjects  int                   `json:"publishedObjects"            yaml:"publishedObjects"`
	MatchingObjects   int                   `json:"matchingObjects"             yaml:"matchingObjects"`
	Status            StorageCapacityStatus `json:"status"                      yaml:"status"`
	Message           string                `json:"message"                     yaml:"message"`
}

type MigrationPlan struct {
	APIVersion           string                  `json:"apiVersion"                yaml:"apiVersion"`
	Kind                 string                  `json:"kind"                      yaml:"kind"`
	SessionID            string                  `json:"sessionID"                 yaml:"sessionID"`
	SourceNamespace      string                  `json:"sourceNamespace"           yaml:"sourceNamespace"`
	TemporaryNamespace   string                  `json:"temporaryNamespace"        yaml:"temporaryNamespace"`
	DestinationNamespace string                  `json:"destinationNamespace"      yaml:"destinationNamespace"`
	SessionNamespace     string                  `json:"sessionNamespace"          yaml:"sessionNamespace"`
	ToolImage            string                  `json:"toolImage"                 yaml:"toolImage"`
	CapacityAwareness    CapacityAwareness       `json:"capacityAwareness"         yaml:"capacityAwareness"`
	SourceNode           string                  `json:"sourceNode,omitempty"      yaml:"sourceNode,omitempty"`
	TargetNode           string                  `json:"targetNode,omitempty"      yaml:"targetNode,omitempty"`
	Strategies           []string                `json:"strategies,omitempty"      yaml:"strategies,omitempty"`
	Workload             WorkloadSpec            `json:"workload"                  yaml:"workload"`
	Volumes              []PlannedVolume         `json:"volumes"                   yaml:"volumes"`
	Checks               []Check                 `json:"checks"                    yaml:"checks"`
	StorageCapacity      []StorageCapacityReport `json:"storageCapacity,omitempty" yaml:"storageCapacity,omitempty"`
	TemporaryUsage       ResourceEstimate        `json:"temporaryUsage"            yaml:"temporaryUsage"`
	RollbackRetention    ResourceEstimate        `json:"rollbackRetention"         yaml:"rollbackRetention"`
	Ready                bool                    `json:"ready"                     yaml:"ready"`
	SessionSpec          SessionSpec             `json:"-"                         yaml:"-"`
}

// OrphanCleanupPlan describes a session ownership record whose ConfigMap is
// gone. It is deliberately resource-scoped so an administrator can review
// every UID and relationship before removing retained migration metadata.
type OrphanCleanupPlan struct {
	APIVersion       string                       `json:"apiVersion"               yaml:"apiVersion"`
	Kind             string                       `json:"kind"                     yaml:"kind"`
	SessionID        string                       `json:"sessionID"                yaml:"sessionID"`
	SessionNamespace string                       `json:"sessionNamespace"         yaml:"sessionNamespace"`
	Mode             OrphanCleanupMode            `json:"mode"                     yaml:"mode"`
	PreActivation    *OrphanPreActivationCleanup  `json:"preActivation,omitempty"  yaml:"preActivation,omitempty"`
	PostActivation   *OrphanPostActivationCleanup `json:"postActivation,omitempty" yaml:"postActivation,omitempty"`
	Checks           []Check                      `json:"checks"                   yaml:"checks"`
	Ready            bool                         `json:"ready"                    yaml:"ready"`
}

type OrphanPreActivationCleanup struct {
	SourcePVC         ObjectReference                      `json:"sourcePVC"                   yaml:"sourcePVC"`
	SourcePV          ObjectReference                      `json:"sourcePV"                    yaml:"sourcePV"`
	DestinationPVC    ObjectReference                      `json:"destinationPVC,omitempty"    yaml:"destinationPVC,omitempty"`
	DestinationPV     ObjectReference                      `json:"destinationPV,omitempty"     yaml:"destinationPV,omitempty"`
	DestinationPolicy corev1.PersistentVolumeReclaimPolicy `json:"destinationPolicy,omitempty" yaml:"destinationPolicy,omitempty"`
}

type OrphanPostActivationCleanup struct {
	SourcePVC      ObjectReference                      `json:"sourcePVC"      yaml:"sourcePVC"`
	ActivePV       ObjectReference                      `json:"activePV"       yaml:"activePV"`
	RollbackPV     ObjectReference                      `json:"rollbackPV"     yaml:"rollbackPV"`
	RollbackPolicy corev1.PersistentVolumeReclaimPolicy `json:"rollbackPolicy" yaml:"rollbackPolicy"`
}

func (p *OrphanCleanupPlan) AddCheck(check Check) {
	p.Checks = append(p.Checks, check)
	if check.Severity == SeverityError && !check.Passed {
		p.Ready = false
	}
}

func (p *MigrationPlan) AddCheck(check Check) {
	p.Checks = append(p.Checks, check)
	if check.Severity == SeverityError && !check.Passed {
		p.Ready = false
	}
}
