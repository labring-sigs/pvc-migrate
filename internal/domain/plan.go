package domain

import (
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

type CheckSeverity string

const (
	SeverityInfo    CheckSeverity = "info"
	SeverityWarning CheckSeverity = "warning"
	SeverityError   CheckSeverity = "error"
)

type CapacityAwareness string

const (
	CapacityAwarenessAuto    CapacityAwareness = "auto"
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
	Name     string        `json:"name" yaml:"name"`
	Severity CheckSeverity `json:"severity" yaml:"severity"`
	Passed   bool          `json:"passed" yaml:"passed"`
	Message  string        `json:"message" yaml:"message"`
}

type ResourceEstimate struct {
	StorageRequests    string            `json:"storageRequests" yaml:"storageRequests"`
	PVCs               int               `json:"persistentVolumeClaims" yaml:"persistentVolumeClaims"`
	Pods               int               `json:"pods" yaml:"pods"`
	Jobs               int               `json:"jobs" yaml:"jobs"`
	Services           int               `json:"services" yaml:"services"`
	Secrets            int               `json:"secrets" yaml:"secrets"`
	ConfigMaps         int               `json:"configMaps" yaml:"configMaps"`
	ServiceAccounts    int               `json:"serviceAccounts" yaml:"serviceAccounts"`
	Leases             int               `json:"leases,omitempty" yaml:"leases,omitempty"`
	ByStorageClass     map[string]string `json:"byStorageClass" yaml:"byStorageClass"`
	PVCsByStorageClass map[string]int    `json:"persistentVolumeClaimsByStorageClass,omitempty" yaml:"persistentVolumeClaimsByStorageClass,omitempty"`
}

type PlannedVolume struct {
	SourcePVC      ObjectReference                     `json:"sourcePVC" yaml:"sourcePVC"`
	SourcePV       ObjectReference                     `json:"sourcePV" yaml:"sourcePV"`
	DestinationPVC ObjectReference                     `json:"destinationPVC" yaml:"destinationPVC"`
	Capacity       string                              `json:"capacity" yaml:"capacity"`
	AccessModes    []corev1.PersistentVolumeAccessMode `json:"accessModes" yaml:"accessModes"`
	VolumeMode     corev1.PersistentVolumeMode         `json:"volumeMode" yaml:"volumeMode"`
	StorageClass   string                              `json:"storageClass" yaml:"storageClass"`
	BindingMode    storagev1.VolumeBindingMode         `json:"bindingMode" yaml:"bindingMode"`
	CSIProvisioner string                              `json:"csiProvisioner" yaml:"csiProvisioner"`
}

type StorageCapacityReport struct {
	StorageClass      string                `json:"storageClass" yaml:"storageClass"`
	CSIProvisioner    string                `json:"csiProvisioner" yaml:"csiProvisioner"`
	TargetNode        string                `json:"targetNode" yaml:"targetNode"`
	RequestedCapacity string                `json:"requestedCapacity" yaml:"requestedCapacity"`
	LargestVolume     string                `json:"largestVolume" yaml:"largestVolume"`
	ReportedCapacity  string                `json:"reportedCapacity,omitempty" yaml:"reportedCapacity,omitempty"`
	MaximumVolumeSize string                `json:"maximumVolumeSize,omitempty" yaml:"maximumVolumeSize,omitempty"`
	PublishedObjects  int                   `json:"publishedObjects" yaml:"publishedObjects"`
	MatchingObjects   int                   `json:"matchingObjects" yaml:"matchingObjects"`
	Status            StorageCapacityStatus `json:"status" yaml:"status"`
	Message           string                `json:"message" yaml:"message"`
}

type MigrationPlan struct {
	APIVersion           string                  `json:"apiVersion" yaml:"apiVersion"`
	Kind                 string                  `json:"kind" yaml:"kind"`
	SessionID            string                  `json:"sessionID" yaml:"sessionID"`
	SourceNamespace      string                  `json:"sourceNamespace" yaml:"sourceNamespace"`
	TemporaryNamespace   string                  `json:"temporaryNamespace" yaml:"temporaryNamespace"`
	DestinationNamespace string                  `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     string                  `json:"sessionNamespace" yaml:"sessionNamespace"`
	ToolImage            string                  `json:"toolImage" yaml:"toolImage"`
	CapacityAwareness    CapacityAwareness       `json:"capacityAwareness" yaml:"capacityAwareness"`
	SourceNode           string                  `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	TargetNode           string                  `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	Strategies           []string                `json:"strategies,omitempty" yaml:"strategies,omitempty"`
	Workload             WorkloadSpec            `json:"workload" yaml:"workload"`
	Volumes              []PlannedVolume         `json:"volumes" yaml:"volumes"`
	Checks               []Check                 `json:"checks" yaml:"checks"`
	StorageCapacity      []StorageCapacityReport `json:"storageCapacity,omitempty" yaml:"storageCapacity,omitempty"`
	TemporaryUsage       ResourceEstimate        `json:"temporaryUsage" yaml:"temporaryUsage"`
	RollbackRetention    ResourceEstimate        `json:"rollbackRetention" yaml:"rollbackRetention"`
	Ready                bool                    `json:"ready" yaml:"ready"`
	SessionSpec          SessionSpec             `json:"-" yaml:"-"`
}

func (p *MigrationPlan) AddCheck(check Check) {
	p.Checks = append(p.Checks, check)
	if check.Severity == SeverityError && !check.Passed {
		p.Ready = false
	}
}
