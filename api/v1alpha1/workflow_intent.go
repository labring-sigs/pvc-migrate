package v1alpha1

// VolumeRequest selects a source and optionally constrains its identity.
// The controller resolves and freezes all discovered properties in status.plan.
type VolumeRequest struct {
	SourcePVC      LocalResourceReference  `json:"sourcePVC"`
	SourcePV       *LocalResourceReference `json:"sourcePV,omitempty"`
	DestinationPVC *LocalResourceReference `json:"destinationPVC,omitempty"`
	Capacity       string                  `json:"capacity,omitempty"`
	TransferScope  *TransferScope          `json:"transferScope,omitempty"`
}

type TransferOptions struct {
	// DestinationPVCReclaimPolicy controls workflow-owned destination storage during cleanup.
	// +kubebuilder:validation:Enum=Retain;Delete
	DestinationPVCReclaimPolicy PVReclaimPolicy `json:"destinationPVCReclaimPolicy,omitempty" yaml:"destinationPVCReclaimPolicy,omitempty"`
	DestinationCapacity         string          `json:"destinationCapacity,omitempty"`
	SourcePath                  string          `json:"sourcePath,omitempty"`
	DestinationPath             string          `json:"destinationPath,omitempty"`
	DestinationStorageClass     string          `json:"destinationStorageClass,omitempty"`
	SourceNode                  string          `json:"sourceNode,omitempty"`
	TargetNode                  string          `json:"targetNode,omitempty"`
	// +kubebuilder:validation:Enum=auto;require;off
	CapacityAwareness string `json:"capacityAwareness,omitempty"`
	// +kubebuilder:validation:MaxItems=32
	Strategies     []string `json:"strategies,omitempty"`
	VerifyChecksum bool     `json:"verifyChecksum,omitempty"`
	// DeleteExtraneous defaults to true so a raw workflow CR means the same
	// thing as the CLI default: the destination mirrors the source exactly.
	// +kubebuilder:default=true
	DeleteExtraneous     bool `json:"deleteExtraneous,omitempty"`
	AllowVolumeShrink    bool `json:"allowVolumeShrink,omitempty"`
	SkipSourceUsageCheck bool `json:"skipSourceUsageCheck,omitempty"`
	// CopyTimeout bounds one data-transfer attempt (warm copy, final sync, or
	// copy pass). Unset keeps the operation-level bound only. The pattern
	// requires at least one unit-suffixed component ("30m", "1h30m", "90s")
	// so empty or unitless values are rejected at admission instead of
	// silently falling back to the default during planning.
	// +kubebuilder:validation:Pattern=`^([0-9]+(h|m|s))+$`
	// +optional
	CopyTimeout *string `json:"copyTimeout,omitempty" yaml:"copyTimeout,omitempty"`
	// RsyncMaxRetries overrides how many times the rsync job re-runs on a
	// failed attempt within one transfer. Unset keeps the upstream default.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	RsyncMaxRetries *int32 `json:"rsyncMaxRetries,omitempty" yaml:"rsyncMaxRetries,omitempty"`
	// RetryPolicy overrides the retry behavior of the data-transfer attempts
	// for this workflow. Zero values keep the controller defaults. Only
	// copy-bearing workflows consume it.
	// +optional
	RetryPolicy *RetryPolicySpec `json:"retryPolicy,omitempty" yaml:"retryPolicy,omitempty"`
}

// RetryPolicySpec overrides the data-transfer retry defaults for one
// workflow. Durations accept Go duration strings ("30m", "2h").
type RetryPolicySpec struct {
	// Retries overrides the number of data-transfer attempts.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	Retries *int32 `json:"retries,omitempty" yaml:"retries,omitempty"`
	// RetryBackoff is the delay before the first retry; it doubles on every
	// further attempt. The pattern requires at least one unit-suffixed
	// component ("2s", "1m30s").
	// +kubebuilder:validation:Pattern=`^([0-9]+(h|m|s))+$`
	// +optional
	RetryBackoff *string `json:"retryBackoff,omitempty" yaml:"retryBackoff,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="has(self.volumes) && size(self.volumes) > 0",message="at least one source PVC is required"
type MigrationSpec struct {
	// The inactive source PV is retained by default. Mutable until cleanup.
	// +kubebuilder:validation:Enum=Retain;Delete
	SourcePVReclaimPolicy PVReclaimPolicy `json:"sourcePVReclaimPolicy,omitempty"`
	TransferOptions       `                json:",inline"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes []VolumeRequest `json:"volumes"`
}

// +kubebuilder:validation:XValidation:rule="(has(self.volumes) && size(self.volumes) > 0) || has(self.pod)",message="select source volumes or a Pod; volumes alongside a Pod are overrides"
type CopySpec struct {
	TransferOptions `json:",inline"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes []VolumeRequest         `json:"volumes,omitempty"`
	Pod     *LocalResourceReference `json:"pod,omitempty"`
	Online  bool                    `json:"online,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="(has(self.volumes) && size(self.volumes) > 0) || has(self.pod)",message="select source volumes or a Pod; volumes alongside a Pod are overrides"
type ReservationSpec struct {
	// Reservation retains these settings for a later copy of the reserved volumes.
	TransferOptions `json:",inline"`
	// +kubebuilder:validation:MaxItems=1024
	Volumes []VolumeRequest         `json:"volumes,omitempty"`
	Pod     *LocalResourceReference `json:"pod,omitempty"`
}

type PodMigrationSpec struct {
	// The inactive source PV is retained by default. Mutable until cleanup.
	// +kubebuilder:validation:Enum=Retain;Delete
	SourcePVReclaimPolicy PVReclaimPolicy `json:"sourcePVReclaimPolicy,omitempty"`
	TransferOptions       `                       json:",inline"`
	Pod                   LocalResourceReference `json:"pod"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +optional
	PrecopyPasses          int    `json:"precopyPasses"`
	SwitchoverCandidate    string `json:"switchoverCandidate,omitempty"`
	AllowLeaderDowntime    bool   `json:"allowLeaderDowntime,omitempty"`
	ForceReprovision       bool   `json:"forceReprovision,omitempty"`
	OpenEBSLVMEnableShared bool   `json:"openebsLvmEnableShared,omitempty"`
	// AllowPlacementViolation proceeds when the recreated Pod may violate
	// required podAffinity, required podAntiAffinity, or DoNotSchedule
	// topologySpread constraints on the target node — for topologies where
	// the operator will re-balance the remaining replicas afterwards.
	AllowPlacementViolation bool `json:"allowPlacementViolation,omitempty"`
	// Optional per-volume capacity and path settings, keyed by source PVC name.
	// +kubebuilder:validation:MaxItems=1024
	Volumes []VolumeRequest `json:"volumes,omitempty"`
}

type RenameSpec struct {
	SourcePVC      LocalResourceReference  `json:"sourcePVC"`
	SourcePV       *LocalResourceReference `json:"sourcePV,omitempty"`
	DestinationPVC LocalResourceReference  `json:"destinationPVC"`
}

type MoveSpec struct {
	SourceNamespace      NamespaceName           `json:"sourceNamespace"`
	DestinationNamespace NamespaceName           `json:"destinationNamespace"`
	SessionNamespace     NamespaceName           `json:"sessionNamespace,omitempty"`
	SourcePVC            LocalResourceReference  `json:"sourcePVC"`
	SourcePV             *LocalResourceReference `json:"sourcePV,omitempty"`
	DestinationPVC       *LocalResourceReference `json:"destinationPVC,omitempty"`
}

type ClusterMigrationSpec struct {
	MigrationSpec      `              json:",inline"`
	SourceNamespace    NamespaceName `json:"sourceNamespace"`
	TemporaryNamespace NamespaceName `json:"temporaryNamespace,omitempty"`
	SessionNamespace   NamespaceName `json:"sessionNamespace,omitempty"`
}

type ClusterPodMigrationSpec struct {
	PodMigrationSpec   `              json:",inline"`
	SourceNamespace    NamespaceName `json:"sourceNamespace"`
	TemporaryNamespace NamespaceName `json:"temporaryNamespace,omitempty"`
	SessionNamespace   NamespaceName `json:"sessionNamespace,omitempty"`
}

type ClusterCopySpec struct {
	CopySpec             `              json:",inline"`
	SourceNamespace      NamespaceName `json:"sourceNamespace"`
	DestinationNamespace NamespaceName `json:"destinationNamespace"`
	SessionNamespace     NamespaceName `json:"sessionNamespace,omitempty"`
}

type ClusterReservationSpec struct {
	ReservationSpec      `              json:",inline"`
	SourceNamespace      NamespaceName `json:"sourceNamespace"`
	DestinationNamespace NamespaceName `json:"destinationNamespace"`
	SessionNamespace     NamespaceName `json:"sessionNamespace,omitempty"`
}

type BackupSpec struct {
	SourcePVC LocalResourceReference  `json:"sourcePVC"`
	SourcePV  *LocalResourceReference `json:"sourcePV,omitempty"`
	Path      string                  `json:"path,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name                   string               `json:"name"`
	RepositoryRef          LocalObjectReference `json:"repositoryRef"`
	Online                 bool                 `json:"online,omitempty"`
	OpenEBSLVMEnableShared bool                 `json:"openebsLvmEnableShared,omitempty"`
}

type RestoreSpec struct {
	DestinationPVC LocalResourceReference `json:"destinationPVC"`
	Path           string                 `json:"path,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name                    string               `json:"name"`
	RepositoryRef           LocalObjectReference `json:"repositoryRef"`
	CreatePVC               bool                 `json:"createPVC,omitempty"`
	DestinationStorageClass string               `json:"destinationStorageClass,omitempty" yaml:"destinationStorageClass,omitempty"`
	DestinationAccessMode   string               `json:"destinationAccessMode,omitempty"   yaml:"destinationAccessMode,omitempty"`
	DestinationCapacity     string               `json:"destinationCapacity,omitempty"     yaml:"destinationCapacity,omitempty"`
	AllowMounted            bool                 `json:"allowMounted,omitempty"            yaml:"allowMounted,omitempty"`
	TargetNode              string               `json:"targetNode,omitempty"              yaml:"targetNode,omitempty"`
	DeleteExtraneous        bool                 `json:"deleteExtraneous,omitempty"        yaml:"deleteExtraneous,omitempty"`
}
