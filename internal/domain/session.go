package domain

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	SessionAPIVersion = "pvc-migrate.io/v1alpha1"
	SessionKind       = "MigrationSession"
)

type Operation string

const (
	OperationMigrate    Operation = "Migrate"
	OperationMigratePod Operation = "MigratePod"
	OperationReserve    Operation = "Reserve"
	OperationCopy       Operation = "Copy"
	OperationFinalSync  Operation = "FinalSync"
	OperationActivate   Operation = "Activate"
	OperationRename     Operation = "Rename"
	OperationMove       Operation = "Move"
	OperationBackup     Operation = "Backup"
	OperationRestore    Operation = "Restore"
)

func (o Operation) RebindsPVC() bool {
	return o == OperationRename || o == OperationMove
}

// RecreatesPVC reports workflows that delete the source PVC and create a new
// PVC identity for activation.
func (o Operation) RecreatesPVC() bool {
	return o == OperationMigrate || o == OperationMigratePod || o.RebindsPVC()
}

type Phase string

const (
	PhasePlanned      Phase = "Planned"
	PhaseReserving    Phase = "Reserving"
	PhaseReserved     Phase = "Reserved"
	PhaseWarmCopying  Phase = "WarmCopying"
	PhaseWarmCopied   Phase = "WarmCopied"
	PhasePausing      Phase = "Pausing"
	PhasePaused       Phase = "Paused"
	PhaseFinalSyncing Phase = "FinalSyncing"
	PhaseFinalSynced  Phase = "FinalSynced"
	PhaseActivating   Phase = "Activating"
	PhaseActivated    Phase = "Activated"
	PhaseResuming     Phase = "Resuming"
	PhaseCompleted    Phase = "Completed"
	PhaseAborting     Phase = "Aborting"
	PhaseAborted      Phase = "Aborted"
	PhaseRollingBack  Phase = "RollingBack"
	PhaseRolledBack   Phase = "RolledBack"
	PhaseRenaming     Phase = "Renaming"
	PhaseMoving       Phase = "Moving"
	PhaseFailed       Phase = "Failed"
)

var allowedTransitions = map[Phase][]Phase{
	PhasePlanned:      {PhaseReserving, PhaseRenaming, PhaseMoving, PhaseAborting, PhaseFailed},
	PhaseRenaming:     {PhaseCompleted, PhaseAborting, PhaseFailed},
	PhaseMoving:       {PhaseCompleted, PhaseAborting, PhaseFailed},
	PhaseReserving:    {PhaseReserved, PhaseAborting, PhaseFailed},
	PhaseReserved:     {PhaseWarmCopying, PhasePausing, PhaseAborting, PhaseFailed},
	PhaseWarmCopying:  {PhaseWarmCopied, PhaseAborting, PhaseFailed},
	PhaseWarmCopied:   {PhaseWarmCopying, PhasePausing, PhaseAborting, PhaseFailed},
	PhasePausing:      {PhasePaused, PhaseAborting, PhaseFailed},
	PhasePaused:       {PhaseFinalSyncing, PhaseResuming, PhaseRollingBack, PhaseAborting, PhaseFailed},
	PhaseFinalSyncing: {PhaseFinalSynced, PhaseAborting, PhaseFailed},
	PhaseFinalSynced:  {PhaseFinalSyncing, PhaseActivating, PhaseRollingBack, PhaseAborting, PhaseFailed},
	PhaseActivating:   {PhaseActivated, PhaseRollingBack, PhaseFailed},
	PhaseActivated:    {PhaseResuming, PhaseRollingBack, PhaseFailed},
	PhaseResuming:     {PhaseCompleted, PhaseRolledBack, PhaseFailed},
	PhaseCompleted:    {PhaseRollingBack},
	PhaseAborting:     {PhaseAborted, PhaseFailed},
	PhaseFailed:       {PhaseReserving, PhaseWarmCopying, PhasePausing, PhaseFinalSyncing, PhaseActivating, PhaseResuming, PhaseRollingBack, PhaseAborting, PhaseRenaming, PhaseMoving},
	PhaseRollingBack:  {PhaseRolledBack, PhaseFailed},
	PhaseRolledBack:   {PhaseResuming, PhaseCompleted},
}

type ObjectReference struct {
	APIVersion      string    `json:"apiVersion,omitempty" yaml:"apiVersion,omitempty"`
	Kind            string    `json:"kind,omitempty" yaml:"kind,omitempty"`
	Namespace       string    `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name            string    `json:"name" yaml:"name"`
	UID             types.UID `json:"uid,omitempty" yaml:"uid,omitempty"`
	ResourceVersion string    `json:"resourceVersion,omitempty" yaml:"resourceVersion,omitempty"`
}

type WorkloadKind string

const (
	WorkloadNone         WorkloadKind = "None"
	WorkloadStandalone   WorkloadKind = "StandalonePod"
	WorkloadStatefulSet  WorkloadKind = "StatefulSet"
	WorkloadVictoriaLogs WorkloadKind = "VictoriaLogs"
	WorkloadKubeBlocks   WorkloadKind = "KubeBlocks"
	WorkloadVMCluster    WorkloadKind = "VMCluster"
	WorkloadGrafana      WorkloadKind = "Grafana"
)

type WorkloadSpec struct {
	Adapter          WorkloadKind      `json:"adapter" yaml:"adapter"`
	Pod              ObjectReference   `json:"pod,omitempty" yaml:"pod,omitempty"`
	Controller       ObjectReference   `json:"controller,omitempty" yaml:"controller,omitempty"`
	OriginalReplicas *int32            `json:"originalReplicas,omitempty" yaml:"originalReplicas,omitempty"`
	Ordinal          *int32            `json:"ordinal,omitempty" yaml:"ordinal,omitempty"`
	AffectedPods     []ObjectReference `json:"affectedPods,omitempty" yaml:"affectedPods,omitempty"`
	OriginalObject   json.RawMessage   `json:"originalObject,omitempty" yaml:"originalObject,omitempty"`
	KubeBlocks       *KubeBlocksSpec   `json:"kubeBlocks,omitempty" yaml:"kubeBlocks,omitempty"`
	VMCluster        *VMClusterSpec    `json:"vmCluster,omitempty" yaml:"vmCluster,omitempty"`
	Grafana          *GrafanaSpec      `json:"grafana,omitempty" yaml:"grafana,omitempty"`
}

// SessionType identifies the durable workflow represented by a session. The
// operation is kept in the planner as a command-stage concern; persisted
// sessions carry the workflow type and its concrete payload.
type SessionType string

const (
	SessionTypeReserve    SessionType = "Reserve"
	SessionTypeMigrate    SessionType = "Migrate"
	SessionTypeMigratePod SessionType = "MigratePod"
	SessionTypeCopy       SessionType = "Copy"
	SessionTypeRename     SessionType = "Rename"
	SessionTypeMove       SessionType = "Move"
)

type SessionCommon struct {
	SourceNamespace      string       `json:"sourceNamespace" yaml:"sourceNamespace"`
	TemporaryNamespace   string       `json:"temporaryNamespace" yaml:"temporaryNamespace"`
	DestinationNamespace string       `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     string       `json:"sessionNamespace" yaml:"sessionNamespace"`
	CreatedBy            string       `json:"createdBy,omitempty" yaml:"createdBy,omitempty"`
	Volumes              []VolumeSpec `json:"volumes" yaml:"volumes"`
}

type SessionWorkflowOptions struct {
	SourceNode             string   `json:"sourceNode,omitempty" yaml:"sourceNode,omitempty"`
	TargetNode             string   `json:"targetNode,omitempty" yaml:"targetNode,omitempty"`
	ToolImage              string   `json:"toolImage,omitempty" yaml:"toolImage,omitempty"`
	Strategies             []string `json:"strategies,omitempty" yaml:"strategies,omitempty"`
	VerifyChecksum         bool     `json:"verifyChecksum,omitempty" yaml:"verifyChecksum,omitempty"`
	DeleteExtraneous       bool     `json:"deleteExtraneous" yaml:"deleteExtraneous"`
	PrecopyPasses          int      `json:"-" yaml:"-"`
	OpenEBSLVMEnableShared bool     `json:"openebsLvmEnableShared,omitempty" yaml:"openebsLvmEnableShared,omitempty"`
}

const (
	StrategyAuto         = AutoValue
	StrategyMount        = "mount"
	StrategyClusterIP    = "clusterip"
	StrategyLoadBalancer = "loadbalancer"
	StrategyNodePort     = "nodeport"
	StrategyLocal        = "local"
)

type MigrateSessionSpec struct {
	SessionWorkflowOptions `json:",inline" yaml:",inline"`
	Workload               WorkloadSpec `json:"workload" yaml:"workload"`
	PrecopyPasses          int          `json:"precopyPasses" yaml:"precopyPasses"`
}

type ReserveSessionSpec struct {
	SessionWorkflowOptions `json:",inline" yaml:",inline"`
}

type MigratePodSessionSpec struct {
	SessionWorkflowOptions `json:",inline" yaml:",inline"`
	Workload               WorkloadSpec `json:"workload" yaml:"workload"`
	PrecopyPasses          int          `json:"precopyPasses" yaml:"precopyPasses"`
}

type CopySessionSpec struct {
	SessionWorkflowOptions `json:",inline" yaml:",inline"`
	Online                 bool `json:"online,omitempty" yaml:"online,omitempty"`
}

type RenameSessionSpec struct{}

type MoveSessionSpec struct{}

type KubeBlocksSpec struct {
	Cluster                  string                       `json:"cluster" yaml:"cluster"`
	Component                string                       `json:"component" yaml:"component"`
	Instance                 string                       `json:"instance" yaml:"instance"`
	Role                     string                       `json:"role,omitempty" yaml:"role,omitempty"`
	SwitchoverCandidate      string                       `json:"switchoverCandidate,omitempty" yaml:"switchoverCandidate,omitempty"`
	SwitchoverStrategy       KubeBlocksSwitchoverStrategy `json:"switchoverStrategy" yaml:"switchoverStrategy"`
	SwitchoverContainer      string                       `json:"switchoverContainer,omitempty" yaml:"switchoverContainer,omitempty"`
	OpsAPIVersion            string                       `json:"opsAPIVersion" yaml:"opsAPIVersion"`
	ClusterUID               types.UID                    `json:"clusterUID" yaml:"clusterUID"`
	OriginalStops            map[string]bool              `json:"originalStops" yaml:"originalStops"`
	OriginalPaused           bool                         `json:"originalPaused,omitempty" yaml:"originalPaused,omitempty"`
	OriginalPausedConfigured bool                         `json:"originalPausedConfigured,omitempty" yaml:"originalPausedConfigured,omitempty"`
}

// KubeBlocksSwitchoverStrategy is the durable leader-handoff mechanism chosen during planning.
type KubeBlocksSwitchoverStrategy string

const (
	KubeBlocksSwitchoverOpsRequest    KubeBlocksSwitchoverStrategy = "opsrequest"
	KubeBlocksSwitchoverMongoDBNative KubeBlocksSwitchoverStrategy = "mongodb-native"
)

type VMClusterSpec struct {
	APIVersion                      string    `json:"apiVersion" yaml:"apiVersion"`
	Name                            string    `json:"name" yaml:"name"`
	UID                             types.UID `json:"uid,omitempty" yaml:"uid,omitempty"`
	Component                       string    `json:"component" yaml:"component"`
	OriginalPaused                  bool      `json:"originalPaused" yaml:"originalPaused"`
	OriginalPausedConfigured        bool      `json:"originalPausedConfigured" yaml:"originalPausedConfigured"`
	OriginalClusterPaused           bool      `json:"originalClusterPaused" yaml:"originalClusterPaused"`
	OriginalClusterPausedConfigured bool      `json:"originalClusterPausedConfigured" yaml:"originalClusterPausedConfigured"`
	OriginalReplicas                int32     `json:"originalReplicas" yaml:"originalReplicas"`
	OriginalReplicasConfigured      bool      `json:"originalReplicasConfigured" yaml:"originalReplicasConfigured"`
}

type GrafanaSpec struct {
	APIVersion                string    `json:"apiVersion" yaml:"apiVersion"`
	Name                      string    `json:"name" yaml:"name"`
	UID                       types.UID `json:"uid,omitempty" yaml:"uid,omitempty"`
	OriginalSuspend           bool      `json:"originalSuspend" yaml:"originalSuspend"`
	OriginalSuspendConfigured bool      `json:"originalSuspendConfigured" yaml:"originalSuspendConfigured"`
	OriginalReplicas          int32     `json:"originalReplicas" yaml:"originalReplicas"`
}

type SessionSpec struct {
	SessionCommon `json:",inline" yaml:",inline"`
	Type          SessionType            `json:"type" yaml:"type"`
	Reserve       *ReserveSessionSpec    `json:"reserve,omitempty" yaml:"reserve,omitempty"`
	Migrate       *MigrateSessionSpec    `json:"migrate,omitempty" yaml:"migrate,omitempty"`
	MigratePod    *MigratePodSessionSpec `json:"migratePod,omitempty" yaml:"migratePod,omitempty"`
	Copy          *CopySessionSpec       `json:"copy,omitempty" yaml:"copy,omitempty"`
	Rename        *RenameSessionSpec     `json:"rename,omitempty" yaml:"rename,omitempty"`
	Move          *MoveSessionSpec       `json:"move,omitempty" yaml:"move,omitempty"`
}

type PVCMetadata struct {
	Labels          map[string]string       `json:"labels,omitempty" yaml:"labels,omitempty"`
	Annotations     map[string]string       `json:"annotations,omitempty" yaml:"annotations,omitempty"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences,omitempty" yaml:"ownerReferences,omitempty"`
}

type VolumeSpec struct {
	SourcePVC           ObjectReference                      `json:"sourcePVC" yaml:"sourcePVC"`
	SourcePV            ObjectReference                      `json:"sourcePV" yaml:"sourcePV"`
	SourceReclaimPolicy corev1.PersistentVolumeReclaimPolicy `json:"sourceReclaimPolicy" yaml:"sourceReclaimPolicy"`
	SourcePVCSpec       corev1.PersistentVolumeClaimSpec     `json:"sourcePVCSpec" yaml:"sourcePVCSpec"`
	SourcePVCMetadata   PVCMetadata                          `json:"sourcePVCMetadata" yaml:"sourcePVCMetadata"`
	DestinationPVC      ObjectReference                      `json:"destinationPVC" yaml:"destinationPVC"`
	DestinationPV       ObjectReference                      `json:"destinationPV,omitempty" yaml:"destinationPV,omitempty"`
	DestinationPolicy   corev1.PersistentVolumeReclaimPolicy `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	Capacity            string                               `json:"capacity" yaml:"capacity"`
	StorageClass        string                               `json:"storageClass" yaml:"storageClass"`
	AccessModes         []corev1.PersistentVolumeAccessMode  `json:"accessModes" yaml:"accessModes"`
	VolumeMode          corev1.PersistentVolumeMode          `json:"volumeMode" yaml:"volumeMode"`
}

type SyncState struct {
	WarmCompletedAt  *metav1.Time `json:"warmCompletedAt,omitempty" yaml:"warmCompletedAt,omitempty"`
	FinalCompletedAt *metav1.Time `json:"finalCompletedAt,omitempty" yaml:"finalCompletedAt,omitempty"`
	Attempts         int          `json:"attempts" yaml:"attempts"`
	BytesCopied      int64        `json:"bytesCopied,omitempty" yaml:"bytesCopied,omitempty"`
	ChecksumVerified bool         `json:"checksumVerified,omitempty" yaml:"checksumVerified,omitempty"`
	LastError        string       `json:"lastError,omitempty" yaml:"lastError,omitempty"`
}

type ActivationState struct {
	TemporaryPVCDeleted bool            `json:"temporaryPVCDeleted,omitempty" yaml:"temporaryPVCDeleted,omitempty"`
	SourcePVCDeleted    bool            `json:"sourcePVCDeleted,omitempty" yaml:"sourcePVCDeleted,omitempty"`
	DestinationReserved bool            `json:"destinationReserved,omitempty" yaml:"destinationReserved,omitempty"`
	ActivePVC           ObjectReference `json:"activePVC,omitempty" yaml:"activePVC,omitempty"`
	ActivatedAt         *metav1.Time    `json:"activatedAt,omitempty" yaml:"activatedAt,omitempty"`
	RolledBackAt        *metav1.Time    `json:"rolledBackAt,omitempty" yaml:"rolledBackAt,omitempty"`
}

type VolumeStatus struct {
	SourcePVCName string          `json:"sourcePVCName" yaml:"sourcePVCName"`
	Reserved      bool            `json:"reserved,omitempty" yaml:"reserved,omitempty"`
	Sync          SyncState       `json:"sync" yaml:"sync"`
	Activation    ActivationState `json:"activation" yaml:"activation"`
}

type Condition struct {
	Type               string                 `json:"type" yaml:"type"`
	Status             metav1.ConditionStatus `json:"status" yaml:"status"`
	Reason             string                 `json:"reason,omitempty" yaml:"reason,omitempty"`
	Message            string                 `json:"message,omitempty" yaml:"message,omitempty"`
	LastTransitionTime metav1.Time            `json:"lastTransitionTime" yaml:"lastTransitionTime"`
}

type HistoryEntry struct {
	Phase   Phase       `json:"phase" yaml:"phase"`
	Time    metav1.Time `json:"time" yaml:"time"`
	Message string      `json:"message,omitempty" yaml:"message,omitempty"`
}

// OpenEBSLVMSharedMount records a temporary LVMVolume.spec.shared change made
// by this session. PreviousSharedSet distinguishes an absent field from an
// explicitly empty value so cleanup can restore the original CR exactly.
type OpenEBSLVMSharedMount struct {
	SourcePV          ObjectReference `json:"sourcePV" yaml:"sourcePV"`
	LVMVolume         ObjectReference `json:"lvmVolume" yaml:"lvmVolume"`
	PreviousShared    string          `json:"previousShared,omitempty" yaml:"previousShared,omitempty"`
	PreviousSharedSet bool            `json:"previousSharedSet,omitempty" yaml:"previousSharedSet,omitempty"`
}

type SessionStatus struct {
	Phase                  Phase                   `json:"phase" yaml:"phase"`
	ResumeFrom             Phase                   `json:"resumeFrom,omitempty" yaml:"resumeFrom,omitempty"`
	WarmPassesCompleted    int                     `json:"warmPassesCompleted" yaml:"warmPassesCompleted"`
	ObservedGeneration     int64                   `json:"observedGeneration,omitempty" yaml:"observedGeneration,omitempty"`
	StartedAt              metav1.Time             `json:"startedAt" yaml:"startedAt"`
	UpdatedAt              metav1.Time             `json:"updatedAt" yaml:"updatedAt"`
	CompletedAt            *metav1.Time            `json:"completedAt,omitempty" yaml:"completedAt,omitempty"`
	Message                string                  `json:"message,omitempty" yaml:"message,omitempty"`
	Conditions             []Condition             `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	Volumes                []VolumeStatus          `json:"volumes" yaml:"volumes"`
	History                []HistoryEntry          `json:"history,omitempty" yaml:"history,omitempty"`
	OpenEBSLVMSharedMounts []OpenEBSLVMSharedMount `json:"openebsLvmSharedMounts,omitempty" yaml:"openebsLvmSharedMounts,omitempty"`
}

type Session struct {
	APIVersion      string        `json:"apiVersion" yaml:"apiVersion"`
	Kind            string        `json:"kind" yaml:"kind"`
	ID              string        `json:"id" yaml:"id"`
	Generation      int64         `json:"generation" yaml:"generation"`
	ResourceVersion string        `json:"-" yaml:"-"`
	Spec            SessionSpec   `json:"spec" yaml:"spec"`
	Status          SessionStatus `json:"status" yaml:"status"`
}

func SessionTypeForOperation(operation Operation) SessionType {
	switch operation {
	case OperationReserve:
		return SessionTypeReserve
	case OperationMigratePod:
		return SessionTypeMigratePod
	case OperationCopy:
		return SessionTypeCopy
	case OperationRename:
		return SessionTypeRename
	case OperationMove:
		return SessionTypeMove
	default:
		return SessionTypeMigrate
	}
}

func NewSessionSpec(operation Operation, common SessionCommon, workload WorkloadSpec, online bool, options SessionWorkflowOptions) SessionSpec {
	options.Strategies = slices.Clone(options.Strategies)
	spec := SessionSpec{SessionCommon: common, Type: SessionTypeForOperation(operation)}
	switch spec.Type {
	case SessionTypeReserve:
		spec.Reserve = &ReserveSessionSpec{SessionWorkflowOptions: options}
	case SessionTypeMigrate:
		spec.Migrate = &MigrateSessionSpec{SessionWorkflowOptions: options, Workload: workload, PrecopyPasses: options.PrecopyPasses}
	case SessionTypeMigratePod:
		spec.MigratePod = &MigratePodSessionSpec{SessionWorkflowOptions: options, Workload: workload, PrecopyPasses: options.PrecopyPasses}
	case SessionTypeCopy:
		spec.Copy = &CopySessionSpec{SessionWorkflowOptions: options, Online: online}
	case SessionTypeRename:
		spec.Rename = &RenameSessionSpec{}
	case SessionTypeMove:
		spec.Move = &MoveSessionSpec{}
	}
	return spec
}

func (s SessionSpec) WorkflowOptions() SessionWorkflowOptions {
	if options := s.WorkflowOptionsPtr(); options != nil {
		copy := *options
		copy.Strategies = slices.Clone(options.Strategies)
		copy.PrecopyPasses = s.PrecopyPasses()
		return copy
	}
	return SessionWorkflowOptions{}
}

func (s SessionSpec) PrecopyPasses() int {
	switch s.Type {
	case SessionTypeMigrate:
		if s.Migrate != nil {
			return s.Migrate.PrecopyPasses
		}
	case SessionTypeMigratePod:
		if s.MigratePod != nil {
			return s.MigratePod.PrecopyPasses
		}
	}
	return 0
}

func (s *Session) CompleteWarmPass() {
	if s == nil {
		return
	}
	s.Status.WarmPassesCompleted++
}

func (s *SessionSpec) WorkflowOptionsPtr() *SessionWorkflowOptions {
	switch s.Type {
	case SessionTypeReserve:
		if s.Reserve != nil {
			return &s.Reserve.SessionWorkflowOptions
		}
	case SessionTypeMigrate:
		if s.Migrate != nil {
			return &s.Migrate.SessionWorkflowOptions
		}
	case SessionTypeMigratePod:
		if s.MigratePod != nil {
			return &s.MigratePod.SessionWorkflowOptions
		}
	case SessionTypeCopy:
		if s.Copy != nil {
			return &s.Copy.SessionWorkflowOptions
		}
	}
	return nil
}

func (s SessionSpec) Operation() Operation {
	switch s.Type {
	case SessionTypeReserve:
		return OperationReserve
	case SessionTypeMigratePod:
		return OperationMigratePod
	case SessionTypeCopy:
		return OperationCopy
	case SessionTypeRename:
		return OperationRename
	case SessionTypeMove:
		return OperationMove
	default:
		return OperationMigrate
	}
}

func (s SessionSpec) Workload() WorkloadSpec {
	switch s.Type {
	case SessionTypeMigrate:
		if s.Migrate != nil {
			return s.Migrate.Workload
		}
	case SessionTypeMigratePod:
		if s.MigratePod != nil {
			return s.MigratePod.Workload
		}
	}
	return WorkloadSpec{Adapter: WorkloadNone}
}

func (s *SessionSpec) WorkloadPtr() *WorkloadSpec {
	switch s.Type {
	case SessionTypeMigrate:
		if s.Migrate != nil {
			return &s.Migrate.Workload
		}
	case SessionTypeMigratePod:
		if s.MigratePod != nil {
			return &s.MigratePod.Workload
		}
	}
	return nil
}

func (s SessionSpec) Online() bool {
	return s.Type == SessionTypeCopy && s.Copy != nil && s.Copy.Online
}

func (s SessionSpec) Orchestrated() bool {
	return s.Type == SessionTypeMigrate || s.Type == SessionTypeMigratePod
}

func (s *SessionSpec) SetWorkload(workload WorkloadSpec) error {
	switch s.Type {
	case SessionTypeMigrate:
		if s.Migrate == nil {
			s.Migrate = &MigrateSessionSpec{}
		}
		s.Migrate.Workload = workload
	case SessionTypeMigratePod:
		if s.MigratePod == nil {
			s.MigratePod = &MigratePodSessionSpec{}
		}
		s.MigratePod.Workload = workload
	default:
		return NewError(ErrorValidation, "session", fmt.Sprintf("workflow type %s has no workload payload", s.Type))
	}
	return nil
}

func NewSession(id string, spec SessionSpec, now time.Time) *Session {
	t := metav1.NewTime(now.UTC())
	volumeStatus := make([]VolumeStatus, 0, len(spec.Volumes))
	for _, volume := range spec.Volumes {
		volumeStatus = append(volumeStatus, VolumeStatus{SourcePVCName: volume.SourcePVC.Name})
	}
	return &Session{
		APIVersion: SessionAPIVersion,
		Kind:       SessionKind,
		ID:         id,
		Generation: 1,
		Spec:       spec,
		Status: SessionStatus{
			Phase:     PhasePlanned,
			StartedAt: t,
			UpdatedAt: t,
			Volumes:   volumeStatus,
			History:   []HistoryEntry{{Phase: PhasePlanned, Time: t, Message: fmt.Sprintf("%s session planned", spec.Operation())}},
		},
	}
}

func (s *Session) Transition(next Phase, message string, now time.Time) error {
	if s.Status.Phase == next {
		return nil
	}
	if !slices.Contains(allowedTransitions[s.Status.Phase], next) {
		return NewError(ErrorConflict, "transition", fmt.Sprintf("phase %s cannot transition to %s", s.Status.Phase, next))
	}
	t := metav1.NewTime(now.UTC())
	if next == PhaseFailed || ((next == PhaseAborting || next == PhaseRollingBack) && s.Status.Phase != PhaseFailed) {
		s.Status.ResumeFrom = s.Status.Phase
	}
	s.Status.Phase = next
	s.Status.Message = message
	s.Status.UpdatedAt = t
	s.Status.History = append(s.Status.History, HistoryEntry{Phase: next, Time: t, Message: message})
	if next == PhaseCompleted || next == PhaseAborted || next == PhaseRolledBack {
		s.Status.CompletedAt = &t
	} else {
		s.Status.CompletedAt = nil
	}
	return nil
}

func (s *Session) SetCondition(condition Condition) {
	for i := range s.Status.Conditions {
		if s.Status.Conditions[i].Type == condition.Type {
			s.Status.Conditions[i] = condition
			return
		}
	}
	s.Status.Conditions = append(s.Status.Conditions, condition)
}

func (s *Session) VolumeStatus(name string) (*VolumeStatus, error) {
	for i := range s.Status.Volumes {
		if s.Status.Volumes[i].SourcePVCName == name {
			return &s.Status.Volumes[i], nil
		}
	}
	return nil, NewError(ErrorInternal, "session", fmt.Sprintf("volume status missing for %s", name))
}

func (s *Session) Validate() error {
	if s.APIVersion != SessionAPIVersion || s.Kind != SessionKind {
		return NewError(ErrorValidation, "session", "unsupported session schema")
	}
	if s.ID == "" || s.Spec.SourceNamespace == "" || s.Spec.SessionNamespace == "" {
		return NewError(ErrorValidation, "session", "session identity and namespaces are required")
	}
	if s.Spec.Type == "" {
		return NewError(ErrorValidation, "session", "session type is required")
	}
	payloads := 0
	if s.Spec.Reserve != nil {
		payloads++
	}
	if s.Spec.Migrate != nil {
		payloads++
	}
	if s.Spec.MigratePod != nil {
		payloads++
	}
	if s.Spec.Copy != nil {
		payloads++
	}
	if s.Spec.Rename != nil {
		payloads++
	}
	if s.Spec.Move != nil {
		payloads++
	}
	if payloads != 1 {
		return NewError(ErrorValidation, "session", "exactly one concrete session payload is required")
	}
	validPayload := (s.Spec.Type == SessionTypeReserve && s.Spec.Reserve != nil) ||
		(s.Spec.Type == SessionTypeMigrate && s.Spec.Migrate != nil) ||
		(s.Spec.Type == SessionTypeMigratePod && s.Spec.MigratePod != nil) ||
		(s.Spec.Type == SessionTypeCopy && s.Spec.Copy != nil) ||
		(s.Spec.Type == SessionTypeRename && s.Spec.Rename != nil) ||
		(s.Spec.Type == SessionTypeMove && s.Spec.Move != nil)
	if !validPayload {
		return NewError(ErrorValidation, "session", fmt.Sprintf("payload does not match session type %s", s.Spec.Type))
	}
	if s.Spec.Online() && s.Spec.Type != SessionTypeCopy {
		return NewError(ErrorValidation, "session", "online mode is only valid for copy sessions")
	}
	if s.Spec.Type == SessionTypeRename && s.Spec.SourceNamespace != s.Spec.DestinationNamespace {
		return NewError(ErrorValidation, "session", "rename sessions must stay within one namespace")
	}
	if s.Spec.Type == SessionTypeMove && s.Spec.SourceNamespace == s.Spec.DestinationNamespace {
		return NewError(ErrorValidation, "session", "move sessions require different source and destination namespaces")
	}
	if len(s.Spec.Volumes) == 0 {
		return NewError(ErrorValidation, "session", "session contains no volumes")
	}
	if len(s.Spec.Volumes) != len(s.Status.Volumes) {
		return NewError(ErrorValidation, "session", "volume specification and status counts differ")
	}
	if _, known := allowedTransitions[s.Status.Phase]; !known && s.Status.Phase != PhaseAborted {
		return NewError(ErrorValidation, "session", fmt.Sprintf("unsupported session phase %q", s.Status.Phase))
	}
	seen := make(map[string]struct{}, len(s.Spec.Volumes))
	for index := range s.Spec.Volumes {
		volume := &s.Spec.Volumes[index]
		name := volume.SourcePVC.Name
		if volume.SourcePVC.Namespace == "" || name == "" || volume.SourcePVC.UID == "" {
			return NewError(ErrorValidation, "session", fmt.Sprintf("source PVC namespace, name, and UID are required for volume %d", index))
		}
		if volume.SourcePV.Name == "" || volume.SourcePV.UID == "" {
			return NewError(ErrorValidation, "session", fmt.Sprintf("source PV name and UID are required for volume %d", index))
		}
		if volume.DestinationPVC.Namespace == "" || volume.DestinationPVC.Name == "" {
			return NewError(ErrorValidation, "session", fmt.Sprintf("destination PVC namespace and name are required for volume %d", index))
		}
		if (volume.DestinationPV.Name == "") != (volume.DestinationPV.UID == "") {
			return NewError(ErrorValidation, "session", fmt.Sprintf("destination PV name and UID must be recorded together for volume %d", index))
		}
		status := &s.Status.Volumes[index]
		if status.Reserved && (volume.DestinationPVC.UID == "" || volume.DestinationPV.Name == "" || volume.DestinationPV.UID == "") {
			return NewError(ErrorValidation, "session", fmt.Sprintf("reserved destination PVC and PV identities are incomplete for volume %d", index))
		}
		active := status.Activation.ActivePVC
		if active.Name != "" || active.Namespace != "" || active.UID != "" {
			if active.Namespace == "" || active.Name == "" || active.UID == "" {
				return NewError(ErrorValidation, "session", fmt.Sprintf("active PVC namespace, name, and UID must be recorded together for volume %d", index))
			}
		}
		if _, exists := seen[name]; exists {
			return NewError(ErrorValidation, "session", fmt.Sprintf("duplicate source PVC %q", name))
		}
		seen[name] = struct{}{}
		if s.Status.Volumes[index].SourcePVCName != name {
			return NewError(ErrorValidation, "session", fmt.Sprintf("volume status %d does not match source PVC %q", index, name))
		}
	}
	if s.Spec.Orchestrated() {
		if err := validateWorkloadIdentity(s.Spec.Workload()); err != nil {
			return err
		}
	}
	for index, mount := range s.Status.OpenEBSLVMSharedMounts {
		if mount.SourcePV.Name == "" || mount.SourcePV.UID == "" {
			return NewError(ErrorValidation, "session", fmt.Sprintf("OpenEBS shared mount %d has an incomplete source PV identity", index))
		}
		if mount.LVMVolume.Namespace == "" || mount.LVMVolume.Name == "" || mount.LVMVolume.UID == "" {
			return NewError(ErrorValidation, "session", fmt.Sprintf("OpenEBS shared mount %d has an incomplete LVMVolume identity", index))
		}
	}
	return nil
}

func validateWorkloadIdentity(workload WorkloadSpec) error {
	if workload.Adapter == WorkloadNone {
		return nil
	}
	if err := validateWorkloadObjectReference(workload.Pod, "workload Pod"); err != nil {
		return err
	}
	if workload.Adapter == WorkloadStandalone {
		return nil
	}
	if err := validateWorkloadObjectReference(workload.Controller, "workload controller"); err != nil {
		return err
	}
	for index, ref := range workload.AffectedPods {
		if err := validateWorkloadObjectReference(ref, fmt.Sprintf("affected Pod %d", index)); err != nil {
			return err
		}
	}
	switch workload.Adapter {
	case WorkloadStatefulSet, WorkloadVictoriaLogs:
		return nil
	case WorkloadKubeBlocks:
		if workload.KubeBlocks == nil || workload.KubeBlocks.Cluster == "" || workload.KubeBlocks.ClusterUID == "" {
			return NewError(ErrorValidation, "session", "KubeBlocks workload Cluster name and UID are required")
		}
	case WorkloadVMCluster:
		if workload.VMCluster == nil || workload.VMCluster.Name == "" || workload.VMCluster.UID == "" {
			return NewError(ErrorValidation, "session", "VMCluster workload name and UID are required")
		}
	case WorkloadGrafana:
		if workload.Grafana == nil || workload.Grafana.Name == "" || workload.Grafana.UID == "" {
			return NewError(ErrorValidation, "session", "Grafana workload name and UID are required")
		}
	default:
		return NewError(ErrorValidation, "session", fmt.Sprintf("unsupported workload adapter %q", workload.Adapter))
	}
	return nil
}

func validateWorkloadObjectReference(ref ObjectReference, description string) error {
	if ref.Namespace == "" || ref.Name == "" || ref.UID == "" {
		return NewError(ErrorValidation, "session", fmt.Sprintf("%s namespace, name, and UID are required", description))
	}
	return nil
}
