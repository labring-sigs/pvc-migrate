package domain

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	SessionAPIGroup   = "migrate.sealos.io"
	SessionAPIVersion = SessionAPIGroup + "/v1alpha1"
	SessionKind       = "MigrationSession"
	// Workflow API specs and statuses are owned by api/v1alpha1; these names
	// form the shared discovery, storage, watch, and CLI routing contract.
	MigrationResource           = "migrations"
	PodMigrationResource        = "podmigrations"
	ReservationResource         = "reservations"
	CopyResource                = "copies"
	BackupResource              = "backups"
	RestoreResource             = "restores"
	RenameResource              = "renames"
	ClusterMigrationResource    = "clustermigrations"
	ClusterPodMigrationResource = "clusterpodmigrations"
	ClusterReservationResource  = "clusterreservations"
	ClusterCopyResource         = "clustercopies"
	MoveResource                = "moves"
	BackupRepositoryResource    = "backuprepositories"
	// Workflow status is user-visible and persisted in the API server. Keep
	// controller-generated history and messages bounded even when a lower
	// layer returns an unexpectedly large error string.
	MaxWorkflowHistoryEntries     = 256
	MaxWorkflowConditions         = 32
	MaxWorkflowConditionTypeBytes = 64
	MaxWorkflowReasonBytes        = 128
	MaxWorkflowMessageBytes       = 8192
	// A standalone Pod snapshot is required for a later workload restore, but
	// accepting arbitrary-size JSON here would let a tenant inflate CRD/cache
	// objects. The API uses an opaque JSON field, so this byte limit is enforced
	// at the domain/controller boundary.
	MaxOriginalPodSnapshotBytes = 512 * 1024
)

type Operation string

const (
	OperationMigrate    Operation = "Migrate"
	OperationMigratePod Operation = "MigratePod"
	OperationReserve    Operation = "Reserve"
	OperationCopy       Operation = "Copy"
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

// buildTransitionPolicy creates the shared transition policy used by the
// state machine. Callers that inspect it receive a copy from allowedTransitions.
func buildTransitionPolicy() map[Phase][]Phase {
	return map[Phase][]Phase{
		PhasePlanned:   {PhaseReserving, PhaseRenaming, PhaseMoving, PhaseAborting, PhaseFailed},
		PhaseRenaming:  {PhaseCompleted, PhaseAborting, PhaseFailed},
		PhaseMoving:    {PhaseCompleted, PhaseAborting, PhaseFailed},
		PhaseReserving: {PhaseReserved, PhaseAborting, PhaseFailed},
		PhaseReserved: {
			PhaseWarmCopying,
			PhasePausing,
			PhaseFinalSyncing,
			PhaseAborting,
			PhaseFailed,
		},
		PhaseWarmCopying: {PhaseWarmCopied, PhaseAborting, PhaseFailed},
		PhaseWarmCopied:  {PhaseWarmCopying, PhasePausing, PhaseAborting, PhaseFailed},
		PhasePausing:     {PhasePaused, PhaseAborting, PhaseFailed},
		PhasePaused: {
			PhaseFinalSyncing,
			PhaseResuming,
			PhaseRollingBack,
			PhaseAborting,
			PhaseFailed,
		},
		PhaseFinalSyncing: {PhaseFinalSynced, PhaseAborting, PhaseFailed},
		PhaseFinalSynced: {
			PhaseFinalSyncing,
			PhaseActivating,
			PhaseRollingBack,
			PhaseAborting,
			PhaseFailed,
		},
		PhaseActivating: {PhaseActivated, PhaseRollingBack, PhaseFailed},
		PhaseActivated:  {PhaseCompleted, PhaseResuming, PhaseRollingBack, PhaseFailed},
		PhaseResuming:   {PhaseCompleted, PhaseRolledBack, PhaseFailed},
		PhaseCompleted:  {PhaseRollingBack},
		PhaseAborting:   {PhaseAborted, PhaseFailed},
		PhaseFailed: {
			PhaseReserving,
			PhaseWarmCopying,
			PhasePausing,
			PhaseFinalSyncing,
			PhaseActivating,
			PhaseResuming,
			PhaseRollingBack,
			PhaseAborting,
			PhaseRenaming,
			PhaseMoving,
		},
		PhaseRollingBack: {PhaseRolledBack, PhaseFailed},
		PhaseRolledBack:  {PhaseResuming, PhaseCompleted},
	}
}

var transitionPolicy = buildTransitionPolicy()

// allowedTransitions returns an isolated policy copy for validation and tests.
func allowedTransitions() map[Phase][]Phase {
	result := make(map[Phase][]Phase, len(transitionPolicy))
	for phase, next := range transitionPolicy {
		result[phase] = slices.Clone(next)
	}

	return result
}

type ObjectReference struct {
	APIVersion      string    `json:"apiVersion,omitempty"      yaml:"apiVersion,omitempty"`
	Kind            string    `json:"kind,omitempty"            yaml:"kind,omitempty"`
	Namespace       string    `json:"namespace,omitempty"       yaml:"namespace,omitempty"`
	Name            string    `json:"name"                      yaml:"name"`
	UID             types.UID `json:"uid,omitempty"             yaml:"uid,omitempty"`
	ResourceVersion string    `json:"resourceVersion,omitempty" yaml:"resourceVersion,omitempty"`
}

// DeepCopyInto lets controller-gen deep-copy API objects that use the small
// reference value without exposing the domain session envelope in their
// Kubernetes schema.
//
//nolint:wsl_v5 // generated deepcopy helpers intentionally stay compact.
func (in *ObjectReference) DeepCopyInto(out *ObjectReference) {
	if in == nil || out == nil {
		return
	}
	*out = *in
}

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

type WorkloadSpec struct {
	Adapter          WorkloadKind      `json:"adapter"                    yaml:"adapter"`
	Pod              ObjectReference   `json:"pod,omitempty"              yaml:"pod,omitempty"`
	Controller       ObjectReference   `json:"controller,omitempty"       yaml:"controller,omitempty"`
	OriginalReplicas *int32            `json:"originalReplicas,omitempty" yaml:"originalReplicas,omitempty"`
	Ordinal          *int32            `json:"ordinal,omitempty"          yaml:"ordinal,omitempty"`
	AffectedPods     []ObjectReference `json:"affectedPods,omitempty"     yaml:"affectedPods,omitempty"`
	OriginalObject   json.RawMessage   `json:"originalObject,omitempty"   yaml:"originalObject,omitempty"`
	KubeBlocks       *KubeBlocksSpec   `json:"kubeBlocks,omitempty"       yaml:"kubeBlocks,omitempty"`
	VMCluster        *VMClusterSpec    `json:"vmCluster,omitempty"        yaml:"vmCluster,omitempty"`
	Grafana          *GrafanaSpec      `json:"grafana,omitempty"          yaml:"grafana,omitempty"`
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
	SessionTypeBackup     SessionType = "Backup"
	SessionTypeRename     SessionType = "Rename"
	SessionTypeMove       SessionType = "Move"
	SessionTypeRestore    SessionType = "Restore"
)

// ControllerKind identifies an operation-specific workflow CRD. Keep these
// values in the domain registry so discovery, storage, watches, and controller
// setup cannot drift onto different Kind spellings.
type ControllerKind string

const (
	ControllerKindMigration           ControllerKind = "Migration"
	ControllerKindPodMigration        ControllerKind = "PodMigration"
	ControllerKindReservation         ControllerKind = "Reservation"
	ControllerKindCopy                ControllerKind = "Copy"
	ControllerKindBackup              ControllerKind = "Backup"
	ControllerKindRestore             ControllerKind = "Restore"
	ControllerKindRename              ControllerKind = "Rename"
	ControllerKindClusterMigration    ControllerKind = "ClusterMigration"
	ControllerKindClusterPodMigration ControllerKind = "ClusterPodMigration"
	ControllerKindClusterReservation  ControllerKind = "ClusterReservation"
	ControllerKindClusterCopy         ControllerKind = "ClusterCopy"
	ControllerKindMove                ControllerKind = "Move"
)

// ControllerWorkflow identifies one operation-specific controller API. This
// registry is the authoritative mapping used by API discovery, CRD storage,
// controller dispatch, and CLI diagnostics.
type ControllerWorkflow struct {
	Type            SessionType
	Kind            ControllerKind
	Resource        string
	Singular        string
	ClusterKind     ControllerKind
	ClusterResource string
	ClusterSingular string
}

// ControllerResource identifies one concrete namespaced or cluster-scoped
// resource selected from an operation's workflow contract.
type ControllerResource struct {
	Type     SessionType
	Kind     ControllerKind
	Resource string
	Singular string
	Cluster  bool
}

// controllerWorkflowRegistry constructs a fresh registry for each caller so
// the routing metadata cannot be mutated across concurrent sessions.
func controllerWorkflowRegistry() []ControllerWorkflow {
	return []ControllerWorkflow{
		{
			Type:            SessionTypeMigrate,
			Kind:            ControllerKindMigration,
			Resource:        MigrationResource,
			Singular:        "migration",
			ClusterKind:     ControllerKindClusterMigration,
			ClusterResource: ClusterMigrationResource,
			ClusterSingular: "clustermigration",
		},
		{
			Type:            SessionTypeMigratePod,
			Kind:            ControllerKindPodMigration,
			Resource:        PodMigrationResource,
			Singular:        "podmigration",
			ClusterKind:     ControllerKindClusterPodMigration,
			ClusterResource: ClusterPodMigrationResource,
			ClusterSingular: "clusterpodmigration",
		},
		{
			Type:            SessionTypeReserve,
			Kind:            ControllerKindReservation,
			Resource:        ReservationResource,
			Singular:        "reservation",
			ClusterKind:     ControllerKindClusterReservation,
			ClusterResource: ClusterReservationResource,
			ClusterSingular: "clusterreservation",
		},
		{
			Type:            SessionTypeCopy,
			Kind:            ControllerKindCopy,
			Resource:        CopyResource,
			Singular:        "copy",
			ClusterKind:     ControllerKindClusterCopy,
			ClusterResource: ClusterCopyResource,
			ClusterSingular: "clustercopy",
		},
		{
			Type:     SessionTypeBackup,
			Kind:     ControllerKindBackup,
			Resource: BackupResource,
			Singular: "backup",
		},
		{
			Type:     SessionTypeRestore,
			Kind:     ControllerKindRestore,
			Resource: RestoreResource,
			Singular: "restore",
		},
		{
			Type:     SessionTypeRename,
			Kind:     ControllerKindRename,
			Resource: RenameResource,
			Singular: "rename",
		},
		{
			Type:            SessionTypeMove,
			ClusterKind:     ControllerKindMove,
			ClusterResource: MoveResource,
			ClusterSingular: "move",
		},
	}
}

// ControllerWorkflowResources returns the resource names served by the
// operation-specific workflow CRDs. Return a fresh slice so callers cannot
// mutate the process-wide workflow registry.
func ControllerWorkflowResources() []string {
	workflows := controllerWorkflowRegistry()

	resources := make([]string, 0, len(workflows)*2)
	for _, workflow := range workflows {
		if workflow.Resource != "" {
			resources = append(resources, workflow.Resource)
		}

		if workflow.ClusterResource != "" {
			resources = append(resources, workflow.ClusterResource)
		}
	}

	return resources
}

func ControllerWorkflows() []ControllerWorkflow {
	return controllerWorkflowRegistry()
}

func ControllerWorkflowForType(sessionType SessionType) (ControllerWorkflow, bool) {
	for _, workflow := range controllerWorkflowRegistry() {
		if workflow.Type == sessionType {
			return workflow, true
		}
	}

	return ControllerWorkflow{}, false
}

func ControllerWorkflowForKind(kind ControllerKind) (ControllerWorkflow, bool) {
	if kind == "" {
		return ControllerWorkflow{}, false
	}

	for _, workflow := range controllerWorkflowRegistry() {
		if workflow.Kind != "" && workflow.Kind == kind ||
			workflow.ClusterKind != "" && workflow.ClusterKind == kind {
			return workflow, true
		}
	}

	return ControllerWorkflow{}, false
}

// ControllerWorkflowForTypeAndScope returns the operation workflow for the
// requested resource scope. The CLI selects cluster-scoped resources for
// cross-namespace roles by default, while the API also permits equal namespace
// roles when an administrator explicitly submits a cluster-scoped object.
func ControllerWorkflowForTypeAndScope(
	sessionType SessionType,
	clusterScope bool,
) (ControllerWorkflow, bool) {
	workflow, ok := ControllerWorkflowForType(sessionType)
	if !ok || clusterScope && workflow.ClusterKind == "" || !clusterScope && workflow.Kind == "" {
		return ControllerWorkflow{}, false
	}

	return workflow, true
}

func ClusterControllerKindForType(sessionType SessionType) (ControllerKind, bool) {
	workflow, ok := ControllerWorkflowForType(sessionType)
	if !ok || workflow.ClusterKind == "" {
		return "", false
	}

	return workflow.ClusterKind, true
}

func ControllerKindForTypeAndScope(
	sessionType SessionType,
	clusterScope bool,
) (ControllerKind, bool) {
	workflow, ok := ControllerWorkflowForTypeAndScope(sessionType, clusterScope)
	if !ok {
		return "", false
	}

	if clusterScope {
		return workflow.ClusterKind, true
	}

	return workflow.Kind, true
}

func IsClusterControllerKind(kind ControllerKind) bool {
	workflow, ok := ControllerWorkflowForKind(kind)
	return ok && workflow.ClusterKind == kind
}

func ControllerResourceForKind(kind ControllerKind) (ControllerResource, bool) {
	workflow, ok := ControllerWorkflowForKind(kind)
	if !ok {
		return ControllerResource{}, false
	}

	if workflow.Kind == kind {
		return ControllerResource{
			Type: workflow.Type, Kind: kind,
			Resource: workflow.Resource, Singular: workflow.Singular,
		}, true
	}

	return ControllerResource{
		Type: workflow.Type, Kind: kind,
		Resource: workflow.ClusterResource, Singular: workflow.ClusterSingular,
		Cluster: true,
	}, true
}

// ControllerResourceForSession selects the durable API represented by a
// session. A persisted BackendResource is authoritative; new sessions derive
// scope from their namespace roles and operation semantics.
func ControllerResourceForSession(session *Session) (ControllerResource, bool) {
	if session == nil {
		return ControllerResource{}, false
	}

	if session.BackendResource != "" {
		resource, ok := ControllerResourceForKind(session.BackendResource)
		if ok && resource.Type == session.Spec.Type {
			return resource, true
		}

		return ControllerResource{}, false
	}

	return ControllerResourceForSpec(session.Spec)
}

// ControllerResourceForSpec selects the desired API for a new workflow or an
// explicit operation hand-off. It deliberately ignores persistence metadata.
func ControllerResourceForSpec(spec SessionSpec) (ControllerResource, bool) {
	clusterScope := spec.Type == SessionTypeMove ||
		spec.SourceNamespace != spec.SessionNamespace ||
		spec.TemporaryNamespace != "" &&
			spec.TemporaryNamespace != spec.SessionNamespace
	if spec.Type != SessionTypeReserve && spec.Type != SessionTypeCopy {
		clusterScope = clusterScope || spec.DestinationNamespace != "" &&
			spec.DestinationNamespace != spec.SessionNamespace
	}

	kind, ok := ControllerKindForTypeAndScope(spec.Type, clusterScope)
	if !ok {
		return ControllerResource{}, false
	}

	return ControllerResourceForKind(kind)
}

type SessionCommon struct {
	SourceNamespace      string       `json:"sourceNamespace"      yaml:"sourceNamespace"`
	TemporaryNamespace   string       `json:"temporaryNamespace"   yaml:"temporaryNamespace"`
	DestinationNamespace string       `json:"destinationNamespace" yaml:"destinationNamespace"`
	SessionNamespace     string       `json:"sessionNamespace"     yaml:"sessionNamespace"`
	Volumes              []VolumeSpec `json:"volumes,omitempty"    yaml:"volumes,omitempty"`
}

type SessionWorkflowOptions struct {
	SourceNode     string   `json:"sourceNode,omitempty"     yaml:"sourceNode,omitempty"`
	TargetNode     string   `json:"targetNode,omitempty"     yaml:"targetNode,omitempty"`
	ToolImage      string   `json:"toolImage,omitempty"      yaml:"toolImage,omitempty"`
	Strategies     []string `json:"strategies,omitempty"     yaml:"strategies,omitempty"`
	VerifyChecksum bool     `json:"verifyChecksum,omitempty" yaml:"verifyChecksum,omitempty"`
	// +optional
	DeleteExtraneous     bool `json:"deleteExtraneous"               yaml:"deleteExtraneous"`
	SkipSourceUsageCheck bool `json:"skipSourceUsageCheck,omitempty" yaml:"skipSourceUsageCheck,omitempty"`
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
}

type ReserveSessionSpec struct {
	SessionWorkflowOptions `json:",inline" yaml:",inline"`
}

type MigratePodSessionSpec struct {
	SessionWorkflowOptions `             json:",inline"                          yaml:",inline"`
	Workload               WorkloadSpec `json:"workload"                         yaml:"workload"`
	PrecopyPasses          int          `json:"precopyPasses"                    yaml:"precopyPasses"`
	OpenEBSLVMEnableShared bool         `json:"openebsLvmEnableShared,omitempty" yaml:"openebsLvmEnableShared,omitempty"`
}

type CopySessionSpec struct {
	SessionWorkflowOptions `     json:",inline"          yaml:",inline"`
	Online                 bool `json:"online,omitempty" yaml:"online,omitempty"`
}

// BackupBackend identifies the data-plane implementation used by an inline
// session-mode backup configuration. Controller workflows leave this empty:
// their backend is selected by the referenced BackupRepository at reconcile
// time, so adding a repository type does not change the workflow contract.
type BackupBackend string

const BackupBackendS3 BackupBackend = "s3"

// BackupSessionSpec is the backup-specific payload carried by the durable
// Session envelope. Object-store credentials are deliberately excluded.
type BackupSessionSpec struct {
	SessionWorkflowOptions `                json:",inline"                         yaml:",inline"`
	Online                 bool            `json:"online,omitempty"                yaml:"online,omitempty"`
	SourcePVC              ObjectReference `json:"sourcePVC"                       yaml:"sourcePVC"`
	SourcePV               ObjectReference `json:"sourcePV"                        yaml:"sourcePV"`
	Path                   string          `json:"path,omitempty"                  yaml:"path,omitempty"`
	Backend                BackupBackend   `json:"backend"                         yaml:"backend"`
	Bucket                 string          `json:"bucket"                          yaml:"bucket"`
	Prefix                 string          `json:"prefix,omitempty"                yaml:"prefix,omitempty"`
	Name                   string          `json:"name"                            yaml:"name"`
	Provider               string          `json:"provider,omitempty"              yaml:"provider,omitempty"`
	Endpoint               string          `json:"endpoint,omitempty"              yaml:"endpoint,omitempty"`
	Region                 string          `json:"region,omitempty"                yaml:"region,omitempty"`
	AllowInsecureEndpoint  bool            `json:"allowInsecureEndpoint,omitempty" yaml:"allowInsecureEndpoint,omitempty"`
	ServerSideEncryption   string          `json:"serverSideEncryption,omitempty"  yaml:"serverSideEncryption,omitempty"`
	SSEKMSKeyID            string          `json:"sseKmsKeyID,omitempty"           yaml:"sseKmsKeyID,omitempty"`
	// BackupRepository names a user-owned namespaced repository containing the
	// complete object-store location and a same-namespace credentials Secret.
	BackupRepository string `json:"backupRepository,omitempty" yaml:"backupRepository,omitempty"`
	// BackupRepositoryNamespace is populated for cluster-scoped workflows.
	// Namespaced workflows always use SessionNamespace and do not expose a
	// namespace selector in their API.
	BackupRepositoryNamespace string          `json:"backupRepositoryNamespace,omitempty" yaml:"backupRepositoryNamespace,omitempty"`
	CredentialsSecret         ObjectReference `json:"credentialsSecret,omitempty"         yaml:"credentialsSecret,omitempty"`
	OpenEBSLVMEnableShared    bool            `json:"openebsLvmEnableShared,omitempty"    yaml:"openebsLvmEnableShared,omitempty"`
}

// RestoreSessionSpec is the durable payload for object-store restore. The
// credentials are referenced by Secret and never embedded in this resource.
type RestoreSessionSpec struct {
	SessionWorkflowOptions    `                json:",inline"                             yaml:",inline"`
	DestinationPVC            ObjectReference `json:"destinationPVC"                      yaml:"destinationPVC"`
	DestinationPV             ObjectReference `json:"destinationPV,omitempty"             yaml:"destinationPV,omitempty"`
	Path                      string          `json:"path,omitempty"                      yaml:"path,omitempty"`
	Backend                   BackupBackend   `json:"backend"                             yaml:"backend"`
	Bucket                    string          `json:"bucket"                              yaml:"bucket"`
	Prefix                    string          `json:"prefix,omitempty"                    yaml:"prefix,omitempty"`
	Name                      string          `json:"name"                                yaml:"name"`
	Provider                  string          `json:"provider,omitempty"                  yaml:"provider,omitempty"`
	Endpoint                  string          `json:"endpoint,omitempty"                  yaml:"endpoint,omitempty"`
	Region                    string          `json:"region,omitempty"                    yaml:"region,omitempty"`
	AllowInsecureEndpoint     bool            `json:"allowInsecureEndpoint,omitempty"     yaml:"allowInsecureEndpoint,omitempty"`
	ServerSideEncryption      string          `json:"serverSideEncryption,omitempty"      yaml:"serverSideEncryption,omitempty"`
	SSEKMSKeyID               string          `json:"sseKmsKeyID,omitempty"               yaml:"sseKmsKeyID,omitempty"`
	BackupRepository          string          `json:"backupRepository,omitempty"          yaml:"backupRepository,omitempty"`
	BackupRepositoryNamespace string          `json:"backupRepositoryNamespace,omitempty" yaml:"backupRepositoryNamespace,omitempty"`
	CredentialsSecret         ObjectReference `json:"credentialsSecret,omitempty"         yaml:"credentialsSecret,omitempty"`
	CreatePVC                 bool            `json:"createPVC,omitempty"                 yaml:"createPVC,omitempty"`
	DestinationStorageClass   string          `json:"destinationStorageClass,omitempty"   yaml:"destinationStorageClass,omitempty"`
	DestinationAccessMode     string          `json:"destinationAccessMode,omitempty"     yaml:"destinationAccessMode,omitempty"`
	DestinationCapacity       string          `json:"destinationCapacity,omitempty"       yaml:"destinationCapacity,omitempty"`
	AllowMounted              bool            `json:"allowMounted,omitempty"              yaml:"allowMounted,omitempty"`
}

type RenameSessionSpec struct{}

type MoveSessionSpec struct{}

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

// KubeBlocksSwitchoverStrategy is the durable leader-handoff mechanism chosen during planning.
type KubeBlocksSwitchoverStrategy string

const (
	KubeBlocksSwitchoverOpsRequest    KubeBlocksSwitchoverStrategy = "opsrequest"
	KubeBlocksSwitchoverMongoDBNative KubeBlocksSwitchoverStrategy = "mongodb-native"
)

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

type SessionSpec struct {
	SessionCommon `json:",inline" yaml:",inline"`
	// +kubebuilder:validation:Enum=Reserve;Migrate;MigratePod;Copy;Backup;Restore;Rename;Move
	Type       SessionType            `json:"type"                 yaml:"type"`
	Reserve    *ReserveSessionSpec    `json:"reserve,omitempty"    yaml:"reserve,omitempty"`
	Migrate    *MigrateSessionSpec    `json:"migrate,omitempty"    yaml:"migrate,omitempty"`
	MigratePod *MigratePodSessionSpec `json:"migratePod,omitempty" yaml:"migratePod,omitempty"`
	Copy       *CopySessionSpec       `json:"copy,omitempty"       yaml:"copy,omitempty"`
	Backup     *BackupSessionSpec     `json:"backup,omitempty"     yaml:"backup,omitempty"`
	Restore    *RestoreSessionSpec    `json:"restore,omitempty"    yaml:"restore,omitempty"`
	Rename     *RenameSessionSpec     `json:"rename,omitempty"     yaml:"rename,omitempty"`
	Move       *MoveSessionSpec       `json:"move,omitempty"       yaml:"move,omitempty"`
}

// DeepCopyInto copies every mutable member of a session spec. The domain
// object is passed between the planner, persistence adapters, controller
// cache, and service stages, so a shallow copy would let one request mutate
// another request's state through shared slices or pointers.
func (in *SessionSpec) DeepCopyInto(out *SessionSpec) {
	if in == nil || out == nil {
		return
	}

	*out = *in

	out.SessionCommon = deepCopySessionCommon(in.SessionCommon)
	if in.Reserve != nil {
		out.Reserve = &ReserveSessionSpec{
			SessionWorkflowOptions: deepCopyWorkflowOptions(in.Reserve.SessionWorkflowOptions),
		}
	}

	if in.Migrate != nil {
		out.Migrate = &MigrateSessionSpec{
			SessionWorkflowOptions: deepCopyWorkflowOptions(in.Migrate.SessionWorkflowOptions),
		}
	}

	if in.MigratePod != nil {
		out.MigratePod = &MigratePodSessionSpec{
			SessionWorkflowOptions: deepCopyWorkflowOptions(in.MigratePod.SessionWorkflowOptions),
			Workload:               deepCopyWorkloadSpec(in.MigratePod.Workload),
			PrecopyPasses:          in.MigratePod.PrecopyPasses,
			OpenEBSLVMEnableShared: in.MigratePod.OpenEBSLVMEnableShared,
		}
	}

	if in.Copy != nil {
		out.Copy = &CopySessionSpec{
			SessionWorkflowOptions: deepCopyWorkflowOptions(in.Copy.SessionWorkflowOptions),
			Online:                 in.Copy.Online,
		}
	}

	if in.Backup != nil {
		backup := *in.Backup
		backup.SessionWorkflowOptions = deepCopyWorkflowOptions(in.Backup.SessionWorkflowOptions)
		out.Backup = &backup
	}

	if in.Restore != nil {
		restore := *in.Restore
		restore.SessionWorkflowOptions = deepCopyWorkflowOptions(in.Restore.SessionWorkflowOptions)
		out.Restore = &restore
	}

	if in.Rename != nil {
		out.Rename = &RenameSessionSpec{}
	}

	if in.Move != nil {
		out.Move = &MoveSessionSpec{}
	}
}

func (in *SessionSpec) DeepCopy() *SessionSpec {
	if in == nil {
		return nil
	}

	out := new(SessionSpec)
	in.DeepCopyInto(out)

	return out
}

func (in *SessionStatus) DeepCopyInto(out *SessionStatus) {
	if in == nil || out == nil {
		return
	}

	*out = *in
	if in.CompletedAt != nil {
		out.CompletedAt = in.CompletedAt.DeepCopy()
	}

	if in.Conditions != nil {
		out.Conditions = slices.Clone(in.Conditions)
	}

	if in.Volumes != nil {
		out.Volumes = make([]VolumeStatus, len(in.Volumes))
		for i := range in.Volumes {
			out.Volumes[i] = deepCopyVolumeStatus(in.Volumes[i])
		}
	}

	if in.History != nil {
		out.History = slices.Clone(in.History)
	}

	if in.OpenEBSLVMSharedMounts != nil {
		out.OpenEBSLVMSharedMounts = slices.Clone(in.OpenEBSLVMSharedMounts)
	}
}

func (in *SessionStatus) DeepCopy() *SessionStatus {
	if in == nil {
		return nil
	}

	out := new(SessionStatus)
	in.DeepCopyInto(out)

	return out
}

func deepCopyWorkflowOptions(in SessionWorkflowOptions) SessionWorkflowOptions {
	out := in
	out.Strategies = slices.Clone(in.Strategies)
	return out
}

func deepCopySessionCommon(in SessionCommon) SessionCommon {
	out := in
	if in.Volumes != nil {
		out.Volumes = make([]VolumeSpec, len(in.Volumes))
		for i := range in.Volumes {
			out.Volumes[i] = deepCopyVolumeSpec(in.Volumes[i])
		}
	}

	return out
}

func deepCopyWorkloadSpec(in WorkloadSpec) WorkloadSpec {
	out := in
	out.OriginalReplicas = cloneInt32(in.OriginalReplicas)
	out.Ordinal = cloneInt32(in.Ordinal)
	out.AffectedPods = slices.Clone(in.AffectedPods)

	out.OriginalObject = slices.Clone(in.OriginalObject)
	if in.KubeBlocks != nil {
		kubeBlocks := *in.KubeBlocks
		out.KubeBlocks = &kubeBlocks
	}

	if in.VMCluster != nil {
		vmCluster := *in.VMCluster
		out.VMCluster = &vmCluster
	}

	if in.Grafana != nil {
		grafana := *in.Grafana
		out.Grafana = &grafana
	}

	return out
}

func deepCopyVolumeSpec(in VolumeSpec) VolumeSpec {
	out := in
	out.SourcePVCSpec = *in.SourcePVCSpec.DeepCopy()
	out.SourcePVCMetadata = PVCMetadata{
		Labels:          maps.Clone(in.SourcePVCMetadata.Labels),
		Annotations:     maps.Clone(in.SourcePVCMetadata.Annotations),
		OwnerReferences: slices.Clone(in.SourcePVCMetadata.OwnerReferences),
	}

	out.AccessModes = slices.Clone(in.AccessModes)
	if in.TransferScope != nil {
		transferScope := *in.TransferScope
		out.TransferScope = &transferScope
	}

	return out
}

func deepCopyVolumeStatus(in VolumeStatus) VolumeStatus {
	out := in
	if in.Sync.WarmCompletedAt != nil {
		out.Sync.WarmCompletedAt = in.Sync.WarmCompletedAt.DeepCopy()
	}

	if in.Sync.FinalCompletedAt != nil {
		out.Sync.FinalCompletedAt = in.Sync.FinalCompletedAt.DeepCopy()
	}

	if in.Activation.ActivatedAt != nil {
		out.Activation.ActivatedAt = in.Activation.ActivatedAt.DeepCopy()
	}

	if in.Activation.RolledBackAt != nil {
		out.Activation.RolledBackAt = in.Activation.RolledBackAt.DeepCopy()
	}

	return out
}

func cloneInt32(in *int32) *int32 {
	if in == nil {
		return nil
	}

	out := *in

	return &out
}

type PVCMetadata struct {
	Labels          map[string]string       `json:"labels,omitempty"          yaml:"labels,omitempty"`
	Annotations     map[string]string       `json:"annotations,omitempty"     yaml:"annotations,omitempty"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences,omitempty" yaml:"ownerReferences,omitempty"`
}

type VolumeSpec struct {
	SourcePVC           ObjectReference                      `json:"sourcePVC"                          yaml:"sourcePVC"`
	SourcePV            ObjectReference                      `json:"sourcePV"                           yaml:"sourcePV"`
	SourceReclaimPolicy corev1.PersistentVolumeReclaimPolicy `json:"sourceReclaimPolicy,omitempty"      yaml:"sourceReclaimPolicy,omitempty"`
	SourcePVCSpec       corev1.PersistentVolumeClaimSpec     `json:"sourcePVCSpec,omitempty"            yaml:"sourcePVCSpec,omitempty"`
	SourcePVCMetadata   PVCMetadata                          `json:"sourcePVCMetadata,omitempty"        yaml:"sourcePVCMetadata,omitempty"`
	DestinationPVC      ObjectReference                      `json:"destinationPVC"                     yaml:"destinationPVC"`
	DestinationPV       ObjectReference                      `json:"destinationPV,omitempty"            yaml:"destinationPV,omitempty"`
	DestinationPolicy   corev1.PersistentVolumeReclaimPolicy `json:"destinationReclaimPolicy,omitempty" yaml:"destinationReclaimPolicy,omitempty"`
	// Capacity is the requested destination PVC capacity. SourceCapacity
	// records the original PV capacity used for resize safety checks.
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

func (v VolumeSpec) RequiresConcurrentRWOMount() bool {
	return v.ConcurrentConsumers > 1 &&
		slices.Contains(v.AccessModes, corev1.ReadWriteOnce)
}

type SyncState struct {
	WarmCompletedAt  *metav1.Time `json:"warmCompletedAt,omitempty"  yaml:"warmCompletedAt,omitempty"`
	FinalCompletedAt *metav1.Time `json:"finalCompletedAt,omitempty" yaml:"finalCompletedAt,omitempty"`
	Attempts         int          `json:"attempts"                   yaml:"attempts"`
	BytesCopied      int64        `json:"bytesCopied,omitempty"      yaml:"bytesCopied,omitempty"`
	ChecksumVerified bool         `json:"checksumVerified,omitempty" yaml:"checksumVerified,omitempty"`
	LastError        string       `json:"lastError,omitempty"        yaml:"lastError,omitempty"`
}

type ActivationState struct {
	TemporaryPVCDeleted bool            `json:"temporaryPVCDeleted,omitempty" yaml:"temporaryPVCDeleted,omitempty"`
	SourcePVCDeleted    bool            `json:"sourcePVCDeleted,omitempty"    yaml:"sourcePVCDeleted,omitempty"`
	DestinationReserved bool            `json:"destinationReserved,omitempty" yaml:"destinationReserved,omitempty"`
	ActivePVC           ObjectReference `json:"activePVC,omitempty"           yaml:"activePVC,omitempty"`
	ActivatedAt         *metav1.Time    `json:"activatedAt,omitempty"         yaml:"activatedAt,omitempty"`
	RolledBackAt        *metav1.Time    `json:"rolledBackAt,omitempty"        yaml:"rolledBackAt,omitempty"`
}

type VolumeStatus struct {
	SourcePVCName string          `json:"sourcePVCName"      yaml:"sourcePVCName"`
	Reserved      bool            `json:"reserved,omitempty" yaml:"reserved,omitempty"`
	Sync          SyncState       `json:"sync"               yaml:"sync"`
	Activation    ActivationState `json:"activation"         yaml:"activation"`
}

type Condition struct {
	Type               string                 `json:"type"               yaml:"type"`
	Status             metav1.ConditionStatus `json:"status"             yaml:"status"`
	Reason             string                 `json:"reason,omitempty"   yaml:"reason,omitempty"`
	Message            string                 `json:"message,omitempty"  yaml:"message,omitempty"`
	LastTransitionTime metav1.Time            `json:"lastTransitionTime" yaml:"lastTransitionTime"`
}

type HistoryEntry struct {
	Phase   Phase       `json:"phase"             yaml:"phase"`
	Time    metav1.Time `json:"time"              yaml:"time"`
	Message string      `json:"message,omitempty" yaml:"message,omitempty"`
}

// SessionFailureReason identifies failures that require a new session instead
// of retrying the persisted workflow inputs.
type SessionFailureReason string

const (
	FailureDestinationCapacityExhausted SessionFailureReason = "DestinationCapacityExhausted"
)

// OpenEBSLVMSharedMount records a temporary LVMVolume.spec.shared change made
// by this session. PreviousSharedSet distinguishes an absent field from an
// explicitly empty value so cleanup can restore the original CR exactly.
type OpenEBSLVMSharedMount struct {
	SourcePV          ObjectReference `json:"sourcePV"                    yaml:"sourcePV"`
	LVMVolume         ObjectReference `json:"lvmVolume"                   yaml:"lvmVolume"`
	PreviousShared    string          `json:"previousShared,omitempty"    yaml:"previousShared,omitempty"`
	PreviousSharedSet bool            `json:"previousSharedSet,omitempty" yaml:"previousSharedSet,omitempty"`
}

// BackupRepositoryType identifies the repository data plane selected by a
// controller workflow. It is separate from BackupBackend, which describes the
// inline session-mode transfer configuration.
type BackupRepositoryType string

const (
	BackupRepositoryTypeS3  BackupRepositoryType = "s3"
	BackupRepositoryTypePVC BackupRepositoryType = "pvc"
)

// S3BackupRepositoryBindingStatus pins the S3-specific dependency used by a
// running workflow. Secret contents may rotate in place; replacing the Secret
// changes its UID and requires a new workflow.
type S3BackupRepositoryBindingStatus struct {
	CredentialsSecretUID types.UID `json:"credentialsSecretUID" yaml:"credentialsSecretUID"`
}

// PVCBackupRepositoryBindingStatus pins the PVC identity used by a future PVC
// repository adapter. Keeping it backend-specific prevents object-store
// credentials from becoming part of every repository's status contract.
type PVCBackupRepositoryBindingStatus struct {
	ClaimUID types.UID `json:"claimUID" yaml:"claimUID"`
}

// BackupRepositoryBindingStatus records the immutable resource identities
// resolved for one running workflow.
type BackupRepositoryBindingStatus struct {
	Type       BackupRepositoryType              `json:"type"          yaml:"type"`
	UID        types.UID                         `json:"uid"           yaml:"uid"`
	Generation int64                             `json:"generation"    yaml:"generation"`
	S3         *S3BackupRepositoryBindingStatus  `json:"s3,omitempty"  yaml:"s3,omitempty"`
	PVC        *PVCBackupRepositoryBindingStatus `json:"pvc,omitempty" yaml:"pvc,omitempty"`
}

type SessionStatus struct {
	Phase               Phase                `json:"phase"                   yaml:"phase"`
	ResumeFrom          Phase                `json:"resumeFrom,omitempty"    yaml:"resumeFrom,omitempty"`
	FailureReason       SessionFailureReason `json:"failureReason,omitempty" yaml:"failureReason,omitempty"`
	ErrorCategory       ErrorCategory        `json:"errorCategory,omitempty" yaml:"errorCategory,omitempty"`
	WarmPassesCompleted int                  `json:"warmPassesCompleted"     yaml:"warmPassesCompleted"`
	// OriginalPodSnapshotHash records the controller-captured standalone Pod
	// snapshot used for a later workload resume. It is populated only for
	// controller-backed PodMigration workflows.
	OriginalPodSnapshotHash string                         `json:"originalPodSnapshotHash,omitempty" yaml:"originalPodSnapshotHash,omitempty"`
	BackupRepository        *BackupRepositoryBindingStatus `json:"backupRepository,omitempty"        yaml:"backupRepository,omitempty"`
	ObservedGeneration      int64                          `json:"observedGeneration,omitempty"      yaml:"observedGeneration,omitempty"`
	StartedAt               metav1.Time                    `json:"startedAt"                         yaml:"startedAt"`
	UpdatedAt               metav1.Time                    `json:"updatedAt"                         yaml:"updatedAt"`
	CompletedAt             *metav1.Time                   `json:"completedAt,omitempty"             yaml:"completedAt,omitempty"`
	Message                 string                         `json:"message,omitempty"                 yaml:"message,omitempty"`
	Conditions              []Condition                    `json:"conditions,omitempty"              yaml:"conditions,omitempty"`
	Volumes                 []VolumeStatus                 `json:"volumes"                           yaml:"volumes"`
	History                 []HistoryEntry                 `json:"history,omitempty"                 yaml:"history,omitempty"`
	OpenEBSLVMSharedMounts  []OpenEBSLVMSharedMount        `json:"openebsLvmSharedMounts,omitempty"  yaml:"openebsLvmSharedMounts,omitempty"`
}

type Session struct {
	APIVersion      string `json:"apiVersion" yaml:"apiVersion"`
	Kind            string `json:"kind"       yaml:"kind"`
	ID              string `json:"id"         yaml:"id"`
	Generation      int64  `json:"generation" yaml:"generation"`
	ResourceVersion string `json:"-"          yaml:"-"`
	// Backend is populated by the persistence adapter and is intentionally not
	// serialized. It lets a routing store send updates to the same backend.
	Backend string `json:"-" yaml:"-"`
	// BackendResource identifies the operation-specific CRD kind when Backend
	// is SessionBackendCRD. It is intentionally not serialized.
	BackendResource ControllerKind `json:"-" yaml:"-"`
	// BackendUID identifies the operation CR used as an owner for generated
	// credentials. It is process-local metadata and is never persisted.
	BackendUID types.UID `json:"-" yaml:"-"`
	// Deleting is populated by the CRD adapter from metadata.deletionTimestamp
	// and is intentionally process-local. A controller must never start new
	// work after a user has requested deletion of the workflow resource.
	Deleting bool          `json:"-"      yaml:"-"`
	Spec     SessionSpec   `json:"spec"   yaml:"spec"`
	Status   SessionStatus `json:"status" yaml:"status"`
}

func sessionTypeForOperation(operation Operation) SessionType {
	switch operation {
	case OperationReserve:
		return SessionTypeReserve
	case OperationCopy:
		return SessionTypeCopy
	case OperationBackup:
		return SessionTypeBackup
	case OperationRestore:
		return SessionTypeRestore
	case OperationRename:
		return SessionTypeRename
	case OperationMove:
		return SessionTypeMove
	default:
		return ""
	}
}

func NewSessionSpec(
	operation Operation,
	common SessionCommon,
	online bool,
	options SessionWorkflowOptions,
) SessionSpec {
	common = deepCopySessionCommon(common)
	options.Strategies = slices.Clone(options.Strategies)

	spec := SessionSpec{SessionCommon: common, Type: sessionTypeForOperation(operation)}
	switch spec.Type {
	case SessionTypeReserve:
		spec.Reserve = &ReserveSessionSpec{SessionWorkflowOptions: options}
	case SessionTypeCopy:
		spec.Copy = &CopySessionSpec{SessionWorkflowOptions: options, Online: online}
	case SessionTypeBackup:
		spec.Backup = &BackupSessionSpec{SessionWorkflowOptions: options, Online: online}
	case SessionTypeRestore:
		spec.Restore = &RestoreSessionSpec{SessionWorkflowOptions: options}
	case SessionTypeRename:
		spec.Rename = &RenameSessionSpec{}
	case SessionTypeMove:
		spec.Move = &MoveSessionSpec{}
	}

	return spec
}

func NewOfflineMigrationSessionSpec(
	common SessionCommon,
	options SessionWorkflowOptions,
) SessionSpec {
	common = deepCopySessionCommon(common)
	options.Strategies = slices.Clone(options.Strategies)

	return SessionSpec{
		SessionCommon: common,
		Type:          SessionTypeMigrate,
		Migrate:       &MigrateSessionSpec{SessionWorkflowOptions: options},
	}
}

func NewPodMigrationSessionSpec(
	common SessionCommon,
	workload WorkloadSpec,
	options SessionWorkflowOptions,
	precopyPasses int,
	openEBSLVMEnableShared bool,
) SessionSpec {
	common = deepCopySessionCommon(common)
	options.Strategies = slices.Clone(options.Strategies)

	return SessionSpec{
		SessionCommon: common,
		Type:          SessionTypeMigratePod,
		MigratePod: &MigratePodSessionSpec{
			SessionWorkflowOptions: options,
			Workload:               deepCopyWorkloadSpec(workload),
			PrecopyPasses:          precopyPasses,
			OpenEBSLVMEnableShared: openEBSLVMEnableShared,
		},
	}
}

func (s SessionSpec) WorkflowOptions() SessionWorkflowOptions {
	if options := s.WorkflowOptionsPtr(); options != nil {
		cloned := *options
		cloned.Strategies = slices.Clone(options.Strategies)
		return cloned
	}

	return SessionWorkflowOptions{}
}

func (s SessionSpec) PrecopyPasses() int {
	if s.Type == SessionTypeMigratePod && s.MigratePod != nil {
		return s.MigratePod.PrecopyPasses
	}

	return 0
}

func (s SessionSpec) OpenEBSLVMSharedMountEnabled() bool {
	switch s.Type {
	case SessionTypeMigratePod:
		return s.MigratePod != nil && s.MigratePod.OpenEBSLVMEnableShared
	case SessionTypeBackup:
		return s.Backup != nil && s.Backup.OpenEBSLVMEnableShared
	default:
		return false
	}
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
	case SessionTypeBackup:
		if s.Backup != nil {
			return &s.Backup.SessionWorkflowOptions
		}
	case SessionTypeRestore:
		if s.Restore != nil {
			return &s.Restore.SessionWorkflowOptions
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
	case SessionTypeBackup:
		return OperationBackup
	case SessionTypeRestore:
		return OperationRestore
	case SessionTypeRename:
		return OperationRename
	case SessionTypeMove:
		return OperationMove
	default:
		return OperationMigrate
	}
}

func (s SessionSpec) Workload() WorkloadSpec {
	if s.Type == SessionTypeMigratePod && s.MigratePod != nil {
		return s.MigratePod.Workload
	}

	return WorkloadSpec{Adapter: WorkloadNone}
}

// KubeBlocksPodMigration reports whether this session is the real-time Pod
// workflow for a discovered KubeBlocks workload. Offline migrate sessions do
// not use this classification even when workload metadata is present.
func (s SessionSpec) KubeBlocksPodMigration() (*KubeBlocksSpec, bool) {
	if s.Operation() != OperationMigratePod {
		return nil, false
	}

	workload := s.Workload()
	if workload.Adapter != WorkloadKubeBlocks || workload.KubeBlocks == nil {
		return nil, false
	}

	return workload.KubeBlocks, true
}

func (s *SessionSpec) WorkloadPtr() *WorkloadSpec {
	if s.Type == SessionTypeMigratePod && s.MigratePod != nil {
		return &s.MigratePod.Workload
	}

	return nil
}

func (s SessionSpec) Online() bool {
	return (s.Type == SessionTypeCopy && s.Copy != nil && s.Copy.Online) ||
		(s.Type == SessionTypeBackup && s.Backup != nil && s.Backup.Online)
}

func (s *SessionSpec) SetWorkload(workload WorkloadSpec) error {
	switch s.Type {
	case SessionTypeMigratePod:
		if s.MigratePod == nil {
			s.MigratePod = &MigratePodSessionSpec{}
		}

		s.MigratePod.Workload = deepCopyWorkloadSpec(workload)
	default:
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("workflow type %s has no workload payload", s.Type),
		)
	}

	return nil
}

func NewSession(id string, spec SessionSpec, now time.Time) *Session {
	ownedSpec := SessionSpec{}
	spec.DeepCopyInto(&ownedSpec)

	t := metav1.NewTime(now.UTC())

	volumeStatus := make([]VolumeStatus, 0, len(ownedSpec.Volumes))
	for _, volume := range ownedSpec.Volumes {
		volumeStatus = append(volumeStatus, VolumeStatus{SourcePVCName: volume.SourcePVC.Name})
	}

	return &Session{
		APIVersion: SessionAPIVersion,
		Kind:       SessionKind,
		ID:         id,
		Generation: 1,
		Spec:       ownedSpec,
		Status: SessionStatus{
			Phase:     PhasePlanned,
			StartedAt: t,
			UpdatedAt: t,
			Volumes:   volumeStatus,
			History: []HistoryEntry{
				{
					Phase:   PhasePlanned,
					Time:    t,
					Message: fmt.Sprintf("%s session planned", ownedSpec.Operation()),
				},
			},
		},
	}
}

func (s *Session) Transition(next Phase, message string, now time.Time) error {
	if s.Status.Phase == next {
		return nil
	}

	backupTransition := (s.Spec.Type == SessionTypeBackup || s.Spec.Type == SessionTypeRestore) &&
		((s.Status.Phase == PhasePlanned && next == PhaseWarmCopying) ||
			(s.Status.Phase == PhaseWarmCopied && next == PhaseCompleted))
	if !backupTransition && !slices.Contains(transitionPolicy[s.Status.Phase], next) {
		return NewError(
			ErrorConflict,
			"transition",
			fmt.Sprintf("phase %s cannot transition to %s", s.Status.Phase, next),
		)
	}

	t := metav1.NewTime(now.UTC())

	if next == PhaseFailed ||
		((next == PhaseAborting || next == PhaseRollingBack) && s.Status.Phase != PhaseFailed) {
		s.Status.ResumeFrom = s.Status.Phase
	}

	s.Status.Phase = next
	if next != PhaseFailed {
		s.Status.FailureReason = ""
		s.Status.ErrorCategory = ""
	}

	message = BoundWorkflowMessage(message)
	s.Status.Message = message
	s.Status.UpdatedAt = t

	s.Status.History = append(
		s.Status.History,
		HistoryEntry{Phase: next, Time: t, Message: message},
	)
	trimWorkflowHistory(&s.Status)

	if next == PhaseCompleted || next == PhaseAborted || next == PhaseRolledBack {
		s.Status.CompletedAt = &t
	} else {
		s.Status.CompletedAt = nil
	}

	return nil
}

// Reactivate moves a failed workflow back to the checkpoint recorded in
// ResumeFrom. It is intentionally separate from Transition because a failed
// session is a terminal checkpoint and must only be reopened by an explicit
// user/controller resume action.
func (s *Session) Reactivate(message string, now time.Time) error {
	if s == nil || s.Status.Phase != PhaseFailed {
		return NewError(ErrorPrecondition, "reactivate", "only failed sessions can be reactivated")
	}

	if s.Status.ResumeFrom == "" {
		return NewError(ErrorPrecondition, "reactivate", "failed session has no resume checkpoint")
	}

	t := metav1.NewTime(now.UTC())
	s.Status.Phase = s.Status.ResumeFrom
	s.Status.FailureReason = ""
	s.Status.ErrorCategory = ""
	message = BoundWorkflowMessage(message)
	s.Status.Message = message
	s.Status.UpdatedAt = t
	s.Status.CompletedAt = nil
	s.Status.History = append(s.Status.History, HistoryEntry{
		Phase:   s.Status.Phase,
		Time:    t,
		Message: message,
	})
	trimWorkflowHistory(&s.Status)

	return nil
}

func (s *Session) SetCondition(condition Condition) {
	condition.Type = BoundWorkflowConditionType(condition.Type)
	condition.Reason = BoundWorkflowReason(condition.Reason)

	condition.Message = BoundWorkflowMessage(condition.Message)
	for i := range s.Status.Conditions {
		if s.Status.Conditions[i].Type == condition.Type {
			s.Status.Conditions[i] = condition
			trimWorkflowConditions(&s.Status)
			return
		}
	}

	s.Status.Conditions = append(s.Status.Conditions, condition)
	trimWorkflowConditions(&s.Status)
}

func trimWorkflowConditions(status *SessionStatus) {
	if status == nil || len(status.Conditions) <= MaxWorkflowConditions {
		return
	}

	status.Conditions = slices.Clone(
		status.Conditions[len(status.Conditions)-MaxWorkflowConditions:],
	)
}

func trimWorkflowHistory(status *SessionStatus) {
	if status == nil || len(status.History) <= MaxWorkflowHistoryEntries {
		return
	}

	status.History = slices.Clone(status.History[len(status.History)-MaxWorkflowHistoryEntries:])
}

// BoundWorkflowMessage keeps controller-generated status text within the CRD
// schema while preserving valid UTF-8 at the byte boundary.
func BoundWorkflowMessage(message string) string {
	return boundWorkflowText(message, MaxWorkflowMessageBytes)
}

// BoundWorkflowConditionType bounds the stable condition type identifier.
func BoundWorkflowConditionType(value string) string {
	return boundWorkflowText(value, MaxWorkflowConditionTypeBytes)
}

// BoundWorkflowReason bounds the condition reason identifier.
func BoundWorkflowReason(value string) string {
	return boundWorkflowText(value, MaxWorkflowReasonBytes)
}

func boundWorkflowText(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}

	bounded := value[:maxBytes]
	for !utf8.ValidString(bounded) {
		bounded = bounded[:len(bounded)-1]
	}

	return bounded
}

func (s *Session) VolumeStatus(name string) (*VolumeStatus, error) {
	for i := range s.Status.Volumes {
		if s.Status.Volumes[i].SourcePVCName == name {
			return &s.Status.Volumes[i], nil
		}
	}

	return nil, NewError(ErrorInternal, "session", "volume status missing for "+name)
}

func (s *Session) Validate() error {
	if err := validateSessionHeader(s); err != nil {
		return err
	}

	if err := validateSessionMode(s); err != nil {
		return err
	}

	if err := validateSessionStatus(s); err != nil {
		return err
	}

	if s.Spec.Type == SessionTypeBackup {
		if err := validateBackupSession(s); err != nil {
			return err
		}
	} else if s.Spec.Type == SessionTypeRestore {
		if err := validateRestoreSession(s); err != nil {
			return err
		}
	} else if err := validateSessionVolumes(s); err != nil {
		return err
	}

	if s.Spec.Type == SessionTypeMigratePod {
		if err := validateWorkloadIdentity(s.Spec.Workload()); err != nil {
			return err
		}
	}

	return validateSharedMounts(s.Status.OpenEBSLVMSharedMounts)
}

func validateSessionHeader(s *Session) error {
	if s.APIVersion != SessionAPIVersion || s.Kind != SessionKind {
		return NewError(ErrorValidation, "session", "unsupported session schema")
	}

	if s.ID == "" || s.Spec.SourceNamespace == "" || s.Spec.SessionNamespace == "" {
		return NewError(ErrorValidation, "session", "session identity and namespaces are required")
	}

	if s.Spec.Type == "" {
		return NewError(ErrorValidation, "session", "session type is required")
	}

	if concreteSessionPayloadCount(s.Spec) != 1 {
		return NewError(
			ErrorValidation,
			"session",
			"exactly one concrete session payload is required",
		)
	}

	if !sessionPayloadMatchesType(s.Spec) {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("payload does not match session type %s", s.Spec.Type),
		)
	}

	return nil
}

func concreteSessionPayloadCount(spec SessionSpec) int {
	payloads := []bool{
		spec.Reserve != nil,
		spec.Migrate != nil,
		spec.MigratePod != nil,
		spec.Copy != nil,
		spec.Backup != nil,
		spec.Restore != nil,
		spec.Rename != nil,
		spec.Move != nil,
	}

	count := 0
	for _, present := range payloads {
		if present {
			count++
		}
	}

	return count
}

func sessionPayloadMatchesType(spec SessionSpec) bool {
	switch spec.Type {
	case SessionTypeReserve:
		return spec.Reserve != nil
	case SessionTypeMigrate:
		return spec.Migrate != nil
	case SessionTypeMigratePod:
		return spec.MigratePod != nil
	case SessionTypeCopy:
		return spec.Copy != nil
	case SessionTypeBackup:
		return spec.Backup != nil
	case SessionTypeRestore:
		return spec.Restore != nil
	case SessionTypeRename:
		return spec.Rename != nil
	case SessionTypeMove:
		return spec.Move != nil
	default:
		return false
	}
}

func validateSessionMode(s *Session) error {
	if s.Spec.Online() && s.Spec.Type != SessionTypeCopy && s.Spec.Type != SessionTypeBackup {
		return NewError(
			ErrorValidation,
			"session",
			"online mode is only valid for copy or backup sessions",
		)
	}

	if s.Spec.Type == SessionTypeMigratePod &&
		s.Spec.SourceNamespace != s.Spec.DestinationNamespace {
		return NewError(
			ErrorValidation,
			"session",
			"pod migration must keep workload and PVC identities in the source namespace",
		)
	}

	if s.Spec.Type == SessionTypeRename && s.Spec.SourceNamespace != s.Spec.DestinationNamespace {
		return NewError(
			ErrorValidation,
			"session",
			"rename sessions must stay within one namespace",
		)
	}

	return nil
}

func validateSessionStatus(s *Session) error {
	if s.Spec.Type != SessionTypeBackup && s.Spec.Type != SessionTypeRestore &&
		len(s.Spec.Volumes) == 0 {
		return NewError(ErrorValidation, "session", "session contains no volumes")
	}

	if len(s.Spec.Volumes) != len(s.Status.Volumes) {
		return NewError(ErrorValidation, "session", "volume specification and status counts differ")
	}

	if _, known := transitionPolicy[s.Status.Phase]; !known && s.Status.Phase != PhaseAborted {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("unsupported session phase %q", s.Status.Phase),
		)
	}

	if s.Status.FailureReason != "" {
		if s.Status.FailureReason != FailureDestinationCapacityExhausted {
			return NewError(
				ErrorValidation,
				"session",
				fmt.Sprintf("unsupported failure reason %q", s.Status.FailureReason),
			)
		}

		if s.Status.Phase != PhaseFailed {
			return NewError(
				ErrorValidation,
				"session",
				"failure reason is only valid in phase Failed",
			)
		}
	}

	return nil
}

func validateBackupSession(s *Session) error {
	payload := s.Spec.Backup
	if payload == nil {
		return NewError(ErrorValidation, "backup session", "backup payload is required")
	}

	if payload.SourcePVC.Namespace == "" || payload.SourcePVC.Name == "" ||
		payload.SourcePVC.UID == "" {
		return NewError(
			ErrorValidation,
			"backup session",
			"source PVC namespace, name, and UID are required",
		)
	}

	if payload.SourcePV.Name == "" || payload.SourcePV.UID == "" {
		return NewError(ErrorValidation, "backup session", "source PV name and UID are required")
	}

	if payload.Name == "" || payload.BackupRepository == "" &&
		(payload.Backend != BackupBackendS3 || payload.Bucket == "") {
		return NewError(
			ErrorValidation,
			"backup session",
			"name and either backupRepository or an inline s3 backend with bucket are required",
		)
	}

	if payload.BackupRepository != "" && payload.Backend != "" {
		return NewError(
			ErrorValidation,
			"backup session",
			"backend is selected by BackupRepository and must not be set on controller workflows",
		)
	}

	if payload.CredentialsSecret.Name != "" &&
		payload.CredentialsSecret.Namespace != s.Spec.SessionNamespace {
		return NewError(
			ErrorValidation,
			"backup session",
			"credentials Secret must be in the session namespace",
		)
	}

	if len(s.Spec.Volumes) != 0 || len(s.Status.Volumes) != 0 {
		return NewError(
			ErrorValidation,
			"backup session",
			"backup sessions cannot contain migration volumes",
		)
	}

	return nil
}

func validateRestoreSession(s *Session) error {
	payload := s.Spec.Restore
	if payload == nil {
		return NewError(ErrorValidation, "restore session", "restore payload is required")
	}

	if payload.DestinationPVC.Namespace == "" || payload.DestinationPVC.Name == "" {
		return NewError(
			ErrorValidation,
			"restore session",
			"destination PVC namespace and name are required",
		)
	}

	if (payload.DestinationPV.Name == "") != (payload.DestinationPV.UID == "") {
		return NewError(
			ErrorValidation,
			"restore session",
			"destination PV name and UID must be recorded together",
		)
	}

	if payload.Name == "" || payload.BackupRepository == "" &&
		(payload.Backend != BackupBackendS3 || payload.Bucket == "") {
		return NewError(
			ErrorValidation,
			"restore session",
			"name and either backupRepository or an inline s3 backend with bucket are required",
		)
	}

	if payload.BackupRepository != "" && payload.Backend != "" {
		return NewError(
			ErrorValidation,
			"restore session",
			"backend is selected by BackupRepository and must not be set on controller workflows",
		)
	}

	if payload.CredentialsSecret.Name != "" &&
		payload.CredentialsSecret.Namespace != s.Spec.SessionNamespace {
		return NewError(
			ErrorValidation,
			"restore session",
			"credentials Secret must be in the session namespace",
		)
	}

	if len(s.Spec.Volumes) != 0 || len(s.Status.Volumes) != 0 {
		return NewError(
			ErrorValidation,
			"restore session",
			"restore sessions cannot contain migration volumes",
		)
	}

	return nil
}

func validateSessionVolumes(s *Session) error {
	seen := make(map[string]struct{}, len(s.Spec.Volumes))
	for index := range s.Spec.Volumes {
		volume := &s.Spec.Volumes[index]

		name := volume.SourcePVC.Name
		if err := validateSessionVolume(s, index, volume); err != nil {
			return err
		}

		if _, exists := seen[name]; exists {
			return NewError(
				ErrorValidation,
				"session",
				fmt.Sprintf("duplicate source PVC %q", name),
			)
		}

		seen[name] = struct{}{}
		if s.Status.Volumes[index].SourcePVCName != name {
			return NewError(
				ErrorValidation,
				"session",
				fmt.Sprintf("volume status %d does not match source PVC %q", index, name),
			)
		}
	}

	return nil
}

func validateSessionVolume(s *Session, index int, volume *VolumeSpec) error {
	name := volume.SourcePVC.Name
	if volume.SourcePVC.Namespace == "" || name == "" || volume.SourcePVC.UID == "" {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("source PVC namespace, name, and UID are required for volume %d", index),
		)
	}

	if volume.SourcePV.Name == "" || volume.SourcePV.UID == "" {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("source PV name and UID are required for volume %d", index),
		)
	}

	if volume.DestinationPVC.Namespace == "" || volume.DestinationPVC.Name == "" {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("destination PVC namespace and name are required for volume %d", index),
		)
	}

	if volume.ConcurrentConsumers < 0 {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("concurrent consumers cannot be negative for volume %d", index),
		)
	}

	if err := ValidateTransferScope(volume.TransferScope); err != nil {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("invalid transfer scope for source PVC %s: %v", name, err),
		)
	}

	if volume.TransferScope != nil &&
		(s.Spec.Type == SessionTypeRename || s.Spec.Type == SessionTypeMove) {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("%s sessions cannot contain transfer paths", s.Spec.Type),
		)
	}

	if (volume.DestinationPV.Name == "") != (volume.DestinationPV.UID == "") {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf(
				"destination PV name and UID must be recorded together for volume %d",
				index,
			),
		)
	}

	status := &s.Status.Volumes[index]
	if status.Reserved &&
		(volume.DestinationPVC.UID == "" || volume.DestinationPV.Name == "" || volume.DestinationPV.UID == "") {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf(
				"reserved destination PVC and PV identities are incomplete for volume %d",
				index,
			),
		)
	}

	active := status.Activation.ActivePVC
	if (active.Name != "" || active.Namespace != "" || active.UID != "") &&
		(active.Namespace == "" || active.Name == "" || active.UID == "") {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf(
				"active PVC namespace, name, and UID must be recorded together for volume %d",
				index,
			),
		)
	}

	return nil
}

func validateSharedMounts(mounts []OpenEBSLVMSharedMount) error {
	for index, mount := range mounts {
		if mount.SourcePV.Name == "" || mount.SourcePV.UID == "" {
			return NewError(
				ErrorValidation,
				"session",
				fmt.Sprintf("OpenEBS shared mount %d has an incomplete source PV identity", index),
			)
		}

		if mount.LVMVolume.Namespace == "" || mount.LVMVolume.Name == "" ||
			mount.LVMVolume.UID == "" {
			return NewError(
				ErrorValidation,
				"session",
				fmt.Sprintf("OpenEBS shared mount %d has an incomplete LVMVolume identity", index),
			)
		}
	}

	return nil
}

func validateWorkloadIdentity(workload WorkloadSpec) error {
	if len(workload.OriginalObject) > MaxOriginalPodSnapshotBytes {
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf(
				"workload original object exceeds the %d-byte limit",
				MaxOriginalPodSnapshotBytes,
			),
		)
	}

	if workload.Adapter == WorkloadNone {
		return nil
	}

	if err := validateWorkloadObjectReference(workload.Pod, "workload Pod"); err != nil {
		return err
	}

	if workload.Adapter == WorkloadStandalone {
		return nil
	}

	if err := validateWorkloadObjectReference(
		workload.Controller,
		"workload controller",
	); err != nil {
		return err
	}

	for index, ref := range workload.AffectedPods {
		if err := validateWorkloadObjectReference(
			ref,
			fmt.Sprintf("affected Pod %d", index),
		); err != nil {
			return err
		}
	}

	switch workload.Adapter {
	case WorkloadDeployment:
		return validateDeploymentWorkload(workload)
	case WorkloadStatefulSet, WorkloadVictoriaLogs:
		return nil
	case WorkloadKubeBlocks:
		if workload.KubeBlocks == nil || workload.KubeBlocks.Cluster == "" ||
			workload.KubeBlocks.ClusterUID == "" {
			return NewError(
				ErrorValidation,
				"session",
				"KubeBlocks workload Cluster name and UID are required",
			)
		}

	case WorkloadVMCluster:
		if workload.VMCluster == nil || workload.VMCluster.Name == "" ||
			workload.VMCluster.UID == "" {
			return NewError(
				ErrorValidation,
				"session",
				"VMCluster workload name and UID are required",
			)
		}
	case WorkloadGrafana:
		if workload.Grafana == nil || workload.Grafana.Name == "" || workload.Grafana.UID == "" {
			return NewError(
				ErrorValidation,
				"session",
				"Grafana workload name and UID are required",
			)
		}
	default:
		return NewError(
			ErrorValidation,
			"session",
			fmt.Sprintf("unsupported workload adapter %q", workload.Adapter),
		)
	}

	return nil
}

func validateDeploymentWorkload(workload WorkloadSpec) error {
	if workload.OriginalReplicas == nil || *workload.OriginalReplicas <= 0 {
		return NewError(
			ErrorValidation,
			"session",
			"Deployment workload original replicas must be positive",
		)
	}

	if len(workload.AffectedPods) == 0 {
		return NewError(
			ErrorValidation,
			"session",
			"Deployment workload must record at least one affected Pod",
		)
	}

	for _, ref := range workload.AffectedPods {
		if ref.UID == workload.Pod.UID {
			return nil
		}
	}

	return NewError(
		ErrorValidation,
		"session",
		"Deployment workload selected Pod is outside the affected set",
	)
}

func validateWorkloadObjectReference(ref ObjectReference, description string) error {
	if ref.Namespace == "" || ref.Name == "" || ref.UID == "" {
		return NewError(
			ErrorValidation,
			"session",
			description+" namespace, name, and UID are required",
		)
	}

	return nil
}
