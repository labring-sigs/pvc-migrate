package domain

import (
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	SessionAPIGroup   = "migrate.sealos.io"
	SessionAPIVersion = SessionAPIGroup + "/v1alpha1"
	// Workflow API specs and statuses are owned by api/v1alpha1; these names
	// form the shared discovery, storage, watch, and CLI routing contract.
	MigrationResource          = "migrations"
	PodMigrationResource       = "podmigrations"
	ReservationResource        = "reservations"
	CopyResource               = "copies"
	BackupResource             = "backups"
	RestoreResource            = "restores"
	RenameResource             = "renames"
	ClusterMigrationResource   = "clustermigrations"
	ClusterReservationResource = "clusterreservations"
	ClusterCopyResource        = "clustercopies"
	MoveResource               = "moves"
	BackupRepositoryResource   = "backuprepositories"
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

type Phase = v1alpha1.WorkflowPhase

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

// SessionType identifies the durable workflow kind a CLI command or workflow
// family operates on.
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
	ControllerKindMigration          ControllerKind = "Migration"
	ControllerKindPodMigration       ControllerKind = "PodMigration"
	ControllerKindReservation        ControllerKind = "Reservation"
	ControllerKindCopy               ControllerKind = "Copy"
	ControllerKindBackup             ControllerKind = "Backup"
	ControllerKindRestore            ControllerKind = "Restore"
	ControllerKindRename             ControllerKind = "Rename"
	ControllerKindClusterMigration   ControllerKind = "ClusterMigration"
	ControllerKindClusterReservation ControllerKind = "ClusterReservation"
	ControllerKindClusterCopy        ControllerKind = "ClusterCopy"
	ControllerKindMove               ControllerKind = "Move"
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
// the routing metadata cannot be mutated across concurrent users.
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
			Type:     SessionTypeMigratePod,
			Kind:     ControllerKindPodMigration,
			Resource: PodMigrationResource,
			Singular: "podmigration",
			// Pod migration is a same-namespace operation by design: a
			// workload cannot be recreated in another namespace, so there is
			// no cluster-scoped form.
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

const (
	StrategyAuto         = AutoValue
	StrategyMount        = "mount"
	StrategyClusterIP    = "clusterip"
	StrategyLoadBalancer = "loadbalancer"
	StrategyNodePort     = "nodeport"
	StrategyLocal        = "local"
)

const (
	FailureDestinationCapacityExhausted = "DestinationCapacityExhausted"
)

// Backup backend identifiers. S3 is the only supported repository backend.
const (
	BackupBackendS3 = "s3"
)

// TransitionRepository records the repository-backed backup/restore phase
// machine. These workflows run warm-copy style phases instead of the full
// migration lifecycle.
func TransitionRepository(
	lifecycle *v1alpha1.WorkflowStatus,
	next Phase,
	message string,
	now time.Time,
) error {
	if lifecycle.Phase == next {
		return nil
	}

	policy := map[Phase][]Phase{
		PhasePlanned:     {PhaseWarmCopying, PhaseAborting, PhaseFailed},
		PhaseWarmCopying: {PhaseWarmCopied, PhaseAborting, PhaseFailed},
		PhaseWarmCopied:  {PhaseCompleted, PhaseAborting, PhaseFailed},
		PhaseAborting:    {PhaseAborted, PhaseFailed},
		PhaseFailed:      {PhaseWarmCopying, PhaseAborting},
	}
	if !slices.Contains(policy[lifecycle.Phase], next) {
		return NewError(
			ErrorConflict,
			"transition",
			fmt.Sprintf("repository phase %s cannot transition to %s", lifecycle.Phase, next),
		)
	}

	RecordWorkflowTransition(lifecycle, next, message, now)

	return nil
}

// ReactivateWorkflow reopens a failed workflow at its recorded resume
// checkpoint. The explicit resume command performs admission checks.
func ReactivateWorkflow(status *v1alpha1.WorkflowStatus, message string, now time.Time) error {
	if status == nil || status.Phase != PhaseFailed || status.ResumeFrom == "" {
		return NewError(
			ErrorPrecondition,
			"reactivate",
			"a failed workflow with a resume checkpoint is required",
		)
	}

	t := metav1.NewTime(now.UTC())
	status.Phase = status.ResumeFrom
	status.FailureReason = ""
	status.ErrorCategory = ""
	message = BoundWorkflowMessage(message)
	status.Message = message
	status.UpdatedAt = t
	status.CompletedAt = nil
	status.History = append(status.History, v1alpha1.WorkflowHistoryEntry{
		Phase:   status.Phase,
		Time:    t,
		Message: message,
	})
	trimWorkflowHistory(status)

	return nil
}

func SetWorkflowCondition(status *v1alpha1.WorkflowStatus, condition v1alpha1.WorkflowCondition) {
	condition.Type = BoundWorkflowConditionType(condition.Type)
	condition.Reason = BoundWorkflowReason(condition.Reason)

	condition.Message = BoundWorkflowMessage(condition.Message)
	for i := range status.Conditions {
		if status.Conditions[i].Type == condition.Type {
			status.Conditions[i] = condition
			trimWorkflowConditions(status)
			return
		}
	}

	status.Conditions = append(status.Conditions, condition)
	trimWorkflowConditions(status)
}

func trimWorkflowConditions(status *v1alpha1.WorkflowStatus) {
	if status == nil || len(status.Conditions) <= MaxWorkflowConditions {
		return
	}

	status.Conditions = slices.Clone(
		status.Conditions[len(status.Conditions)-MaxWorkflowConditions:],
	)
}

func trimWorkflowHistory(status *v1alpha1.WorkflowStatus) {
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
