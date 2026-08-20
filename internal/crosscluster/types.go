package crosscluster

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	APIVersion     = "pvc-migrate.io/v1alpha1"
	Kind           = "CrossClusterCopySession"
	ManagedByLabel = "pvc-migrate.io/managed-by"
	ManagedBy      = "pvc-migrate-cross-cluster"
	SessionKey     = "pvc-migrate.io/cross-cluster-session"
)

type Phase string

const (
	PhasePlanned      Phase = "Planned"
	PhaseReserving    Phase = "Reserving"
	PhaseReserved     Phase = "Reserved"
	PhaseTransferring Phase = "Transferring"
	PhaseCompleted    Phase = "Completed"
	PhaseFailed       Phase = "Failed"
	PhaseCleaning     Phase = "Cleaning"
	PhaseCleaned      Phase = "Cleaned"
)

type ClusterResourceRef struct {
	ClusterID       string    `json:"clusterID"                 yaml:"clusterID"`
	APIVersion      string    `json:"apiVersion,omitempty"      yaml:"apiVersion,omitempty"`
	Kind            string    `json:"kind"                      yaml:"kind"`
	Namespace       string    `json:"namespace,omitempty"       yaml:"namespace,omitempty"`
	Name            string    `json:"name"                      yaml:"name"`
	UID             types.UID `json:"uid,omitempty"             yaml:"uid,omitempty"`
	ResourceVersion string    `json:"resourceVersion,omitempty" yaml:"resourceVersion,omitempty"`
}

type SourceVolumeSpec struct {
	PVC      ClusterResourceRef `json:"pvc"      yaml:"pvc"`
	PV       ClusterResourceRef `json:"pv"       yaml:"pv"`
	Capacity string             `json:"capacity" yaml:"capacity"`
}

type DestinationVolumeSpec struct {
	PVC          ClusterResourceRef                  `json:"pvc"          yaml:"pvc"`
	PV           ClusterResourceRef                  `json:"pv,omitempty" yaml:"pv,omitempty"`
	Capacity     string                              `json:"capacity"     yaml:"capacity"`
	StorageClass ClusterResourceRef                  `json:"storageClass" yaml:"storageClass"`
	AccessModes  []corev1.PersistentVolumeAccessMode `json:"accessModes"  yaml:"accessModes"`
	VolumeMode   corev1.PersistentVolumeMode         `json:"volumeMode"   yaml:"volumeMode"`
}

type TransferSpec struct {
	SourcePath      string `json:"sourcePath"      yaml:"sourcePath"`
	DestinationPath string `json:"destinationPath" yaml:"destinationPath"`
}

type VolumeSpec struct {
	Source      SourceVolumeSpec      `json:"source"      yaml:"source"`
	Destination DestinationVolumeSpec `json:"destination" yaml:"destination"`
	Transfer    TransferSpec          `json:"transfer"    yaml:"transfer"`
}

type Spec struct {
	SessionNamespace     string               `json:"sessionNamespace"               yaml:"sessionNamespace"`
	SourceCluster        kube.ClusterIdentity `json:"sourceCluster"                  yaml:"sourceCluster"`
	DestinationCluster   kube.ClusterIdentity `json:"destinationCluster"             yaml:"destinationCluster"`
	SourceNamespace      string               `json:"sourceNamespace"                yaml:"sourceNamespace"`
	DestinationNamespace string               `json:"destinationNamespace"           yaml:"destinationNamespace"`
	ToolImage            string               `json:"toolImage"                      yaml:"toolImage"`
	Strategies           []string             `json:"strategies"                     yaml:"strategies"`
	Online               bool                 `json:"online,omitempty"               yaml:"online,omitempty"`
	VerifyChecksum       bool                 `json:"verifyChecksum"                 yaml:"verifyChecksum"`
	DeleteExtraneous     bool                 `json:"deleteExtraneous"               yaml:"deleteExtraneous"`
	AllowVolumeShrink    bool                 `json:"allowVolumeShrink,omitempty"    yaml:"allowVolumeShrink,omitempty"`
	SkipSourceUsageCheck bool                 `json:"skipSourceUsageCheck,omitempty" yaml:"skipSourceUsageCheck,omitempty"`
	TargetNode           string               `json:"targetNode,omitempty"           yaml:"targetNode,omitempty"`
	Volumes              []VolumeSpec         `json:"volumes"                        yaml:"volumes"`
}

type ReservationStatus struct {
	PVC         ClusterResourceRef `json:"pvc,omitempty"         yaml:"pvc,omitempty"`
	PV          ClusterResourceRef `json:"pv,omitempty"          yaml:"pv,omitempty"`
	ConsumerPod ClusterResourceRef `json:"consumerPod,omitempty" yaml:"consumerPod,omitempty"`
	CompletedAt *metav1.Time       `json:"completedAt,omitempty" yaml:"completedAt,omitempty"`
}

type TransferStatus struct {
	Attempts    int          `json:"attempts,omitempty"    yaml:"attempts,omitempty"`
	CompletedAt *metav1.Time `json:"completedAt,omitempty" yaml:"completedAt,omitempty"`
	LastError   string       `json:"lastError,omitempty"   yaml:"lastError,omitempty"`
	BytesCopied int64        `json:"bytesCopied,omitempty" yaml:"bytesCopied,omitempty"`
}

type VolumeStatus struct {
	SourcePVCName string            `json:"sourcePVCName" yaml:"sourcePVCName"`
	Reservation   ReservationStatus `json:"reservation"   yaml:"reservation"`
	Transfer      TransferStatus    `json:"transfer"      yaml:"transfer"`
}

type Status struct {
	Phase       Phase          `json:"phase"                 yaml:"phase"`
	StartedAt   metav1.Time    `json:"startedAt"             yaml:"startedAt"`
	UpdatedAt   metav1.Time    `json:"updatedAt"             yaml:"updatedAt"`
	CompletedAt *metav1.Time   `json:"completedAt,omitempty" yaml:"completedAt,omitempty"`
	Message     string         `json:"message,omitempty"     yaml:"message,omitempty"`
	Volumes     []VolumeStatus `json:"volumes"               yaml:"volumes"`
}

type Session struct {
	APIVersion      string `json:"apiVersion" yaml:"apiVersion"`
	Kind            string `json:"kind"       yaml:"kind"`
	ID              string `json:"id"         yaml:"id"`
	ResourceVersion string `json:"-"          yaml:"-"`
	Spec            Spec   `json:"spec"       yaml:"spec"`
	Status          Status `json:"status"     yaml:"status"`
}

type VolumePlan struct {
	SourceNamespace      string    `json:"sourceNamespace"      yaml:"sourceNamespace"`
	SourcePVC            string    `json:"sourcePVC"            yaml:"sourcePVC"`
	SourcePVCUID         types.UID `json:"sourcePVCUID"         yaml:"sourcePVCUID"`
	SourcePVUID          types.UID `json:"sourcePVUID"          yaml:"sourcePVUID"`
	DestinationNamespace string    `json:"destinationNamespace" yaml:"destinationNamespace"`
	DestinationPVC       string    `json:"destinationPVC"       yaml:"destinationPVC"`
	SourceCapacity       string    `json:"sourceCapacity"       yaml:"sourceCapacity"`
	Capacity             string    `json:"capacity"             yaml:"capacity"`
	StorageClass         string    `json:"storageClass"         yaml:"storageClass"`
	StorageClassUID      types.UID `json:"storageClassUID"      yaml:"storageClassUID"`
	SourcePath           string    `json:"sourcePath"           yaml:"sourcePath"`
	DestinationPath      string    `json:"destinationPath"      yaml:"destinationPath"`
}

type Check struct {
	Name    string `json:"name"    yaml:"name"`
	Passed  bool   `json:"passed"  yaml:"passed"`
	Message string `json:"message" yaml:"message"`
}

type Plan struct {
	APIVersion           string               `json:"apiVersion"           yaml:"apiVersion"`
	Kind                 string               `json:"kind"                 yaml:"kind"`
	SessionID            string               `json:"sessionID"            yaml:"sessionID"`
	SourceCluster        kube.ClusterIdentity `json:"sourceCluster"        yaml:"sourceCluster"`
	DestinationCluster   kube.ClusterIdentity `json:"destinationCluster"   yaml:"destinationCluster"`
	SourceNamespace      string               `json:"sourceNamespace"      yaml:"sourceNamespace"`
	DestinationNamespace string               `json:"destinationNamespace" yaml:"destinationNamespace"`
	Strategies           []string             `json:"strategies"           yaml:"strategies"`
	TargetNode           string               `json:"targetNode"           yaml:"targetNode"`
	Volumes              []VolumePlan         `json:"volumes"              yaml:"volumes"`
	Checks               []Check              `json:"checks"               yaml:"checks"`
	Ready                bool                 `json:"ready"                yaml:"ready"`
}

func (p *Plan) AddCheck(name string, passed bool, message string) {
	p.Checks = append(p.Checks, Check{Name: name, Passed: passed, Message: message})
	if !passed {
		p.Ready = false
	}
}

func (s *Session) Validate() error {
	if err := validateCrossClusterHeader(s); err != nil {
		return err
	}

	if err := validateCrossClusterStatus(s); err != nil {
		return err
	}

	return validateCrossClusterVolumes(s)
}

func validateCrossClusterHeader(s *Session) error {
	if s == nil || s.APIVersion != APIVersion || s.Kind != Kind || ValidateSessionID(s.ID) != nil {
		return errors.New("invalid cross-cluster session identity")
	}

	if s.Spec.SessionNamespace == "" || s.Spec.SourceNamespace == "" ||
		s.Spec.DestinationNamespace == "" ||
		len(s.Spec.Volumes) == 0 ||
		s.Spec.SourceCluster.ID == "" ||
		s.Spec.DestinationCluster.ID == "" ||
		s.Spec.SourceCluster.ID == s.Spec.DestinationCluster.ID {
		return errors.New("cross-cluster session has incomplete spec")
	}

	return nil
}

func validateCrossClusterStatus(s *Session) error {
	switch s.Status.Phase {
	case PhasePlanned,
		PhaseReserving,
		PhaseReserved,
		PhaseTransferring,
		PhaseCompleted,
		PhaseFailed,
		PhaseCleaning,
		PhaseCleaned:
	default:
		return fmt.Errorf("cross-cluster session has unknown phase %q", s.Status.Phase)
	}

	if len(s.Spec.Volumes) != len(s.Status.Volumes) {
		return errors.New("cross-cluster session volume status is misaligned")
	}

	return nil
}

func validateCrossClusterVolumes(s *Session) error {
	seenSources := make(map[string]struct{}, len(s.Spec.Volumes))

	seenDestinations := make(map[string]struct{}, len(s.Spec.Volumes))
	for i, v := range s.Spec.Volumes {
		if err := validateCrossClusterVolume(s, i, v); err != nil {
			return err
		}

		if s.Status.Volumes[i].SourcePVCName != v.Source.PVC.Name {
			return fmt.Errorf("cross-cluster volume %d status mismatch", i)
		}

		if _, exists := seenSources[v.Source.PVC.Name]; exists {
			return fmt.Errorf(
				"cross-cluster source PVC %s is referenced more than once",
				v.Source.PVC.Name,
			)
		}

		seenSources[v.Source.PVC.Name] = struct{}{}

		destinationKey := v.Destination.PVC.Namespace + "/" + v.Destination.PVC.Name
		if _, exists := seenDestinations[destinationKey]; exists {
			return fmt.Errorf(
				"cross-cluster destination PVC %s is referenced more than once",
				destinationKey,
			)
		}

		seenDestinations[destinationKey] = struct{}{}
	}

	return nil
}

func validateCrossClusterVolume(s *Session, i int, v VolumeSpec) error {
	if crossClusterVolumeIdentityIncomplete(s, v) {
		return fmt.Errorf("cross-cluster volume %d has incomplete identities", i)
	}

	if err := validateCrossClusterCapacity(s, i, v); err != nil {
		return err
	}

	if err := validateCrossClusterStorage(i, v); err != nil {
		return err
	}

	if err := validateCrossClusterPaths(i, v); err != nil {
		return err
	}

	if (v.Destination.PV.Name == "") != (v.Destination.PV.UID == "") ||
		(v.Destination.PV.Name != "" && v.Destination.PV.ClusterID != s.Spec.DestinationCluster.ID) {
		return fmt.Errorf("cross-cluster volume %d destination PV identity is incomplete", i)
	}

	return nil
}

func crossClusterVolumeIdentityIncomplete(s *Session, v VolumeSpec) bool {
	return v.Source.PVC.ClusterID != s.Spec.SourceCluster.ID ||
		v.Source.PV.ClusterID != s.Spec.SourceCluster.ID ||
		v.Destination.PVC.ClusterID != s.Spec.DestinationCluster.ID ||
		v.Destination.StorageClass.ClusterID != s.Spec.DestinationCluster.ID ||
		v.Source.PVC.Name == "" ||
		v.Source.PVC.Namespace != s.Spec.SourceNamespace ||
		v.Source.PVC.UID == "" ||
		v.Source.PV.Name == "" ||
		v.Source.PV.UID == "" ||
		v.Destination.PVC.Name == "" ||
		v.Destination.PVC.Namespace != s.Spec.DestinationNamespace ||
		v.Destination.StorageClass.Name == "" ||
		v.Destination.StorageClass.UID == "" ||
		v.Destination.StorageClass.Namespace != ""
}

func validateCrossClusterCapacity(s *Session, i int, v VolumeSpec) error {
	sourceCapacity, err := resource.ParseQuantity(v.Source.Capacity)
	if err != nil || sourceCapacity.Sign() <= 0 {
		return fmt.Errorf("cross-cluster volume %d source capacity is invalid", i)
	}

	destinationCapacity, err := resource.ParseQuantity(v.Destination.Capacity)
	if err != nil || destinationCapacity.Sign() <= 0 {
		return fmt.Errorf("cross-cluster volume %d destination capacity is invalid", i)
	}

	if destinationCapacity.Cmp(sourceCapacity) < 0 &&
		(!s.Spec.AllowVolumeShrink || !s.Spec.SkipSourceUsageCheck) {
		return fmt.Errorf("cross-cluster volume %d shrink approval is incomplete", i)
	}

	return nil
}

func validateCrossClusterStorage(i int, v VolumeSpec) error {
	for _, accessMode := range v.Destination.AccessModes {
		switch accessMode {
		case corev1.ReadWriteOnce,
			corev1.ReadOnlyMany,
			corev1.ReadWriteMany,
			corev1.ReadWriteOncePod:
		default:
			return fmt.Errorf(
				"cross-cluster volume %d has unsupported access mode %q",
				i,
				accessMode,
			)
		}
	}

	if len(v.Destination.AccessModes) == 0 ||
		v.Destination.VolumeMode != corev1.PersistentVolumeFilesystem {
		return fmt.Errorf(
			"cross-cluster volume %d must use filesystem storage and at least one access mode",
			i,
		)
	}

	if !kube.HasWritableAccessMode(v.Destination.AccessModes) {
		return fmt.Errorf(
			"cross-cluster volume %d has no writable access mode for the destination copy",
			i,
		)
	}

	return nil
}

func validateCrossClusterPaths(i int, v VolumeSpec) error {
	for name, pathValue := range map[string]string{
		"source": v.Transfer.SourcePath, "destination": v.Transfer.DestinationPath,
	} {
		normalized, pathErr := domain.NormalizeTransferPath(pathValue)
		if pathErr != nil || normalized != pathValue {
			return fmt.Errorf("cross-cluster volume %d %s path is invalid", i, name)
		}
	}

	return nil
}

// ValidateSessionID ensures the ID is safe for ConfigMap, Lease, Pod, and
// Helm-derived resource names as well as the session label value.
func ValidateSessionID(id string) error {
	if id == "" {
		return errors.New("session ID is required")
	}

	if problems := validation.IsDNS1123Label(id); len(problems) > 0 {
		return fmt.Errorf("session ID %q is invalid: %s", id, strings.Join(problems, "; "))
	}

	if problems := validation.IsDNS1123Subdomain(sessionName(id)); len(problems) > 0 {
		return fmt.Errorf(
			"session ID %q produces an invalid resource name: %s",
			id,
			strings.Join(problems, "; "),
		)
	}

	return nil
}

func NewSession(id string, spec Spec, now time.Time) *Session {
	statuses := make([]VolumeStatus, len(spec.Volumes))
	for i, v := range spec.Volumes {
		statuses[i].SourcePVCName = v.Source.PVC.Name
	}

	t := metav1.NewTime(now.UTC())

	return &Session{
		APIVersion: APIVersion,
		Kind:       Kind,
		ID:         id,
		Spec:       spec,
		Status:     Status{Phase: PhasePlanned, StartedAt: t, UpdatedAt: t, Volumes: statuses},
	}
}
