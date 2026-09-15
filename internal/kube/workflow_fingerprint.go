package kube

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkflowExecutionIntentHash fingerprints admitted execution input. Cleanup
// policy edits do not retarget execution; metadata and status are not input.
func WorkflowExecutionIntentHash(object crclient.Object) (string, error) {
	if object == nil || reflect.ValueOf(object).Kind() != reflect.Pointer ||
		reflect.ValueOf(object).IsNil() {
		return "", domain.NewError(
			domain.ErrorValidation,
			"workflow fingerprint",
			"workflow resource is required",
		)
	}

	var spec any

	switch current := object.(type) {
	case *v1alpha1.Migration:
		input := current.Spec.DeepCopy()
		input.SourcePVReclaimPolicy, input.DestinationPVCReclaimPolicy = "", ""
		spec = input
	case *v1alpha1.ClusterMigration:
		input := current.Spec.DeepCopy()
		input.SourcePVReclaimPolicy, input.DestinationPVCReclaimPolicy = "", ""
		spec = input
	case *v1alpha1.PodMigration:
		input := current.Spec.DeepCopy()
		input.SourcePVReclaimPolicy, input.DestinationPVCReclaimPolicy = "", ""
		spec = input
	case *v1alpha1.ClusterPodMigration:
		input := current.Spec.DeepCopy()
		input.SourcePVReclaimPolicy, input.DestinationPVCReclaimPolicy = "", ""
		spec = input
	case *v1alpha1.Copy:
		input := current.Spec.DeepCopy()
		input.DestinationPVCReclaimPolicy = ""
		spec = input
	case *v1alpha1.ClusterCopy:
		input := current.Spec.DeepCopy()
		input.DestinationPVCReclaimPolicy = ""
		spec = input
	case *v1alpha1.Reservation:
		input := current.Spec.DeepCopy()
		input.DestinationPVCReclaimPolicy = ""
		spec = input
	case *v1alpha1.ClusterReservation:
		input := current.Spec.DeepCopy()
		input.DestinationPVCReclaimPolicy = ""
		spec = input
	case *v1alpha1.Backup:
		spec = current.Spec
	case *v1alpha1.Restore:
		spec = current.Spec
	case *v1alpha1.Rename:
		spec = current.Spec
	case *v1alpha1.Move:
		spec = current.Spec
	default:
		return "", domain.NewError(
			domain.ErrorValidation,
			"workflow fingerprint",
			"unsupported workflow resource",
		)
	}

	data, err := json.Marshal(spec)
	if err != nil {
		return "", domain.WrapError(
			domain.ErrorValidation,
			"workflow fingerprint",
			"encode workflow spec",
			err,
		)
	}

	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
