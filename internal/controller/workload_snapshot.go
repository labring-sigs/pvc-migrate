package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ensureStandalonePodSnapshot makes declarative PodMigration input safe to
// execute with the controller ServiceAccount. A snapshot supplied by a CR is
// never trusted until the controller has captured the referenced live Pod and
// persisted its digest in status. Later reconciles verify the durable digest,
// including after a pause, restart, or process hand-off.
func (r *WorkflowReconciler) ensureStandalonePodSnapshot(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil || session.Spec.Type != domain.SessionTypeMigratePod {
		return nil
	}

	workload := session.Spec.WorkloadPtr()
	if workload == nil || workload.Adapter != domain.WorkloadStandalone {
		return nil
	}

	resource, ok := domain.ControllerResourceForSession(session)
	if !ok {
		return domain.NewError(
			domain.ErrorValidation,
			"controller workload snapshot",
			"workflow resource type is not supported",
		)
	}

	expectedNamespace := session.Spec.SessionNamespace
	if resource.Cluster {
		expectedNamespace = session.Spec.SourceNamespace
	}

	if workload.Pod.Namespace != expectedNamespace {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller workload snapshot",
			"standalone Pod must be in namespace "+expectedNamespace,
		)
	}

	if len(workload.OriginalObject) > domain.MaxOriginalPodSnapshotBytes {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller workload snapshot",
			fmt.Sprintf(
				"standalone Pod snapshot exceeds the %d-byte limit",
				domain.MaxOriginalPodSnapshotBytes,
			),
		)
	}

	if len(workload.OriginalObject) > 0 {
		var supplied corev1.Pod
		if err := json.Unmarshal(workload.OriginalObject, &supplied); err != nil {
			return domain.WrapError(
				domain.ErrorValidation,
				"controller workload snapshot",
				"decode standalone Pod snapshot",
				err,
			)
		}

		if supplied.Namespace != workload.Pod.Namespace || supplied.Name != workload.Pod.Name ||
			(supplied.UID != "" && supplied.UID != workload.Pod.UID) {
			return domain.NewError(
				domain.ErrorConflict,
				"controller workload snapshot",
				"standalone Pod snapshot identity does not match the workflow reference",
			)
		}
	}

	if session.Status.OriginalPodSnapshotHash != "" {
		if len(workload.OriginalObject) == 0 ||
			session.Status.OriginalPodSnapshotHash != podSnapshotHash(workload.OriginalObject) {
			return domain.NewError(
				domain.ErrorConflict,
				"controller workload snapshot",
				"standalone Pod snapshot does not match the controller-captured digest",
			)
		}

		return nil
	}

	if r == nil || r.kubeClient == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"controller workload snapshot",
			"Kubernetes client is not configured",
		)
	}

	live, err := r.kubeClient.CoreV1().Pods(workload.Pod.Namespace).
		Get(ctx, workload.Pod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller workload snapshot",
			"referenced standalone Pod is missing before its snapshot was captured",
		)
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"controller workload snapshot",
			"read referenced standalone Pod",
			err,
		)
	}

	if live.UID != workload.Pod.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"controller workload snapshot",
			fmt.Sprintf("standalone Pod %s/%s UID changed", live.Namespace, live.Name),
		)
	}

	raw, err := json.Marshal(live)
	if err != nil {
		return domain.WrapError(
			domain.ErrorInternal,
			"controller workload snapshot",
			"encode referenced standalone Pod",
			err,
		)
	}

	if len(raw) > domain.MaxOriginalPodSnapshotBytes {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller workload snapshot",
			fmt.Sprintf(
				"referenced standalone Pod snapshot exceeds the %d-byte limit",
				domain.MaxOriginalPodSnapshotBytes,
			),
		)
	}

	workload.OriginalObject = raw

	session.Status.OriginalPodSnapshotHash = podSnapshotHash(raw)
	if err := r.store.Update(ctx, session); err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"controller workload snapshot",
			"persist captured standalone Pod snapshot",
			err,
		)
	}

	return nil
}

func podSnapshotHash(raw []byte) string {
	var pod corev1.Pod
	if err := json.Unmarshal(raw, &pod); err == nil {
		if canonical, marshalErr := json.Marshal(&pod); marshalErr == nil {
			raw = canonical
		}
	}

	digest := sha256.Sum256(raw)

	return hex.EncodeToString(digest[:])
}
