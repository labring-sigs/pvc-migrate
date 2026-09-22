package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// CaptureStandalonePodSnapshot validates a persisted digest or captures the
// referenced live Pod. The caller owns namespace admission and atomically
// persists the returned snapshot and digest. Returned bytes never alias input.
func CaptureStandalonePodSnapshot(
	ctx context.Context,
	client kubernetes.Interface,
	pod v1alpha1.ObjectReference,
	snapshot *apiextensionsv1.JSON,
	digest string,
) (*apiextensionsv1.JSON, string, error) {
	if err := validatePodSnapshot(pod, snapshot, digest); err != nil {
		return nil, "", err
	}

	if digest != "" {
		return snapshot.DeepCopy(), digest, nil
	}

	if client == nil {
		return nil, "", domain.NewError(
			domain.ErrorKubernetes,
			"workload snapshot",
			"Kubernetes client is not configured",
		)
	}

	live, err := client.CoreV1().Pods(pod.Namespace).
		Get(ctx, pod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, "", domain.NewError(
			domain.ErrorPrecondition,
			"workload snapshot",
			"referenced standalone Pod is missing before its snapshot was captured",
		)
	}

	if err != nil {
		return nil, "", domain.WrapError(
			domain.ErrorKubernetes,
			"workload snapshot",
			"read referenced standalone Pod",
			err,
		)
	}

	if live.UID != pod.UID {
		return nil, "", domain.NewError(
			domain.ErrorConflict,
			"workload snapshot",
			fmt.Sprintf("standalone Pod %s/%s UID changed", live.Namespace, live.Name),
		)
	}

	raw, err := json.Marshal(live)
	if err != nil {
		return nil, "", domain.WrapError(
			domain.ErrorInternal,
			"workload snapshot",
			"encode referenced standalone Pod",
			err,
		)
	}

	if len(raw) > domain.MaxOriginalPodSnapshotBytes {
		return nil, "", domain.NewError(
			domain.ErrorPrecondition,
			"workload snapshot",
			fmt.Sprintf(
				"referenced standalone Pod snapshot exceeds the %d-byte limit",
				domain.MaxOriginalPodSnapshotBytes,
			),
		)
	}

	return &apiextensionsv1.JSON{Raw: raw}, PodSnapshotHash(raw), nil
}

// VerifyStandalonePodSnapshot validates a durable snapshot without consulting a
// live Pod, which may already have been removed during workload migration.
func VerifyStandalonePodSnapshot(
	pod v1alpha1.ObjectReference, snapshot *apiextensionsv1.JSON, digest string,
) error {
	if digest == "" {
		return domain.NewError(domain.ErrorValidation, "workload snapshot",
			"standalone Pod snapshot requires a persisted digest")
	}

	return validatePodSnapshot(pod, snapshot, digest)
}

func validatePodSnapshot(
	pod v1alpha1.ObjectReference, snapshot *apiextensionsv1.JSON, digest string,
) error {
	if pod.Namespace == "" || pod.Name == "" || pod.UID == "" {
		return domain.NewError(domain.ErrorValidation,
			"workload snapshot", "standalone Pod identity is incomplete")
	}

	if snapshot != nil &&
		len(snapshot.Raw) > domain.MaxOriginalPodSnapshotBytes {
		return domain.NewError(
			domain.ErrorPrecondition,
			"workload snapshot",
			fmt.Sprintf(
				"standalone Pod snapshot exceeds the %d-byte limit",
				domain.MaxOriginalPodSnapshotBytes,
			),
		)
	}

	if snapshot != nil && len(snapshot.Raw) > 0 {
		var supplied corev1.Pod
		if err := json.Unmarshal(snapshot.Raw, &supplied); err != nil {
			return domain.WrapError(
				domain.ErrorValidation,
				"workload snapshot",
				"decode standalone Pod snapshot",
				err,
			)
		}

		if supplied.Namespace != pod.Namespace || supplied.Name != pod.Name ||
			(supplied.UID != "" && supplied.UID != pod.UID) {
			return domain.NewError(
				domain.ErrorConflict,
				"workload snapshot",
				"standalone Pod snapshot identity does not match the workflow reference",
			)
		}
	}

	if digest != "" {
		if snapshot == nil || len(snapshot.Raw) == 0 ||
			digest != PodSnapshotHash(
				snapshot.Raw,
			) {
			return domain.NewError(
				domain.ErrorConflict,
				"workload snapshot",
				"standalone Pod snapshot does not match the captured digest",
			)
		}

		return nil
	}

	return nil
}

func PodSnapshotHash(raw []byte) string {
	var pod corev1.Pod
	if err := json.Unmarshal(raw, &pod); err == nil {
		if canonical, marshalErr := json.Marshal(&pod); marshalErr == nil {
			raw = canonical
		}
	}

	digest := sha256.Sum256(raw)

	return hex.EncodeToString(digest[:])
}
