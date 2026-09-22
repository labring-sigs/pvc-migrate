package planner

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func plannerClient(objects ...runtime.Object) *kubernetesfake.Clientset {
	for _, object := range objects {
		quota, ok := object.(*corev1.ResourceQuota)
		if !ok {
			continue
		}

		quota.Status.Hard = quota.Spec.Hard.DeepCopy()
		if quota.Status.Used == nil {
			quota.Status.Used = corev1.ResourceList{}
		}

		for name := range quota.Status.Hard {
			if _, exists := quota.Status.Used[name]; !exists {
				quota.Status.Used[name] = resource.MustParse("0")
			}
		}
	}

	client := kubernetesfake.NewClientset(objects...)
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review, err := testutil.ActionObject[*authorizationv1.SelfSubjectAccessReview](action)
			if err != nil {
				return true, nil, err
			}

			review = review.DeepCopy()
			review.Status.Allowed = true

			return true, review, nil
		},
	)

	return client
}

func plannerObjects(capacity string) []runtime.Object {
	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	pvcUID := types.UID("pvc-uid")

	return []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-b",
				Labels: map[string]string{
					corev1.LabelHostname:     "node-b",
					corev1.LabelTopologyZone: "zone-b",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
		&storagev1.StorageClass{
			ObjectMeta:        metav1.ObjectMeta{Name: storageClass},
			Provisioner:       "example.csi.io",
			VolumeBindingMode: &wffc,
			AllowedTopologies: []corev1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{
						{Key: corev1.LabelTopologyZone, Values: []string{"zone-b"}},
					},
				},
			},
		},
		&storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
			Spec: storagev1.CSINodeSpec{
				Drivers: []storagev1.CSINodeDriver{{Name: "example.csi.io", NodeID: "node-b"}},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       "app",
				Name:            "data",
				UID:             pvcUID,
				ResourceVersion: "10",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(capacity),
					},
				},
				StorageClassName: &storageClass,
				VolumeMode:       &mode,
				VolumeName:       "pv-source",
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "pv-source",
				UID:             types.UID("pv-uid"),
				ResourceVersion: "20",
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(capacity),
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       pvcUID,
				},
				StorageClassName: storageClass,
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		},
	}
}

func plannerObjectsWithTwoPVCs(t *testing.T) []runtime.Object {
	t.Helper()

	objects := plannerObjects("2Gi")
	dataPVC := testutil.MustType[*corev1.PersistentVolumeClaim](t, objects[5])
	dataPV := testutil.MustType[*corev1.PersistentVolume](t, objects[6])

	logsPVC := dataPVC.DeepCopy()
	logsPVC.Name = "logs"
	logsPVC.UID = types.UID("logs-pvc-uid")
	logsPVC.Spec.VolumeName = "pv-logs"
	logsPV := dataPV.DeepCopy()
	logsPV.Name = "pv-logs"
	logsPV.UID = types.UID("logs-pv-uid")
	logsPV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: "app",
		Name:      "logs",
		UID:       logsPVC.UID,
	}

	return append(objects,
		logsPVC,
		logsPV,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "default"}},
	)
}

func hasFailedCheckContaining(
	checks []domain.Check,
	name domain.CheckName,
	message string,
) bool {
	for _, check := range checks {
		if check.Name == name && !check.Passed && strings.Contains(check.Message, message) {
			return true
		}
	}

	return false
}

func hasPassedCheck(checks []domain.Check, name domain.CheckName) bool {
	for _, check := range checks {
		if check.Name == name && check.Passed {
			return true
		}
	}

	return false
}

func hasWarningCheck(checks []domain.Check, name domain.CheckName) bool {
	// A warning check passes overall while flagging degraded confidence.
	for _, check := range checks {
		if check.Name == name && check.Severity == domain.SeverityWarning {
			return true
		}
	}

	return false
}

func hasFailedCheck(checks []domain.Check, name domain.CheckName) bool {
	for _, check := range checks {
		if check.Name == name && !check.Passed && check.Severity == domain.SeverityError {
			return true
		}
	}

	return false
}
