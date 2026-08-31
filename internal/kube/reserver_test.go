package kube

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestReserverWaitLogsBeforePolling(t *testing.T) {
	var logs bytes.Buffer

	reserver := NewReserver(
		fake.NewClientset(),
	).WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))

	reserver.poll = time.Millisecond
	if err := reserver.waitFor(
		context.Background(),
		"reservation Pod readiness",
		func(context.Context) (bool, error) { return true, nil },
	); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(logs.String(), "waiting for Kubernetes resource") ||
		!strings.Contains(logs.String(), "reservation Pod readiness") {
		t.Fatalf("logs=%q", logs.String())
	}
}

func TestReserveVolumeProvisionsOnTargetAndRetainsBothPVs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	sourcePVCUID := types.UID("source-pvc-uid")
	sourcePVUID := types.UID("source-pv-uid")
	client := fake.NewClientset(
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClass}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: sourcePVCUID},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-source",
				StorageClassName: &storageClass,
				VolumeMode:       &mode,
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-source", UID: sourcePVUID},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       sourcePVCUID,
				},
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-b",
				Labels: map[string]string{corev1.LabelHostname: "node-b"},
			},
			Spec: corev1.NodeSpec{
				Taints: []corev1.Taint{
					{Key: "storage", Value: "local", Effect: corev1.TaintEffectNoSchedule},
				},
			},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			}}},
		},
	)

	var (
		toolTolerations         []corev1.Toleration
		toolDeletePreconditions *metav1.Preconditions
	)

	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			toolTolerations = append([]corev1.Toleration(nil), pod.Spec.Tolerations...)
			pod.UID = types.UID("tool-uid")
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			}

			pvcObject, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), "system", "data-migrated")
			if err != nil {
				return true, nil, err
			}

			pvc := testutil.MustType[*corev1.PersistentVolumeClaim](t, pvcObject)
			pvc.UID = types.UID("destination-pvc-uid")
			pvc.Spec.VolumeName = "pv-destination"
			pvc.Status.Phase = corev1.ClaimBound

			pvc.Annotations["volume.kubernetes.io/selected-node"] = "node-b"
			if err := client.Tracker().
				Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), pvc, "system"); err != nil {
				return true, nil, err
			}

			if err := client.Tracker().
				Create(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-destination",
						UID:  types.UID("destination-pv-uid"),
					},
					Spec: corev1.PersistentVolumeSpec{
						Capacity: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
						ClaimRef: &corev1.ObjectReference{
							Namespace: "system",
							Name:      "data-migrated",
							UID:       pvc.UID,
						},
					},
					Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
				}, ""); err != nil {
				return true, nil, err
			}

			return false, nil, nil
		},
	)
	client.PrependReactor(
		"delete",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			toolDeletePreconditions = testutil.MustType[clienttesting.DeleteAction](t, action).
				GetDeleteOptions().Preconditions
			return false, nil, nil
		},
	)

	session := domain.NewSession(
		"session",
		domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
			SourceNamespace:      "app",
			TemporaryNamespace:   "system",
			DestinationNamespace: "app",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace: "app",
						Name:      "data",
						UID:       sourcePVCUID,
					},
					SourcePV: domain.ObjectReference{Name: "pv-source", UID: sourcePVUID},
					DestinationPVC: domain.ObjectReference{
						Namespace: "system",
						Name:      "data-migrated",
					},
					Capacity:     "1Gi",
					StorageClass: storageClass,
					AccessModes:  []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					VolumeMode:   mode,
				},
			},
		}, false, domain.SessionWorkflowOptions{TargetNode: "node-b"}),

		time.Now(),
	)
	reserver := NewReserver(client)
	reserver.poll = time.Millisecond
	volume := &session.Spec.Volumes[0]

	status := &session.Status.Volumes[0]
	if err := reserver.ReserveVolume(ctx, session, volume, status, false); err != nil {
		t.Fatal(err)
	}

	if !status.Reserved || volume.DestinationPV.Name != "pv-destination" ||
		volume.DestinationPV.UID != types.UID("destination-pv-uid") {
		t.Fatalf("reservation state: volume=%#v status=%#v", volume, status)
	}

	if len(toolTolerations) != 1 || toolTolerations[0].Key != "storage" ||
		toolTolerations[0].Value != "local" {
		t.Fatalf("tool tolerations: %#v", toolTolerations)
	}

	if toolDeletePreconditions == nil || toolDeletePreconditions.UID == nil ||
		*toolDeletePreconditions.UID != types.UID("tool-uid") {
		t.Fatalf("tool delete preconditions: %#v", toolDeletePreconditions)
	}

	for _, name := range []string{"pv-source", "pv-destination"} {
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain ||
			pv.Labels[SessionKey] != "session" {
			t.Fatalf("retained PV %s: %#v", name, pv)
		}
	}

	if err := reserver.ReserveVolume(ctx, session, volume, status, false); err != nil {
		t.Fatalf("idempotent reservation: %v", err)
	}
}

func TestReserveVolumeRejectsDestinationOwnedByAnotherSession(t *testing.T) {
	ctx := context.Background()
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	sourcePVCUID := types.UID("source-pvc-uid")
	sourcePVUID := types.UID("source-pv-uid")
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: sourcePVCUID},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-source",
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &storageClass,
				VolumeMode:       &mode,
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-source", UID: sourcePVUID},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef:                      &corev1.ObjectReference{UID: sourcePVCUID},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "system",
				Name:        "data-migrated",
				Labels:      map[string]string{SessionKey: "other"},
				Annotations: map[string]string{SourcePVCUIDAnnotation: string(sourcePVCUID)},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeMode:       &mode,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
			},
		},
	)
	session := domain.NewSession(
		"session",
		domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
			SourceNamespace:    "app",
			TemporaryNamespace: "system",
			SessionNamespace:   "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace: "app",
						Name:      "data",
						UID:       sourcePVCUID,
					},
					SourcePV: domain.ObjectReference{Name: "pv-source", UID: sourcePVUID},
					DestinationPVC: domain.ObjectReference{
						Namespace: "system",
						Name:      "data-migrated",
					},
					Capacity:     "1Gi",
					StorageClass: storageClass,
					AccessModes:  []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					VolumeMode:   mode,
				},
			},
		}, false, domain.SessionWorkflowOptions{}),

		time.Now(),
	)

	err := NewReserver(
		client,
	).ReserveVolume(ctx, session, &session.Spec.Volumes[0], &session.Status.Volumes[0], false)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}
