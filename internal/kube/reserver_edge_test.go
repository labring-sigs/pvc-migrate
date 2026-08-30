package kube

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func reserveSourceObjects() (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	pvcUID := types.UID("source-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "app",
			Name:            "data",
			UID:             pvcUID,
			ResourceVersion: "10",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-source", StorageClassName: &storageClass, VolumeMode: &mode,
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pv-source",
			UID:             types.UID("source-pv-uid"),
			ResourceVersion: "20",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Namespace: "app",
				Name:      "data",
				UID:       pvcUID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	return pvc, pv
}

func pvUpdateCount(client *fake.Clientset) int {
	count := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" && action.GetResource().Resource == "persistentvolumes" {
			count++
		}
	}

	return count
}

func reserveTestSession() *domain.Session {
	mode := corev1.PersistentVolumeFilesystem

	return domain.NewSession(
		"session",
		domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
			SourceNamespace:      "app",
			TemporaryNamespace:   "system",
			DestinationNamespace: "app",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace:       "app",
						Name:            "data",
						UID:             types.UID("source-pvc-uid"),
						ResourceVersion: "10",
					},
					SourcePV: domain.ObjectReference{
						Name:            "pv-source",
						UID:             types.UID("source-pv-uid"),
						ResourceVersion: "20",
					},
					DestinationPVC: domain.ObjectReference{
						Namespace: "system",
						Name:      "data-migrated",
					},
					Capacity:     "1Gi",
					StorageClass: "fast",
					AccessModes:  []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					VolumeMode:   mode,
				},
			},
		}, false, domain.SessionWorkflowOptions{TargetNode: "node-b"}),

		time.Unix(100, 0),
	)
}

func reservationToolPod(session *domain.Session, uid types.UID) *corev1.Pod {
	volume := &session.Spec.Volumes[0]

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: volume.DestinationPVC.Namespace,
			Name:      toolPodName(session.ID, volume.SourcePVC.Name),
			UID:       uid,
			Labels: map[string]string{
				ManagedByLabel:    ManagedByValue,
				SessionKey:        session.ID,
				ResourceRoleLabel: ResourceRoleReservationConsumer,
			},
		},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: volume.DestinationPVC.Name,
					},
				},
			},
		}},
	}
}

func TestReservationPodRequiresServerIdentity(t *testing.T) {
	session := reserveTestSession()
	pod := reservationToolPod(session, "")

	err := validateReservationPod(pod, session.ID, session.Spec.Volumes[0].DestinationPVC.Name)
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRetainPVRejectsMissingExpectedUID(t *testing.T) {
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv"}}
	client := fake.NewClientset(pv)

	err := NewReserver(
		client,
	).retainPV(context.Background(), pv.Name, "", "session", ResourceRoleSource)
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if updates := pvUpdateCount(client); updates != 0 {
		t.Fatalf("PV updates=%d", updates)
	}
}

func TestReserveVolumeDryRunHasNoPersistentOwnershipChanges(t *testing.T) {
	ctx := context.Background()
	sourcePVC, sourcePV := reserveSourceObjects()
	client := fake.NewClientset(sourcePVC, sourcePV)

	var createOptions metav1.CreateOptions
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			createOptions = testutil.MustType[interface {
				GetCreateOptions() metav1.CreateOptions
			}](t, action).GetCreateOptions()

			return true, testutil.MustActionObject[runtime.Object](t, action), nil
		},
	)

	session := reserveTestSession()
	if err := NewReserver(
		client,
	).ReserveVolume(ctx, session, &session.Spec.Volumes[0], &session.Status.Volumes[0], true); err != nil {
		t.Fatal(err)
	}

	if len(createOptions.DryRun) != 1 || createOptions.DryRun[0] != metav1.DryRunAll {
		t.Fatalf("PVC create dry-run options: %#v", createOptions.DryRun)
	}

	currentPVC, _ := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(ctx, "data", metav1.GetOptions{})

	currentPV, _ := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	if currentPVC.Annotations[SessionKey] != "" || currentPV.Labels[SessionKey] != "" ||
		currentPV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Fatalf("dry-run mutated source resources: pvc=%#v pv=%#v", currentPVC, currentPV)
	}

	if session.Status.Volumes[0].Reserved {
		t.Fatal("dry-run marked the volume reserved")
	}
}

func TestReserveVolumeDryRunRejectsForeignSourceOwnership(t *testing.T) {
	ctx := context.Background()
	sourcePVC, sourcePV := reserveSourceObjects()
	sourcePVC.Annotations = map[string]string{SessionKey: "foreign-session"}
	client := fake.NewClientset(sourcePVC, sourcePV)
	session := reserveTestSession()

	err := NewReserver(
		client,
	).ReserveVolume(ctx, session, &session.Spec.Volumes[0], &session.Status.Volumes[0], true)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if pvUpdateCount(client) != 0 {
		t.Fatalf("foreign source ownership caused PV updates: %d", pvUpdateCount(client))
	}
}

func TestReserveVolumeDryRunValidatesExistingDestinationOwnership(t *testing.T) {
	ctx := context.Background()
	sourcePVC, sourcePV := reserveSourceObjects()
	class := "fast"
	existing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "data-migrated",
			Labels:      map[string]string{SessionKey: "foreign-session"},
			Annotations: map[string]string{SourcePVCUIDAnnotation: "source-pvc-uid"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &class,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
	}
	client := fake.NewClientset(sourcePVC, sourcePV, existing)
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewAlreadyExists(
				corev1.Resource("persistentvolumeclaims"),
				existing.Name,
			)
		},
	)

	session := reserveTestSession()

	err := NewReserver(
		client,
	).ReserveVolume(ctx, session, &session.Spec.Volumes[0], &session.Status.Volumes[0], true)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestVerifySourceIdentityRejectsChangedObjects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
	}{
		{
			"PVC UID",
			func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.UID = "replacement" },
		},
		{
			"PVC binding",
			func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.Spec.VolumeName = "other-pv" },
		},
		{"PVC phase", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
			pvc.Status.Phase = corev1.ClaimPending
		}},
		{
			"PV UID",
			func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.UID = "replacement" },
		},
		{
			"PV claimRef missing",
			func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.Spec.ClaimRef = nil },
		},
		{"PV claimRef UID", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Spec.ClaimRef.UID = "replacement"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pvc, pv := reserveSourceObjects()
			test.mutate(pvc, pv)
			client := fake.NewClientset(pvc, pv)
			session := reserveTestSession()

			err := NewReserver(
				client,
			).verifySourceIdentity(context.Background(), session.ID, &session.Spec.Volumes[0])
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestValidateDestinationPVCConflictMatrix(t *testing.T) {
	session := reserveTestSession()
	volume := &session.Spec.Volumes[0]
	capacity := resource.MustParse(volume.Capacity)
	base := func() *corev1.PersistentVolumeClaim {
		storageClass := volume.StorageClass

		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: volume.DestinationPVC.Namespace,
				Name:      volume.DestinationPVC.Name,
				Labels: map[string]string{
					ManagedByLabel:    ManagedByValue,
					SessionKey:        session.ID,
					ResourceRoleLabel: ResourceRoleDestination,
				},
				Annotations: map[string]string{
					SessionKey:             session.ID,
					SourcePVCUIDAnnotation: string(volume.SourcePVC.UID),
					SourcePVAnnotation:     volume.SourcePV.Name,
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: append(
					[]corev1.PersistentVolumeAccessMode(nil),
					volume.AccessModes...,
				),
				StorageClassName: &storageClass,
				VolumeMode:       &volume.VolumeMode,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceStorage: capacity,
				}},
			},
		}
	}
	now := metav1.Now()

	tests := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim)
	}{
		{"terminating", func(pvc *corev1.PersistentVolumeClaim) { pvc.DeletionTimestamp = &now }},
		{
			"foreign session",
			func(pvc *corev1.PersistentVolumeClaim) { pvc.Labels[SessionKey] = "other" },
		},
		{
			"different source",
			func(pvc *corev1.PersistentVolumeClaim) { pvc.Annotations[SourcePVCUIDAnnotation] = "other" },
		},
		{
			"storage class absent",
			func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.StorageClassName = nil },
		},
		{
			"storage class changed",
			func(pvc *corev1.PersistentVolumeClaim) { value := "slow"; pvc.Spec.StorageClassName = &value },
		},
		{"volume mode changed", func(pvc *corev1.PersistentVolumeClaim) {
			mode := corev1.PersistentVolumeBlock
			pvc.Spec.VolumeMode = &mode
		}},
		{"access modes changed", func(pvc *corev1.PersistentVolumeClaim) {
			pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		}},
		{"capacity reduced", func(pvc *corev1.PersistentVolumeClaim) {
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("512Mi")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pvc := base()
			test.mutate(pvc)

			err := validateDestinationPVC(pvc, session.ID, volume, capacity)
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}

	pvc := base()

	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")
	if err := validateDestinationPVC(pvc, session.ID, volume, capacity); err != nil {
		t.Fatalf("larger compatible PVC: %v", err)
	}
}

func TestReserveVolumeDryRunRejectsRecordedDestinationIdentityChanges(t *testing.T) {
	ctx := context.Background()
	sourcePVC, sourcePV := reserveSourceObjects()
	session := reserveTestSession()
	volume := &session.Spec.Volumes[0]
	volume.DestinationPVC.UID = types.UID("destination-pvc-uid")
	volume.DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("destination-pv-uid"),
	}
	class := volume.StorageClass
	destinationPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: volume.DestinationPVC.Namespace,
			Name:      volume.DestinationPVC.Name,
			UID:       volume.DestinationPVC.UID,
			Labels:    map[string]string{SessionKey: session.ID},
			Annotations: map[string]string{
				SourcePVCUIDAnnotation: string(volume.SourcePVC.UID),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: append(
				[]corev1.PersistentVolumeAccessMode(nil),
				volume.AccessModes...,
			),
			StorageClassName: &class,
			VolumeMode:       &volume.VolumeMode,
			VolumeName:       volume.DestinationPV.Name,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(volume.Capacity),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	destinationPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volume.DestinationPV.Name,
			UID:  volume.DestinationPV.UID,
		},
		Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
			Namespace: destinationPVC.Namespace, Name: destinationPVC.Name, UID: destinationPVC.UID,
		}},
	}
	for _, mutate := range []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
	}{
		{name: "PVC UID", mutate: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.UID = "replacement-pvc-uid" }},
		{name: "PV UID", mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.UID = "replacement-pv-uid" }},
		{name: "PV claimRef", mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Spec.ClaimRef.UID = "replacement-pvc-uid"
		}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			pvc := destinationPVC.DeepCopy()
			pv := destinationPV.DeepCopy()
			mutate.mutate(pvc, pv)
			client := fake.NewClientset(sourcePVC, sourcePV, pvc, pv)

			err := NewReserver(
				client,
			).ReserveVolume(ctx, session, volume, &session.Status.Volumes[0], true)
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestProvisionOnTargetRejectsInvalidNodeAndTool(t *testing.T) {
	t.Run("target node required", func(t *testing.T) {
		session := reserveTestSession()
		session.Spec.WorkflowOptionsPtr().TargetNode = ""

		err := NewReserver(
			fake.NewClientset(),
		).provisionOnTarget(context.Background(), session, &session.Spec.Volumes[0])
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
	t.Run("hostname label required", func(t *testing.T) {
		session := reserveTestSession()
		client := fake.NewClientset(
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: session.Spec.WorkflowOptions().TargetNode},
			},
		)

		err := NewReserver(
			client,
		).provisionOnTarget(context.Background(), session, &session.Spec.Volumes[0])
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	for _, test := range []struct {
		name     string
		owner    string
		phase    corev1.PodPhase
		category domain.ErrorCategory
	}{
		{name: "foreign tool", owner: "other", phase: corev1.PodRunning, category: domain.ErrorConflict},
		{name: "failed tool", owner: "session", phase: corev1.PodFailed, category: domain.ErrorPrecondition},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := reserveTestSession()
			volume := &session.Spec.Volumes[0]
			tool := reservationToolPod(session, "tool-uid")
			tool.Labels[SessionKey] = test.owner
			tool.Status.Phase = test.phase
			client := fake.NewClientset(
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "node-b",
						Labels: map[string]string{corev1.LabelHostname: "node-b"},
					},
				},
				tool,
			)
			reserver := NewReserver(client)
			reserver.poll = time.Millisecond

			err := reserver.provisionOnTarget(context.Background(), session, volume)
			if domain.CategoryOf(err) != test.category {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestProvisionOnTargetSurfacesToolCleanupFailure(t *testing.T) {
	session := reserveTestSession()
	volume := &session.Spec.Volumes[0]
	tool := reservationToolPod(session, "tool-uid")
	tool.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}},
	}
	client := fake.NewClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-b",
				Labels: map[string]string{corev1.LabelHostname: "node-b"},
			},
		},
		tool,
	)
	client.PrependReactor(
		"delete",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("injected tool cleanup failure")
		},
	)
	reserver := NewReserver(client)
	reserver.poll = time.Millisecond

	err := reserver.provisionOnTarget(context.Background(), session, volume)
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestProvisionOnTargetRevalidatesCreateAlreadyExistsRace(t *testing.T) {
	session := reserveTestSession()
	volume := &session.Spec.Volumes[0]
	foreign := reservationToolPod(session, "foreign-tool-uid")
	foreign.Labels[SessionKey] = "other-session"
	client := fake.NewClientset(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-b",
			Labels: map[string]string{corev1.LabelHostname: "node-b"},
		},
	})
	gets := 0
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 1 {
			return true, nil, apierrors.NewNotFound(corev1.Resource("pods"), foreign.Name)
		}

		return true, foreign, nil
	})
	client.PrependReactor(
		"create",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewAlreadyExists(corev1.Resource("pods"), foreign.Name)
		},
	)

	err := NewReserver(client).provisionOnTarget(context.Background(), session, volume)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if gets != 2 {
		t.Fatalf("Pod gets=%d, want initial lookup plus race revalidation", gets)
	}
}

func TestReserveVolumeCleansStaleReservationPodForBoundDestination(t *testing.T) {
	ctx := context.Background()
	session := reserveTestSession()
	volume := &session.Spec.Volumes[0]
	sourcePVC, sourcePV := reserveSourceObjects()
	storageClass := volume.StorageClass
	destinationPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: volume.DestinationPVC.Namespace,
			Name:      volume.DestinationPVC.Name,
			UID:       "destination-pvc-uid",
			Labels: map[string]string{
				ManagedByLabel:    ManagedByValue,
				SessionKey:        session.ID,
				ResourceRoleLabel: ResourceRoleDestination,
			},
			Annotations: map[string]string{
				SessionKey:                           session.ID,
				SourcePVCUIDAnnotation:               string(volume.SourcePVC.UID),
				SourcePVAnnotation:                   volume.SourcePV.Name,
				"volume.kubernetes.io/selected-node": session.Spec.WorkflowOptions().TargetNode,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: append(
				[]corev1.PersistentVolumeAccessMode(nil),
				volume.AccessModes...,
			),
			StorageClassName: &storageClass,
			VolumeMode:       &volume.VolumeMode,
			VolumeName:       "pv-destination",
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(volume.Capacity),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	destinationPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: "destination-pv-uid"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Namespace: destinationPVC.Namespace,
				Name:      destinationPVC.Name,
				UID:       destinationPVC.UID,
			},
		},
	}
	tool := reservationToolPod(session, "stale-tool-uid")
	client := fake.NewClientset(
		sourcePVC,
		sourcePV,
		destinationPVC,
		destinationPV,
		tool,
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-b",
				Labels: map[string]string{corev1.LabelHostname: "node-b"},
			},
		},
	)

	var preconditions *metav1.Preconditions
	client.PrependReactor(
		"delete",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			preconditions = testutil.MustType[clienttesting.DeleteAction](t, action).
				GetDeleteOptions().Preconditions
			return false, nil, nil
		},
	)
	reserver := NewReserver(client)

	reserver.poll = time.Millisecond
	if err := reserver.ReserveVolume(
		ctx,
		session,
		volume,
		&session.Status.Volumes[0],
		false,
	); err != nil {
		t.Fatal(err)
	}

	if preconditions == nil || preconditions.UID == nil || *preconditions.UID != tool.UID {
		t.Fatalf("delete preconditions=%#v", preconditions)
	}

	if _, err := client.CoreV1().
		Pods(tool.Namespace).
		Get(ctx, tool.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("stale reservation Pod still exists: %v", err)
	}

	if !session.Status.Volumes[0].Reserved {
		t.Fatal("bound destination was not marked reserved")
	}
}

func TestCleanupReservationPodOwnershipAndDeletionRaces(t *testing.T) {
	t.Run("already deleted", func(t *testing.T) {
		session := reserveTestSession()
		if err := NewReserver(
			fake.NewClientset(),
		).cleanupReservationPod(context.Background(), session, &session.Spec.Volumes[0]); err != nil {
			t.Fatal(err)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "foreign session", mutate: func(pod *corev1.Pod) { pod.Labels[SessionKey] = "other" }},
		{name: "wrong role", mutate: func(pod *corev1.Pod) { pod.Labels[ResourceRoleLabel] = ResourceRoleDestination }},
		{name: "wrong PVC", mutate: func(pod *corev1.Pod) { pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := reserveTestSession()
			tool := reservationToolPod(session, "tool-uid")
			test.mutate(tool)
			client := fake.NewClientset(tool)

			err := NewReserver(
				client,
			).cleanupReservationPod(context.Background(), session, &session.Spec.Volumes[0])
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			if _, getErr := client.CoreV1().
				Pods(tool.Namespace).
				Get(context.Background(), tool.Name, metav1.GetOptions{}); getErr != nil {
				t.Fatalf("foreign Pod was deleted: %v", getErr)
			}
		})
	}

	t.Run("delete precondition conflict", func(t *testing.T) {
		session := reserveTestSession()
		tool := reservationToolPod(session, "tool-uid")
		client := fake.NewClientset(tool)
		client.PrependReactor(
			"delete",
			"pods",
			func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewConflict(
					corev1.Resource("pods"),
					tool.Name,
					errors.New("UID changed"),
				)
			},
		)

		err := NewReserver(
			client,
		).cleanupReservationPod(context.Background(), session, &session.Spec.Volumes[0])
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
	t.Run("name reused while waiting", func(t *testing.T) {
		session := reserveTestSession()
		tool := reservationToolPod(session, "tool-uid")
		client := fake.NewClientset(tool)
		client.PrependReactor(
			"delete",
			"pods",
			func(clienttesting.Action) (bool, runtime.Object, error) {
				resource := corev1.SchemeGroupVersion.WithResource("pods")
				if err := client.Tracker().Delete(resource, tool.Namespace, tool.Name); err != nil {
					return true, nil, err
				}

				replacement := reservationToolPod(session, "replacement-uid")
				if err := client.Tracker().
					Create(resource, replacement, replacement.Namespace); err != nil {
					return true, nil, err
				}

				return true, nil, nil
			},
		)
		reserver := NewReserver(client)
		reserver.poll = time.Millisecond

		err := reserver.cleanupReservationPod(
			context.Background(),
			session,
			&session.Spec.Volumes[0],
		)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestProvisionOnTargetRejectsReplacementWhileWaiting(t *testing.T) {
	session := reserveTestSession()
	volume := &session.Spec.Volumes[0]
	client := fake.NewClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-b",
				Labels: map[string]string{corev1.LabelHostname: "node-b"},
			},
		},
	)

	var created *corev1.Pod
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			created = testutil.MustActionObject[*corev1.Pod](t, action)
			created.UID = "created-uid"
			return false, nil, nil
		},
	)
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if created == nil {
			return false, nil, nil
		}

		replacement := created.DeepCopy()
		replacement.UID = "replacement-uid"
		replacement.Status = corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		}

		return true, replacement, nil
	})
	reserver := NewReserver(client)
	reserver.poll = time.Millisecond

	err := reserver.provisionOnTarget(context.Background(), session, volume)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replaced while waiting") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRetainPVPreservesPolicyAndRejectsOwnershipConflicts(t *testing.T) {
	ctx := context.Background()
	uid := types.UID("pv-uid")
	client := fake.NewClientset(&corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: uid},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		},
	})

	reserver := NewReserver(client)
	if err := reserver.retainPV(ctx, "pv", uid, "session", "source"); err != nil {
		t.Fatal(err)
	}

	updatesAfterFirstRole := pvUpdateCount(client)

	if err := reserver.retainPV(ctx, "pv", uid, "session", "active"); err != nil {
		t.Fatalf("idempotent retain: %v", err)
	}

	if pvUpdateCount(client) != updatesAfterFirstRole+1 {
		t.Fatalf("role change did not update PV exactly once: updates=%d", pvUpdateCount(client))
	}

	updatesAfterRoleChange := pvUpdateCount(client)

	if err := reserver.retainPV(ctx, "pv", uid, "session", "active"); err != nil {
		t.Fatalf("repeat retain: %v", err)
	}

	if pvUpdateCount(client) != updatesAfterRoleChange {
		t.Fatalf("unchanged retain issued update: updates=%d", pvUpdateCount(client))
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain ||
		pv.Annotations[OriginalPolicyAnnotation] != string(corev1.PersistentVolumeReclaimDelete) ||
		pv.Labels[ResourceRoleLabel] != "active" {
		t.Fatalf("retained PV: %#v", pv)
	}

	pv.Labels[SessionKey] = "other-session"
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := reserver.retainPV(
		ctx,
		"pv",
		uid,
		"session",
		"source",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("foreign owner category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := reserver.retainPV(
		ctx,
		"pv",
		types.UID("replacement"),
		"other-session",
		"source",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("UID conflict category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestReserveVolumeRejectsBoundDestinationTopologyAndClaimRefConflicts(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
		category domain.ErrorCategory
	}{
		{
			name: "selected node changed",
			mutate: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
				pvc.Annotations["volume.kubernetes.io/selected-node"] = "node-a"
			},
			category: domain.ErrorPrecondition,
		},
		{
			name: "PV claimRef UID changed",
			mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.Spec.ClaimRef.UID = "other-pvc"
			},
			category: domain.ErrorConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sourcePVC, sourcePV := reserveSourceObjects()
			session := reserveTestSession()
			volume := &session.Spec.Volumes[0]
			storageClass := volume.StorageClass
			destinationUID := types.UID("destination-pvc-uid")
			destinationPVC := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: volume.DestinationPVC.Namespace,
					Name:      volume.DestinationPVC.Name,
					UID:       destinationUID,
					Labels: map[string]string{
						ManagedByLabel:    ManagedByValue,
						SessionKey:        session.ID,
						ResourceRoleLabel: ResourceRoleDestination,
					},
					Annotations: map[string]string{
						SessionKey:             session.ID,
						SourcePVCUIDAnnotation: string(volume.SourcePVC.UID),
						SourcePVAnnotation:     volume.SourcePV.Name,
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: append(
						[]corev1.PersistentVolumeAccessMode(nil),
						volume.AccessModes...,
					),
					StorageClassName: &storageClass,
					VolumeMode:       &volume.VolumeMode,
					VolumeName:       "pv-destination",
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					}},
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}
			destinationPV := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-destination",
					UID:  types.UID("destination-pv-uid"),
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					ClaimRef: &corev1.ObjectReference{
						Namespace: destinationPVC.Namespace,
						Name:      destinationPVC.Name,
						UID:       destinationUID,
					},
				},
			}
			test.mutate(destinationPVC, destinationPV)
			client := fake.NewClientset(sourcePVC, sourcePV, destinationPVC, destinationPV)

			err := NewReserver(
				client,
			).ReserveVolume(context.Background(), session, volume, &session.Status.Volumes[0], false)
			if domain.CategoryOf(err) != test.category {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}
