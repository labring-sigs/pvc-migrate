package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestCleanupDeletesReservationPodsAcrossDestinationNamespaces(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseCompleted
	session.Spec.Volumes[1].DestinationPVC.Namespace = "backup"
	reservationPod := func(namespace, name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       types.UID(name + "-uid"),
			Labels: map[string]string{
				kube.ManagedByLabel:    kube.ManagedByValue,
				kube.SessionKey:        session.ID,
				kube.ResourceRoleLabel: "reservation-consumer",
			},
		}}
	}
	client := fake.NewClientset(
		reservationPod("system", "reserve-data"),
		reservationPod("backup", "reserve-logs"),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "other-session", UID: types.UID("other-uid"),
			Labels: map[string]string{kube.SessionKey: "another-session", kube.ResourceRoleLabel: "reservation-consumer"},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "application", UID: types.UID("application-uid"),
		}},
	)
	service := &Service{client: client, store: &memoryStore{}}

	if err := service.Cleanup(ctx, session, CleanupOptions{}); err != nil {
		t.Fatal(err)
	}
	for namespace, name := range map[string]string{"system": "reserve-data", "backup": "reserve-logs"} {
		if _, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("reservation Pod %s/%s still exists: %v", namespace, name, err)
		}
	}
	for _, name := range []string{"other-session", "application"} {
		if _, err := client.CoreV1().Pods("system").Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Fatalf("unrelated Pod %s: %v", name, err)
		}
	}
}

func TestCleanupRejectsReplacementReservationPodWhileWaitingForDeletion(t *testing.T) {
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "system", Name: "reserve-data", UID: "original-uid",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: kube.ResourceRoleReservationConsumer,
		},
	}}
	client := fake.NewClientset(pod)
	deleted := false
	client.PrependReactor("delete", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		deleted = true
		return true, nil, nil
	})
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if !deleted {
			return false, nil, nil
		}
		replacement := pod.DeepCopy()
		replacement.UID = "replacement-uid"
		return true, replacement, nil
	})
	service := &Service{client: client, store: &memoryStore{}}
	if err := service.deleteReservationPods(context.Background(), session); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestInspectPVCUnusedFailsOnEmptyPVCObject(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	service := &Service{client: client}

	_, err := service.inspectPVCUnused(context.Background(), domain.ObjectReference{Namespace: "app", Name: "data"}, "session-123")
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if !strings.Contains(err.Error(), "PVC app/data returned an empty object") {
		t.Fatalf("error=%v", err)
	}
}

func TestCleanupRejectsActiveSessionsAndOpenRollbackWindow(t *testing.T) {
	t.Run("active session", func(t *testing.T) {
		session := appTestSession()
		session.Status.Phase = domain.PhasePaused
		service := &Service{client: fake.NewClientset(), store: &memoryStore{}}
		err := service.Cleanup(context.Background(), session, CleanupOptions{DeleteRollback: true, Finalize: true, DeleteSession: true})
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("rollback PV retained", func(t *testing.T) {
		session := appTestSession()
		session.Status.Phase = domain.PhaseCompleted
		service := &Service{client: fake.NewClientset(), store: &memoryStore{}}
		err := service.Cleanup(context.Background(), session, CleanupOptions{Finalize: true, DeleteSession: true})
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestCleanupTemporaryPVCRequiresRecordedIdentityAndOwnership(t *testing.T) {
	tests := []struct {
		name      string
		uid       types.UID
		sessionID string
	}{
		{name: "replacement PVC", uid: types.UID("replacement-uid"), sessionID: "session-123"},
		{name: "foreign PVC", uid: types.UID("reserved-pvc-uid"), sessionID: "another-session"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			session := appTestSession()
			session.Status.Phase = domain.PhaseAborted
			session.Spec.Volumes[0].DestinationPVC.UID = types.UID("reserved-pvc-uid")
			client := fake.NewClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: "system", Name: "data-migrated", UID: test.uid,
				Labels: map[string]string{kube.SessionKey: test.sessionID},
			}})
			service := &Service{client: client, store: &memoryStore{}}

			err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true})
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
			if _, err := client.CoreV1().PersistentVolumeClaims("system").Get(ctx, "data-migrated", metav1.GetOptions{}); err != nil {
				t.Fatalf("protected PVC: %v", err)
			}
		})
	}
}

func TestCleanupDeletesOnlyOwnedTemporaryPVCs(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseAborted
	for index := range session.Spec.Volumes {
		uid := types.UID("temporary-uid-" + session.Spec.Volumes[index].SourcePVC.Name)
		session.Spec.Volumes[index].DestinationPVC.UID = uid
	}
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "data-migrated", UID: types.UID("temporary-uid-data"), ResourceVersion: "1",
			Labels: map[string]string{kube.SessionKey: session.ID},
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "logs-migrated", UID: types.UID("temporary-uid-logs"), ResourceVersion: "1",
			Labels: map[string]string{kube.SessionKey: session.ID},
		}},
	)
	service := &Service{client: client, store: &memoryStore{}}

	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"data-migrated", "logs-migrated"} {
		if _, err := client.CoreV1().PersistentVolumeClaims("system").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("temporary PVC %s still exists: %v", name, err)
		}
	}
}

func TestCleanupValidatesEveryTemporaryPVCBeforeDeletion(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("temporary-uid-data")
	session.Spec.Volumes[1].DestinationPVC.UID = types.UID("recorded-uid-logs")
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "data-migrated", UID: types.UID("temporary-uid-data"), ResourceVersion: "1",
			Labels: map[string]string{kube.SessionKey: session.ID},
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "system", Name: "logs-migrated", UID: types.UID("replacement-uid-logs"), ResourceVersion: "1",
			Labels: map[string]string{kube.SessionKey: session.ID},
		}},
	)
	service := &Service{client: client, store: &memoryStore{}}

	err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true})
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	for _, name := range []string{"data-migrated", "logs-migrated"} {
		if _, getErr := client.CoreV1().PersistentVolumeClaims("system").Get(ctx, name, metav1.GetOptions{}); getErr != nil {
			t.Fatalf("PVC %s mutated before batch validation: %v", name, getErr)
		}
	}
}

func TestCleanupValidationAccountsForOwnedReservationPod(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("temporary-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       session.Spec.Volumes[0].DestinationPVC.UID,
		Labels:    map[string]string{kube.SessionKey: session.ID},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: pvc.Namespace,
		Name:      "reservation-consumer",
		UID:       types.UID("reservation-pod-uid"),
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: "reservation-consumer",
		},
	}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
		Name:         "data",
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}},
	}}}}
	client := fake.NewClientset(pvc, pod)
	service := &Service{client: client, store: &memoryStore{}}

	if err := service.ValidateCleanup(ctx, session, CleanupOptions{DeleteTemporary: true}); err != nil {
		t.Fatalf("cleanup dry-run blocked by owned reservation Pod: %v", err)
	}
	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("reservation Pod still exists: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("temporary PVC still exists: %v", err)
	}
}

func TestCleanupRecoversDestinationRefsAfterCheckpointLoss(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	pvcUID := types.UID("recovered-destination-pvc-uid")
	pvUID := types.UID("recovered-destination-pv-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       pvcUID,
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: "destination",
		},
		Annotations: map[string]string{
			kube.SessionKey:             session.ID,
			kube.SourcePVCUIDAnnotation: string(session.Spec.Volumes[0].SourcePVC.UID),
		},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"}}
	pv := managedPV("pv-destination", string(pvUID), session.ID, "destination", corev1.VolumeReleased)
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvcUID}
	client := fake.NewClientset(pvc, pv)
	service := &Service{client: client, store: &memoryStore{}}

	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true, DeleteRollback: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("recovered destination PVC still exists: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("recovered destination PV still exists: %v", err)
	}
}

func TestCleanupRecoversUncheckpointedProvisionedDestinationPV(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	pvcUID := types.UID("uncheckpointed-destination-pvc-uid")
	pvUID := types.UID("uncheckpointed-destination-pv-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace:       session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:            session.Spec.Volumes[0].DestinationPVC.Name,
		UID:             pvcUID,
		ResourceVersion: "1",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: kube.ResourceRoleDestination,
		},
		Annotations: map[string]string{
			kube.SessionKey:             session.ID,
			kube.SourcePVCUIDAnnotation: string(session.Spec.Volumes[0].SourcePVC.UID),
		},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-uncheckpointed"}}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-uncheckpointed", UID: pvUID, ResourceVersion: "1"},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef:                      &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvcUID},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].SourcePVC.Namespace,
		Name:      session.Spec.Volumes[0].SourcePVC.Name,
		UID:       session.Spec.Volumes[0].SourcePVC.UID,
		Annotations: map[string]string{
			kube.SessionKey: session.ID,
		},
	}}
	sourcePV := managedPV(session.Spec.Volumes[0].SourcePV.Name, string(session.Spec.Volumes[0].SourcePV.UID), session.ID, kube.ResourceRoleSource, corev1.VolumeBound)
	sourcePV.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	client := fake.NewClientset(pvc, pv, sourcePVC, sourcePV)
	store := &memoryStore{}
	service := &Service{client: client, store: store}
	options := CleanupOptions{DeleteTemporary: true, DeleteRollback: true, Finalize: true, DeleteSession: true}

	if err := service.ValidateCleanup(ctx, session, options); err != nil {
		t.Fatalf("validate cleanup: %v", err)
	}
	if err := service.Cleanup(ctx, session, options); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("temporary PVC still exists: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("uncheckpointed destination PV still exists: %v", err)
	}
	if store.deletes != 1 {
		t.Fatalf("session deletes=%d", store.deletes)
	}
}

type cleanupCheckpointStore struct {
	memoryStore
	snapshot *domain.Session
}

func (s *cleanupCheckpointStore) Update(ctx context.Context, session *domain.Session) error {
	snapshot := *session
	snapshot.Spec = session.Spec
	snapshot.Spec.Volumes = append([]domain.VolumeSpec(nil), session.Spec.Volumes...)
	s.snapshot = &snapshot
	return s.memoryStore.Update(ctx, session)
}

func TestCleanupPersistsRecoveredRefsBeforeRetryableDeletion(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	pvcUID := types.UID("retry-destination-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       pvcUID,
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: kube.ResourceRoleDestination,
		},
		Annotations: map[string]string{
			kube.SessionKey:             session.ID,
			kube.SourcePVCUIDAnnotation: string(session.Spec.Volumes[0].SourcePVC.UID),
		},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-retry"}}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-retry", UID: types.UID("retry-destination-pv-uid")},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef:                      &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvcUID},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
	}
	client := fake.NewClientset(pvc, pv)
	deleteFailed := false
	client.PrependReactor("delete", "persistentvolumes", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		if !deleteFailed && deleteAction.GetName() == pv.Name {
			deleteFailed = true
			return true, nil, errors.New("injected PV delete failure")
		}
		return false, nil, nil
	})
	store := &cleanupCheckpointStore{}
	service := &Service{client: client, store: store}

	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true, DeleteRollback: true}); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("first cleanup category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("destination PVC still exists after first cleanup: %v", err)
	}
	if store.snapshot == nil || store.snapshot.Spec.Volumes[0].DestinationPVC.UID != pvcUID || store.snapshot.Spec.Volumes[0].DestinationPV.Name != pv.Name {
		t.Fatalf("recovered references were not checkpointed: %#v", store.snapshot)
	}

	reloaded := *store.snapshot
	service = &Service{client: client, store: store}
	if err := service.Cleanup(ctx, &reloaded, CleanupOptions{DeleteTemporary: true, DeleteRollback: true}); err != nil {
		t.Fatalf("retry cleanup: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("destination PV still exists after retry: %v", err)
	}
}

type checkpointFailureStore struct {
	memoryStore
	err error
}

func (s *checkpointFailureStore) Update(context.Context, *domain.Session) error {
	s.updates++
	return s.err
}

func TestCleanupStopsWhenRecoveredRefsCheckpointFails(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	pvcUID := types.UID("checkpoint-failure-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       pvcUID,
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: kube.ResourceRoleDestination,
		},
		Annotations: map[string]string{
			kube.SessionKey:             session.ID,
			kube.SourcePVCUIDAnnotation: string(session.Spec.Volumes[0].SourcePVC.UID),
		},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-checkpoint-failure"}}
	pv := managedPV("pv-checkpoint-failure", "checkpoint-failure-pv-uid", session.ID, kube.ResourceRoleDestination, corev1.VolumeReleased)
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvcUID}
	store := &checkpointFailureStore{err: domain.NewError(domain.ErrorKubernetes, "cleanup", "checkpoint recovered references")}
	service := &Service{client: fake.NewClientset(pvc, pv), store: store}

	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true, DeleteRollback: true}); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if store.updates != 1 {
		t.Fatalf("checkpoint updates=%d", store.updates)
	}
	if session.Spec.Volumes[0].DestinationPVC.UID != "" || session.Spec.Volumes[0].DestinationPV.Name != "" {
		t.Fatalf("session references changed after checkpoint failure: %#v", session.Spec.Volumes[0])
	}
	if _, err := service.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("destination PVC changed after checkpoint failure: %v", err)
	}
	if _, err := service.client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("destination PV changed after checkpoint failure: %v", err)
	}
	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true, DeleteRollback: true}); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("retry category=%s error=%v", domain.CategoryOf(err), err)
	}
	if store.updates != 2 {
		t.Fatalf("retry checkpoint updates=%d", store.updates)
	}
	if _, err := service.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("destination PVC changed after retry checkpoint failure: %v", err)
	}
	if _, err := service.client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("destination PV changed after retry checkpoint failure: %v", err)
	}
}

func TestCleanupRejectsUncheckpointedDestinationPVWithUnsafeIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*corev1.PersistentVolume)
	}{
		{name: "retain policy", mutate: func(pv *corev1.PersistentVolume) {
			pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		}},
		{name: "foreign session", mutate: func(pv *corev1.PersistentVolume) { pv.Labels = map[string]string{kube.SessionKey: "other"} }},
		{name: "foreign manager", mutate: func(pv *corev1.PersistentVolume) {
			pv.Labels = map[string]string{kube.ManagedByLabel: "external-controller"}
		}},
		{name: "claim UID changed", mutate: func(pv *corev1.PersistentVolume) { pv.Spec.ClaimRef.UID = types.UID("replacement") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := appTestSession()
			session.Status.Phase = domain.PhaseAborted
			pvcUID := types.UID("destination-pvc-uid")
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
				Name:      session.Spec.Volumes[0].DestinationPVC.Name,
				UID:       pvcUID,
				Labels: map[string]string{
					kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: session.ID, kube.ResourceRoleLabel: kube.ResourceRoleDestination,
				},
				Annotations: map[string]string{kube.SessionKey: session.ID, kube.SourcePVCUIDAnnotation: string(session.Spec.Volumes[0].SourcePVC.UID)},
			}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-uncheckpointed"}}
			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: "pv-uncheckpointed", UID: types.UID("destination-pv-uid")},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef:                      &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvcUID},
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				},
			}
			test.mutate(pv)
			service := &Service{client: fake.NewClientset(pvc, pv), store: &memoryStore{}}

			err := service.Cleanup(context.Background(), session, CleanupOptions{DeleteTemporary: true, DeleteRollback: true})
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
			if _, err := service.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(context.Background(), pvc.Name, metav1.GetOptions{}); err != nil {
				t.Fatalf("protected PVC: %v", err)
			}
		})
	}
}

func TestCleanupSessionDeletionRequiresDiscoveredRollbackPV(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	pvcUID := types.UID("delete-check-destination-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       pvcUID,
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: "destination",
		},
		Annotations: map[string]string{
			kube.SessionKey:             session.ID,
			kube.SourcePVCUIDAnnotation: string(session.Spec.Volumes[0].SourcePVC.UID),
		},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"}}
	pv := managedPV("pv-destination", "delete-check-destination-pv-uid", session.ID, "destination", corev1.VolumeReleased)
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvcUID}
	service := &Service{client: fake.NewClientset(pvc, pv), store: &memoryStore{}}

	err := service.Cleanup(ctx, session, CleanupOptions{Finalize: true, DeleteSession: true})
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := service.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("destination PVC was changed: %v", err)
	}
}

func TestCleanupOrphanRecoveryProtectsForeignDestinationPVC(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       types.UID("foreign-destination-pvc-uid"),
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        "another-session",
			kube.ResourceRoleLabel: "destination",
		},
	}}
	client := fake.NewClientset(pvc)
	service := &Service{client: client, store: &memoryStore{}}

	err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true})
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("foreign destination PVC was changed: %v", err)
	}
}

func TestCleanupBlocksTerminalPVCConsumers(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			ctx := context.Background()
			session := appTestSession()
			session.Status.Phase = domain.PhaseAborted
			session.Spec.Volumes[0].DestinationPVC.UID = types.UID("temporary-pvc-uid")
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
				Name:      session.Spec.Volumes[0].DestinationPVC.Name,
				UID:       session.Spec.Volumes[0].DestinationPVC.UID,
				Labels:    map[string]string{kube.SessionKey: session.ID},
			}}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: pvc.Namespace, Name: "terminal-consumer"},
				Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				}}}},
				Status: corev1.PodStatus{Phase: phase},
			}
			client := fake.NewClientset(pvc, pod)
			service := &Service{client: client, store: &memoryStore{}}
			options := CleanupOptions{DeleteTemporary: true}
			if err := service.ValidateCleanup(ctx, session, options); domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "delete the Pod object") {
				t.Fatalf("validate category=%s error=%v", domain.CategoryOf(err), err)
			}
			if err := service.Cleanup(ctx, session, options); domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), string(phase)) {
				t.Fatalf("cleanup category=%s error=%v", domain.CategoryOf(err), err)
			}
			if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
				t.Fatalf("temporary PVC was changed: %v", err)
			}
		})
	}
}

func TestCleanupPodBlockerReportsOwningController(t *testing.T) {
	controller := true
	for _, test := range []struct {
		name          string
		pod           *corev1.Pod
		objects       []runtime.Object
		wantOwnerKind string
		wantOwnerName string
		wantOwned     bool
		wantVerified  bool
	}{
		{
			name: "Job",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "copy-tool", Labels: map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: "session-123"},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "copy-job", UID: types.UID("job-uid"), Controller: &controller}},
			}},
			objects:       []runtime.Object{&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "copy-job", UID: types.UID("job-uid")}}},
			wantOwnerKind: "Job",
			wantOwnerName: "copy-job",
			wantOwned:     true,
			wantVerified:  true,
		},
		{
			name: "Deployment through ReplicaSet",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "copy-tool", Labels: map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: "session-123", kube.AppInstanceLabel: "pv-migrate-pm-test-clusterip", kube.AppComponentLabel: "rsync"},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "copy-rs", UID: types.UID("rs-uid"), Controller: &controller}},
			}},
			objects: []runtime.Object{
				&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
					Namespace: "system", Name: "copy-rs", UID: types.UID("rs-uid"),
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "copy-deployment", UID: types.UID("deployment-uid"), Controller: &controller}},
				}},
				&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "copy-deployment", UID: types.UID("deployment-uid")}},
			},
			wantOwnerKind: "Deployment",
			wantOwnerName: "copy-deployment",
			wantOwned:     true,
			wantVerified:  true,
		},
		{
			name: "Session label collision",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "foreign-tool", Labels: map[string]string{kube.SessionKey: "session-123"},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "foreign-job", UID: types.UID("foreign-job-uid"), Controller: &controller}},
			}},
			objects:       []runtime.Object{&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "foreign-job", UID: types.UID("foreign-job-uid")}}},
			wantOwnerKind: "Job",
			wantOwnerName: "foreign-job",
			wantVerified:  true,
		},
		{
			name: "Replacement Job UID",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "copy-tool", Labels: map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: "session-123"},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "copy-job", UID: types.UID("old-job-uid"), Controller: &controller}},
			}},
			objects:       []runtime.Object{&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "copy-job", UID: types.UID("replacement-job-uid")}}},
			wantOwnerKind: "Job",
			wantOwnerName: "copy-job",
			wantOwned:     true,
		},
		{
			name: "Replacement ReplicaSet UID",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "copy-tool", Labels: map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: "session-123"},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "copy-rs", UID: types.UID("old-rs-uid"), Controller: &controller}},
			}},
			objects:       []runtime.Object{&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "copy-rs", UID: types.UID("replacement-rs-uid")}}},
			wantOwnerKind: "ReplicaSet",
			wantOwnerName: "copy-rs",
			wantOwned:     true,
		},
		{
			name: "Replacement Deployment UID falls back to verified ReplicaSet",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "copy-tool", Labels: map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: "session-123"},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "copy-rs", UID: types.UID("rs-uid"), Controller: &controller}},
			}},
			objects: []runtime.Object{
				&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
					Namespace: "system", Name: "copy-rs", UID: types.UID("rs-uid"),
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "copy-deployment", UID: types.UID("old-deployment-uid"), Controller: &controller}},
				}},
				&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "copy-deployment", UID: types.UID("replacement-deployment-uid")}},
			},
			wantOwnerKind: "ReplicaSet",
			wantOwnerName: "copy-rs",
			wantOwned:     true,
			wantVerified:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "data-migrated", UID: types.UID("destination-pvc-uid")}}
			test.pod.Namespace = pvc.Namespace
			test.pod.Spec.NodeName = "node-a"
			test.pod.Spec.Volumes = []corev1.Volume{{VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}}
			test.pod.Status.Phase = corev1.PodFailed
			objects := append([]runtime.Object{pvc, test.pod}, test.objects...)
			service := &Service{client: fake.NewClientset(objects...)}

			_, err := service.inspectPVCUnused(context.Background(), domain.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID}, "session-123")
			var blocker *CleanupPodBlockerError
			if !errors.As(err, &blocker) {
				t.Fatalf("error=%T %v", err, err)
			}
			if blocker.OwnerKind != test.wantOwnerKind || blocker.OwnerName != test.wantOwnerName || blocker.SessionOwned != test.wantOwned || blocker.OwnerVerified != test.wantVerified || !blocker.Terminal {
				t.Fatalf("blocker=%+v", blocker)
			}
		})
	}
}

func TestCleanupPVMigratePodOwnershipUsesSessionOperationID(t *testing.T) {
	session := appTestSession()
	session.Status.Volumes[0].Sync.Attempts = 1
	operationID := copyengine.OperationID(copyengine.Request{
		SessionID: session.ID,
		Source:    session.Spec.Volumes[0].SourcePVC,
		Mode:      copyengine.ModeFinal,
		Attempt:   1,
	})
	otherOperationID := copyengine.OperationID(copyengine.Request{
		SessionID: "other-session",
		Source:    session.Spec.Volumes[0].SourcePVC,
		Mode:      copyengine.ModeFinal,
		Attempt:   1,
	})
	for _, test := range []struct {
		name        string
		instance    string
		withSession bool
		wantOwned   bool
	}{
		{name: "current session", instance: "pv-migrate-" + operationID + "-clusterip", withSession: true, wantOwned: true},
		{name: "other operation", instance: "pv-migrate-" + otherOperationID + "-clusterip", withSession: true, wantOwned: false},
		{name: "operation prefix collision", instance: "pv-migrate-" + operationID + "extra-clusterip", withSession: true, wantOwned: false},
		{name: "orphan cleanup has no operation identity", instance: "pv-migrate-" + operationID + "-clusterip", wantOwned: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "data-migrated", UID: types.UID("destination-pvc-uid")}}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: pvc.Namespace,
					Name:      "copy-tool",
					Labels: map[string]string{
						kube.AppInstanceLabel:  test.instance,
						kube.AppComponentLabel: "rsync",
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "node-a",
					Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
					}}},
				},
				Status: corev1.PodStatus{Phase: corev1.PodFailed},
			}
			service := &Service{client: fake.NewClientset(pvc, pod)}
			var err error
			if test.withSession {
				_, err = service.inspectPVCUnusedForSession(context.Background(), domain.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID}, session)
			} else {
				_, err = service.inspectPVCUnused(context.Background(), domain.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID}, session.ID)
			}
			var blocker *CleanupPodBlockerError
			if !errors.As(err, &blocker) {
				t.Fatalf("error=%T %v", err, err)
			}
			if blocker.SessionOwned != test.wantOwned {
				t.Fatalf("SessionOwned=%t, want %t", blocker.SessionOwned, test.wantOwned)
			}
		})
	}
}

func TestCleanupIgnoresUnscheduledTerminalPVCConsumers(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("temporary-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       session.Spec.Volumes[0].DestinationPVC.UID,
		Labels:    map[string]string{kube.SessionKey: session.ID},
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: pvc.Namespace, Name: "unscheduled-terminal"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
		}}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
	client := fake.NewClientset(pvc, pod)
	service := &Service{client: client, store: &memoryStore{}}
	if err := service.ValidateCleanup(ctx, session, CleanupOptions{DeleteTemporary: true}); err != nil {
		t.Fatalf("validate cleanup error=%v", err)
	}
}

func TestCleanupBlocksRunningPVCConsumer(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("temporary-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       session.Spec.Volumes[0].DestinationPVC.UID,
		Labels:    map[string]string{kube.SessionKey: session.ID},
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: pvc.Namespace, Name: "running-consumer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
		}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(pvc, pod)
	service := &Service{client: client, store: &memoryStore{}}
	err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true})
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("cleanup category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("running consumer PVC was changed: %v", err)
	}
}

func TestCleanupBlocksAttachedDestinationPV(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("temporary-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       session.Spec.Volumes[0].DestinationPVC.UID,
		Labels:    map[string]string{kube.SessionKey: session.ID},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"}}
	pvName := "pv-destination"
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "attachment"},
		Spec:       storagev1.VolumeAttachmentSpec{Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName}, NodeName: "node-a"},
		Status:     storagev1.VolumeAttachmentStatus{Attached: true},
	}
	client := fake.NewClientset(pvc, attachment)
	service := &Service{client: client, store: &memoryStore{}}
	err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true})
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("cleanup category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("attached destination PVC was changed: %v", err)
	}
}

func TestValidateCleanupDiscoversDestinationRefsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	pvcUID := types.UID("validate-destination-pvc-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       pvcUID,
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: "destination",
		},
		Annotations: map[string]string{
			kube.SessionKey:             session.ID,
			kube.SourcePVCUIDAnnotation: string(session.Spec.Volumes[0].SourcePVC.UID),
		},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"}}
	pv := managedPV("pv-destination", "validate-destination-pv-uid", session.ID, "destination", corev1.VolumeReleased)
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvcUID}
	client := fake.NewClientset(pvc, pv)
	service := &Service{client: client, store: &memoryStore{}}

	if err := service.ValidateCleanup(ctx, session, CleanupOptions{DeleteTemporary: true, DeleteRollback: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("validation removed destination PVC: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("validation removed destination PV: %v", err)
	}
}

func TestValidateCleanupAccountsForTemporaryPVCDeletionBeforePVDeletion(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("temporary-pvc-uid")
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("destination-pv-uid")}
	pv := managedPV("pv-destination", "destination-pv-uid", session.ID, "destination", corev1.VolumeBound)
	pv.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
		Name:      session.Spec.Volumes[0].DestinationPVC.Name,
		UID:       session.Spec.Volumes[0].DestinationPVC.UID,
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace,
			Name:      session.Spec.Volumes[0].DestinationPVC.Name,
			UID:       session.Spec.Volumes[0].DestinationPVC.UID,
			Labels:    map[string]string{kube.SessionKey: session.ID},
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
	}
	service := &Service{client: fake.NewClientset(pvc, pv), store: &memoryStore{}}

	options := CleanupOptions{DeleteTemporary: true, DeleteRollback: true}
	if err := service.ValidateCleanup(ctx, session, options); err != nil {
		t.Fatal(err)
	}
	if _, err := service.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("validation mutated the temporary PVC: %v", err)
	}

	pv.Spec.ClaimRef.UID = types.UID("replacement-pvc-uid")
	service = &Service{client: fake.NewClientset(pvc, pv), store: &memoryStore{}}
	if err := service.ValidateCleanup(ctx, session, options); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("replacement claim category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCleanupRollbackPVRequiresOwnershipRoleAndReleasedState(t *testing.T) {
	tests := []struct {
		name         string
		uid          string
		sessionID    string
		role         string
		phase        corev1.PersistentVolumePhase
		wantCategory domain.ErrorCategory
	}{
		{name: "replacement PV", uid: "replacement-uid", sessionID: "session-123", role: "rollback", phase: corev1.VolumeReleased, wantCategory: domain.ErrorConflict},
		{name: "foreign PV", uid: "source-pv-uid", sessionID: "another-session", role: "rollback", phase: corev1.VolumeReleased, wantCategory: domain.ErrorConflict},
		{name: "active role", uid: "source-pv-uid", sessionID: "session-123", role: "active", phase: corev1.VolumeReleased, wantCategory: domain.ErrorConflict},
		{name: "still bound", uid: "source-pv-uid", sessionID: "session-123", role: "rollback", phase: corev1.VolumeBound, wantCategory: domain.ErrorPrecondition},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := appTestSession()
			session.Status.Phase = domain.PhaseCompleted
			client := fake.NewClientset(managedPV("pv-source", test.uid, test.sessionID, test.role, test.phase))
			service := &Service{client: client, store: &memoryStore{}}

			err := service.Cleanup(context.Background(), session, CleanupOptions{DeleteRollback: true})
			if domain.CategoryOf(err) != test.wantCategory {
				t.Fatalf("category=%s want=%s error=%v", domain.CategoryOf(err), test.wantCategory, err)
			}
		})
	}
}

func TestValidateCleanupChecksActivePVWhenRollbackPVIsMissing(t *testing.T) {
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("destination-pv-uid")}
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	active := managedPV("pv-destination", "replacement-uid", session.ID, "active", corev1.VolumeReleased)
	service := &Service{client: fake.NewClientset(active), store: &memoryStore{}}

	err := service.ValidateCleanup(context.Background(), session, CleanupOptions{DeleteRollback: true, Finalize: true})
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCleanupWaitsForBoundPVAfterClaimDeletion(t *testing.T) {
	pv := managedPV("pv-destination", "dest-pv-uid", "session-123", "destination", corev1.VolumeBound)
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: "system", Name: "data-migrated", UID: types.UID("temporary-pvc-uid")}
	client := fake.NewClientset(pv)
	getCalls := 0
	client.PrependReactor("get", "persistentvolumes", func(clienttesting.Action) (bool, runtime.Object, error) {
		getCalls++
		if getCalls != 2 {
			return false, nil, nil
		}
		resource := corev1.SchemeGroupVersion.WithResource("persistentvolumes")
		stored, err := client.Tracker().Get(resource, "", pv.Name)
		if err != nil {
			return true, nil, err
		}
		current := stored.(*corev1.PersistentVolume).DeepCopy()
		current.Status.Phase = corev1.VolumeReleased
		if err := client.Tracker().Update(resource, current, ""); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
	service := &Service{client: client, store: &memoryStore{}}

	err := service.deleteRollbackPV(context.Background(), "session-123", domain.ObjectReference{Name: pv.Name, UID: pv.UID}, "destination", nil)
	if err != nil {
		t.Fatal(err)
	}
	if getCalls < 2 {
		t.Fatalf("PV reads=%d want at least 2", getCalls)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("rollback PV still exists: %v", err)
	}
}

func TestCleanupFinalizeRequiresRecordedPolicyAndOwnedActivePV(t *testing.T) {
	t.Run("missing policy", func(t *testing.T) {
		session := appTestSession()
		session.Status.Phase = domain.PhaseCompleted
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("dest-pv-uid")}
		service := &Service{client: fake.NewClientset(), store: &memoryStore{}}

		err := service.Cleanup(context.Background(), session, CleanupOptions{Finalize: true})
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("foreign active PV", func(t *testing.T) {
		session := appTestSession()
		session.Status.Phase = domain.PhaseCompleted
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("dest-pv-uid")}
		session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
		client := fake.NewClientset(managedPV("pv-destination", "dest-pv-uid", "another-session", "active", corev1.VolumeBound))
		service := &Service{client: client, store: &memoryStore{}}

		err := service.Cleanup(context.Background(), session, CleanupOptions{Finalize: true})
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestCleanupRenameFinalizesSourcePVWithoutRollbackDeletion(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Spec = domain.NewSessionSpec(domain.OperationRename, session.Spec.SessionCommon, session.Spec.Workload(), false, domain.SessionWorkflowOptions{})
	session.Status.Phase = domain.PhaseCompleted
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		Namespace: "app", Name: "renamed-data", UID: types.UID("renamed-pvc-uid"),
	}
	client := fake.NewClientset(
		managedPV("pv-source", "source-pv-uid", session.ID, "rename", corev1.VolumeBound),
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "renamed-data", UID: types.UID("renamed-pvc-uid"),
			Annotations: map[string]string{kube.SessionKey: session.ID},
		}},
	)
	store := &memoryStore{}
	service := &Service{client: client, store: store}

	if err := service.Cleanup(ctx, session, CleanupOptions{Finalize: true, DeleteSession: true}); err != nil {
		t.Fatal(err)
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete || pv.Labels[kube.SessionKey] != "" {
		t.Fatalf("finalized rename PV=%+v", pv)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "renamed-data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations[kube.SessionKey] != "" || store.deletes != 1 {
		t.Fatalf("PVC annotations=%v session deletes=%d", pvc.Annotations, store.deletes)
	}
}

func TestCleanupFinalizeReleasesPVOnlyAfterPVCCheckpoint(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("dest-pv-uid")}
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
	}
	client := fake.NewClientset(
		managedPV("pv-destination", "dest-pv-uid", session.ID, "active", corev1.VolumeBound),
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
			Annotations: map[string]string{kube.SessionKey: session.ID},
		}},
	)
	failed := false
	client.PrependReactor("update", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		if !failed {
			failed = true
			return true, nil, errors.New("injected PVC checkpoint failure")
		}
		return false, nil, nil
	})
	service := &Service{client: client, store: &memoryStore{}}

	if err := service.Cleanup(ctx, session, CleanupOptions{Finalize: true}); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-destination", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pv.Labels[kube.SessionKey] != session.ID {
		t.Fatalf("PV ownership cleared before PVC checkpoint: labels=%v", pv.Labels)
	}
	if err := service.Cleanup(ctx, session, CleanupOptions{Finalize: true}); err != nil {
		t.Fatal(err)
	}
	pv, err = client.CoreV1().PersistentVolumes().Get(ctx, "pv-destination", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pv.Labels[kube.SessionKey] != "" {
		t.Fatalf("PV ownership remains after retry: labels=%v", pv.Labels)
	}
}
