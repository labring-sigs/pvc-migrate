package app

import (
	"context"
	"errors"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
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

func TestCleanupIgnoresTerminalPVCConsumers(t *testing.T) {
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
				Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
				}}}},
				Status: corev1.PodStatus{Phase: phase},
			}
			client := fake.NewClientset(pvc, pod)
			service := &Service{client: client, store: &memoryStore{}}
			if err := service.Cleanup(ctx, session, CleanupOptions{DeleteTemporary: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("temporary PVC still exists: %v", err)
			}
		})
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

	err := service.deleteRollbackPV(context.Background(), "session-123", domain.ObjectReference{Name: pv.Name, UID: pv.UID}, "destination")
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
	session.Spec = domain.NewSessionSpec(domain.OperationRename, session.Spec.SessionCommon, session.Spec.Workload(), false)
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
