package app

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCleanupFinalizesActivePVAndClosesRollbackWindow(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("dest-pv-uid")}
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid")}
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "data",
				UID:         types.UID("active-pvc-uid"),
				Annotations: map[string]string{kube.SessionAnnotation: session.ID},
			},
		},
		managedPV("pv-destination", "dest-pv-uid", session.ID, "active", corev1.VolumeBound),
		managedPV("pv-source", "source-pv-uid", session.ID, "rollback", corev1.VolumeReleased),
	)
	store := &memoryStore{}
	service := &Service{client: client, store: store}
	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteRollback: true, Finalize: true, DeleteSession: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("rollback PV still exists: %v", err)
	}
	active, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-destination", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if active.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Fatalf("active reclaim policy=%s", active.Spec.PersistentVolumeReclaimPolicy)
	}
	if active.Labels[kube.SessionLabel] != "" || active.Labels[kube.ResourceRoleLabel] != "" || active.Labels[kube.ManagedByLabel] != "" {
		t.Fatalf("active PV ownership labels=%v", active.Labels)
	}
	if active.Annotations[kube.OriginalPolicyAnnotation] != "" || active.Annotations["pvc-migrate.io/paired-pv"] != "" {
		t.Fatalf("active PV migration annotations=%v", active.Annotations)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations[kube.SessionAnnotation] != "" {
		t.Fatalf("active PVC remains owned by %q", pvc.Annotations[kube.SessionAnnotation])
	}
	if store.deletes != 1 {
		t.Fatalf("session deletes=%d", store.deletes)
	}
}

func TestCleanupAbortedSessionReleasesSourceAndDeletesDestination(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("dest-pv-uid")}
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "data",
				UID:         types.UID("source-pvc-uid"),
				Annotations: map[string]string{kube.SessionAnnotation: session.ID},
			},
		},
		managedPV("pv-source", "source-pv-uid", session.ID, "source", corev1.VolumeBound),
		managedPV("pv-destination", "dest-pv-uid", session.ID, "destination", corev1.VolumeReleased),
	)
	store := &memoryStore{}
	service := &Service{client: client, store: store}
	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteRollback: true, Finalize: true, DeleteSession: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-destination", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("destination PV still exists: %v", err)
	}
	source, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if source.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete || source.Labels[kube.SessionLabel] != "" {
		t.Fatalf("finalized source PV=%#v", source)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations[kube.SessionAnnotation] != "" {
		t.Fatalf("source PVC remains owned by %q", pvc.Annotations[kube.SessionAnnotation])
	}
}

func TestCleanupAbortedSessionSkipsReplacedSourceResources(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("dest-pv-uid")}
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("recreated-pvc-uid"),
		}},
		managedPV("pv-destination", "dest-pv-uid", session.ID, "destination", corev1.VolumeReleased),
	)
	store := &memoryStore{}
	service := &Service{client: client, store: store}
	if err := service.Cleanup(ctx, session, CleanupOptions{DeleteRollback: true, Finalize: true, DeleteSession: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{}); err != nil {
		t.Fatalf("recreated workload PVC was changed: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-destination", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("destination PV still exists: %v", err)
	}
	if store.deletes != 1 {
		t.Fatalf("session deletes=%d", store.deletes)
	}
}

func TestCleanupRequiresRollbackClosureBeforeSessionDeletion(t *testing.T) {
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	service := &Service{client: fake.NewClientset(), store: &memoryStore{}}
	err := service.Cleanup(context.Background(), session, CleanupOptions{DeleteSession: true})
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCleanupSingleStageSessionsRemovesDestinationAndFinalizesSource(t *testing.T) {
	for _, tc := range []struct {
		name      string
		operation domain.Operation
		phase     domain.Phase
	}{
		{name: "reserve", operation: domain.OperationReserve, phase: domain.PhaseReserved},
		{name: "copy", operation: domain.OperationCopy, phase: domain.PhaseWarmCopied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			session := appTestSession()
			setSessionOperation(session, tc.operation)
			session.Status.Phase = tc.phase
			session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("destination-pv-uid")}
			session.Spec.Volumes[0].DestinationPVC.UID = types.UID("destination-pvc-uid")
			session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete

			sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid"),
				Annotations: map[string]string{kube.SessionAnnotation: session.ID},
			}}
			destinationPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: "system", Name: "data-migrated", UID: types.UID("destination-pvc-uid"),
				Labels:      map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionLabel: session.ID, kube.ResourceRoleLabel: "destination"},
				Annotations: map[string]string{kube.SessionAnnotation: session.ID},
			}}
			sourcePV := managedPV("pv-source", "source-pv-uid", session.ID, "source", corev1.VolumeBound)
			destinationPV := managedPV("pv-destination", "destination-pv-uid", session.ID, "destination", corev1.VolumeReleased)
			client := fake.NewClientset(sourcePVC, destinationPVC, sourcePV, destinationPV)
			store := &memoryStore{}
			service := &Service{client: client, store: store}
			options := CleanupOptions{DeleteTemporary: true, DeleteRollback: true, Finalize: true, DeleteSession: true}

			if err := service.ValidateCleanup(ctx, session, options); err != nil {
				t.Fatalf("validate cleanup: %v", err)
			}
			if err := service.Cleanup(ctx, session, options); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			if _, err := client.CoreV1().PersistentVolumeClaims("system").Get(ctx, destinationPVC.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("destination PVC still exists: %v", err)
			}
			if _, err := client.CoreV1().PersistentVolumes().Get(ctx, destinationPV.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("destination PV still exists: %v", err)
			}
			finalSourcePVC, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, sourcePVC.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if finalSourcePVC.Annotations[kube.SessionAnnotation] != "" {
				t.Fatalf("source PVC remains owned by %q", finalSourcePVC.Annotations[kube.SessionAnnotation])
			}
			finalSourcePV, err := client.CoreV1().PersistentVolumes().Get(ctx, sourcePV.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if finalSourcePV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete || finalSourcePV.Labels[kube.SessionLabel] != "" {
				t.Fatalf("source PV was not finalized: %#v", finalSourcePV)
			}
			if store.deletes != 1 {
				t.Fatalf("session deletes=%d", store.deletes)
			}
		})
	}
}

func TestCleanupRejectsSingleStageNonTerminalPhase(t *testing.T) {
	for _, operation := range []domain.Operation{domain.OperationReserve, domain.OperationCopy} {
		session := appTestSession()
		setSessionOperation(session, operation)
		session.Status.Phase = domain.PhaseFailed
		service := &Service{client: fake.NewClientset(), store: &memoryStore{}}
		err := service.ValidateCleanup(context.Background(), session, CleanupOptions{})
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("operation=%s category=%s error=%v", operation, domain.CategoryOf(err), err)
		}
	}
}

func managedPV(name, uid, sessionID, role string, phase corev1.PersistentVolumePhase) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             types.UID(uid),
			ResourceVersion: "1",
			Labels: map[string]string{
				kube.ManagedByLabel:    kube.ManagedByValue,
				kube.SessionLabel:      sessionID,
				kube.ResourceRoleLabel: role,
			},
			Annotations: map[string]string{
				kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
				"pvc-migrate.io/paired-pv":    "paired",
			},
		},
		Spec:   corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain},
		Status: corev1.PersistentVolumeStatus{Phase: phase},
	}
}
