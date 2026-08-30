package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func orphanFixture() (*fake.Clientset, OrphanCleanupOptions) {
	const sessionID = "orphan-session"

	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "app",
		Name:            "data",
		UID:             types.UID("pvc-uid"),
		ResourceVersion: "1",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        sessionID,
			kube.ResourceRoleLabel: "active",
		},
		Annotations: map[string]string{
			kube.SessionKey:           sessionID,
			kube.RollbackPVAnnotation: "pv-rollback",
			kube.SourcePVAnnotation:   "pv-source",
		},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-active"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	active := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name:            "pv-active",
		UID:             types.UID("active-uid"),
		ResourceVersion: "2",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        sessionID,
			kube.ResourceRoleLabel: "active",
		},
		Annotations: map[string]string{
			kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
			kube.PairedPVAnnotation:       "pv-rollback",
		},
	}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, ClaimRef: &corev1.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")}}}
	rollback := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name:            "pv-rollback",
		UID:             types.UID("rollback-uid"),
		ResourceVersion: "3",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        sessionID,
			kube.ResourceRoleLabel: "rollback",
		},
		Annotations: map[string]string{
			kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
			kube.PairedPVAnnotation:       "pv-active",
		},
	}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased}}
	client := fake.NewClientset(pvc, active, rollback)
	assignLeaseUIDs(client)

	return client, OrphanCleanupOptions{
		SessionID:        sessionID,
		SessionNamespace: "system",
		SourceNamespace:  "app",
		SourcePVC:        "data",
	}
}

func preActivationOrphanFixture() (*fake.Clientset, OrphanCleanupOptions) {
	const sessionID = "pre-orphan-session"

	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "source", UID: types.UID("source-pvc-uid"), ResourceVersion: "10",
		Annotations: map[string]string{kube.SessionKey: sessionID},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-source"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	sourcePV := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name:            "pv-source",
		UID:             types.UID("source-pv-uid"),
		ResourceVersion: "11",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        sessionID,
			kube.ResourceRoleLabel: kube.ResourceRoleSource,
		},
		Annotations: map[string]string{
			kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
		},
	}, Spec: corev1.PersistentVolumeSpec{
		PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		ClaimRef: &corev1.ObjectReference{
			Namespace: sourcePVC.Namespace,
			Name:      sourcePVC.Name,
			UID:       sourcePVC.UID,
		},
	}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	destinationPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "system",
		Name:            "destination",
		UID:             types.UID("destination-pvc-uid"),
		ResourceVersion: "20",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        sessionID,
			kube.ResourceRoleLabel: kube.ResourceRoleDestination,
		},
		Annotations: map[string]string{
			kube.SessionKey:             sessionID,
			kube.SourcePVCUIDAnnotation: string(sourcePVC.UID),
		},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	destinationPV := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name:            "pv-destination",
		UID:             types.UID("destination-pv-uid"),
		ResourceVersion: "21",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        sessionID,
			kube.ResourceRoleLabel: kube.ResourceRoleDestination,
		},
		Annotations: map[string]string{
			kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
		},
	}, Spec: corev1.PersistentVolumeSpec{
		PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		ClaimRef: &corev1.ObjectReference{
			Namespace: destinationPVC.Namespace,
			Name:      destinationPVC.Name,
			UID:       destinationPVC.UID,
		},
	}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	reservationPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: destinationPVC.Namespace,
		Name:      "reservation",
		UID:       "reservation-uid",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        sessionID,
			kube.ResourceRoleLabel: kube.ResourceRoleReservationConsumer,
		},
	}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{
		{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: destinationPVC.Name,
				},
			},
		},
	}}}
	client := fake.NewClientset(sourcePVC, sourcePV, destinationPVC, destinationPV, reservationPod)
	assignLeaseUIDs(client)

	return client, OrphanCleanupOptions{
		SessionID:        sessionID,
		SessionNamespace: "system",
		SourceNamespace:  "app",
		SourcePVC:        "source",
	}
}

func assignLeaseUIDs(client *fake.Clientset) {
	client.PrependReactor(
		"create",
		"leases",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			lease, err := testutil.ActionObject[*coordinationv1.Lease](action)
			if err != nil {
				return true, nil, err
			}

			if lease.UID == "" {
				lease.UID = types.UID("lease-" + lease.Name)
			}

			return false, nil, nil
		},
	)
}

func releaseDestinationPVOnPVCDelete(t *testing.T, client *fake.Clientset) {
	t.Helper()
	client.PrependReactor(
		"delete",
		"persistentvolumeclaims",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			deleted := testutil.MustType[k8stesting.DeleteAction](t, action)
			if deleted.GetNamespace() != "system" || deleted.GetName() != "destination" {
				return false, nil, nil
			}

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), "", "pv-destination")
			if err != nil {
				return true, nil, err
			}

			pv := testutil.MustType[*corev1.PersistentVolume](t, object)

			pv.Status.Phase = corev1.VolumeReleased
			if err := client.Tracker().
				Update(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), pv, ""); err != nil {
				return true, nil, err
			}

			return false, nil, nil
		},
	)
}

func TestPlanOrphanCleanupRecognizesPreActivationResources(t *testing.T) {
	client, options := preActivationOrphanFixture()

	plan, err := (&Service{client: client, store: &memoryStore{}}).PlanOrphanCleanup(
		context.Background(),
		options,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || plan.Mode != domain.OrphanCleanupPreActivation || plan.PreActivation == nil {
		t.Fatalf("plan=%#v", plan)
	}

	resources := plan.PreActivation
	if resources.SourcePV.Name != "pv-source" || resources.DestinationPVC.Name != "destination" ||
		resources.DestinationPV.Name != "pv-destination" {
		t.Fatalf("resources=%#v", resources)
	}
}

func TestCleanupOrphanRemovesPreActivationDestinationAndFinalizesSource(t *testing.T) {
	client, options := preActivationOrphanFixture()
	releaseDestinationPVOnPVCDelete(t, client)
	service := &Service{client: client, store: kube.NewConfigMapSessionStore(client)}

	plan, err := service.CleanupOrphan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan == nil || !plan.Ready || plan.Mode != domain.OrphanCleanupPreActivation {
		t.Fatalf("plan=%#v", plan)
	}

	if _, err := client.CoreV1().
		PersistentVolumeClaims("system").
		Get(context.Background(), "destination", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("destination PVC still exists: %v", err)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-destination", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("destination PV still exists: %v", err)
	}

	if _, err := client.CoreV1().
		Pods("system").
		Get(context.Background(), "reservation", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("reservation Pod still exists: %v", err)
	}

	sourcePVC, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "source", metav1.GetOptions{})
	if err != nil || sourcePVC.Annotations[kube.SessionKey] != "" {
		t.Fatalf("source PVC was not finalized: pvc=%#v err=%v", sourcePVC, err)
	}

	sourcePV, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if sourcePV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete ||
		sourcePV.Labels[kube.SessionKey] != "" ||
		sourcePV.Annotations[kube.OriginalPolicyAnnotation] != "" {
		t.Fatalf("source PV was not finalized: %#v", sourcePV)
	}

	if _, err := client.CoordinationV1().
		Leases(options.SessionNamespace).
		Get(context.Background(), kube.SessionLockName(options.SessionID), metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("session Lease still exists: %v", err)
	}
}

func TestPlanPreActivationOrphanRejectsUnsafeDestinationStates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
		want   string
	}{
		{
			name: "unexpected reclaim policy",
			mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRecycle
			},
			want: "Retain or its recorded original policy",
		},
		{
			name: "claim UID mismatch",
			mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.Spec.ClaimRef.UID = "replacement"
			},
			want: "claimRef does not match",
		},
		{
			name: "foreign PV ownership",
			mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.Labels[kube.SessionKey] = "other"
			},
			want: "ownership changed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, options := preActivationOrphanFixture()
			pvc, _ := client.CoreV1().
				PersistentVolumeClaims("system").
				Get(context.Background(), "destination", metav1.GetOptions{})
			pv, _ := client.CoreV1().
				PersistentVolumes().
				Get(context.Background(), "pv-destination", metav1.GetOptions{})
			test.mutate(pvc, pv)

			if _, err := client.CoreV1().
				PersistentVolumeClaims("system").
				Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			if _, err := client.CoreV1().
				PersistentVolumes().
				Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			plan, err := (&Service{client: client, store: &memoryStore{}}).PlanOrphanCleanup(
				context.Background(),
				options,
			)
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready || !containsOrphanCheck(plan, test.want) {
				t.Fatalf("plan=%#v", plan)
			}
		})
	}
}

func TestPlanPreActivationOrphanRejectsExternalDestinationConsumer(t *testing.T) {
	client, options := preActivationOrphanFixture()

	external := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "external"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "destination",
					},
				},
			},
		}},
	}
	if _, err := client.CoreV1().
		Pods("system").
		Create(context.Background(), external, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := (&Service{client: client, store: &memoryStore{}}).PlanOrphanCleanup(
		context.Background(),
		options,
	)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !containsOrphanCheck(plan, "referenced by Pod external") {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestCleanupPreActivationOrphanKeepsLeaseForOtherVolumes(t *testing.T) {
	client, options := preActivationOrphanFixture()
	sourcePVC, _ := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "source", metav1.GetOptions{})
	sourcePV, _ := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-source", metav1.GetOptions{})
	destinationPVC, _ := client.CoreV1().
		PersistentVolumeClaims("system").
		Get(context.Background(), "destination", metav1.GetOptions{})
	destinationPV, _ := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-destination", metav1.GetOptions{})
	sourcePVC = sourcePVC.DeepCopy()
	sourcePVC.Name, sourcePVC.UID, sourcePVC.ResourceVersion = "source-2", "source-pvc-uid-2", "30"
	sourcePVC.Spec.VolumeName = "pv-source-2"
	sourcePV = sourcePV.DeepCopy()
	sourcePV.Name, sourcePV.UID, sourcePV.ResourceVersion = "pv-source-2", "source-pv-uid-2", "31"
	sourcePV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: sourcePVC.Namespace,
		Name:      sourcePVC.Name,
		UID:       sourcePVC.UID,
	}
	destinationPVC = destinationPVC.DeepCopy()
	destinationPVC.Name, destinationPVC.UID, destinationPVC.ResourceVersion = "destination-2", "destination-pvc-uid-2", "40"
	destinationPVC.Spec.VolumeName = "pv-destination-2"
	destinationPVC.Annotations[kube.SourcePVCUIDAnnotation] = string(sourcePVC.UID)
	destinationPV = destinationPV.DeepCopy()
	destinationPV.Name, destinationPV.UID, destinationPV.ResourceVersion = "pv-destination-2", "destination-pv-uid-2", "41"
	destinationPV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: destinationPVC.Namespace,
		Name:      destinationPVC.Name,
		UID:       destinationPVC.UID,
	}

	if _, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), sourcePVC, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Create(context.Background(), sourcePV, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		PersistentVolumeClaims("system").
		Create(context.Background(), destinationPVC, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Create(context.Background(), destinationPV, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	releaseDestinationPVOnPVCDelete(t, client)
	service := &Service{client: client, store: kube.NewConfigMapSessionStore(client)}

	plan, err := service.CleanupOrphan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || !containsPassedOrphanCheck(plan, "additional destination PVCs") {
		t.Fatalf("plan=%#v", plan)
	}

	if _, err := client.CoordinationV1().
		Leases(options.SessionNamespace).
		Get(context.Background(), kube.SessionLockName(options.SessionID), metav1.GetOptions{}); err != nil {
		t.Fatalf("session Lease should remain for other volumes: %v", err)
	}
}

func TestCleanupPreActivationOrphanResumesAfterDestinationPVCDeletion(t *testing.T) {
	client, options := preActivationOrphanFixture()
	if err := client.Tracker().
		Delete(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), "system", "destination"); err != nil {
		t.Fatal(err)
	}

	pv, _ := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-destination", metav1.GetOptions{})

	pv.Status.Phase = corev1.VolumeReleased
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	service := &Service{client: client, store: &memoryStore{}}

	plan, err := service.PlanOrphanCleanup(context.Background(), options)
	if err != nil || !plan.Ready || plan.PreActivation == nil ||
		plan.PreActivation.DestinationPV.Name != "pv-destination" {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}

	if _, err := service.CleanupOrphan(context.Background(), options); err != nil {
		t.Fatal(err)
	}
}

func TestPlanOrphanCleanupRequiresExactResourceRelationship(t *testing.T) {
	client, options := orphanFixture()
	service := &Service{client: client, store: &memoryStore{}}

	plan, err := service.PlanOrphanCleanup(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || len(plan.Checks) < 2 {
		t.Fatalf("plan=%#v", plan)
	}

	if plan.Mode != domain.OrphanCleanupPostActivation || plan.PostActivation == nil ||
		plan.PostActivation.ActivePV.Name != "pv-active" ||
		plan.PostActivation.RollbackPV.Name != "pv-rollback" {
		t.Fatalf("refs=%#v", plan)
	}
}

func TestCleanupOrphanDeletesOnlyReleasedRollbackAndFinalizesMetadata(t *testing.T) {
	client, options := orphanFixture()
	service := &Service{client: client, store: &memoryStore{}}

	plan, err := service.CleanupOrphan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan == nil || !plan.Ready {
		t.Fatalf("result=%#v", plan)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-rollback", metav1.GetOptions{}); err == nil {
		t.Fatal("rollback PV still exists")
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pvc.Annotations[kube.SessionKey] != "" || pvc.Labels[kube.SessionKey] != "" {
		t.Fatalf("PVC ownership remains: labels=%v annotations=%v", pvc.Labels, pvc.Annotations)
	}

	active, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-active", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if active.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete ||
		active.Labels[kube.SessionKey] != "" ||
		active.Annotations[kube.OriginalPolicyAnnotation] != "" {
		t.Fatalf("active PV remains managed: %#v", active)
	}
}

func TestCleanupOrphanAcceptsAnnotationOnlyPVCOwnership(t *testing.T) {
	client, options := orphanFixture()
	pvc, _ := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data", metav1.GetOptions{})
	delete(pvc.Labels, kube.SessionKey)

	if _, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	service := &Service{client: client, store: &memoryStore{}}

	plan, err := service.PlanOrphanCleanup(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("plan=%#v", plan)
	}

	if _, err := service.CleanupOrphan(context.Background(), options); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupOrphanResumesAfterRollbackAndPVCCheckpoints(t *testing.T) {
	client, options := orphanFixture()
	if err := client.CoreV1().
		PersistentVolumes().
		Delete(context.Background(), "pv-rollback", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	pvc, _ := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data", metav1.GetOptions{})
	delete(pvc.Labels, kube.ManagedByLabel)
	delete(pvc.Labels, kube.SessionKey)
	delete(pvc.Labels, kube.ResourceRoleLabel)
	delete(pvc.Annotations, kube.SessionKey)
	delete(pvc.Annotations, kube.RollbackPVAnnotation)
	delete(pvc.Annotations, kube.SourcePVAnnotation)
	delete(pvc.Annotations, kube.SourcePVCUIDAnnotation)

	if _, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	service := &Service{client: client, store: &memoryStore{}}

	plan, err := service.PlanOrphanCleanup(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("partial cleanup plan=%#v", plan)
	}

	if _, err := service.CleanupOrphan(context.Background(), options); err != nil {
		t.Fatal(err)
	}
}

func TestPlanOrphanCleanupHandlesRollbackPVAlreadyGone(t *testing.T) {
	client, options := orphanFixture()
	client.PrependReactor(
		"get",
		"persistentvolumes",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			get, ok := action.(k8stesting.GetAction)
			if ok && get.GetName() == "pv-rollback" {
				return true, nil, apierrors.NewNotFound(
					schema.GroupResource{Resource: "persistentvolumes"},
					get.GetName(),
				)
			}

			return false, nil, nil
		},
	)
	service := &Service{client: client, store: &memoryStore{}}

	plan, err := service.PlanOrphanCleanup(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || plan.PostActivation == nil ||
		plan.PostActivation.RollbackPV.Name != "pv-rollback" {
		t.Fatalf("plan=%#v", plan)
	}

	if _, err := service.CleanupOrphan(context.Background(), options); err != nil {
		t.Fatal(err)
	}
}

func TestPlanOrphanCleanupRejectsUnsafeStates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *corev1.PersistentVolume)
		want   string
	}{
		{
			name:   "session exists",
			mutate: func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *corev1.PersistentVolume) {},
			want:   "session ConfigMap",
		},
		{
			name: "rollback bound",
			mutate: func(_ *corev1.PersistentVolumeClaim, _, rollback *corev1.PersistentVolume) {
				rollback.Status.Phase = corev1.VolumeBound
			},
			want: "Released or Available",
		},
		{
			name: "rollback unexpected policy",
			mutate: func(_ *corev1.PersistentVolumeClaim, _, rollback *corev1.PersistentVolume) {
				rollback.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRecycle
			},
			want: "Retain or its recorded original policy",
		},
		{
			name: "active claim UID missing",
			mutate: func(_ *corev1.PersistentVolumeClaim, active, _ *corev1.PersistentVolume) {
				active.Spec.ClaimRef.UID = ""
			},
			want: "claimRef does not match",
		},
		{
			name: "PVC and active claim UIDs missing",
			mutate: func(pvc *corev1.PersistentVolumeClaim, active, _ *corev1.PersistentVolume) {
				pvc.UID = ""
				active.Spec.ClaimRef.UID = ""
			},
			want: "claimRef does not match",
		},
		{
			name: "missing original policy",
			mutate: func(_ *corev1.PersistentVolumeClaim, active, _ *corev1.PersistentVolume) {
				active.Annotations = nil
			},
			want: "original-reclaim-policy",
		},
		{
			name: "missing rollback original policy",
			mutate: func(_ *corev1.PersistentVolumeClaim, _, rollback *corev1.PersistentVolume) {
				delete(rollback.Annotations, kube.OriginalPolicyAnnotation)
			},
			want: "no valid original reclaim policy",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, options := orphanFixture()
			pvc, _ := client.CoreV1().
				PersistentVolumeClaims("app").
				Get(context.Background(), "data", metav1.GetOptions{})
			active, _ := client.CoreV1().
				PersistentVolumes().
				Get(context.Background(), "pv-active", metav1.GetOptions{})
			rollback, _ := client.CoreV1().
				PersistentVolumes().
				Get(context.Background(), "pv-rollback", metav1.GetOptions{})
			test.mutate(pvc, active, rollback)

			if test.name == "session exists" {
				session := domain.NewSession(
					options.SessionID,
					domain.NewOfflineMigrationSessionSpec(
						domain.SessionCommon{
							SourceNamespace:      "app",
							TemporaryNamespace:   "system",
							DestinationNamespace: "app",
							SessionNamespace:     "system",
							Volumes: []domain.VolumeSpec{
								{
									SourcePVC: domain.ObjectReference{
										Namespace: "app",
										Name:      "data",
										UID:       pvc.UID,
									},
									SourcePV: domain.ObjectReference{
										Name: "pv-active",
										UID:  active.UID,
									},
									DestinationPVC: domain.ObjectReference{
										Namespace: "system",
										Name:      "data-migrated",
									},
								},
							},
						},
						domain.SessionWorkflowOptions{},
					),
					time.Now(),
				)
				if err := kube.NewConfigMapSessionStore(client).
					Create(context.Background(), session); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := client.CoreV1().
				PersistentVolumeClaims("app").
				Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			if _, err := client.CoreV1().
				PersistentVolumes().
				Update(context.Background(), active, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			if _, err := client.CoreV1().
				PersistentVolumes().
				Update(context.Background(), rollback, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			plan, err := (&Service{client: client, store: &memoryStore{}}).PlanOrphanCleanup(
				context.Background(),
				options,
			)
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready || !containsOrphanCheck(plan, test.want) {
				t.Fatalf("plan=%#v", plan)
			}
		})
	}
}

func TestDeleteOrphanRollbackPVRejectsUnexpectedPolicy(t *testing.T) {
	client, options := orphanFixture()

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-rollback", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRecycle
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	service := &Service{client: client, store: &memoryStore{}}

	err = service.deleteOrphanRollbackPV(
		context.Background(),
		options.SessionID,
		kube.PVReference(pv),
		corev1.PersistentVolumeReclaimDelete,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), pv.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("rollback PV was deleted: %v", err)
	}
}

func TestDeleteOrphanRollbackPVRejectsChangeAfterPolicyRestore(t *testing.T) {
	client, options := orphanFixture()

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-rollback", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	client.PrependReactor(
		"delete",
		"persistentvolumes",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			deleted := testutil.MustType[k8stesting.DeleteAction](t, action)

			preconditions := deleted.GetDeleteOptions().Preconditions
			if preconditions == nil || preconditions.ResourceVersion == nil {
				return false, nil, nil
			}

			resource := corev1.SchemeGroupVersion.WithResource("persistentvolumes")

			stored, err := client.Tracker().Get(resource, "", pv.Name)
			if err != nil {
				return true, nil, err
			}

			changed := testutil.MustType[*corev1.PersistentVolume](t, stored).DeepCopy()

			changed.ResourceVersion = "concurrent-update"
			if err := client.Tracker().Update(resource, changed, ""); err != nil {
				return true, nil, err
			}

			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "persistentvolumes"},
				pv.Name,
				errors.New("simulated concurrent PV update"),
			)
		},
	)
	service := &Service{client: client, store: &memoryStore{}}

	err = service.deleteOrphanRollbackPV(
		context.Background(),
		options.SessionID,
		kube.PVReference(pv),
		corev1.PersistentVolumeReclaimDelete,
	)
	if err == nil {
		t.Fatal("delete succeeded after the validated orphan PV changed")
	}

	if _, err := client.CoreV1().PersistentVolumes().Get(
		context.Background(),
		pv.Name,
		metav1.GetOptions{},
	); err != nil {
		t.Fatalf("orphan rollback PV was deleted after a concurrent change: %v", err)
	}
}

func TestCleanupOrphanDeletesLeaseWhileLockIsHeld(t *testing.T) {
	client, options := orphanFixture()

	service := &Service{client: client, store: kube.NewConfigMapSessionStore(client)}
	if _, err := service.CleanupOrphan(context.Background(), options); err != nil {
		t.Fatal(err)
	}

	deleteIndex, releaseIndex := -1, -1
	for index, action := range client.Actions() {
		if action.GetResource().Resource != "leases" {
			continue
		}

		switch action.GetVerb() {
		case "delete":
			deleteIndex = index
		case "update":
			releaseIndex = index
		}
	}

	if deleteIndex < 0 || releaseIndex >= 0 && releaseIndex < deleteIndex {
		t.Fatalf("lease deletion happened after lock release: actions=%v", client.Actions())
	}
}

func containsOrphanCheck(plan *domain.OrphanCleanupPlan, text string) bool {
	for _, check := range plan.Checks {
		if !check.Passed && strings.Contains(check.Message, text) {
			return true
		}
	}

	return false
}

func containsPassedOrphanCheck(plan *domain.OrphanCleanupPlan, text string) bool {
	for _, check := range plan.Checks {
		if check.Passed && strings.Contains(check.Message, text) {
			return true
		}
	}

	return false
}

func TestOrphanPVRoleName(t *testing.T) {
	for role, want := range map[string]string{
		kube.ResourceRoleSource:      "source",
		kube.ResourceRoleDestination: "destination",
		kube.ResourceRoleActive:      "active",
		"unknown":                    "orphan",
	} {
		if got := orphanPVRoleName(role); got != want {
			t.Fatalf("role %q rendered as %q, want %q", role, got, want)
		}
	}
}
