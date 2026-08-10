package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
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
		Namespace: "app", Name: "data", UID: types.UID("pvc-uid"), ResourceVersion: "1",
		Labels:      map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: sessionID, kube.ResourceRoleLabel: "active"},
		Annotations: map[string]string{kube.SessionKey: sessionID, kube.RollbackPVAnnotation: "pv-rollback", kube.SourcePVAnnotation: "pv-source"},
	}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-active"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	active := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name: "pv-active", UID: types.UID("active-uid"), ResourceVersion: "2",
		Labels:      map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: sessionID, kube.ResourceRoleLabel: "active"},
		Annotations: map[string]string{kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete), kube.PairedPVAnnotation: "pv-rollback"},
	}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, ClaimRef: &corev1.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")}}}
	rollback := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name: "pv-rollback", UID: types.UID("rollback-uid"), ResourceVersion: "3",
		Labels:      map[string]string{kube.ManagedByLabel: kube.ManagedByValue, kube.SessionKey: sessionID, kube.ResourceRoleLabel: "rollback"},
		Annotations: map[string]string{kube.PairedPVAnnotation: "pv-active"},
	}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased}}
	return fake.NewClientset(pvc, active, rollback), OrphanCleanupOptions{SessionID: sessionID, SessionNamespace: "system", SourceNamespace: "app", SourcePVC: "data"}
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
	if plan.ActivePV.Name != "pv-active" || plan.RollbackPV.Name != "pv-rollback" {
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
	if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-rollback", metav1.GetOptions{}); err == nil {
		t.Fatal("rollback PV still exists")
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations[kube.SessionKey] != "" || pvc.Labels[kube.SessionKey] != "" {
		t.Fatalf("PVC ownership remains: labels=%v annotations=%v", pvc.Labels, pvc.Annotations)
	}
	active, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-active", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if active.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete || active.Labels[kube.SessionKey] != "" || active.Annotations[kube.OriginalPolicyAnnotation] != "" {
		t.Fatalf("active PV remains managed: %#v", active)
	}
}

func TestCleanupOrphanAcceptsAnnotationOnlyPVCOwnership(t *testing.T) {
	client, options := orphanFixture()
	pvc, _ := client.CoreV1().PersistentVolumeClaims("app").Get(context.Background(), "data", metav1.GetOptions{})
	delete(pvc.Labels, kube.SessionKey)
	if _, err := client.CoreV1().PersistentVolumeClaims("app").Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
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
	if err := client.CoreV1().PersistentVolumes().Delete(context.Background(), "pv-rollback", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	pvc, _ := client.CoreV1().PersistentVolumeClaims("app").Get(context.Background(), "data", metav1.GetOptions{})
	delete(pvc.Labels, kube.ManagedByLabel)
	delete(pvc.Labels, kube.SessionKey)
	delete(pvc.Labels, kube.ResourceRoleLabel)
	delete(pvc.Annotations, kube.SessionKey)
	delete(pvc.Annotations, kube.RollbackPVAnnotation)
	delete(pvc.Annotations, kube.SourcePVAnnotation)
	delete(pvc.Annotations, kube.SourcePVCUIDAnnotation)
	if _, err := client.CoreV1().PersistentVolumeClaims("app").Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
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
	client.PrependReactor("get", "persistentvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		get, ok := action.(k8stesting.GetAction)
		if ok && get.GetName() == "pv-rollback" {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumes"}, get.GetName())
		}
		return false, nil, nil
	})
	service := &Service{client: client, store: &memoryStore{}}
	plan, err := service.PlanOrphanCleanup(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Ready || plan.RollbackPV.Name != "pv-rollback" {
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
		{name: "session exists", mutate: func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *corev1.PersistentVolume) {}, want: "session ConfigMap"},
		{name: "rollback bound", mutate: func(_ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, rollback *corev1.PersistentVolume) {
			rollback.Status.Phase = corev1.VolumeBound
		}, want: "Released or Available"},
		{name: "rollback delete policy", mutate: func(_ *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, rollback *corev1.PersistentVolume) {
			rollback.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
		}, want: "reclaim policy must be Retain"},
		{name: "active claim UID missing", mutate: func(_ *corev1.PersistentVolumeClaim, active *corev1.PersistentVolume, _ *corev1.PersistentVolume) {
			active.Spec.ClaimRef.UID = ""
		}, want: "claimRef does not match"},
		{name: "missing original policy", mutate: func(_ *corev1.PersistentVolumeClaim, active *corev1.PersistentVolume, _ *corev1.PersistentVolume) {
			active.Annotations = nil
		}, want: "original-reclaim-policy"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, options := orphanFixture()
			pvc, _ := client.CoreV1().PersistentVolumeClaims("app").Get(context.Background(), "data", metav1.GetOptions{})
			active, _ := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-active", metav1.GetOptions{})
			rollback, _ := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-rollback", metav1.GetOptions{})
			test.mutate(pvc, active, rollback)
			if test.name == "session exists" {
				session := domain.NewSession(options.SessionID, domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app", SessionNamespace: "system", Volumes: []domain.VolumeSpec{{SourcePVC: domain.ObjectReference{Namespace: "app", Name: "data"}, SourcePV: domain.ObjectReference{Name: "pv-active"}}}}, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false), time.Now())
				if err := kube.NewConfigMapSessionStore(client).Create(context.Background(), session); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := client.CoreV1().PersistentVolumeClaims("app").Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().PersistentVolumes().Update(context.Background(), active, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().PersistentVolumes().Update(context.Background(), rollback, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			plan, err := (&Service{client: client, store: &memoryStore{}}).PlanOrphanCleanup(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Ready || !containsOrphanCheck(plan, test.want) {
				t.Fatalf("plan=%#v", plan)
			}
		})
	}
}

func TestDeleteOrphanRollbackPVRequiresRetainPolicy(t *testing.T) {
	client, options := orphanFixture()
	pv, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-rollback", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	if _, err := client.CoreV1().PersistentVolumes().Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	service := &Service{client: client, store: &memoryStore{}}
	err = service.deleteOrphanRollbackPV(context.Background(), options.SessionID, pvObjectReference(pv))
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("rollback PV was deleted: %v", err)
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
