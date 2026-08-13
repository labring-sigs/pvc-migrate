package kube

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestSwitcherWaitLogsBeforePolling(t *testing.T) {
	var logs bytes.Buffer
	switcher := NewSwitcher(fake.NewClientset()).WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	switcher.poll = time.Millisecond
	if err := switcher.waitFor(context.Background(), "PVC binding", func(context.Context) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "waiting for Kubernetes resource") || !strings.Contains(logs.String(), "PVC binding") {
		t.Fatalf("logs=%q", logs.String())
	}
}

func switcherFixture(t *testing.T) (*Switcher, *domain.Session, *domain.VolumeSpec, *domain.VolumeStatus) {
	t.Helper()
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	sourcePVCUID := types.UID("source-pvc-uid")
	tempPVCUID := types.UID("temp-pvc-uid")
	sourcePVUID := types.UID("source-pv-uid")
	destinationPVUID := types.UID("destination-pv-uid")
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: sourcePVCUID, ResourceVersion: "10"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			StorageClassName: &storageClass,
			VolumeMode:       &mode,
			VolumeName:       "pv-source",
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	tempPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "system",
			Name:            "data-migrated",
			UID:             tempPVCUID,
			ResourceVersion: "11",
			Labels:          map[string]string{SessionKey: "session"},
			Annotations:     map[string]string{SessionKey: "session"},
		},
		Spec:   corev1.PersistentVolumeClaimSpec{StorageClassName: &storageClass, VolumeMode: &mode, VolumeName: "pv-destination"},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	sourcePV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-source", UID: sourcePVUID, ResourceVersion: "20", Labels: map[string]string{SessionKey: "session"}},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef:                      &corev1.ObjectReference{Namespace: "app", Name: "data", UID: sourcePVCUID},
			StorageClassName:              storageClass,
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	destinationPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pv-destination",
			UID:             destinationPVUID,
			ResourceVersion: "21",
			Labels:          map[string]string{SessionKey: "session"},
			Annotations:     map[string]string{OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete)},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef:                      &corev1.ObjectReference{Namespace: "system", Name: "data-migrated", UID: tempPVCUID},
			StorageClassName:              storageClass,
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	client := fake.NewClientset(sourcePVC, tempPVC, sourcePV, destinationPV)
	client.PrependReactor("delete", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		var pvName string
		switch deleteAction.GetNamespace() + "/" + deleteAction.GetName() {
		case "app/data":
			pvcObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), "app", "data")
			if err != nil {
				return true, nil, err
			}
			pvc := pvcObject.(*corev1.PersistentVolumeClaim)
			pvName = pvc.Spec.VolumeName
		case "system/data-migrated":
			pvcObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), "system", "data-migrated")
			if err != nil {
				return true, nil, err
			}
			pvc := pvcObject.(*corev1.PersistentVolumeClaim)
			pvName = pvc.Spec.VolumeName
		}
		if pvName != "" {
			pvObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), "", pvName)
			if err != nil {
				return true, nil, err
			}
			pv := pvObject.(*corev1.PersistentVolume)
			pv.Status.Phase = corev1.VolumeReleased
			if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), pv, ""); err != nil {
				return true, nil, err
			}
		}
		return false, nil, nil
	})
	client.PrependReactor("create", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pvc := action.(clienttesting.CreateAction).GetObject().(*corev1.PersistentVolumeClaim)
		options := action.(interface{ GetCreateOptions() metav1.CreateOptions }).GetCreateOptions()
		if len(options.DryRun) > 0 {
			return true, pvc, nil
		}
		pvc.UID = types.UID("active-" + pvc.Spec.VolumeName)
		pvc.ResourceVersion = "30"
		pvc.Status.Phase = corev1.ClaimBound
		pvObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), "", pvc.Spec.VolumeName)
		if err != nil {
			return true, nil, err
		}
		pv := pvObject.(*corev1.PersistentVolume)
		pv.Spec.ClaimRef = &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID}
		pv.Status.Phase = corev1.VolumeBound
		if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), pv, ""); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
	volume := domain.VolumeSpec{
		SourcePVC:           domain.ObjectReference{Namespace: "app", Name: "data", UID: sourcePVCUID, ResourceVersion: "10"},
		SourcePV:            domain.ObjectReference{Name: "pv-source", UID: sourcePVUID},
		SourceReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		SourcePVCSpec:       *sourcePVC.Spec.DeepCopy(),
		SourcePVCMetadata: domain.PVCMetadata{
			Labels: map[string]string{"app": "database"},
			Annotations: map[string]string{
				"example.com/retained":                     "value",
				"pv.kubernetes.io/bind-completed":          "yes",
				"volume.kubernetes.io/selected-node":       "node-a",
				"volume.kubernetes.io/storage-provisioner": "example.csi",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: types.UID("sts-uid")}},
		},
		DestinationPVC:    domain.ObjectReference{Namespace: "system", Name: "data-migrated", UID: tempPVCUID, ResourceVersion: "11"},
		DestinationPV:     domain.ObjectReference{Name: "pv-destination", UID: destinationPVUID},
		DestinationPolicy: corev1.PersistentVolumeReclaimDelete,
		Capacity:          "1Gi",
		StorageClass:      storageClass,
		AccessModes:       []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		VolumeMode:        mode,
	}
	session := domain.NewSession("session", domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "app",
		SessionNamespace:     "system",
		Volumes:              []domain.VolumeSpec{volume},
	}, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false), time.Now())
	completed := metav1.Now()
	session.Status.Volumes[0].Reserved = true
	session.Status.Volumes[0].Sync.FinalCompletedAt = &completed
	switcher := NewSwitcher(client)
	switcher.poll = time.Millisecond
	return switcher, session, &session.Spec.Volumes[0], &session.Status.Volumes[0]
}

func TestActivateVolumeRecoversAfterTemporaryPVCDeletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	switcher, session, volume, status := switcherFixture(t)
	progressFailure := errors.New("injected persistence failure")
	err := switcher.ActivateVolume(ctx, session, volume, status, func() error { return progressFailure })
	if !errors.Is(err, progressFailure) {
		t.Fatalf("first activation error=%v", err)
	}
	status.Activation.TemporaryPVCDeleted = false
	updates := 0
	if err := switcher.ActivateVolume(ctx, session, volume, status, func() error { updates++; return nil }); err != nil {
		t.Fatal(err)
	}
	if status.Activation.ActivatedAt == nil || status.Activation.ActivePVC.UID == "" {
		t.Fatalf("activation status: %#v", status.Activation)
	}
	active, err := switcher.client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if active.Spec.VolumeName != "pv-destination" || active.Labels["app"] != "database" || len(active.OwnerReferences) != 1 {
		t.Fatalf("active PVC: %#v", active)
	}
	if active.Annotations["example.com/retained"] != "value" || active.Annotations["pv.kubernetes.io/bind-completed"] != "" || active.Annotations["volume.kubernetes.io/selected-node"] != "" || active.Annotations["volume.kubernetes.io/storage-provisioner"] != "" {
		t.Fatalf("active PVC annotations: %#v", active.Annotations)
	}
	destination, _ := switcher.client.CoreV1().PersistentVolumes().Get(ctx, "pv-destination", metav1.GetOptions{})
	if destination.Spec.ClaimRef == nil || destination.Spec.ClaimRef.UID != active.UID || destination.Labels[ResourceRoleLabel] != "active" {
		t.Fatalf("destination PV: %#v", destination)
	}
	if updates < 3 {
		t.Fatalf("progress updates=%d", updates)
	}
	if err := switcher.ActivateVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatalf("idempotent activation: %v", err)
	}
}

func TestRollbackVolumeRestoresOriginalPV(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	switcher, session, volume, status := switcherFixture(t)
	if err := switcher.ActivateVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatal(err)
	}
	if err := switcher.RollbackVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatal(err)
	}
	active, err := switcher.client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if active.Spec.VolumeName != "pv-source" || active.Annotations[RollbackPVAnnotation] != "pv-destination" || status.Activation.RolledBackAt == nil {
		t.Fatalf("rollback PVC/status: pvc=%#v status=%#v", active, status.Activation)
	}
	source, _ := switcher.client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	destination, _ := switcher.client.CoreV1().PersistentVolumes().Get(ctx, "pv-destination", metav1.GetOptions{})
	if source.Labels[ResourceRoleLabel] != "active" || destination.Labels[ResourceRoleLabel] != "rollback" {
		t.Fatalf("PV roles: source=%q destination=%q", source.Labels[ResourceRoleLabel], destination.Labels[ResourceRoleLabel])
	}
}

func TestRollbackVolumeRetainsDestinationBeforeDeletingActivePVC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	switcher, session, volume, status := switcherFixture(t)
	if err := switcher.ActivateVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatal(err)
	}
	destination, err := switcher.client.CoreV1().PersistentVolumes().Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	destination.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	if _, err := switcher.client.CoreV1().PersistentVolumes().Update(ctx, destination, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	client := switcher.client.(*fake.Clientset)
	retainedBeforeDelete := false
	client.PrependReactor("update", "persistentvolumes", func(action clienttesting.Action) (bool, runtime.Object, error) {
		updateAction := action.(clienttesting.UpdateAction)
		pv := updateAction.GetObject().(*corev1.PersistentVolume)
		if pv.Name == volume.DestinationPV.Name && pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
			retainedBeforeDelete = true
		}
		return false, nil, nil
	})
	client.PrependReactor("delete", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		if deleteAction.GetNamespace() != volume.SourcePVC.Namespace || deleteAction.GetName() != volume.SourcePVC.Name {
			return false, nil, nil
		}
		if !retainedBeforeDelete {
			return true, nil, errors.New("destination PV was deleted before reclaim policy was restored")
		}
		return false, nil, nil
	})
	if err := switcher.RollbackVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatal(err)
	}
	if status.Activation.RolledBackAt == nil {
		t.Fatal("rollback status was not recorded")
	}
	if !retainedBeforeDelete {
		t.Fatal("destination PV was not retained before deleting active PVC")
	}
}

func TestActivateVolumeRejectsReusedSourcePVCName(t *testing.T) {
	ctx := context.Background()
	switcher, session, volume, status := switcherFixture(t)
	pvc, _ := switcher.client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	pvc.UID = types.UID("reused-name-uid")
	_, _ = switcher.client.CoreV1().PersistentVolumeClaims("app").Update(ctx, pvc, metav1.UpdateOptions{})
	err := switcher.ActivateVolume(ctx, session, volume, status, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}
