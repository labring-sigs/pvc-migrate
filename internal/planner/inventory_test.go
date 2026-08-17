package planner

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestLoadPlanInventoryKeepsPVCPVAndStorageClassIndexes(t *testing.T) {
	objects := plannerObjects("2Gi")
	mode := corev1.PersistentVolumeFilesystem
	class := "fast"
	objects = append(objects,
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "logs", UID: types.UID("logs-pvc")},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-logs",
				StorageClassName: &class,
				VolumeMode:       &mode,
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-logs", UID: types.UID("logs-pv")},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("4Gi")},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				ClaimRef:                      &corev1.ObjectReference{Namespace: "app", Name: "logs", UID: types.UID("logs-pvc")},
				StorageClassName:              class,
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		},
	)
	inventory := New(plannerClient(objects...), nil).loadPlanInventory(context.Background(), Options{SourceNamespace: "app", TargetNode: "node-b"}, []string{"data", "logs"}, false)
	if len(inventory.pvcs) != 2 || len(inventory.pvs) != 2 {
		t.Fatalf("inventory lengths: pvcs=%d pvs=%d", len(inventory.pvcs), len(inventory.pvs))
	}
	if inventory.pvcs[0].pvc.Name != "data" || inventory.pvs[0].pv.Name != "pv-source" {
		t.Fatalf("data references: pvc=%v pv=%v", inventory.pvcs[0].pvc, inventory.pvs[0].pv)
	}
	if inventory.pvcs[1].pvc.Name != "logs" || inventory.pvs[1].pv.Name != "pv-logs" {
		t.Fatalf("logs references: pvc=%v pv=%v", inventory.pvcs[1].pvc, inventory.pvs[1].pv)
	}
	if inventory.storageClasses["fast"] == nil || inventory.storageClassError["fast"] != nil {
		t.Fatalf("storage class result: classes=%v errors=%v", inventory.storageClasses, inventory.storageClassError)
	}
	if inventory.targetNode == nil || inventory.targetNode.Name != "node-b" || inventory.targetNodeErr != nil {
		t.Fatalf("target node result: node=%v err=%v", inventory.targetNode, inventory.targetNodeErr)
	}
}

func TestLoadPlanInventoryLoadsSourceAndExplicitDestinationClasses(t *testing.T) {
	objects := plannerObjects("2Gi")
	destinationClass := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "archive"}, Provisioner: "archive.example.io"}
	objects = append(objects, destinationClass)

	inventory := New(plannerClient(objects...), nil).loadPlanInventory(context.Background(), Options{
		SourceNamespace:  "app",
		DestinationClass: destinationClass.Name,
	}, []string{"data"}, false)
	for _, name := range []string{"fast", destinationClass.Name} {
		if inventory.storageClasses[name] == nil || inventory.storageClassError[name] != nil {
			t.Fatalf("StorageClass %s result: classes=%v errors=%v", name, inventory.storageClasses, inventory.storageClassError)
		}
	}
}
