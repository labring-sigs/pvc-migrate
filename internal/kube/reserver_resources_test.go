package kube

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestReservationHelperPodUsesZeroResourceQuotaFootprint(t *testing.T) {
	session := reserveTestSession()
	volume := &session.Spec.Volumes[0]
	client := fake.NewClientset(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   session.Spec.WorkflowOptions().TargetNode,
			Labels: map[string]string{corev1.LabelHostname: session.Spec.WorkflowOptions().TargetNode},
		},
	})
	var created *corev1.Pod
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pod := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod)
		created = pod.DeepCopy()
		pod.Status = corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		}
		return false, nil, nil
	})

	reserver := NewReserver(client)
	reserver.poll = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := reserver.provisionOnTarget(ctx, session, volume); err != nil {
		t.Fatal(err)
	}
	if created == nil {
		t.Fatal("reservation helper Pod was not created")
	}
	if len(created.Spec.Containers) != 1 {
		t.Fatalf("helper containers = %d, want 1", len(created.Spec.Containers))
	}
	if got := created.Spec.Containers[0].Command; len(got) != 3 || got[0] != "sh" || got[1] != "-c" || got[2] != "test -d /data && exec sleep 3600" {
		t.Fatalf("helper command=%q", got)
	}
	resources := created.Spec.Containers[0].Resources
	zero := resource.MustParse("0")
	for _, resourceName := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		if got := resources.Requests[resourceName]; got.Cmp(zero) != 0 {
			t.Fatalf("request %s = %s, want 0", resourceName, got.String())
		}
		if resourceName != corev1.ResourceEphemeralStorage {
			if got := resources.Limits[resourceName]; got.Cmp(zero) != 0 {
				t.Fatalf("limit %s = %s, want 0", resourceName, got.String())
			}
		}
	}
	if len(created.Spec.Overhead) != 0 {
		t.Fatalf("pod overhead = %#v, want empty", created.Spec.Overhead)
	}
	if len(resources.Requests) != 3 || len(resources.Limits) != 2 {
		t.Fatalf("unexpected resource keys: requests=%#v limits=%#v", resources.Requests, resources.Limits)
	}
	if _, ok := resources.Limits[corev1.ResourceEphemeralStorage]; ok {
		t.Fatal("zero ephemeral-storage limit would evict every helper")
	}
}

func TestPVSupportsNodeChecksRequiredTopology(t *testing.T) {
	pv := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"zone-b"}}}}}}}}}
	matching := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"topology.kubernetes.io/zone": "zone-b"}}}
	other := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"topology.kubernetes.io/zone": "zone-a"}}}
	if !PVSupportsNode(pv, matching) || PVSupportsNode(pv, other) {
		t.Fatalf("topology matching=%t other=%t", PVSupportsNode(pv, matching), PVSupportsNode(pv, other))
	}
}

func TestReservationKeepsPVCStorageRequestSeparateFromHelperResources(t *testing.T) {
	session := reserveTestSession()
	volume := &session.Spec.Volumes[0]
	sourcePVC, sourcePV := reserveSourceObjects()
	client := fake.NewClientset(sourcePVC, sourcePV, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   session.Spec.WorkflowOptions().TargetNode,
			Labels: map[string]string{corev1.LabelHostname: session.Spec.WorkflowOptions().TargetNode},
		},
	})
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pod := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod)
		pvcObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), volume.DestinationPVC.Namespace, volume.DestinationPVC.Name)
		if err != nil {
			return true, nil, err
		}
		pvc := pvcObject.(*corev1.PersistentVolumeClaim)
		pvc.UID = "destination-pvc-uid"
		pvc.Spec.VolumeName = "pv-destination"
		pvc.Annotations["volume.kubernetes.io/selected-node"] = session.Spec.WorkflowOptions().TargetNode
		pvc.Status.Phase = corev1.ClaimBound
		if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), pvc, pvc.Namespace); err != nil {
			return true, nil, err
		}
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: "destination-pv-uid"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(volume.Capacity)},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				ClaimRef: &corev1.ObjectReference{
					Namespace: pvc.Namespace,
					Name:      pvc.Name,
					UID:       pvc.UID,
				},
			},
		}, ""); err != nil {
			return true, nil, err
		}
		pod.Status = corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		}
		return false, nil, nil
	})
	reserver := NewReserver(client)
	reserver.poll = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := reserver.ReserveVolume(ctx, session, volume, &session.Status.Volumes[0], false); err != nil {
		t.Fatal(err)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims(volume.DestinationPVC.Namespace).Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	storage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if storage.Cmp(resource.MustParse(volume.Capacity)) != 0 {
		t.Fatalf("PVC storage request = %s, want %s", storage.String(), volume.Capacity)
	}
	if len(pvc.Spec.Resources.Requests) != 1 || len(pvc.Spec.Resources.Limits) != 0 {
		t.Fatalf("PVC resource footprint changed: requests=%#v limits=%#v", pvc.Spec.Resources.Requests, pvc.Spec.Resources.Limits)
	}
}
