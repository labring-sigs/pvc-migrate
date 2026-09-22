package kube

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func topologyFixturePVC(pvName string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "data"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func topologyFixturePV() *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data"},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      corev1.LabelHostname,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"storage-node"},
						}},
					}},
				},
			},
		},
	}
}

// TestSchedulerSelectedProbeToleratesPVTopologyTaints pins the probe fix for
// tainted storage nodes: a scheduler-selected probe that mounts a PVC pinned
// by node affinity must carry that topology's taint tolerations, or it can
// never start and the transfer fails before any tool Pod runs.
func TestSchedulerSelectedProbeToleratesPVTopologyTaints(t *testing.T) {
	storageNode := readyProbeNode("storage-node")
	plainNode := readyProbeNode("plain-node")
	plainNode.Spec.Taints = nil

	client := fake.NewClientset(
		storageNode,
		plainNode,
		topologyFixturePVC("pv-data"),
		topologyFixturePV(),
	)

	var created *corev1.Pod
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("probe-uid")
			created = pod.DeepCopy()
			return false, nil, nil
		},
	)
	client.PrependReactor("get", "pods", successfulProbeGetReactor(t, client))

	prober := NewToolImageProber(client)

	if _, err := prober.Probe(context.Background(), ToolImageProbeOptions{
		OperationID: "topology-session",
		Image:       "registry.example/pvc-migrate:test",
		Targets: []ToolProbeTarget{{
			Namespace:  "tenant",
			PVCName:    "data",
			Components: []string{ToolComponentSSHD, ToolComponentShell},
		}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}

	if created == nil {
		t.Fatal("probe Pod was not created")
	}

	if created.Spec.NodeSelector != nil {
		t.Fatalf("scheduler-selected probe pinned a node: %v", created.Spec.NodeSelector)
	}

	if len(created.Spec.Tolerations) != 1 ||
		created.Spec.Tolerations[0].Key != "storage" ||
		created.Spec.Tolerations[0].Effect != corev1.TaintEffectNoSchedule {
		t.Fatalf("probe tolerations=%v", created.Spec.Tolerations)
	}
}

// TestPVCTopologyTolerationsBestEffort keeps the helper from breaking probes
// when the claim, PV, or node listing cannot be read: those probes fall back
// to the historical no-toleration scheduling.
func TestPVCTopologyTolerationsBestEffort(t *testing.T) {
	t.Run("missing claim", func(t *testing.T) {
		client := fake.NewClientset(readyProbeNode("storage-node"))

		if tolerations := NewToolImageProber(client).pvcTopologyTolerations(
			context.Background(), "tenant", "missing",
		); tolerations != nil {
			t.Fatalf("missing claim tolerations=%v", tolerations)
		}
	})

	t.Run("missing volume", func(t *testing.T) {
		client := fake.NewClientset(
			readyProbeNode("storage-node"),
			topologyFixturePVC("pv-gone"),
		)

		if tolerations := NewToolImageProber(client).pvcTopologyTolerations(
			context.Background(), "tenant", "data",
		); tolerations != nil {
			t.Fatalf("missing volume tolerations=%v", tolerations)
		}
	})

	t.Run("node listing restricted", func(t *testing.T) {
		client := fake.NewClientset(
			readyProbeNode("storage-node"),
			topologyFixturePVC("pv-data"),
			topologyFixturePV(),
		)
		client.PrependReactor(
			"list",
			"nodes",
			func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(
					corev1.Resource("nodes"),
					"",
					errors.New("node listing is restricted"),
				)
			},
		)

		if tolerations := NewToolImageProber(client).pvcTopologyTolerations(
			context.Background(), "tenant", "data",
		); tolerations != nil {
			t.Fatalf("restricted listing tolerations=%v", tolerations)
		}
	})
}

// TestPVCTopologyTolerationsSkipUnschedulableNodes keeps cordoned or
// not-Ready topology nodes out of the toleration set: tolerating a node the
// scheduler would refuse anyway only widens the taint surface.
func TestPVCTopologyTolerationsSkipUnschedulableNodes(t *testing.T) {
	cordoned := readyProbeNode("storage-node")
	cordoned.Spec.Unschedulable = true

	client := fake.NewClientset(
		cordoned,
		topologyFixturePVC("pv-data"),
		topologyFixturePV(),
	)

	prober := NewToolImageProber(client)

	if tolerations := prober.pvcTopologyTolerations(
		context.Background(), "tenant", "data",
	); len(tolerations) != 0 {
		t.Fatalf("cordoned node tolerations=%v", tolerations)
	}
}
