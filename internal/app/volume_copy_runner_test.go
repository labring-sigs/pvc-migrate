package app

import (
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestCopyToolDeletionPreflightsAllNamespacesAndChecksFence(t *testing.T) {
	for _, failureMode := range []string{"inventory", "fence"} {
		t.Run(failureMode, func(t *testing.T) {
			resources := make([]runtime.Object, 0, 2)
			for _, namespace := range []string{"source", "destination"} {
				resources = append(resources, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace, Name: "tool", UID: "tool-" + types.UID(namespace),
						Labels: map[string]string{
							kube.AppInstanceLabel:  "pv-migrate-operation-mount",
							kube.AppComponentLabel: kube.ToolComponentRsync,
						},
					},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
						Name: "data", VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					}}},
				})
			}

			client := fake.NewClientset(resources...)
			failure := errors.New("preflight failed")
			lock := &fakeSessionLock{}
			lists := 0
			client.PrependReactor(
				"list",
				"pods",
				func(k8stesting.Action) (bool, runtime.Object, error) {
					lists++
					if lists == 2 {
						if failureMode == "inventory" {
							return true, nil, failure
						}

						lock.err = failure
					}

					return false, nil, nil
				},
			)

			runner := newVolumeCopyRunner(client, nil, VolumeCopyConfig{})

			err := runner.deleteCopyToolPods(kube.WithLeaseFence(t.Context(), lock),
				v1alpha1.ObjectReference{Namespace: "source", Name: "data"},
				v1alpha1.ObjectReference{Namespace: "destination", Name: "data"}, "operation")
			if !errors.Is(err, failure) {
				t.Fatalf("error = %v", err)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					t.Fatal("preflight failure deleted a tool Pod")
				}
			}
		})
	}
}
