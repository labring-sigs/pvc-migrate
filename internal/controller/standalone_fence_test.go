package controller

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestPauseStandaloneReportsFenceLossAfterPodDelete(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "data", Name: "worker", UID: "pod-uid",
	}}
	client := fake.NewClientset(pod)
	lost := errors.New("lease lost after standalone Pod delete")
	fence := &standaloneFence{err: nil}
	client.PrependReactor(
		"delete",
		"pods",
		func(ktesting.Action) (bool, runtime.Object, error) {
			fence.err = lost
			return false, nil, nil
		},
	)

	manager := NewManager(client, nil, nil)

	err := manager.pauseStandalone(
		kube.WithLeaseFence(context.Background(), fence),
		v1alpha1.ObjectReference{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
	)
	if !errors.Is(err, lost) {
		t.Fatalf("error = %v, want lease loss after delete", err)
	}
}

type standaloneFence struct{ err error }

func (f *standaloneFence) Err() error { return f.err }
