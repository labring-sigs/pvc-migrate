package controller

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func readyPod(namespace, name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       types.UID(name + "-uid"),
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

// discoverForTest resolves a workload from a namespace and pod-migration
// request without CLI plumbing.
func (m *Manager) discoverForTest(
	ctx context.Context,
	namespace string,
	spec v1alpha1.PodMigrationSpec,
) (v1alpha1.WorkloadSpec, error) {
	pod, err := m.typed.CoreV1().Pods(namespace).Get(ctx, spec.Pod.Name, metav1.GetOptions{})
	if err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	return m.DiscoverPod(
		ctx,
		pod,
		namespace,
		spec.Pod,
		"",
		spec.AllowLeaderDowntime,
		domain.PresentationCLI,
	)
}

func kubeBlocksInstanceSetObject(apiVersion string, paused *bool) *unstructured.Unstructured {
	spec := map[string]any{}
	if paused != nil {
		spec["paused"] = *paused
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       "InstanceSet",
		"metadata": map[string]any{
			"name":      "cluster-db",
			"namespace": "db",
			"uid":       "instanceset-uid",
		},
		"spec": spec,
	}}
}

// runnerSessionLock is a no-op lease double for reconciler wiring tests.
type runnerSessionLock struct{}

func (runnerSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

func (runnerSessionLock) Err() error { return nil }

func (runnerSessionLock) Release(context.Context) error { return nil }

func (runnerSessionLock) Delete(context.Context) error { return nil }

var _ kube.SessionLock = runnerSessionLock{}

func trustedSnapshotPod(namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "worker",
			UID:       "worker-uid",
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "worker", Image: "example.test/worker:latest",
		}}},
	}
}

// runnerSessionStore hands out one prepared lock for reconciler wiring tests.
type runnerSessionStore struct {
	lock *runnerSessionLock
}

func (s *runnerSessionStore) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	if s.lock == nil {
		return &runnerSessionLock{}, nil
	}
	return s.lock, nil
}

var _ kube.SessionLocker = (*runnerSessionStore)(nil)

func deploymentTestObjects() (*appsv1.Deployment, *appsv1.ReplicaSet, []*corev1.Pod) {
	replicas := int32(2)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "app",
			Name:       "web",
			UID:        types.UID("deployment-uid"),
			Generation: 1,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           replicas,
			UpdatedReplicas:    replicas,
			ReadyReplicas:      replicas,
			AvailableReplicas:  replicas,
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "web-rs", UID: types.UID("rs-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment",
				Name: deployment.Name, UID: deployment.UID, Controller: new(true),
			}},
		},
	}

	pods := []*corev1.Pod{
		readyPod("app", "web-old-1", "node-a"),
		readyPod("app", "web-old-2", "node-a"),
	}
	for _, pod := range pods {
		pod.Labels = map[string]string{"app": "web"}
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet",
			Name: rs.Name, UID: rs.UID, Controller: new(true),
		}}
	}

	return deployment, rs, pods
}
