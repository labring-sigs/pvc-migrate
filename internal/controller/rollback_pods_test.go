package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestCurrentRollbackPodsRejectsDeploymentReplicaDrift(t *testing.T) {
	for _, adapter := range []domain.WorkloadKind{
		domain.WorkloadDeployment,
		domain.WorkloadGrafana,
	} {
		t.Run(string(adapter), func(t *testing.T) {
			deployment, _, pods := deploymentTestObjects()
			current := int32(1)
			deployment.Spec.Replicas = &current

			workload := domain.WorkloadSpec{
				Adapter: adapter,
				Pod:     podReference(pods[0]),
				Controller: objectReference(
					domain.AppsAPIVersion,
					domain.KindDeployment,
					deployment.Namespace,
					deployment.Name,
					deployment.UID,
					deployment.ResourceVersion,
				),
				OriginalReplicas: new(int32(2)),
				AffectedPods:     []domain.ObjectReference{podReference(pods[0])},
			}

			if adapter == domain.WorkloadGrafana {
				deployment.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: grafanaAPIVersion,
					Kind:       domain.KindGrafana,
					Name:       "grafana",
					UID:        "grafana-uid",
					Controller: new(true),
				}}
				workload.Grafana = &domain.GrafanaSpec{
					APIVersion: grafanaAPIVersion,
					Name:       "grafana",
					UID:        "grafana-uid",
				}
			}

			client := kubernetesfake.NewClientset(deployment)
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)

			_, err := manager.CurrentRollbackPods(
				context.Background(),
				controllerSession(workload),
			)
			if domain.CategoryOf(err) != domain.ErrorConflict ||
				!strings.Contains(err.Error(), "replicas changed to 1") {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestVerifyGrafanaPausedRejectsDeploymentOwnerDrift(t *testing.T) {
	replicas := int32(0)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "vm",
			Name:      "grafana",
			UID:       "deployment-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "example.io/v1",
				Kind:       "Application",
				Name:       "replacement-owner",
				UID:        "replacement-owner-uid",
				Controller: new(true),
			}},
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas},
	}
	grafana := grafanaObject("grafana-uid", true)
	grafana.SetAnnotations(map[string]string{pauseSessionAnnotation: "session"})

	client := kubernetesfake.NewClientset(deployment)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana),
		client.Discovery(),
	)
	original := int32(2)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadGrafana,
		Pod:     domain.ObjectReference{Namespace: "vm", Name: "grafana-old", UID: "old-uid"},
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindDeployment,
			deployment.Namespace,
			deployment.Name,
			deployment.UID,
			deployment.ResourceVersion,
		),
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{{
			Namespace: "vm", Name: "grafana-old", UID: "old-uid",
		}},
		Grafana: &domain.GrafanaSpec{
			APIVersion: grafanaAPIVersion,
			Name:       grafana.GetName(),
			UID:        grafana.GetUID(),
		},
	})

	err := manager.VerifyPaused(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "Grafana controller identity changed") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCurrentRollbackPodsRefreshesStatefulSetPodUID(t *testing.T) {
	replicas := int32(3)
	ordinal := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: "sts-uid"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	pod := readyPod("app", "db-1", "node-b")
	pod.UID = "replacement-pod-uid"
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: domain.AppsAPIVersion,
		Kind:       domain.KindStatefulSet,
		Name:       sts.Name,
		UID:        sts.UID,
		Controller: new(true),
	}}

	client := kubernetesfake.NewClientset(sts, pod)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod:     domain.ObjectReference{Namespace: "app", Name: pod.Name, UID: "old-pod-uid"},
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			sts.Namespace,
			sts.Name,
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &replicas,
		Ordinal:          &ordinal,
		AffectedPods: []domain.ObjectReference{{
			Namespace: "app", Name: pod.Name, UID: "old-pod-uid",
		}},
	})

	current, err := manager.CurrentRollbackPods(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}

	if len(current) != 1 || current[0].Name != pod.Name || current[0].UID != pod.UID {
		t.Fatalf("current rollback Pods=%#v", current)
	}
}

func TestCurrentRollbackPodsRejectsStatefulSetReplicaDrift(t *testing.T) {
	current := int32(2)
	original := int32(3)
	ordinal := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: "sts-uid"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &current},
	}
	client := kubernetesfake.NewClientset(sts)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod:     domain.ObjectReference{Namespace: "app", Name: "db-1", UID: "old-pod-uid"},
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			sts.Namespace,
			sts.Name,
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &original,
		Ordinal:          &ordinal,
		AffectedPods: []domain.ObjectReference{{
			Namespace: "app", Name: "db-1", UID: "old-pod-uid",
		}},
	})

	_, err := manager.CurrentRollbackPods(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replicas changed to 2") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCurrentRollbackPodsRequiresAdapterOwnership(t *testing.T) {
	controller := domain.ObjectReference{
		APIVersion: "workloads.kubeblocks.io/v1alpha1",
		Kind:       domain.KindInstanceSet,
		Namespace:  "db",
		Name:       "cluster-db",
		UID:        "instanceset-uid",
	}

	for _, test := range []struct {
		name     string
		workload domain.WorkloadSpec
		pod      *corev1.Pod
	}{
		{
			name: "KubeBlocks controller",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadKubeBlocks,
				Pod:        domain.ObjectReference{Namespace: "db", Name: "cluster-db-0", UID: "old-uid"},
				Controller: controller,
				KubeBlocks: &domain.KubeBlocksSpec{Cluster: "cluster", Instance: "cluster-db-0"},
			},
			pod: func() *corev1.Pod {
				pod := readyPod("db", "cluster-db-0", "node-b")
				pod.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: controller.APIVersion,
					Kind:       controller.Kind,
					Name:       controller.Name,
					UID:        controller.UID,
					Controller: new(true),
				}}

				return pod
			}(),
		},
		{
			name: "standalone session",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadStandalone,
				Pod:     domain.ObjectReference{Namespace: "app", Name: "worker", UID: "old-uid"},
			},
			pod: func() *corev1.Pod {
				pod := readyPod("app", "worker", "node-b")
				pod.Annotations = map[string]string{kube.SessionKey: "session"}
				return pod
			}(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := kubernetesfake.NewClientset(test.pod)
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)
			session := controllerSession(test.workload)

			current, err := manager.CurrentRollbackPods(context.Background(), session)
			if err != nil {
				t.Fatal(err)
			}

			if len(current) != 1 || current[0].UID != test.pod.UID {
				t.Fatalf("current rollback Pods=%#v", current)
			}

			foreign := test.pod.DeepCopy()

			foreign.UID = types.UID("foreign-uid")
			if test.workload.Adapter == domain.WorkloadStandalone {
				foreign.Annotations[kube.SessionKey] = "other-session"
			} else {
				foreign.OwnerReferences = nil
			}

			if _, err := client.CoreV1().Pods(foreign.Namespace).Update(
				context.Background(),
				foreign,
				metav1.UpdateOptions{},
			); err != nil {
				t.Fatal(err)
			}

			_, err = manager.CurrentRollbackPods(context.Background(), session)
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("foreign Pod category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}
