package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func statefulSetAutoscaler(namespace, name string) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name + "-autoscaler"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: domain.AppsAPIVersion,
				Kind:       domain.KindStatefulSet,
				Name:       name,
			},
			MaxReplicas: 5,
		},
	}
}

func TestHorizontalPodAutoscalerTargetMatchesAPIGroup(t *testing.T) {
	for _, test := range []struct {
		name       string
		apiVersion string
		wantReject bool
	}{
		{
			name:       "same API group different version",
			apiVersion: "apps/v1beta1",
			wantReject: true,
		},
		{
			name:       "different API group",
			apiVersion: "workloads.example.io/v1",
			wantReject: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			hpa := &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web-autoscaler"},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
						APIVersion: test.apiVersion,
						Kind:       domain.KindDeployment,
						Name:       "web",
					},
					MaxReplicas: 5,
				},
			}
			client := kubernetesfake.NewClientset(hpa)
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)

			err := manager.rejectHorizontalPodAutoscaler(
				context.Background(),
				"app",
				domain.KindDeployment,
				"web",
				"test autoscaling target",
			)
			if test.wantReject {
				if domain.CategoryOf(err) != domain.ErrorPrecondition {
					t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStatefulSetDiscoveryRejectsHorizontalPodAutoscaler(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "db",
			UID:       types.UID("sts-uid"),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
	pod := readyPod(sts.Namespace, sts.Name+"-0", "node-a")
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: domain.AppsAPIVersion,
		Kind:       domain.KindStatefulSet,
		Name:       sts.Name,
		UID:        sts.UID,
		Controller: new(true),
	}}
	client := kubernetesfake.NewClientset(sts, pod, statefulSetAutoscaler(sts.Namespace, sts.Name))
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)

	_, err := manager.Discover(
		context.Background(),
		DiscoverOptions{
			Namespace:           pod.Namespace,
			PodName:             pod.Name,
			AllowLeaderDowntime: true,
		},
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "HorizontalPodAutoscaler") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStatefulSetOperationsRejectHorizontalPodAutoscalerWithoutScaling(t *testing.T) {
	for _, test := range []struct {
		name     string
		replicas int32
		run      func(context.Context, *Manager, *domain.Session) error
	}{
		{
			name:     "pause",
			replicas: 3,
			run: func(ctx context.Context, manager *Manager, session *domain.Session) error {
				return manager.Pause(ctx, session)
			},
		},
		{
			name:     "verify paused",
			replicas: 1,
			run: func(ctx context.Context, manager *Manager, session *domain.Session) error {
				return manager.VerifyPaused(ctx, session)
			},
		},
		{
			name:     "resume",
			replicas: 1,
			run: func(ctx context.Context, manager *Manager, session *domain.Session) error {
				return manager.Resume(ctx, session)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "app",
					Name:      "db",
					UID:       types.UID("sts-uid"),
				},
				Spec: appsv1.StatefulSetSpec{Replicas: &test.replicas},
			}
			client := kubernetesfake.NewClientset(
				sts,
				statefulSetAutoscaler(sts.Namespace, sts.Name),
			)
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)
			original, ordinal := int32(3), int32(1)
			session := controllerSession(domain.WorkloadSpec{
				Adapter: domain.WorkloadStatefulSet,
				Pod: domain.ObjectReference{
					Namespace: sts.Namespace,
					Name:      sts.Name + "-1",
					UID:       types.UID("pod-uid"),
				},
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       domain.KindStatefulSet,
					Namespace:  sts.Namespace,
					Name:       sts.Name,
					UID:        sts.UID,
				},
				OriginalReplicas: &original,
				Ordinal:          &ordinal,
			})

			err := test.run(context.Background(), manager, session)
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), "HorizontalPodAutoscaler") {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			current, err := client.AppsV1().StatefulSets(sts.Namespace).Get(
				context.Background(),
				sts.Name,
				metav1.GetOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}

			if got := statefulSetReplicas(current); got != test.replicas {
				t.Fatalf("replicas=%d want=%d", got, test.replicas)
			}
		})
	}
}

func TestValidateStatefulSetResumeAcceptsOnlyPausedOrOriginalReplicas(t *testing.T) {
	for _, test := range []struct {
		name         string
		replicas     int32
		wantConflict bool
	}{
		{name: "paused", replicas: 1},
		{name: "already restored", replicas: 3},
		{name: "drifted", replicas: 2, wantConflict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "app",
					Name:      "db",
					UID:       "sts-uid",
				},
				Spec: appsv1.StatefulSetSpec{Replicas: &test.replicas},
			}
			client := kubernetesfake.NewClientset(sts)
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)
			original, ordinal := int32(3), int32(1)
			session := controllerSession(domain.WorkloadSpec{
				Adapter: domain.WorkloadStatefulSet,
				Pod: domain.ObjectReference{
					Namespace: sts.Namespace,
					Name:      sts.Name + "-1",
					UID:       "pod-uid",
				},
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       domain.KindStatefulSet,
					Namespace:  sts.Namespace,
					Name:       sts.Name,
					UID:        sts.UID,
				},
				OriginalReplicas: &original,
				Ordinal:          &ordinal,
			})

			err := manager.ValidateResume(context.Background(), session)
			if !test.wantConflict && err != nil {
				t.Fatal(err)
			}

			if test.wantConflict &&
				(domain.CategoryOf(err) != domain.ErrorConflict ||
					!strings.Contains(err.Error(), "replicas changed to 2 while restoring 3")) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestStatefulSetResumeRejectsHorizontalPodAutoscalerAddedWhileWaiting(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: "sts-uid"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	pod := readyPod(sts.Namespace, sts.Name+"-1", "node-a")
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: domain.AppsAPIVersion,
		Kind:       domain.KindStatefulSet,
		Name:       sts.Name,
		UID:        sts.UID,
		Controller: new(true),
	}}

	client := kubernetesfake.NewClientset(sts, pod)
	hpaCreated := false
	client.PrependReactor(
		"get",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			if !hpaCreated {
				hpaCreated = true

				hpa := statefulSetAutoscaler(sts.Namespace, sts.Name)
				if err := client.Tracker().Create(
					autoscalingv2.SchemeGroupVersion.WithResource("horizontalpodautoscalers"),
					hpa,
					hpa.Namespace,
				); err != nil {
					t.Fatal(err)
				}
			}

			return false, nil, nil
		},
	)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	original, ordinal := int32(2), int32(1)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod: domain.ObjectReference{
			Namespace: pod.Namespace,
			Name:      pod.Name,
			UID:       "old-pod-uid",
		},
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindStatefulSet,
			Namespace:  sts.Namespace,
			Name:       sts.Name,
			UID:        sts.UID,
		},
		OriginalReplicas: &original,
		Ordinal:          &ordinal,
		AffectedPods: []domain.ObjectReference{{
			Namespace: pod.Namespace,
			Name:      pod.Name,
			UID:       "old-pod-uid",
		}},
	})

	err := manager.Resume(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "HorizontalPodAutoscaler") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestGrafanaResumeRejectsHorizontalPodAutoscalerAddedAfterScaling(t *testing.T) {
	replicas := int32(0)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "vm",
			Name:      "grafana",
			UID:       "deployment-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: grafanaAPIVersion,
				Kind:       domain.KindGrafana,
				Name:       "grafana",
				UID:        "grafana-uid",
				Controller: new(true),
			}},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "grafana"}},
		},
	}
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: deployment.Namespace,
		Name:      "grafana-rs",
		UID:       "replicaset-uid",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindDeployment,
			Name:       deployment.Name,
			UID:        deployment.UID,
			Controller: new(true),
		}},
	}}
	pod := readyPod(deployment.Namespace, "grafana-new", "node-a")
	pod.Labels = map[string]string{"app": "grafana"}
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: domain.AppsAPIVersion,
		Kind:       domain.KindReplicaSet,
		Name:       replicaSet.Name,
		UID:        replicaSet.UID,
		Controller: new(true),
	}}

	client := kubernetesfake.NewClientset(deployment, replicaSet, pod)
	client.PrependReactor(
		"update",
		"deployments",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*appsv1.Deployment](t, action)
			restored := deploymentReplicas(updated)
			updated.Status = appsv1.DeploymentStatus{
				ObservedGeneration: updated.Generation,
				Replicas:           restored,
				UpdatedReplicas:    restored,
				ReadyReplicas:      restored,
				AvailableReplicas:  restored,
			}

			hpa := &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: deployment.Namespace,
					Name:      "grafana-autoscaler",
				},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
						APIVersion: domain.AppsAPIVersion,
						Kind:       domain.KindDeployment,
						Name:       deployment.Name,
					},
					MaxReplicas: 3,
				},
			}
			if err := client.Tracker().Create(
				autoscalingv2.SchemeGroupVersion.WithResource("horizontalpodautoscalers"),
				hpa,
				hpa.Namespace,
			); err != nil {
				t.Fatal(err)
			}

			return false, nil, nil
		},
	)

	grafana := grafanaObject("grafana-uid", true)
	grafana.SetAnnotations(map[string]string{pauseSessionAnnotation: "session"})
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana),
		client.Discovery(),
	)
	original := int32(1)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadGrafana,
		Pod: domain.ObjectReference{
			Namespace: deployment.Namespace,
			Name:      "grafana-old",
			UID:       "old-pod-uid",
		},
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindDeployment,
			Namespace:  deployment.Namespace,
			Name:       deployment.Name,
			UID:        deployment.UID,
		},
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{{
			Namespace: deployment.Namespace,
			Name:      "grafana-old",
			UID:       "old-pod-uid",
		}},
		Grafana: &domain.GrafanaSpec{
			APIVersion:       grafanaAPIVersion,
			Name:             "grafana",
			UID:              "grafana-uid",
			OriginalReplicas: original,
		},
	})

	err := manager.Resume(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "HorizontalPodAutoscaler") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestOperatorAdaptersRejectHorizontalPodAutoscalerBeforeScaling(t *testing.T) {
	for _, test := range []struct {
		name       string
		adapter    domain.WorkloadKind
		targetKind string
	}{
		{name: "Grafana", adapter: domain.WorkloadGrafana, targetKind: domain.KindDeployment},
		{name: "VMCluster", adapter: domain.WorkloadVMCluster, targetKind: domain.KindStatefulSet},
		{name: "Victoria Logs", adapter: domain.WorkloadVictoriaLogs, targetKind: domain.KindStatefulSet},
	} {
		t.Run(test.name, func(t *testing.T) {
			const namespace = "app"

			const name = "data"

			replicas := int32(2)

			objects := []runtime.Object{&autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "autoscaler"},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
						APIVersion: domain.AppsAPIVersion,
						Kind:       test.targetKind,
						Name:       name,
					},
					MaxReplicas: 5,
				},
			}}

			if test.targetKind == domain.KindDeployment {
				objects = append(objects, &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
						Name:      name,
						UID:       "controller-uid",
					},
					Spec: appsv1.DeploymentSpec{Replicas: &replicas},
				})
			} else {
				objects = append(objects, &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
						Name:      name,
						UID:       "controller-uid",
					},
					Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
				})
			}

			client := kubernetesfake.NewClientset(objects...)
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)
			ordinal := int32(1)

			workload := domain.WorkloadSpec{
				Adapter: test.adapter,
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       test.targetKind,
					Namespace:  namespace,
					Name:       name,
					UID:        "controller-uid",
				},
				OriginalReplicas: &replicas,
				Ordinal:          &ordinal,
			}
			switch test.adapter {
			case domain.WorkloadGrafana:
				workload.Grafana = &domain.GrafanaSpec{}
			case domain.WorkloadVMCluster:
				workload.VMCluster = &domain.VMClusterSpec{}
			}

			err := manager.Pause(context.Background(), controllerSession(workload))
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), "HorizontalPodAutoscaler") {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			var got int32
			if test.targetKind == domain.KindDeployment {
				current, getErr := client.AppsV1().Deployments(namespace).Get(
					context.Background(),
					name,
					metav1.GetOptions{},
				)
				if getErr != nil {
					t.Fatal(getErr)
				}

				got = deploymentReplicas(current)
			} else {
				current, getErr := client.AppsV1().StatefulSets(namespace).Get(
					context.Background(),
					name,
					metav1.GetOptions{},
				)
				if getErr != nil {
					t.Fatal(getErr)
				}

				got = statefulSetReplicas(current)
			}

			if got != replicas {
				t.Fatalf("replicas=%d want=%d", got, replicas)
			}
		})
	}
}
