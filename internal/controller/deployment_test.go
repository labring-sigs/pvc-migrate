package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

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
				APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
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
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindReplicaSet,
			Name: rs.Name, UID: rs.UID, Controller: new(true),
		}}
	}

	return deployment, rs, pods
}

func TestDeploymentDiscoveryPauseAndResumeScalesWholeDeployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	deployment, rs, pods := deploymentTestObjects()
	objects := []runtime.Object{deployment, rs, pods[0], pods[1]}
	client := kubernetesfake.NewClientset(objects...)
	podsResource := corev1.SchemeGroupVersion.WithResource("pods")
	client.PrependReactor(
		"update",
		"deployments",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*appsv1.Deployment](t, action)
			replicas := deploymentReplicas(updated)
			updated.Status.ObservedGeneration = updated.Generation
			updated.Status.Replicas = replicas
			updated.Status.UpdatedReplicas = replicas
			updated.Status.ReadyReplicas = replicas
			updated.Status.AvailableReplicas = replicas
			updated.Status.UnavailableReplicas = 0

			if replicas == 0 {
				for _, pod := range pods {
					_ = client.Tracker().Delete(podsResource, "app", pod.Name)
				}
			} else {
				for index, old := range pods {
					resumed := readyPod("app", "web-new-"+string(rune('1'+index)), "node-b")
					resumed.UID = types.UID("resumed-" + old.Name)
					resumed.Labels = map[string]string{"app": "web"}
					resumed.OwnerReferences = old.OwnerReferences
					_ = client.Tracker().Create(podsResource, resumed, "app")
				}
			}

			return false, nil, nil
		},
	)

	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond

	workload, err := manager.DiscoverPod(
		ctx,
		pods[0],
		DiscoverOptions{Namespace: "app", PodName: pods[0].Name},
	)
	if err != nil {
		t.Fatal(err)
	}

	if workload.Adapter != domain.WorkloadDeployment || len(workload.AffectedPods) != 2 ||
		workload.OriginalReplicas == nil || *workload.OriginalReplicas != 2 {
		t.Fatalf("workload=%#v", workload)
	}

	session := controllerSession(workload)
	if err := manager.Pause(ctx, session); err != nil {
		t.Fatal(err)
	}

	if err := manager.VerifyPaused(ctx, session); err != nil {
		t.Fatal(err)
	}

	if err := manager.Resume(ctx, session); err != nil {
		t.Fatal(err)
	}

	if len(session.Spec.Workload().AffectedPods) != 2 {
		t.Fatalf("resumed affected pods=%#v", session.Spec.Workload().AffectedPods)
	}

	for _, ref := range session.Spec.Workload().AffectedPods {
		if !strings.HasPrefix(ref.Name, "web-new-") ||
			!strings.HasPrefix(string(ref.UID), "resumed-") {
			t.Fatalf("stale resumed reference=%#v", ref)
		}
	}
}

func TestObserveDeploymentPodsCachesReplicaSetReads(t *testing.T) {
	deployment, replicaSet, pods := deploymentTestObjects()
	client := kubernetesfake.NewClientset(deployment, replicaSet, pods[0], pods[1])
	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)

	current, ready, err := manager.observeDeploymentPods(context.Background(), deployment)
	if err != nil {
		t.Fatal(err)
	}

	if !ready || len(current) != len(pods) {
		t.Fatalf("ready=%t current=%#v", ready, current)
	}

	reads := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "get" && action.GetResource().Resource == "replicasets" {
			reads++
		}
	}

	if reads != 1 {
		t.Fatalf(
			"ReplicaSet reads=%d, want 1 for %d Pods owned by one ReplicaSet",
			reads,
			len(pods),
		)
	}
}

func TestResumeDeploymentFailureRecordsObservedPods(t *testing.T) {
	deployment, rs, oldPods := deploymentTestObjects()
	deployment.ResourceVersion = "7"
	deployment.Spec.Replicas = new(int32)
	deployment.Status = appsv1.DeploymentStatus{
		ObservedGeneration: deployment.Generation,
	}
	client := kubernetesfake.NewClientset(deployment, rs)
	podsResource := corev1.SchemeGroupVersion.WithResource("pods")
	pending := oldPods[0].DeepCopy()
	pending.Name = "web-new-pending"
	pending.UID = "web-new-pending-uid"
	pending.Status = corev1.PodStatus{Phase: corev1.PodPending}

	client.PrependReactor(
		"update",
		"deployments",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*appsv1.Deployment](t, action)
			if deploymentReplicas(updated) != 2 {
				return false, nil, nil
			}

			updated.Status.ObservedGeneration = updated.Generation
			updated.Status.Replicas = 1
			updated.Status.UnavailableReplicas = 1

			if err := client.Tracker().Create(
				podsResource,
				pending,
				pending.Namespace,
			); err != nil {
				t.Fatal(err)
			}

			return false, nil, nil
		},
	)

	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond

	original := int32(2)
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Pod:     podReference(oldPods[0]),
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{
			podReference(oldPods[0]),
			podReference(oldPods[1]),
		},
	}
	session := controllerSession(workload)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := manager.Resume(ctx, session)
	if err == nil {
		t.Fatal("Resume() succeeded with a Pending replacement Pod")
	}

	resumed := session.Spec.Workload()
	if len(resumed.AffectedPods) != 1 || resumed.AffectedPods[0].Name != pending.Name ||
		resumed.AffectedPods[0].UID != pending.UID {
		t.Fatalf("affected pods=%#v", resumed.AffectedPods)
	}

	if resumed.Pod.Name != pending.Name || resumed.Pod.UID != pending.UID {
		t.Fatalf("selected pod=%#v", resumed.Pod)
	}
}

func TestUpdateDeploymentPodReferencesDoesNotPreferPreviousName(t *testing.T) {
	previous := domain.ObjectReference{Namespace: "app", Name: "web-old", UID: "old-uid"}
	current := []domain.ObjectReference{
		{Namespace: "app", Name: "web-new", UID: "new-uid"},
		{Namespace: "app", Name: "web-old", UID: "replacement-uid"},
	}
	workload := domain.WorkloadSpec{
		Adapter:      domain.WorkloadDeployment,
		Pod:          previous,
		AffectedPods: []domain.ObjectReference{previous},
	}

	updateDeploymentPodReferences(&workload, current)

	if workload.Pod != current[0] || !slices.Equal(workload.AffectedPods, current) {
		t.Fatalf("generated Pod references=%+v", workload)
	}
}

func TestCurrentRollbackPodsUsesDeploymentOwnership(t *testing.T) {
	deployment, rs, oldPods := deploymentTestObjects()
	replacement := oldPods[0].DeepCopy()
	replacement.Name = "web-replacement"
	replacement.UID = "web-replacement-uid"
	external := oldPods[1].DeepCopy()
	external.Name = "web-external"
	external.UID = "web-external-uid"
	external.OwnerReferences = nil

	client := kubernetesfake.NewClientset(deployment, rs, replacement, external)
	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	original := int32(2)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Pod:     podReference(oldPods[0]),
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{
			podReference(oldPods[0]),
			podReference(oldPods[1]),
		},
	})

	current, err := manager.CurrentRollbackPods(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}

	if len(current) != 1 || current[0].Name != replacement.Name ||
		current[0].UID != replacement.UID {
		t.Fatalf("current rollback Pods=%#v, want only %s", current, replacement.Name)
	}
}

func TestResumeDeploymentRejectsReplicaDriftWhileWaiting(t *testing.T) {
	deployment, rs, pods := deploymentTestObjects()
	deployment.ResourceVersion = "7"
	deployment.Spec.Replicas = new(int32)
	deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation}
	client := kubernetesfake.NewClientset(deployment, rs, pods[0], pods[1])
	deploymentsResource := appsv1.SchemeGroupVersion.WithResource("deployments")
	one := int32(1)

	client.PrependReactor(
		"update",
		"deployments",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*appsv1.Deployment](t, action)
			if deploymentReplicas(updated) != 2 {
				return false, nil, nil
			}

			drifted := updated.DeepCopy()
			drifted.Spec.Replicas = &one
			drifted.Generation++

			drifted.Status = appsv1.DeploymentStatus{
				ObservedGeneration: drifted.Generation,
				Replicas:           2,
				UpdatedReplicas:    2,
				ReadyReplicas:      2,
				AvailableReplicas:  2,
			}
			if err := client.Tracker().Update(
				deploymentsResource,
				drifted,
				drifted.Namespace,
			); err != nil {
				t.Fatal(err)
			}

			return true, updated, nil
		},
	)

	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond
	original := int32(2)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Pod:     podReference(pods[0]),
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{
			podReference(pods[0]),
			podReference(pods[1]),
		},
	})

	err := manager.Resume(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replicas changed to 1 while restoring 2") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestValidateDeploymentResumeAcceptsOnlyPausedOrOriginalReplicas(t *testing.T) {
	for _, test := range []struct {
		name         string
		replicas     int32
		wantConflict bool
	}{
		{name: "paused", replicas: 0},
		{name: "already restored", replicas: 2},
		{name: "drifted", replicas: 1, wantConflict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			deployment, _, pods := deploymentTestObjects()
			deployment.Spec.Replicas = &test.replicas
			client := kubernetesfake.NewClientset(deployment)
			manager := NewManager(
				client,
				fake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)
			original := int32(2)
			session := controllerSession(domain.WorkloadSpec{
				Adapter: domain.WorkloadDeployment,
				Pod:     podReference(pods[0]),
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       domain.KindDeployment,
					Namespace:  deployment.Namespace,
					Name:       deployment.Name,
					UID:        deployment.UID,
				},
				OriginalReplicas: &original,
			})

			err := manager.ValidateResume(context.Background(), session)
			if !test.wantConflict && err != nil {
				t.Fatal(err)
			}

			if test.wantConflict &&
				(domain.CategoryOf(err) != domain.ErrorConflict ||
					!strings.Contains(err.Error(), "replicas changed to 1 while restoring 2")) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestPauseDeploymentRollbackScalesUnreadyPods(t *testing.T) {
	deployment, rs, oldPods := deploymentTestObjects()
	deployment.ResourceVersion = "7"
	deployment.Status = appsv1.DeploymentStatus{
		ObservedGeneration:  deployment.Generation,
		Replicas:            1,
		UnavailableReplicas: 1,
	}
	pending := oldPods[0].DeepCopy()
	pending.Name = "web-new-pending"
	pending.UID = "web-new-pending-uid"
	pending.Status = corev1.PodStatus{Phase: corev1.PodPending}
	client := kubernetesfake.NewClientset(deployment, rs, pending)
	podsResource := corev1.SchemeGroupVersion.WithResource("pods")
	client.PrependReactor(
		"update",
		"deployments",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*appsv1.Deployment](t, action)
			if deploymentReplicas(updated) == 0 {
				if err := client.Tracker().Delete(
					podsResource,
					pending.Namespace,
					pending.Name,
				); err != nil {
					t.Fatal(err)
				}
			}

			return false, nil, nil
		},
	)

	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond

	original := int32(2)
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Pod:     podReference(oldPods[0]),
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{
			podReference(oldPods[0]),
			podReference(oldPods[1]),
		},
	}
	session := controllerSession(workload)
	session.Status.Phase = domain.PhaseRollingBack

	if err := manager.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	paused := session.Spec.Workload()
	if len(paused.AffectedPods) != 1 || paused.AffectedPods[0].Name != pending.Name ||
		paused.AffectedPods[0].UID != pending.UID {
		t.Fatalf("affected pods=%#v", paused.AffectedPods)
	}

	if paused.Pod.Name != pending.Name || paused.Pod.UID != pending.UID {
		t.Fatalf("selected pod=%#v", paused.Pod)
	}

	current, err := client.AppsV1().Deployments(deployment.Namespace).Get(
		context.Background(),
		deployment.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if replicas := deploymentReplicas(current); replicas != 0 {
		t.Fatalf("replicas=%d want=0", replicas)
	}
}

func TestPauseDeploymentScaleUsesValidatedSnapshot(t *testing.T) {
	deployment, rs, pods := deploymentTestObjects()
	deployment.ResourceVersion = "7"
	client := kubernetesfake.NewClientset(deployment, rs, pods[0], pods[1])
	deploymentGets := 0
	client.PrependReactor(
		"get",
		"deployments",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			deploymentGets++
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"update",
		"deployments",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*appsv1.Deployment](t, action)
			if updated.ResourceVersion != deployment.ResourceVersion {
				t.Fatalf(
					"resourceVersion=%q want=%q",
					updated.ResourceVersion,
					deployment.ResourceVersion,
				)
			}

			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: "apps", Resource: "deployments"},
				updated.Name,
				errors.New("concurrent rollout"),
			)
		},
	)

	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	original := int32(2)
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Pod:     podReference(pods[0]),
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{
			podReference(pods[0]),
			podReference(pods[1]),
		},
	}

	err := manager.Pause(context.Background(), controllerSession(workload))
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "changed after validation for pause Deployment") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if deploymentGets != 1 {
		t.Fatalf("Deployment GETs=%d want=1", deploymentGets)
	}

	current, err := client.Tracker().Get(
		appsv1.SchemeGroupVersion.WithResource("deployments"),
		deployment.Namespace,
		deployment.Name,
	)
	if err != nil {
		t.Fatal(err)
	}

	currentDeployment, ok := current.(*appsv1.Deployment)
	if !ok {
		t.Fatalf("tracker returned %T, want *appsv1.Deployment", current)
	}

	if replicas := deploymentReplicas(currentDeployment); replicas != original {
		t.Fatalf("replicas=%d want=%d", replicas, original)
	}
}

func TestDeploymentDiscoveryRejectsOperatorOwnedDeployment(t *testing.T) {
	deployment, rs, pods := deploymentTestObjects()
	deployment.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "example.io/v1",
			Kind:       "Application",
			Name:       "web-app",
			UID:        types.UID("app-uid"),
			Controller: new(true),
		},
	}
	client := kubernetesfake.NewClientset(deployment, rs, pods[0], pods[1])
	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	_, err := manager.DiscoverPod(
		context.Background(),
		pods[0],
		DiscoverOptions{Namespace: "app", PodName: pods[0].Name},
	)

	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "cannot be safely scaled directly") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestDeploymentDiscoveryRejectsHPAControlledDeployment(t *testing.T) {
	deployment, rs, pods := deploymentTestObjects()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: deployment.Namespace, Name: "web-autoscaler"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment.Name,
			},
			MaxReplicas: 3,
		},
	}
	client := kubernetesfake.NewClientset(deployment, rs, pods[0], pods[1], hpa)
	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)

	_, err := manager.DiscoverPod(
		context.Background(),
		pods[0],
		DiscoverOptions{Namespace: deployment.Namespace, PodName: pods[0].Name},
	)

	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "HorizontalPodAutoscaler") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestVerifyDeploymentPausedRejectsHPAAddedAfterDiscovery(t *testing.T) {
	deployment, rs, _ := deploymentTestObjects()
	deployment.Spec.Replicas = new(int32)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: deployment.Namespace, Name: "web-autoscaler"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment.Name,
			},
			MaxReplicas: 3,
		},
	}
	client := kubernetesfake.NewClientset(deployment, rs, hpa)
	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)

	original := int32(2)
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
	}

	err := manager.VerifyPaused(context.Background(), controllerSession(workload))

	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "HorizontalPodAutoscaler") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPauseDeploymentRejectsReconciliationDriftWithoutScaling(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*appsv1.Deployment) []runtime.Object
		want      string
	}{
		{
			name: "HPA",
			configure: func(deployment *appsv1.Deployment) []runtime.Object {
				return []runtime.Object{&autoscalingv2.HorizontalPodAutoscaler{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: deployment.Namespace,
						Name:      "web-autoscaler",
					},
					Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
						ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
							APIVersion: "apps/v1",
							Kind:       "Deployment",
							Name:       deployment.Name,
						},
						MaxReplicas: 3,
					},
				}}
			},
			want: "HorizontalPodAutoscaler",
		},
		{
			name: "controller owner",
			configure: func(deployment *appsv1.Deployment) []runtime.Object {
				deployment.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: "example.io/v1",
					Kind:       "Application",
					Name:       "web-app",
					UID:        "app-uid",
					Controller: new(true),
				}}

				return nil
			},
			want: "cannot be safely scaled directly",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deployment, _, _ := deploymentTestObjects()
			objects := append([]runtime.Object{deployment}, test.configure(deployment)...)
			client := kubernetesfake.NewClientset(objects...)
			manager := NewManager(
				client,
				fake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)

			original := int32(2)
			workload := domain.WorkloadSpec{
				Adapter: domain.WorkloadDeployment,
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
					Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
				},
				OriginalReplicas: &original,
			}

			err := manager.Pause(context.Background(), controllerSession(workload))
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			current, err := client.AppsV1().Deployments(deployment.Namespace).Get(
				context.Background(),
				deployment.Name,
				metav1.GetOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}

			if replicas := deploymentReplicas(current); replicas != original {
				t.Fatalf("replicas=%d want=%d", replicas, original)
			}
		})
	}
}

func TestPauseDeploymentRejectsPodSetReplacementWithoutScaling(t *testing.T) {
	deployment, rs, pods := deploymentTestObjects()
	replacement := pods[1].DeepCopy()
	replacement.Name = "web-replacement"
	replacement.UID = "replacement-uid"
	client := kubernetesfake.NewClientset(deployment, rs, pods[0], replacement)
	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)

	original := int32(2)
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{
			podReference(pods[0]),
			podReference(pods[1]),
		},
	}

	err := manager.Pause(context.Background(), controllerSession(workload))
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "Pod set changed") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	current, err := client.AppsV1().Deployments(deployment.Namespace).Get(
		context.Background(),
		deployment.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if replicas := deploymentReplicas(current); replicas != original {
		t.Fatalf("replicas=%d want=%d", replicas, original)
	}
}

func TestPauseDeploymentRetryAcceptsAlreadyScaledDeployment(t *testing.T) {
	deployment, _, pods := deploymentTestObjects()
	deployment.Spec.Replicas = new(int32)
	client := kubernetesfake.NewClientset(deployment)
	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond

	original := int32(2)
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: deployment.Namespace, Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
		AffectedPods: []domain.ObjectReference{
			podReference(pods[0]),
			podReference(pods[1]),
		},
	}

	if err := manager.Pause(context.Background(), controllerSession(workload)); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyDeploymentPausedFindsUnrecordedPod(t *testing.T) {
	deployment, rs, pods := deploymentTestObjects()
	deployment.Spec.Replicas = new(int32)
	client := kubernetesfake.NewClientset(deployment, rs, pods[0], pods[1])
	manager := NewManager(
		client,
		fake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond

	original := int32(2)
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion, Kind: domain.KindDeployment,
			Namespace: "app", Name: deployment.Name, UID: deployment.UID,
		},
		OriginalReplicas: &original,
	}

	if err := manager.VerifyPaused(context.Background(), controllerSession(workload)); err == nil ||
		!strings.Contains(err.Error(), "still present") {
		t.Fatalf("expected unrecorded Deployment Pod to fail verification, error=%v", err)
	}
}
