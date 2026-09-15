package controller

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestPauseCRDReturnsCheckpointWithoutMutatingPlanOnScaleFailure(t *testing.T) {
	deployment, replicaSet, oldPods := deploymentTestObjects()
	replacement := oldPods[0].DeepCopy()
	replacement.Name, replacement.UID = "replacement", "replacement-uid"
	client := kubernetesfake.NewClientset(deployment, replicaSet, replacement)
	injected := errors.New("scale update rejected")
	client.PrependReactor(
		"update",
		"deployments",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, injected
		},
	)

	workload := v1alpha1.WorkloadSpec{
		Adapter: v1alpha1.WorkloadDeployment,
		Pod:     workloadPodReference(oldPods[0]),
		Controller: workloadObjectReference(
			"apps/v1",
			"Deployment",
			deployment.Name,
			deployment.UID,
			"",
		),
		OriginalReplicas: deployment.Spec.Replicas,
		AffectedPods:     []v1alpha1.LocalResourceReference{*workloadPodReference(oldPods[0])},
	}
	original := workload.DeepCopy()
	manager := NewManager(client, nil, nil)

	checkpoint, err := manager.Pause(t.Context(), "migration", deployment.Namespace,
		workload, domain.PhaseRollingBack, "")
	if !errors.Is(err, injected) {
		t.Fatalf("expected scale failure after observing replacement, got %v", err)
	}

	if checkpoint == nil || checkpoint.Pod == nil || checkpoint.Pod.UID != replacement.UID ||
		len(checkpoint.AffectedPods) != 1 || checkpoint.AffectedPods[0].UID != replacement.UID {
		t.Fatalf("replacement identity missing from failure checkpoint: %+v", checkpoint)
	}

	checkpoint.Pod.UID = "caller-modified"
	checkpoint.AffectedPods[0].Name = "caller-modified"

	if !reflect.DeepEqual(workload, *original) {
		t.Fatalf("pause or returned checkpoint mutated the plan: %+v", workload)
	}
}

func TestWorkloadCRDEntriesRequireScopeWithoutSessionContext(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	for _, test := range []struct {
		name      string
		namespace string
		adapter   v1alpha1.WorkloadKind
	}{
		{name: "missing adapter", namespace: "app"},
		{name: "missing namespace", adapter: v1alpha1.WorkloadStandalone},
	} {
		t.Run(test.name, func(t *testing.T) {
			workload := v1alpha1.WorkloadSpec{Adapter: test.adapter}
			_, pauseErr := manager.Pause(t.Context(), "migration", test.namespace, workload, "", "")
			_, queryErr := manager.CurrentRollbackPods(
				t.Context(),
				"migration",
				test.namespace,
				workload,
			)
			verifyErr := manager.VerifyPaused(
				t.Context(),
				"migration",
				test.namespace,
				workload,
				"",
				"",
			)

			resumeErr := manager.ValidateResume(
				t.Context(),
				"migration",
				test.namespace,
				workload,
				"",
				"",
			)

			_, executeResumeErr := manager.Resume(t.Context(), "migration", test.namespace,
				workload, "", "", "", nil)
			for _, err := range []error{pauseErr, queryErr, verifyErr, resumeErr, executeResumeErr} {
				if domain.CategoryOf(err) != domain.ErrorValidation {
					t.Fatalf("expected input validation before Kubernetes access, got %v", err)
				}
			}
		})
	}
}

func TestResumeStandaloneCRDCheckpointsCreationBeforeReadinessFailure(t *testing.T) {
	pod := readyPod("app", "worker", "node-a")

	snapshot, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}

	workload := v1alpha1.WorkloadSpec{
		Adapter:        v1alpha1.WorkloadStandalone,
		Pod:            workloadPodReference(pod),
		OriginalObject: &apiextensionsv1.JSON{Raw: snapshot},
		AffectedPods:   []v1alpha1.LocalResourceReference{*workloadPodReference(pod)},
	}
	original := workload.DeepCopy()
	client := kubernetesfake.NewClientset()
	created := false
	injected := errors.New("readiness query failed")

	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			object := testutil.MustActionObject[*corev1.Pod](t, action)
			object.UID = "created-uid"
			created = true
			return false, nil, nil
		},
	)
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if created {
			return true, nil, injected
		}
		return false, nil, nil
	})
	manager := NewManager(client, nil, nil)

	checkpoint, err := manager.Resume(t.Context(), "migration", pod.Namespace, workload,
		"", domain.PhaseResuming, "", nil)
	if !errors.Is(err, injected) {
		t.Fatalf("expected readiness failure after creation, got %v", err)
	}

	if checkpoint == nil || checkpoint.Pod == nil || checkpoint.Pod.UID != "created-uid" ||
		len(checkpoint.AffectedPods) != 1 || checkpoint.AffectedPods[0].UID != "created-uid" {
		t.Fatalf("creation identity missing from checkpoint: %+v", checkpoint)
	}

	checkpoint.Pod.UID = "caller-modified"
	checkpoint.AffectedPods[0].UID = "caller-modified"

	if !reflect.DeepEqual(workload, *original) {
		t.Fatalf("resume or returned checkpoint mutated its input: %+v", workload)
	}
}

func TestCurrentRollbackPodsUsesCRDLocalReferenceScope(t *testing.T) {
	pod := readyPod("source", "worker", "node-a")
	pod.Annotations = map[string]string{kube.SessionKey: "migration"}
	foreign := pod.DeepCopy()
	foreign.Namespace, foreign.UID = "unrelated", "foreign-uid"
	client := kubernetesfake.NewClientset(pod, foreign)
	manager := NewManager(client, nil, nil)
	workload := v1alpha1.WorkloadSpec{
		Adapter: v1alpha1.WorkloadStandalone,
		Pod:     &v1alpha1.LocalResourceReference{Name: pod.Name, UID: "original-uid"},
	}

	current, err := manager.CurrentRollbackPods(t.Context(), "migration", pod.Namespace, workload)
	if err != nil {
		t.Fatal(err)
	}

	if len(current) != 1 || current[0] != podReference(pod) {
		t.Fatalf("rollback query escaped workload namespace: %+v", current)
	}
}
