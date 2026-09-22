package controller

import (
	"context"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func execAccessManager(allowed bool, reason string) (*Manager, *fake.Clientset) {
	client := fake.NewClientset()
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(clienttesting.CreateAction)
			if !ok {
				return true, nil, nil
			}

			review, ok := create.GetObject().(*authorizationv1.SelfSubjectAccessReview)
			if !ok {
				return true, nil, nil
			}

			if review.Spec.ResourceAttributes == nil ||
				review.Spec.ResourceAttributes.Subresource != "exec" {
				return true, nil, nil
			}

			review.Status = authorizationv1.SubjectAccessReviewStatus{
				Allowed: allowed,
				Reason:  reason,
			}

			return true, review, nil
		},
	)

	return NewManager(client, nil, nil), client
}

func TestPodExecAllowedProbesSubresource(t *testing.T) {
	manager, client := execAccessManager(true, "granted")

	allowed, reason, err := manager.podExecAllowed(context.Background(), "tenants")
	if err != nil || !allowed || reason != "granted" {
		t.Fatalf("probe allowed=%v reason=%q err=%v", allowed, reason, err)
	}

	create, ok := client.Actions()[0].(clienttesting.CreateAction)
	if !ok {
		t.Fatalf("probe action=%T", client.Actions()[0])
	}

	review, ok := create.GetObject().(*authorizationv1.SelfSubjectAccessReview)
	if !ok {
		t.Fatalf("probe object=%T", create.GetObject())
	}

	attributes := review.Spec.ResourceAttributes
	if attributes.Namespace != "tenants" || attributes.Verb != "create" ||
		attributes.Resource != "pods" || attributes.Subresource != "exec" {
		t.Fatalf("probe attributes=%+v", attributes)
	}
}

func TestPodExecAllowedDenied(t *testing.T) {
	manager, _ := execAccessManager(false, "no RBAC match")

	allowed, reason, err := manager.podExecAllowed(context.Background(), "tenants")
	if err != nil || allowed || reason != "no RBAC match" {
		t.Fatalf("probe allowed=%v reason=%q err=%v", allowed, reason, err)
	}
}

func TestPodExecAllowedSurfacesAuthorizationFailure(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, domain.NewError(
				domain.ErrorKubernetes,
				"check pod exec access",
				"authorization endpoint unavailable",
			)
		},
	)

	manager := NewManager(client, nil, nil)

	if _, _, err := manager.podExecAllowed(context.Background(), "tenants"); err == nil {
		t.Fatal("authorization failure was swallowed")
	}
}

func mongoDBExecPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenants",
			Name:      "mg-cluster-0",
			Labels:    map[string]string{"app.kubernetes.io/name": "mongodb"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "mongodb"}},
		},
	}
}

// TestPreflightMongoDBSwitchoverWithoutExec pins the no-exec path: the
// preflight must fail with the operator choices (grant, manual switchover,
// leader downtime) instead of attempting the exec and surfacing a raw
// forbidden error.
func TestPreflightMongoDBSwitchoverWithoutExec(t *testing.T) {
	manager, _ := execAccessManager(false, "no RBAC match")
	manager.commandExecutor = recordingCommandExecutor{}

	_, err := manager.preflightMongoDBNativeSwitchover(
		context.Background(),
		mongoDBExecPod(),
		"mg-cluster",
		"mongodb",
		"mg-cluster-1",
	)
	if err == nil {
		t.Fatal("preflight succeeded without pod exec permission")
	}

	message := err.Error()
	for _, phrase := range []string{
		"requires pod exec",
		"grant create pods/exec in namespace tenants",
		"rs.stepDown()",
		"--allow-leader-downtime",
		"mg-cluster-0",
	} {
		if !strings.Contains(message, phrase) {
			t.Fatalf("preflight error %q missing guidance %q", message, phrase)
		}
	}
}

// TestPreflightMongoDBSwitchoverWithExec pins the privileged path: with the
// permission granted the preflight keeps probing the switchover script
// through the exec endpoint exactly as before.
func TestPreflightMongoDBSwitchoverWithExec(t *testing.T) {
	manager, _ := execAccessManager(true, "")
	manager.commandExecutor = recordingCommandExecutor{}

	container, err := manager.preflightMongoDBNativeSwitchover(
		context.Background(),
		mongoDBExecPod(),
		"mg-cluster",
		"mongodb",
		"mg-cluster-1",
	)
	if err != nil {
		t.Fatal(err)
	}

	if container != "mongodb" {
		t.Fatalf("preflight container=%q", container)
	}
}

// TestRunMongoDBSwitchoverGuardWithoutExec pins the runtime guard: a workflow
// planned while exec was granted must stop with the same operator choices
// after the permission is revoked, before any Pod is read.
func TestRunMongoDBSwitchoverGuardWithoutExec(t *testing.T) {
	manager, _ := execAccessManager(false, "revoked")
	manager.commandExecutor = recordingCommandExecutor{}

	err := manager.runMongoDBNativeSwitchover(
		context.Background(),
		v1alpha1.ObjectReference{Namespace: "tenants", Name: "mg-cluster-0"},
		v1alpha1.ObjectReference{Kind: domain.KindInstanceSet, Name: "mg-cluster"},
		&v1alpha1.KubeBlocksSpec{
			Cluster:             "mg-cluster",
			Component:           "mongodb",
			Instance:            "mg-cluster-0",
			SwitchoverCandidate: "mg-cluster-1",
			SwitchoverContainer: "mongodb",
		},
	)
	if err == nil {
		t.Fatal("runtime guard allowed the switchover without exec permission")
	}

	message := err.Error()
	if !strings.Contains(message, "requires pod exec") ||
		!strings.Contains(message, "--allow-leader-downtime") ||
		!strings.Contains(message, "kubectl --namespace tenants exec mg-cluster-0") {
		t.Fatalf("runtime guard error=%q", message)
	}
}

type recordingCommandExecutor struct{}

func (recordingCommandExecutor) Execute(
	context.Context,
	podCommandRequest,
) (podCommandResult, error) {
	return podCommandResult{}, nil
}
