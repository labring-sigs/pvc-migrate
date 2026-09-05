package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestControllerSubmissionChecksOnlySelectedWorkflow(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(map[bool]string{false: "namespaced", true: "cluster"}[cluster], func(t *testing.T) {
			resource, namespace := "copies", "app"

			spec := domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
				SourceNamespace:      "app",
				TemporaryNamespace:   "app",
				DestinationNamespace: "app",
				SessionNamespace:     "app",
			}, false, domain.SessionWorkflowOptions{})
			if cluster {
				spec.TemporaryNamespace = "destination"
				resource, namespace = "clustercopies", ""
			}

			client := kubernetesfake.NewClientset()
			calls := 0
			client.PrependReactor(
				"create",
				"selfsubjectaccessreviews",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
						t,
						action,
					).DeepCopy()
					a := review.Spec.ResourceAttributes
					calls++
					review.Status.Allowed = a.Group == "migrate.sealos.io" &&
						a.Resource == resource &&
						a.Namespace == namespace &&
						(a.Verb == "create" && a.Name == "" || (a.Verb == "get" || a.Verb == "watch") && a.Name == "transfer")

					return true, review, nil
				},
			)

			plan := &domain.MigrationPlan{Ready: true, SessionID: "transfer"}
			New(
				client,
				nil,
			).WithControllerSubmission(true).
				checkRBAC(context.Background(), plan, spec, false, false)

			if !plan.Ready || calls != 3 {
				t.Fatalf("submission checks=%#v calls=%d", plan.Checks, calls)
			}
		})
	}
}

func TestReservationDoesNotRequireTransferOrSourceDeletionPermissions(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
				t,
				action,
			).DeepCopy()
			a := review.Spec.ResourceAttributes

			review.Status.Allowed = true
			switch a.Resource {
			case "secrets", "services", "serviceaccounts", "jobs", "deployments", "replicasets":
				review.Status.Allowed = false
			case "persistentvolumeclaims":
				review.Status.Allowed = a.Namespace != "app" || a.Verb == "get" ||
					a.Verb == "update"
			}

			return true, review, nil
		},
	)

	spec := domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "destination",
		DestinationNamespace: "destination",
		SessionNamespace:     "system",
	}, false, domain.SessionWorkflowOptions{})
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkRBAC(context.Background(), plan, spec, false, false)

	if !plan.Ready {
		t.Fatalf("reservation requires unrelated permissions: %#v", plan.Checks)
	}
}

func TestAccessReviewsPreserveSubresourcesAndResourceNames(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
				t,
				action,
			).DeepCopy()
			a := review.Spec.ResourceAttributes
			review.Status.Allowed = a.Resource == "pods" && a.Subresource == "log" &&
				a.Verb == "get" ||
				a.Resource == "serviceaccounts" && a.Name == kube.TransferServiceAccountName &&
					a.Verb == "update"

			return true, review, nil
		},
	)

	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkAccessReviews(context.Background(), plan, []rbacAccess{
		{namespace: "app", resource: "pods/log", verb: "get"},
		{
			namespace: "app",
			resource:  "serviceaccounts",
			name:      kube.TransferServiceAccountName,
			verb:      "update",
		},
		{namespace: "app", resource: "pods/log", verb: "get"},
	})

	if !plan.Ready || !strings.Contains(plan.Checks[0].Message, "2 required") {
		t.Fatalf("reviews=%#v", plan.Checks)
	}
}
