package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestSessionNamespaceChecksRejectMissingNamespacesWithoutCreatePermission(t *testing.T) {
	for _, operation := range []domain.Operation{domain.OperationCopy, domain.OperationRename} {
		for _, missing := range []string{"", "sessions", "staging", "destination"} {
			t.Run(string(operation)+"/missing="+missing, func(t *testing.T) {
				objects := []runtime.Object{}
				for _, name := range []string{"sessions", "staging", "destination"} {
					if name != missing {
						objects = append(
							objects,
							&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}},
						)
					}
				}

				client := kubernetesfake.NewClientset(objects...)
				client.PrependReactor(
					"create",
					"selfsubjectaccessreviews",
					func(action clienttesting.Action) (bool, runtime.Object, error) {
						review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
							t,
							action,
						).DeepCopy()
						a := review.Spec.ResourceAttributes
						review.Status.Allowed = a.Resource != "namespaces" ||
							a.Verb == "get" && a.Name != ""

						return true, review, nil
					},
				)

				spec := domain.NewSessionSpec(operation, domain.SessionCommon{
					SourceNamespace: "source", SessionNamespace: "sessions",
					TemporaryNamespace: "staging", DestinationNamespace: "destination",
				}, false, domain.SessionWorkflowOptions{})
				plan := &domain.MigrationPlan{Ready: true}

				planner := New(client, nil)
				if operation == domain.OperationRename {
					planner.checkRenameRBAC(context.Background(), plan, spec)
				} else {
					planner.checkRBAC(context.Background(), plan, spec, false, false)
				}

				if plan.Ready != (missing == "") {
					t.Fatalf("missing=%q checks=%#v", missing, plan.Checks)
				}

				for _, action := range client.Actions() {
					if action.GetResource().Resource == "namespaces" && action.GetVerb() != "get" {
						t.Fatalf("planning mutated namespaces: %#v", action)
					}

					if action.GetResource().Resource == "selfsubjectaccessreviews" {
						review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
							t,
							action,
						)
						if a := review.Spec.ResourceAttributes; a.Resource == "namespaces" &&
							a.Verb != "get" {
							t.Fatalf("requested namespace write permission: %#v", a)
						}
					}
				}
			})
		}
	}
}

func TestNamespacePermissionCheckFailsClosedOnReadError(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor(
		"get",
		"namespaces",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Resource: "namespaces"},
				"app",
				nil,
			)
		},
	)

	plan := &domain.MigrationPlan{Ready: true}

	checks := New(client, nil).namespaceRBAC(context.Background(), plan, domain.SessionSpec{
		SessionCommon: domain.SessionCommon{SessionNamespace: "app", TemporaryNamespace: "app"},
	})
	if plan.Ready || len(checks) != 1 || checks[0].verb != "get" {
		t.Fatalf("checks=%#v plan=%#v", checks, plan.Checks)
	}
}

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

			client := kubernetesfake.NewClientset(
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}},
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "destination"}},
			)
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

func TestControllerSubmissionNamespacePreflight(t *testing.T) {
	for _, forbidden := range []bool{false, true} {
		client := rbacTestClient()
		client.PrependReactor(
			"create",
			"selfsubjectaccessreviews",
			func(action clienttesting.Action) (bool, runtime.Object, error) {
				review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
					t,
					action,
				).DeepCopy()
				review.Status.Allowed = true

				return true, review, nil
			},
		)
		client.PrependReactor(
			"get",
			"namespaces",
			func(action clienttesting.Action) (bool, runtime.Object, error) {
				if forbidden {
					return true, nil, apierrors.NewForbidden(
						schema.GroupResource{Resource: "namespaces"},
						"missing",
						nil,
					)
				}

				return true, nil, apierrors.NewNotFound(
					schema.GroupResource{Resource: "namespaces"},
					"missing",
				)
			},
		)

		spec := domain.NewSessionSpec(
			domain.OperationCopy,
			domain.SessionCommon{
				SessionNamespace:     "missing",
				TemporaryNamespace:   "missing",
				DestinationNamespace: "missing",
			},
			false,
			domain.SessionWorkflowOptions{},
		)
		plan := &domain.MigrationPlan{Ready: true, SessionID: "preflight"}
		New(client, nil).checkControllerSubmissionRBAC(t.Context(), plan, spec)

		if plan.Ready != forbidden {
			t.Fatalf("forbidden=%v ready=%v checks=%v", forbidden, plan.Ready, plan.Checks)
		}

		for _, action := range client.Actions() {
			if action.GetVerb() == "create" &&
				action.GetResource().Resource != "selfsubjectaccessreviews" {
				t.Fatal("preflight created a resource")
			}
		}
	}
}

func TestReservationDoesNotRequireTransferOrSourceDeletionPermissions(t *testing.T) {
	client := rbacTestClient()
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
