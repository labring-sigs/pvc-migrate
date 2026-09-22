package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// podExecAllowed reports whether the current identity may create pods/exec in
// a namespace. The shipped RBAC intentionally grants no pod exec: MongoDB
// native switchover must degrade to an explicit operator choice instead of a
// raw forbidden error from the exec endpoint.
func (m *Manager) podExecAllowed(ctx context.Context, namespace string) (bool, string, error) {
	if m.typed == nil {
		return false, "", domain.NewError(
			domain.ErrorInternal,
			"check pod exec access",
			"kubernetes client is not configured",
		)
	}

	review, err := m.typed.AuthorizationV1().
		SelfSubjectAccessReviews().
		Create(ctx, &authorizationv1.SelfSubjectAccessReview{
			Spec: authorizationv1.SelfSubjectAccessReviewSpec{
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Namespace:   namespace,
					Verb:        "create",
					Resource:    "pods",
					Subresource: "exec",
				},
			},
		}, metav1.CreateOptions{})
	if err != nil {
		return false, "", domain.WrapError(
			domain.ErrorKubernetes,
			"check pod exec access",
			"SelfSubjectAccessReview for pods/exec",
			err,
		)
	}

	reason := review.Status.Reason
	if reason == "" {
		reason = review.Status.EvaluationError
	}

	return review.Status.Allowed, reason, nil
}

// mongoDBSwitchoverWithoutExec explains the three operator choices when the
// automatic MongoDB switchover cannot run without pod exec. instanceCommand
// is the manual switchover one-liner for a client that does hold exec
// permission (for example an administrator's own kubeconfig).
func mongoDBSwitchoverWithoutExec(
	namespace, instance, reason, instanceCommand string,
) error {
	choices := []string{
		fmt.Sprintf(
			"grant create pods/exec in namespace %s to the migrating identity (Role + RoleBinding) and retry",
			namespace,
		),
		"switch the MongoDB primary away from " + instance + " manually before retrying: run rs.stepDown() against the current primary from any authorized MongoDB client",
		"rerun with --allow-leader-downtime to migrate the primary while accepting a leader outage",
	}

	if instanceCommand != "" {
		choices[1] += ", or from a client holding exec permission: " + instanceCommand
	}

	detail := strings.TrimSpace(reason)
	if detail != "" {
		detail = " (" + detail + ")"
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"pause KubeBlocks",
		fmt.Sprintf(
			"KubeBlocks MongoDB automatic switchover requires pod exec, which this identity does not have%s: %s",
			detail,
			strings.Join(choices, "; "),
		),
	)
}
