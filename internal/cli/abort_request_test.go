package cli

import (
	"context"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// abortRequestFakeStore builds a CRD store whose loads simulate the
// controller: convergeTo describes the object the poll observes after the
// request was recorded.
func abortRequestFakeStore(
	t *testing.T,
	object *v1alpha1.Copy,
	convergeTo func(*v1alpha1.Copy),
) kube.WorkflowStore[*v1alpha1.Copy] {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	requested := false
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(object).
		WithObjects(object).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(
				ctx context.Context,
				c crclient.WithWatch,
				key crclient.ObjectKey,
				obj crclient.Object,
				opts ...crclient.GetOption,
			) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}

				// The controller answers the recorded request after the
				// annotation lands; loads before that observe the workflow
				// as the command loaded it.
				if requested {
					if current, ok := obj.(*v1alpha1.Copy); ok {
						convergeTo(current)
					}
				}

				return nil
			},
			Update: func(
				ctx context.Context,
				c crclient.WithWatch,
				obj crclient.Object,
				opts ...crclient.UpdateOption,
			) error {
				if err := c.Update(ctx, obj, opts...); err != nil {
					return err
				}

				if kube.WorkflowAbortRequested(obj) {
					requested = true
				}

				return nil
			},
		}).
		Build()

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	return store
}

func TestRequestControllerAbortWaitsForConvergence(t *testing.T) {
	object := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: "tenant", UID: "workflow"},
		Status: v1alpha1.CopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseWarmCopying},
		},
	}

	store := abortRequestFakeStore(t, object, func(current *v1alpha1.Copy) {
		current.Status.Phase = domain.PhaseAborted
		current.Annotations = nil
	})

	command := &cobra.Command{}

	converged, err := requestControllerAbort(t.Context(), command, store, object)
	if err != nil {
		t.Fatal(err)
	}

	if converged.Status.Phase != domain.PhaseAborted {
		t.Fatalf("converged object was not returned: %s", converged.Status.Phase)
	}
}

func TestRequestControllerAbortReportsRejection(t *testing.T) {
	object := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: "tenant", UID: "workflow"},
		Status: v1alpha1.CopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseWarmCopying},
		},
	}

	// The controller consumes the request without reaching Aborted: the
	// abort was rejected, and waiting longer cannot help.
	store := abortRequestFakeStore(t, object, func(current *v1alpha1.Copy) {
		current.Annotations = nil
	})

	command := &cobra.Command{}
	if _, err := requestControllerAbort(t.Context(), command, store, object); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("rejection was not reported: %v", err)
	}
}
