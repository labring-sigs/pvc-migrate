package kube

import (
	"context"
	"errors"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// CRDWorkflowOwnerFinder resolves ownership from operation-specific workflow
// CRDs. It deliberately has no ConfigMap/session fallback.
type CRDWorkflowOwnerFinder struct{ client dynamic.Interface }

func NewCRDWorkflowOwnerFinder(client dynamic.Interface) *CRDWorkflowOwnerFinder {
	return &CRDWorkflowOwnerFinder{client: client}
}

func (f *CRDWorkflowOwnerFinder) Find(
	ctx context.Context,
	id string,
	namespaces ...string,
) (*WorkflowOwner, error) {
	if f == nil || f.client == nil {
		return nil, errors.New("CRD workflow owner lookup requires a dynamic client")
	}

	for _, resource := range workflowCRDResourceRegistry() {
		workflow, ok := domain.ControllerResourceForKind(resource.kind)
		if !ok {
			continue
		}

		gvr := schema.GroupVersionResource{
			Group:    MetadataDomain,
			Version:  "v1alpha1",
			Resource: workflow.Resource,
		}

		names := namespaces
		if resource.cluster {
			names = []string{""}
		}

		for _, namespace := range names {
			if namespace == "" && !resource.cluster {
				continue
			}

			resourceClient := f.client.Resource(gvr)

			var api dynamic.ResourceInterface = resourceClient
			if !resource.cluster {
				api = resourceClient.Namespace(namespace)
			}

			object, err := api.Get(ctx, id, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				continue
			}

			if err != nil {
				return nil, fmt.Errorf("look up workflow %s: %w", id, err)
			}

			return workflowOwner(object, id, namespace, SessionBackendCRD), nil
		}
	}

	return nil, nil
}
