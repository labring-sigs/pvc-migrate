package kube

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkflowOwnerFinder resolves the durable owner of one workflow identity.
// Planning and orphan recovery only read the ownership summary.
type WorkflowOwnerFinder interface {
	Find(ctx context.Context, id string, namespaces ...string) (*WorkflowOwner, error)
}

// CompositeWorkflowOwnerFinder consults every backend finder until one
// resolves the identity. Controller-mode workflows live in CRDs while
// session-mode workflows persist in ConfigMaps, and both can own source
// identities on the same cluster.
type CompositeWorkflowOwnerFinder struct {
	finders []WorkflowOwnerFinder
}

func NewCompositeWorkflowOwnerFinder(finders ...WorkflowOwnerFinder) *CompositeWorkflowOwnerFinder {
	return &CompositeWorkflowOwnerFinder{finders: finders}
}

func (f *CompositeWorkflowOwnerFinder) Find(
	ctx context.Context,
	id string,
	namespaces ...string,
) (*WorkflowOwner, error) {
	if f == nil || len(f.finders) == 0 {
		return nil, errors.New("workflow owner lookup requires at least one backend finder")
	}

	var failure error
	for _, finder := range f.finders {
		owner, err := finder.Find(ctx, id, namespaces...)
		if owner != nil {
			return owner, nil
		}

		if err != nil {
			failure = errors.Join(failure, err)
		}
	}

	if failure != nil {
		return nil, failure
	}

	return nil, nil
}

// ConfigMapWorkflowOwnerFinder resolves ownership from session-mode workflow
// records stored in ConfigMaps. Session planning records ownership on source
// identities, so later planning and recovery must find the owning record
// without a workflow CRD.
type ConfigMapWorkflowOwnerFinder struct {
	client kubernetes.Interface
}

func NewConfigMapWorkflowOwnerFinder(client kubernetes.Interface) *ConfigMapWorkflowOwnerFinder {
	return &ConfigMapWorkflowOwnerFinder{client: client}
}

func (f *ConfigMapWorkflowOwnerFinder) Find(
	ctx context.Context,
	id string,
	namespaces ...string,
) (*WorkflowOwner, error) {
	if f == nil || f.client == nil {
		return nil, errors.New("ConfigMap workflow owner lookup requires a client")
	}

	for _, namespace := range namespaces {
		if namespace == "" {
			continue
		}

		object, err := LoadConfigMapWorkflow(ctx, f.client, namespace, id)
		if apierrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("look up workflow %s: %w", id, err)
		}

		return configMapWorkflowOwner(object, id, namespace, SessionBackendConfigMap), nil
	}

	return nil, nil
}

// configMapWorkflowOwner converts one stored workflow object into the shared
// non-executable ownership summary. Typed decoders drop TypeMeta, so the kind
// is restored from the concrete Go type before summarizing.
func configMapWorkflowOwner(
	object crclient.Object,
	id, namespace, backend string,
) *WorkflowOwner {
	converted, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		resource, _ := domain.ControllerResourceForKind(workflowKind(object))

		return &WorkflowOwner{
			ID:               id,
			SessionNamespace: namespace,
			Backend:          backend,
			Resource:         resource,
		}
	}

	summary := &unstructured.Unstructured{Object: converted}
	if summary.GetKind() == "" {
		summary.SetKind(string(workflowKind(object)))
		summary.SetAPIVersion(v1alpha1.GroupVersion.Identifier())
	}

	return workflowOwner(summary, id, namespace, backend)
}
