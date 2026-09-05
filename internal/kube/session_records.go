package kube

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// SessionRecords checks both persistence backends before ownership can be
// treated as orphaned. An unreadable record never proves absence.
type SessionRecords struct {
	client  kubernetes.Interface
	dynamic dynamic.Interface
}

func NewSessionRecords(
	client kubernetes.Interface,
	dynamicClient dynamic.Interface,
) *SessionRecords {
	return &SessionRecords{client: client, dynamic: dynamicClient}
}

// Find returns nil only when every candidate record is absent. Both the source
// and session namespaces matter when a new operation uses a different backend.
func (r *SessionRecords) Find(
	ctx context.Context,
	id string,
	namespaces ...string,
) (*domain.Session, error) {
	namespaces = slices.Clone(namespaces)
	slices.Sort(namespaces)
	namespaces = slices.Compact(namespaces)

	lookups := make([]func() (*domain.Session, error), 0)
	for _, namespace := range namespaces {
		if namespace == "" {
			continue
		}

		lookups = append(lookups, func() (*domain.Session, error) {
			return NewConfigMapSessionStore(r.client).Get(ctx, namespace, id)
		})
	}

	if r.dynamic != nil {
		for _, resource := range workflowCRDResourceRegistry() {
			names := namespaces
			if resource.cluster {
				names = []string{""}
			}

			for _, namespace := range names {
				if namespace == "" && !resource.cluster {
					continue
				}

				lookups = append(lookups, func() (*domain.Session, error) {
					workflow, _ := domain.ControllerResourceForKind(resource.kind)
					gvr := schema.GroupVersionResource{
						Group:    MetadataDomain,
						Version:  "v1alpha1",
						Resource: workflow.Resource,
					}

					var api dynamic.ResourceInterface = r.dynamic.Resource(gvr)
					if !resource.cluster {
						api = r.dynamic.Resource(gvr).Namespace(namespace)
					}

					object, err := api.Get(ctx, id, metav1.GetOptions{})
					if err != nil {
						return nil, err
					}

					return decodeControllerWatchObject(object, resource.kind)
				})
			}
		}
	}

	sessions := make([]*domain.Session, len(lookups))
	errs := make([]error, len(lookups))
	parallel.For(len(lookups), func(index int) {
		sessions[index], errs[index] = lookups[index]()
		if apierrors.IsNotFound(errs[index]) {
			errs[index] = nil
		}
	})

	for _, session := range sessions {
		if session != nil {
			return session, nil
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("cannot establish whether session %s exists: %w", id, err)
	}

	return nil, nil
}
