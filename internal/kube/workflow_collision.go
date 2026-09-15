package kube

import (
	"context"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type workflowCollisionLookup struct {
	resource  crdResource
	namespace string
}

// CheckWorkflowIdentityCollision checks the ownership namespace shared by CRD
// and ConfigMap workflows. Callers provide only the namespaces touched by the
// concrete operation; execution payloads are not needed for this lookup.
func CheckWorkflowIdentityCollision(
	ctx context.Context,
	client crclient.Client,
	kubernetesClient kubernetes.Interface,
	kinds []domain.ControllerKind,
	id string,
	currentKind domain.ControllerKind,
	namespaces []string,
	failOnForbidden bool,
) error {
	if client == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"check workflow name collision",
			"workflow client is not configured",
		)
	}

	if id == "" || currentKind == "" || len(namespaces) == 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"check workflow name collision",
			"workflow name, kind and namespace roles are required",
		)
	}

	if slices.Contains(namespaces, "") {
		return domain.NewError(
			domain.ErrorValidation,
			"check workflow name collision",
			"workflow namespace roles must be explicit",
		)
	}

	if err := checkConfigMapNameCollision(
		ctx,
		kubernetesClient,
		id,
		namespaces,
		failOnForbidden,
	); err != nil {
		return err
	}

	resources := workflowCRDResourceRegistry()
	if len(kinds) != 0 {
		resources = slices.DeleteFunc(
			resources,
			func(resource crdResource) bool { return !slices.Contains(kinds, resource.kind) },
		)
	}

	lookups := make([]workflowCollisionLookup, 0, len(resources)*len(namespaces))
	for _, resource := range resources {
		if resource.kind == currentKind {
			continue
		}

		if resource.cluster {
			lookups = append(lookups, workflowCollisionLookup{resource: resource})
			continue
		}

		for _, namespace := range namespaces {
			lookups = append(lookups, workflowCollisionLookup{
				resource: resource, namespace: namespace,
			})
		}
	}

	objects := make([]crclient.Object, len(lookups))
	errors := make([]error, len(lookups))
	parallel.For(len(lookups), func(index int) {
		lookup := lookups[index]
		object := lookup.resource.new()

		errors[index] = client.Get(
			ctx,
			resourceKey(lookup.resource, lookup.namespace, id),
			object,
		)
		if errors[index] == nil {
			objects[index] = object
		}
	})

	for index, lookup := range lookups {
		err := errors[index]
		if apierrors.IsNotFound(err) || apierrors.IsForbidden(err) && !failOnForbidden {
			continue
		}

		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"check workflow name collision",
				"read "+string(lookup.resource.kind),
				err,
			)
		}

		if objects[index] == nil {
			continue
		}

		location := lookup.namespace
		if lookup.resource.cluster {
			location = "cluster scope"
		}

		return domain.NewError(
			domain.ErrorConflict,
			"check workflow name collision",
			fmt.Sprintf(
				"workflow name %q is already used by %s in %s; workflow names must be unique across kinds that share namespace roles",
				id,
				lookup.resource.kind,
				location,
			),
		)
	}

	return nil
}

func checkConfigMapNameCollision(
	ctx context.Context,
	client kubernetes.Interface,
	id string,
	namespaces []string,
	failOnForbidden bool,
) error {
	if client == nil {
		return nil
	}

	for _, namespace := range namespaces {
		name := SessionConfigMapName(id)

		_, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || apierrors.IsForbidden(err) && !failOnForbidden {
			continue
		}

		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"check workflow name collision",
				"read session ConfigMap",
				err,
			)
		}

		return domain.NewError(
			domain.ErrorConflict,
			"check workflow name collision",
			fmt.Sprintf(
				"workflow name %q is already used by ConfigMap session %s/%s; use a different workflow name",
				id,
				namespace,
				name,
			),
		)
	}

	return nil
}

// CheckConfigMapIdentityCollision verifies a workflow identity is unused
// across the session ConfigMap store. Session-local workflows never claim
// API-server CRD identities, so the CRD scan is unnecessary.
func CheckConfigMapIdentityCollision(
	ctx context.Context,
	kubernetesClient kubernetes.Interface,
	id string,
	namespaces []string,
) error {
	return checkConfigMapNameCollision(
		ctx,
		kubernetesClient,
		id,
		namespaces,
		true,
	)
}
