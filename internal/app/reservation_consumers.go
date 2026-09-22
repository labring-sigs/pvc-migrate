package app

import (
	"context"
	"fmt"
	"slices"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Inventory every namespace before deleting any consumer. The returned identities
// fence deletion against replacement or ownership changes after this preflight.
func inventoryReservationPods(
	ctx context.Context,
	client kubernetes.Interface,
	id string,
	namespaces []string,
) ([]v1alpha1.ObjectReference, error) {
	namespaces = slices.Clone(namespaces)
	slices.Sort(namespaces)

	namespaces = slices.Compact(namespaces)
	if id == "" || slices.Contains(namespaces, "") {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"cleanup",
			"workflow identity and explicit namespaces are required",
		)
	}

	type inventory struct {
		refs []v1alpha1.ObjectReference
		err  error
	}

	results := make([]inventory, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		namespace := namespaces[index]

		pods, err := client.CoreV1().
			Pods(namespace).
			List(ctx, metav1.ListOptions{LabelSelector: reservationPodSelector(id)})
		if apierrors.IsNotFound(err) {
			return
		}

		if err != nil {
			results[index].err = domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup",
				"list reservation Pods in "+namespace,
				err,
			)

			return
		}

		if pods == nil {
			results[index].err = domain.NewError(
				domain.ErrorKubernetes,
				"cleanup",
				"reservation Pod list returned an empty object",
			)

			return
		}

		for _, pod := range pods.Items {
			if pod.Labels[kube.ManagedByLabel] != kube.ManagedByValue ||
				pod.Labels[kube.SessionKey] != id ||
				pod.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleReservationConsumer {
				results[index].err = domain.NewError(
					domain.ErrorConflict,
					"cleanup",
					"reservation Pod ownership changed",
				)

				return
			}

			results[index].refs = append(results[index].refs, v1alpha1.ObjectReference{
				APIVersion: "v1", Kind: "Pod", Namespace: namespace,
				Name: pod.Name, UID: pod.UID, ResourceVersion: pod.ResourceVersion,
			})
		}
	})

	var refs []v1alpha1.ObjectReference
	for _, result := range results {
		if result.err != nil {
			return nil, result.err
		}

		refs = append(refs, result.refs...)
	}

	return refs, nil
}

func deleteReservationConsumers(
	ctx context.Context,
	client kubernetes.Interface,
	pods []v1alpha1.ObjectReference,
) error {
	for _, pod := range pods {
		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		uid, version := pod.UID, pod.ResourceVersion
		if err := client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version},
		}); err != nil && !apierrors.IsNotFound(err) {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup",
				"delete reservation Pod "+pod.Name,
				err,
			)
		}

		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		if err := kube.WaitFor(
			ctx,
			time.Second,
			fmt.Sprintf("reservation Pod %s/%s deletion", pod.Namespace, pod.Name),
			func(waitCtx context.Context) (bool, error) {
				current, err := client.CoreV1().
					Pods(pod.Namespace).
					Get(waitCtx, pod.Name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return true, nil
				}

				if err == nil && current.UID != uid {
					return false, domain.NewError(
						domain.ErrorConflict,
						"cleanup",
						"reservation Pod was replaced while waiting for deletion",
					)
				}

				return false, err
			},
		); err != nil {
			return err
		}
	}

	return nil
}

func reservationPodSelector(id string) string {
	return kube.ManagedByLabel + "=" + kube.ManagedByValue + "," + kube.SessionKey + "=" + id + "," + kube.ResourceRoleLabel + "=" + kube.ResourceRoleReservationConsumer
}
