package controller

import (
	"context"
	"fmt"
	"math"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

const (
	vmClusterFieldPaused        = "paused"
	vmClusterFieldReplicaCount  = "replicaCount"
	vmClusterFieldClusterStatus = "clusterStatus"
	vmClusterFieldUpdateStatus  = "updateStatus"
	vmClusterStatusOperational  = "operational"
)

func (m *Manager) verifyVMClusterPaused(
	ctx context.Context,
	workflowID, namespace string,
	controller v1alpha1.ObjectReference,
	ordinal *int32,
	vm *v1alpha1.VMClusterSpec,
) error {
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"session lacks VMCluster state",
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindStatefulSet,
		controller.Name,
		"verify paused",
	); err != nil {
		return err
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"dynamic client is required for VMCluster pause verification",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(namespace).
		Get(ctx, vm.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read VMCluster", err)
	}

	if object.GetUID() != vm.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
		)
	}

	if object.GetAnnotations()[pauseSessionAnnotation] != workflowID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"VMCluster %s/%s pause ownership changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	component, _, nestedErr := unstructured.NestedMap(object.Object, "spec", vm.Component)
	if nestedErr != nil {
		return nestedErr
	}

	if !vm.ComponentPausedSupported {
		// The CRD pruned the paused write: the hold is the reduced
		// replicaCount, which keeps the migrated ordinal beyond the
		// component's replica set while the session owns the pause.
		if ordinal == nil {
			return domain.NewError(
				domain.ErrorInternal,
				"verify paused",
				"session lacks StatefulSet replica state",
			)
		}

		replicas, found, err := unstructured.NestedInt64(
			component,
			vmClusterFieldReplicaCount,
		)
		if err != nil {
			return err
		}

		if !found || replicas > int64(*ordinal) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"verify paused",
				fmt.Sprintf(
					"VMCluster component %s replicaCount %d exceeds migration ordinal %d; the operator may restore the migrated Pod",
					vm.Component,
					replicas,
					*ordinal,
				),
			)
		}

		return nil
	}

	paused, _, _ := unstructured.NestedBool(component, vmClusterFieldPaused)
	if !paused {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf("VMCluster component %s is not paused", vm.Component),
		)
	}

	return nil
}

func (m *Manager) validateVMClusterResume(
	ctx context.Context,
	workflowID, namespace string,
	controller v1alpha1.ObjectReference,
	originalReplicas, ordinal *int32,
	vm *v1alpha1.VMClusterSpec,
) error {
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume VMCluster",
			"session lacks VMCluster state",
		)
	}

	if originalReplicas == nil || ordinal == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume VMCluster",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.validateStatefulSetTransitionReplicas(
		ctx,
		controller,
		originalReplicas,
		ordinal,
		"resume VMCluster",
	); err != nil {
		return err
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume VMCluster",
			"dynamic client is required for VMCluster resume validation",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(namespace).
		Get(ctx, vm.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "resume VMCluster", "read VMCluster", err)
	}

	if object.GetUID() != vm.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"resume VMCluster",
			fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
		)
	}

	component, found, nestedErr := unstructured.NestedMap(object.Object, "spec", vm.Component)
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"resume VMCluster",
			"read component state",
			nestedErr,
		)
	}

	if !found {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume VMCluster",
			fmt.Sprintf("VMCluster component %s is absent", vm.Component),
		)
	}

	paused, _, nestedErr := unstructured.NestedBool(component, vmClusterFieldPaused)
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"resume VMCluster",
			"read component pause state",
			nestedErr,
		)
	}

	currentReplicas, replicasFound, nestedErr := unstructured.NestedInt64(
		component,
		vmClusterFieldReplicaCount,
	)
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"resume VMCluster",
			"read component replica count",
			nestedErr,
		)
	}

	owner := object.GetAnnotations()[pauseSessionAnnotation]
	if vm.OriginalReplicasConfigured && owner != workflowID {
		return domain.NewError(
			domain.ErrorConflict,
			"resume VMCluster",
			fmt.Sprintf(
				"VMCluster %s/%s pause ownership changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	if _, err := validateVMClusterPauseRestoreState(
		workflowID,
		ordinal,
		vm,
		object,
		owner,
		paused,
		currentReplicas,
		replicasFound,
	); err != nil {
		return err
	}

	clusterPaused, clusterPausedFound, nestedErr := unstructured.NestedBool(
		object.Object,
		"spec",
		vmClusterFieldPaused,
	)
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"resume VMCluster",
			"read top-level pause state",
			nestedErr,
		)
	}

	if vm.OriginalClusterPausedConfigured {
		if !clusterPausedFound || clusterPaused != vm.OriginalClusterPaused {
			return domain.NewError(
				domain.ErrorConflict,
				"resume VMCluster",
				fmt.Sprintf(
					"VMCluster %s/%s top-level paused changed",
					object.GetNamespace(),
					object.GetName(),
				),
			)
		}
	} else if clusterPausedFound && clusterPaused {
		return domain.NewError(
			domain.ErrorConflict,
			"resume VMCluster",
			fmt.Sprintf(
				"VMCluster %s/%s was paused externally during migration",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	return nil
}

func (m *Manager) vmClusterWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	parent *metav1.OwnerReference,
	sts *appsv1.StatefulSet,
	allowLeaderDowntime bool,
) (v1alpha1.WorkloadSpec, error) {
	if sts == nil {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorInternal,
			"discover VMCluster",
			"StatefulSet is required",
		)
	}

	base, err := m.statefulSetWorkload(ctx, pod, sts, allowLeaderDowntime)
	if err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	component := ""
	for _, candidate := range []string{"vmstorage", "vmselect", "vminsert"} {
		if strings.Contains(strings.ToLower(sts.Name), candidate) {
			component = candidate
			break
		}
	}

	if component == "" {
		component = sts.Labels[kube.AppComponentLabel]
	}

	if component != "vmstorage" && component != "vmselect" && component != "vminsert" {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover VMCluster",
			fmt.Sprintf(
				"StatefulSet %s/%s has no supported VMCluster component",
				pod.Namespace,
				sts.Name,
			),
		)
	}

	originalPaused := false
	originalPausedConfigured := false
	originalClusterPaused := false
	originalClusterPausedConfigured := false
	originalReplicas := *base.OriginalReplicas
	originalReplicasConfigured := false

	var vmUID types.UID
	if m.dynamic != nil {
		gvr, parseErr := kube.ParseGroupVersionResource(vmClusterAPIVersion, vmClusterResource)
		if parseErr != nil {
			return v1alpha1.WorkloadSpec{}, parseErr
		}

		vm, getErr := m.dynamic.Resource(gvr).
			Namespace(pod.Namespace).
			Get(ctx, parent.Name, metav1.GetOptions{})
		if getErr != nil {
			return v1alpha1.WorkloadSpec{}, domain.WrapError(
				domain.ErrorKubernetes,
				"discover VMCluster",
				"read VMCluster",
				getErr,
			)
		}

		if vm.GetUID() == "" || vm.GetUID() != parent.UID {
			return v1alpha1.WorkloadSpec{}, domain.NewError(
				domain.ErrorConflict,
				"discover VMCluster",
				fmt.Sprintf(
					"StatefulSet %s/%s VMCluster owner UID changed",
					sts.Namespace,
					sts.Name,
				),
			)
		}

		vmUID = vm.GetUID()

		componentObject, found, nestedErr := unstructured.NestedMap(vm.Object, "spec", component)
		if nestedErr != nil {
			return v1alpha1.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				"read component state",
				nestedErr,
			)
		}

		if !found {
			return v1alpha1.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				fmt.Sprintf("VMCluster component %s is absent", component),
			)
		}

		originalPaused, originalPausedConfigured, nestedErr = unstructured.NestedBool(
			componentObject,
			vmClusterFieldPaused,
		)
		if nestedErr != nil {
			return v1alpha1.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				"read component pause state",
				nestedErr,
			)
		}

		configuredReplicas, replicasFound, replicasErr := unstructured.NestedInt64(
			componentObject,
			vmClusterFieldReplicaCount,
		)
		if replicasErr != nil {
			return v1alpha1.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				"read component replica count",
				replicasErr,
			)
		}

		if replicasFound {
			if configuredReplicas <= 0 || configuredReplicas > math.MaxInt32 {
				return v1alpha1.WorkloadSpec{}, domain.NewError(
					domain.ErrorPrecondition,
					"discover VMCluster",
					fmt.Sprintf(
						"VMCluster component %s has invalid replicaCount %d",
						component,
						configuredReplicas,
					),
				)
			}

			if configuredReplicas != int64(*base.OriginalReplicas) {
				return v1alpha1.WorkloadSpec{}, domain.NewError(
					domain.ErrorPrecondition,
					"discover VMCluster",
					fmt.Sprintf(
						"VMCluster component %s replicaCount %d has not converged to StatefulSet replicas %d",
						component,
						configuredReplicas,
						*base.OriginalReplicas,
					),
				)
			}

			originalReplicas = int32(configuredReplicas)
			originalReplicasConfigured = true
		}

		originalClusterPaused, originalClusterPausedConfigured, nestedErr = unstructured.NestedBool(
			vm.Object,
			"spec",
			vmClusterFieldPaused,
		)
		if nestedErr != nil {
			return v1alpha1.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				"read top-level pause state",
				nestedErr,
			)
		}
	}

	return v1alpha1.WorkloadSpec{
		Adapter:          v1alpha1.WorkloadVMCluster,
		Pod:              base.Pod,
		Controller:       base.Controller,
		OriginalReplicas: base.OriginalReplicas,
		Ordinal:          base.Ordinal,
		AffectedPods:     base.AffectedPods,
		VMCluster: &v1alpha1.VMClusterSpec{
			APIVersion:                      vmClusterAPIVersion,
			Name:                            parent.Name,
			UID:                             vmUID,
			Component:                       component,
			OriginalPaused:                  originalPaused,
			OriginalPausedConfigured:        originalPausedConfigured,
			OriginalClusterPaused:           originalClusterPaused,
			OriginalClusterPausedConfigured: originalClusterPausedConfigured,
			OriginalReplicas:                originalReplicas,
			OriginalReplicasConfigured:      originalReplicasConfigured,
		},
	}, nil
}

func (m *Manager) pauseVMCluster(
	ctx context.Context,
	workflowID, namespace string,
	controller v1alpha1.ObjectReference,
	originalReplicas, ordinal *int32,
	affectedPods []v1alpha1.ObjectReference,
	vm *v1alpha1.VMClusterSpec,
) error {
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause VMCluster",
			"session lacks VMCluster state",
		)
	}

	if ordinal == nil || originalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause VMCluster",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindStatefulSet,
		controller.Name,
		"pause VMCluster",
	); err != nil {
		return err
	}

	if err := m.setVMClusterPaused(ctx, workflowID, namespace, vm); err != nil {
		return err
	}
	// Keep lower ordinals available while preventing the operator from
	// restoring the StatefulSet to its original replica count.
	if err := m.setVMClusterReplicaCount(
		ctx,
		workflowID,
		namespace,
		vm,
		*ordinal,
		vm.OriginalReplicas,
	); err != nil {
		if restoreErr := m.restoreVMClusterPause(
			ctx,
			workflowID,
			namespace,
			ordinal,
			vm,
		); restoreErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"pause VMCluster",
				fmt.Sprintf("set component replicas: %v; restore component pause state", err),
				restoreErr,
			)
		}

		return err
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		controller,
		*ordinal,
		*originalReplicas,
	); err != nil {
		if restoreErr := m.restoreVMClusterPause(
			ctx,
			workflowID,
			namespace,
			ordinal,
			vm,
		); restoreErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"pause VMCluster",
				fmt.Sprintf("scale component StatefulSet: %v; restore component pause state", err),
				restoreErr,
			)
		}

		return workloadScaleError("pause VMCluster", "scale component StatefulSet", err)
	}

	for _, pod := range affectedPods {
		if err := m.waitForPodDeletion(ctx, pod, "pause VMCluster"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) resumeVMCluster(
	ctx context.Context,
	workflowID, namespace string,
	controller v1alpha1.ObjectReference,
	originalReplicas, ordinal *int32,
	affectedPods []v1alpha1.ObjectReference,
	vm *v1alpha1.VMClusterSpec,
) ([]v1alpha1.ObjectReference, error) {
	if vm == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"resume VMCluster",
			"session lacks VMCluster state",
		)
	}

	if originalReplicas == nil || ordinal == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"resume VMCluster",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindStatefulSet,
		controller.Name,
		"resume VMCluster",
	); err != nil {
		return nil, err
	}

	if err := m.setVMClusterReplicaCount(
		ctx,
		workflowID,
		namespace,
		vm,
		vm.OriginalReplicas,
		*ordinal,
	); err != nil {
		return nil, err
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		controller,
		*originalReplicas,
		*ordinal,
	); err != nil {
		return nil, workloadScaleError("resume VMCluster", "restore component StatefulSet", err)
	}

	observed, err := m.waitForResumedPods(ctx, affectedPods, controller, "resume VMCluster")
	if err != nil {
		return observed, err
	}

	if err := m.validateStatefulSetResumed(
		ctx,
		controller,
		originalReplicas,
		"resume VMCluster",
	); err != nil {
		return observed, err
	}

	if err := m.restoreVMClusterPause(ctx, workflowID, namespace, ordinal, vm); err != nil {
		return observed, err
	}

	if err := m.waitForVMClusterOperational(ctx, namespace, vm); err != nil {
		return observed, err
	}

	return observed, m.validateStatefulSetResumed(
		ctx,
		controller,
		originalReplicas,
		"resume VMCluster",
	)
}

func (m *Manager) waitForVMClusterOperational(
	ctx context.Context,
	namespace string,
	vm *v1alpha1.VMClusterSpec,
) error {
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"wait for VMCluster",
			"session lacks VMCluster state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"wait for VMCluster",
			"dynamic client is required for convergence checks",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	resource := m.dynamic.Resource(gvr).Namespace(namespace)

	return m.waitFor(
		ctx,
		fmt.Sprintf("VMCluster %s/%s convergence", namespace, vm.Name),
		func(waitCtx context.Context) (bool, error) {
			object, getErr := resource.Get(waitCtx, vm.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, domain.WrapError(
					domain.ErrorKubernetes,
					"wait for VMCluster",
					"read VMCluster",
					getErr,
				)
			}

			if object.GetUID() != vm.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"wait for VMCluster",
					fmt.Sprintf(
						"VMCluster %s/%s UID changed",
						object.GetNamespace(),
						object.GetName(),
					),
				)
			}

			observedGeneration, found, nestedErr := unstructured.NestedInt64(
				object.Object,
				"status",
				"observedGeneration",
			)
			if nestedErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"wait for VMCluster",
					"read observed generation",
					nestedErr,
				)
			}

			if !found || observedGeneration < object.GetGeneration() {
				return false, nil
			}

			currentClusterPaused, clusterPausedFound, nestedErr := unstructured.NestedBool(
				object.Object,
				"spec",
				vmClusterFieldPaused,
			)
			if nestedErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"wait for VMCluster",
					"read top-level pause state",
					nestedErr,
				)
			}

			if vm.OriginalClusterPausedConfigured {
				if !clusterPausedFound || currentClusterPaused != vm.OriginalClusterPaused {
					return false, domain.NewError(
						domain.ErrorConflict,
						"wait for VMCluster",
						fmt.Sprintf(
							"VMCluster %s/%s top-level paused changed from expected %t to %t",
							object.GetNamespace(),
							object.GetName(),
							vm.OriginalClusterPaused,
							currentClusterPaused,
						),
					)
				}
			} else if clusterPausedFound && currentClusterPaused {
				return false, domain.NewError(
					domain.ErrorConflict,
					"wait for VMCluster",
					fmt.Sprintf(
						"VMCluster %s/%s was paused externally during migration",
						object.GetNamespace(),
						object.GetName(),
					),
				)
			}

			clusterStatus, _, nestedErr := unstructured.NestedString(
				object.Object,
				"status",
				vmClusterFieldClusterStatus,
			)
			if nestedErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"wait for VMCluster",
					"read cluster status",
					nestedErr,
				)
			}

			updateStatus, _, nestedErr := unstructured.NestedString(
				object.Object,
				"status",
				vmClusterFieldUpdateStatus,
			)
			if nestedErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"wait for VMCluster",
					"read update status",
					nestedErr,
				)
			}

			if vm.OriginalClusterPausedConfigured && vm.OriginalClusterPaused {
				// A top-level paused VMCluster intentionally remains outside the
				// operator's operational state machine. The observed generation still
				// fences us against an object replacement or a stale read.
				return true, nil
			}

			return strings.EqualFold(clusterStatus, vmClusterStatusOperational) &&
				strings.EqualFold(updateStatus, vmClusterStatusOperational), nil
		},
	)
}

func (m *Manager) restoreVMClusterPause(
	ctx context.Context,
	workflowID, namespace string,
	ordinal *int32,
	vm *v1alpha1.VMClusterSpec,
) error {
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore VMCluster pause",
			"session lacks VMCluster state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore VMCluster pause",
			"dynamic client is required for component pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(namespace)

		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore VMCluster pause",
				"read VMCluster",
				getErr,
			)
		}

		if object.GetUID() != vm.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"restore VMCluster pause",
				fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		componentObject, ok, nestedErr := unstructured.NestedMap(
			object.Object,
			"spec",
			vm.Component,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"restore VMCluster pause",
				"read component pause state",
				nestedErr,
			)
		}

		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"restore VMCluster pause",
				fmt.Sprintf("VMCluster component %s is absent", vm.Component),
			)
		}

		current, _, nestedErr := unstructured.NestedBool(componentObject, vmClusterFieldPaused)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"restore VMCluster pause",
				"read component pause state",
				nestedErr,
			)
		}

		annotations := object.GetAnnotations()

		pauseOwner := annotations[pauseSessionAnnotation]

		currentReplicas, replicasFound, nestedErr := unstructured.NestedInt64(
			componentObject,
			vmClusterFieldReplicaCount,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"restore VMCluster pause",
				"read component replica count",
				nestedErr,
			)
		}

		restoreRequired, validateErr := validateVMClusterPauseRestoreState(
			workflowID,
			ordinal,
			vm,
			object,
			pauseOwner,
			current,
			currentReplicas,
			replicasFound,
		)
		if validateErr != nil || !restoreRequired {
			return validateErr
		}

		// On CRDs that prune the component paused field the write-back is a
		// no-op at the API server; skipping it avoids a pointless update.
		if current != vm.OriginalPaused && vm.ComponentPausedSupported {
			if err := unstructured.SetNestedField(
				componentObject,
				vm.OriginalPaused,
				vmClusterFieldPaused,
			); err != nil {
				return err
			}

			if err := unstructured.SetNestedField(
				object.Object,
				componentObject,
				"spec",
				vm.Component,
			); err != nil {
				return err
			}
		}

		if vm.OriginalReplicasConfigured && replicasFound &&
			ordinal != nil && currentReplicas == int64(*ordinal) {
			if err := unstructured.SetNestedField(
				componentObject,
				int64(vm.OriginalReplicas),
				vmClusterFieldReplicaCount,
			); err != nil {
				return err
			}

			if err := unstructured.SetNestedField(
				object.Object,
				componentObject,
				"spec",
				vm.Component,
			); err != nil {
				return err
			}
		}

		delete(annotations, pauseSessionAnnotation)
		object.SetAnnotations(annotations)

		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore VMCluster pause",
				"clear component pause owner",
				updateErr,
			)
		}

		return nil
	})
}

func validateVMClusterPauseRestoreState(
	workflowID string,
	ordinal *int32,
	vm *v1alpha1.VMClusterSpec,
	object *unstructured.Unstructured,
	pauseOwner string,
	current bool,
	currentReplicas int64,
	replicasFound bool,
) (bool, error) {
	if pauseOwner != "" && pauseOwner != workflowID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster %s/%s pause is owned by session %s",
				object.GetNamespace(),
				object.GetName(),
				pauseOwner,
			),
		)
	}

	if pauseOwner == "" {
		if vm.ComponentPausedSupported && current != vm.OriginalPaused {
			return false, domain.NewError(
				domain.ErrorConflict,
				"restore VMCluster pause",
				fmt.Sprintf(
					"VMCluster component %s paused changed from expected %t to %t",
					vm.Component,
					vm.OriginalPaused,
					current,
				),
			)
		}

		if replicasFound != vm.OriginalReplicasConfigured ||
			(replicasFound && currentReplicas != int64(vm.OriginalReplicas)) {
			return false, domain.NewError(
				domain.ErrorConflict,
				"restore VMCluster pause",
				fmt.Sprintf(
					"VMCluster component %s replicaCount changed while pause ownership was absent",
					vm.Component,
				),
			)
		}

		return false, nil
	}

	if !current && vm.ComponentPausedSupported {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster component %s paused changed while session was active",
				vm.Component,
			),
		)
	}

	if !vm.OriginalReplicasConfigured && replicasFound {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster component %s replicaCount was added while session was active",
				vm.Component,
			),
		)
	}

	if !vm.OriginalReplicasConfigured {
		return true, nil
	}

	if !replicasFound {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster component %s replicaCount was removed while session was active",
				vm.Component,
			),
		)
	}

	if currentReplicas == int64(vm.OriginalReplicas) {
		return true, nil
	}

	if ordinal == nil || currentReplicas != int64(*ordinal) {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster component %s replicaCount changed while session was active",
				vm.Component,
			),
		)
	}

	return true, nil
}

func (m *Manager) setVMClusterReplicaCount(
	ctx context.Context,
	workflowID, namespace string,
	vm *v1alpha1.VMClusterSpec,
	replicas int32,
	allowedCurrent ...int32,
) error {
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"set VMCluster replicas",
			"session lacks VMCluster state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"set VMCluster replicas",
			"dynamic client is required for component replica control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(namespace)

		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"set VMCluster replicas",
				"read VMCluster",
				getErr,
			)
		}

		if object.GetUID() != vm.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"set VMCluster replicas",
				fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		componentObject, ok, nestedErr := unstructured.NestedMap(
			object.Object,
			"spec",
			vm.Component,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"set VMCluster replicas",
				"read component replica count",
				nestedErr,
			)
		}

		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"set VMCluster replicas",
				fmt.Sprintf("VMCluster component %s is absent", vm.Component),
			)
		}

		current, found, nestedErr := unstructured.NestedInt64(
			componentObject,
			vmClusterFieldReplicaCount,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"set VMCluster replicas",
				"read component replica count",
				nestedErr,
			)
		}

		if found != vm.OriginalReplicasConfigured {
			return domain.NewError(
				domain.ErrorConflict,
				"set VMCluster replicas",
				fmt.Sprintf(
					"VMCluster component %s replicaCount representation changed",
					vm.Component,
				),
			)
		}

		if !found {
			return nil
		}

		annotations := object.GetAnnotations()
		if owner := annotations[pauseSessionAnnotation]; owner != workflowID {
			return domain.NewError(
				domain.ErrorConflict,
				"set VMCluster replicas",
				fmt.Sprintf(
					"VMCluster %s/%s pause ownership changed",
					object.GetNamespace(),
					object.GetName(),
				),
			)
		}

		if current == int64(replicas) {
			return nil
		}

		allowed := false
		for _, candidate := range allowedCurrent {
			if current == int64(candidate) {
				allowed = true
				break
			}
		}

		if !allowed {
			return domain.NewError(
				domain.ErrorConflict,
				"set VMCluster replicas",
				fmt.Sprintf(
					"VMCluster component %s replicaCount changed to %d",
					vm.Component,
					current,
				),
			)
		}

		if err := unstructured.SetNestedField(
			componentObject,
			int64(replicas),
			vmClusterFieldReplicaCount,
		); err != nil {
			return err
		}

		if err := unstructured.SetNestedField(
			object.Object,
			componentObject,
			"spec",
			vm.Component,
		); err != nil {
			return err
		}

		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"set VMCluster replicas",
				"update component replica count",
				updateErr,
			)
		}

		return nil
	})
}

func (m *Manager) setVMClusterPaused(
	ctx context.Context,
	workflowID, namespace string,
	vm *v1alpha1.VMClusterSpec,
) error {
	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"VMCluster pause",
			"dynamic client is required for component pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(namespace)

		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"VMCluster pause",
				"read VMCluster",
				getErr,
			)
		}

		if object.GetUID() != vm.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"VMCluster pause",
				fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		componentObject, ok, nestedErr := unstructured.NestedMap(
			object.Object,
			"spec",
			vm.Component,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"VMCluster pause",
				"read component pause state",
				nestedErr,
			)
		}

		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"VMCluster pause",
				fmt.Sprintf("VMCluster component %s is absent", vm.Component),
			)
		}

		current, _, nestedErr := unstructured.NestedBool(componentObject, vmClusterFieldPaused)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"VMCluster pause",
				"read component pause state",
				nestedErr,
			)
		}

		annotations := object.GetAnnotations()

		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != workflowID {
			return domain.NewError(
				domain.ErrorConflict,
				"VMCluster pause",
				fmt.Sprintf(
					"VMCluster %s/%s pause is owned by session %s",
					object.GetNamespace(),
					object.GetName(),
					pauseOwner,
				),
			)
		}

		if pauseOwner == "" && current != vm.OriginalPaused {
			return domain.NewError(
				domain.ErrorConflict,
				"VMCluster pause",
				fmt.Sprintf(
					"VMCluster component %s paused changed from expected %t to %t",
					vm.Component,
					vm.OriginalPaused,
					current,
				),
			)
		}

		if pauseOwner == "" {
			currentReplicas, replicasFound, replicasErr := unstructured.NestedInt64(
				componentObject,
				vmClusterFieldReplicaCount,
			)
			if replicasErr != nil {
				return domain.WrapError(
					domain.ErrorPrecondition,
					"VMCluster pause",
					"read component replica count",
					replicasErr,
				)
			}

			if replicasFound != vm.OriginalReplicasConfigured ||
				(replicasFound && currentReplicas != int64(vm.OriginalReplicas)) {
				return domain.NewError(
					domain.ErrorConflict,
					"VMCluster pause",
					fmt.Sprintf(
						"VMCluster component %s replicaCount changed after discovery",
						vm.Component,
					),
				)
			}
		}

		if pauseOwner == workflowID && current {
			vm.ComponentPausedSupported = true

			return nil
		}

		if pauseOwner == workflowID && !current && vm.ComponentPausedSupported {
			return domain.NewError(
				domain.ErrorConflict,
				"VMCluster pause",
				fmt.Sprintf(
					"VMCluster component %s paused changed while session was active",
					vm.Component,
				),
			)
		}

		// pauseOwner == workflowID && !current on an unsupported CRD falls
		// through: the paused write below is pruned again, and the post-update
		// probe keeps recording support.


		if err := unstructured.SetNestedField(
			componentObject,
			true,
			vmClusterFieldPaused,
		); err != nil {
			return err
		}

		if err := unstructured.SetNestedField(
			object.Object,
			componentObject,
			"spec",
			vm.Component,
		); err != nil {
			return err
		}

		if annotations == nil {
			annotations = map[string]string{}
		}

		annotations[pauseSessionAnnotation] = workflowID

		object.SetAnnotations(annotations)

		_, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{})
		if apierrors.IsConflict(updateErr) {
			return updateErr
		}

		if updateErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"VMCluster pause",
				"update component paused state",
				updateErr,
			)
		}

		// Older VMCluster CRDs prune the per-component paused field with only
		// a warning, so a successful update says nothing about whether the
		// pause took. Re-read and record the outcome: when the field did not
		// persist, pause semantics degrade to holding the component at the
		// reduced replicaCount instead of relying on the paused flag.
		stored, readErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if readErr == nil {
			if component, ok, _ := unstructured.NestedMap(stored.Object, "spec", vm.Component); ok && component != nil {
				stuck, _, _ := unstructured.NestedBool(component, vmClusterFieldPaused)
				vm.ComponentPausedSupported = stuck
			}
		}

		return nil
	})
}
