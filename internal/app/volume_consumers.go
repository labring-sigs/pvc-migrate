package app

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func inspectPVCUnusedWithOperations(
	ctx context.Context,
	client kubernetes.Interface,
	ref v1alpha1.ObjectReference,
	sessionID string,
) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("read PVC %s/%s consumers", ref.Namespace, ref.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("read PVC %s/%s returned an empty object", ref.Namespace, ref.Name),
		)
	}

	if ref.UID == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"cleanup",
			fmt.Sprintf("PVC %s/%s UID is required", ref.Namespace, ref.Name),
		)
	}

	if pvc.UID != ref.UID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("PVC %s/%s identity changed", ref.Namespace, ref.Name),
		)
	}

	var (
		pods                  *corev1.PodList
		attachments           *storagev1.VolumeAttachmentList
		podErr, attachmentErr error
	)

	count := 1
	if pvc.Spec.VolumeName != "" {
		count = 2
	}

	parallel.ForLimit(count, 2, func(index int) {
		if index == 0 {
			pods, podErr = client.CoreV1().Pods(ref.Namespace).List(ctx, metav1.ListOptions{})
			if podErr == nil && pods == nil {
				podErr = fmt.Errorf(
					"list PVC consumers in %s returned an empty object",
					ref.Namespace,
				)
			}

			return
		}

		attachments, attachmentErr = client.StorageV1().
			VolumeAttachments().
			List(ctx, metav1.ListOptions{})
		if attachmentErr == nil && attachments == nil {
			attachmentErr = fmt.Errorf(
				"list VolumeAttachments for PV %s returned an empty object",
				pvc.Spec.VolumeName,
			)
		}
	})

	if podErr != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"list PVC consumers in "+ref.Namespace,
			podErr,
		)
	}

	if pods == nil {
		pods = &corev1.PodList{}
	}

	for _, pod := range pods.Items {
		if sessionID != "" && pod.Labels[kube.SessionKey] == sessionID &&
			pod.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleReservationConsumer {
			continue
		}

		if kube.PodPreventsSafePVCDeletion(&pod, ref.Name) {
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				cause := domain.NewError(
					domain.ErrorPrecondition,
					"cleanup",
					fmt.Sprintf(
						"PVC %s/%s is still protected by terminal Pod %s (phase %s); delete the Pod object before cleanup",
						ref.Namespace,
						ref.Name,
						pod.Name,
						pod.Status.Phase,
					),
				)

				return nil, cleanupPodBlockerError(
					ctx, client,
					ref,
					&pod,
					sessionID,
					true,
					cause,
				)
			}

			cause := domain.NewError(
				domain.ErrorPrecondition,
				"cleanup",
				fmt.Sprintf(
					"PVC %s/%s is still referenced by Pod %s (phase %s); stop its controller or delete the Pod before cleanup",
					ref.Namespace,
					ref.Name,
					pod.Name,
					pod.Status.Phase,
				),
			)

			return nil, cleanupPodBlockerError(
				ctx, client,
				ref,
				&pod,
				sessionID,
				false,
				cause,
			)
		}
	}

	if pvc.Spec.VolumeName == "" {
		return pvc, nil
	}

	if attachmentErr != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"list attachments for PV "+pvc.Spec.VolumeName,
			attachmentErr,
		)
	}

	if attachments == nil {
		attachments = &storagev1.VolumeAttachmentList{}
	}

	for _, attachment := range attachments.Items {
		if attachment.Spec.Source.PersistentVolumeName != nil &&
			*attachment.Spec.Source.PersistentVolumeName == pvc.Spec.VolumeName &&
			attachment.Status.Attached {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"cleanup",
				fmt.Sprintf(
					"PVC %s/%s still has an attached PV on node %s",
					ref.Namespace,
					ref.Name,
					attachment.Spec.NodeName,
				),
			)
		}
	}

	return pvc, nil
}

func cleanupPodBlockerError(
	ctx context.Context,
	client kubernetes.Interface,
	pvc v1alpha1.ObjectReference,
	pod *corev1.Pod,
	sessionID string,
	terminal bool,
	cause error,
) error {
	ownerKind, ownerName, ownerVerified := resolveCleanupPodOwner(ctx, client, pod)
	sessionOwned := sessionID != "" &&
		pod.Labels[kube.ManagedByLabel] == kube.ManagedByValue &&
		pod.Labels[kube.SessionKey] == sessionID

	return &CleanupPodBlockerError{
		PVCNamespace:  pvc.Namespace,
		PVCName:       pvc.Name,
		PodNamespace:  pod.Namespace,
		PodName:       pod.Name,
		PodPhase:      pod.Status.Phase,
		OwnerKind:     ownerKind,
		OwnerName:     ownerName,
		OwnerVerified: ownerVerified,
		SessionOwned:  sessionOwned,
		Terminal:      terminal,
		Cause:         cause,
	}
}

func resolveCleanupPodOwner(
	ctx context.Context,
	client kubernetes.Interface,
	pod *corev1.Pod,
) (string, string, bool) {
	if pod == nil {
		return "", "", false
	}

	owner := controllerOwnerReference(pod.OwnerReferences)
	if owner == nil {
		return "", "", false
	}

	if client == nil {
		return owner.Kind, owner.Name, false
	}

	switch owner.Kind {
	case "Job":
		job, err := client.BatchV1().Jobs(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		return owner.Kind, owner.Name, err == nil && job != nil && ownerReferenceMatches(owner, job)
	case "Deployment":
		deployment, err := client.AppsV1().
			Deployments(pod.Namespace).
			Get(ctx, owner.Name, metav1.GetOptions{})

		return owner.Kind, owner.Name, err == nil && deployment != nil &&
			ownerReferenceMatches(owner, deployment)
	case "StatefulSet":
		statefulSet, err := client.AppsV1().
			StatefulSets(pod.Namespace).
			Get(ctx, owner.Name, metav1.GetOptions{})

		return owner.Kind, owner.Name, err == nil && statefulSet != nil &&
			ownerReferenceMatches(owner, statefulSet)
	case "DaemonSet":
		daemonSet, err := client.AppsV1().
			DaemonSets(pod.Namespace).
			Get(ctx, owner.Name, metav1.GetOptions{})

		return owner.Kind, owner.Name, err == nil && daemonSet != nil &&
			ownerReferenceMatches(owner, daemonSet)
	case "ReplicaSet":
		return resolveCleanupReplicaSetOwner(ctx, client, pod.Namespace, owner)
	default:
		return owner.Kind, owner.Name, false
	}
}

func resolveCleanupReplicaSetOwner(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	owner *metav1.OwnerReference,
) (string, string, bool) {
	replicaSet, err := client.AppsV1().
		ReplicaSets(namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil || replicaSet == nil || !ownerReferenceMatches(owner, replicaSet) {
		return owner.Kind, owner.Name, false
	}

	parent := controllerOwnerReference(replicaSet.OwnerReferences)
	if parent == nil {
		return owner.Kind, owner.Name, true
	}

	if parent.Kind != "Deployment" {
		return parent.Kind, parent.Name, false
	}

	deployment, err := client.AppsV1().
		Deployments(namespace).
		Get(ctx, parent.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) ||
		(err == nil && deployment != nil && !ownerReferenceMatches(parent, deployment)) {
		// The verified ReplicaSet is orphaned; deleting it cannot delete a
		// replacement Deployment that happens to reuse the old name.
		return owner.Kind, owner.Name, true
	}

	if err != nil || deployment == nil || !ownerReferenceMatches(parent, deployment) {
		return parent.Kind, parent.Name, false
	}

	return parent.Kind, parent.Name, true
}

func ownerReferenceMatches(owner *metav1.OwnerReference, object metav1.Object) bool {
	return owner != nil && object != nil && owner.UID != "" && owner.Name == object.GetName() &&
		owner.UID == object.GetUID()
}

func controllerOwnerReference(owners []metav1.OwnerReference) *metav1.OwnerReference {
	for index := range owners {
		if owners[index].Controller != nil && *owners[index].Controller {
			return &owners[index]
		}
	}

	return nil
}
