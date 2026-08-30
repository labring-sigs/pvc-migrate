package app

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) verifySourceStorage(ctx context.Context, session *domain.Session) error {
	results := make([]error, len(session.Spec.Volumes))
	parallel.For(len(session.Spec.Volumes), func(index int) {
		volume := &session.Spec.Volumes[index]
		if volume.SourcePVC.Namespace == "" || volume.SourcePVC.Name == "" ||
			volume.SourcePVC.UID == "" {
			results[index] = domain.NewError(
				domain.ErrorPrecondition,
				verifySourceStoragePhase,
				fmt.Sprintf("source PVC reference for volume %d is incomplete", index),
			)

			return
		}

		if volume.SourcePV.Name == "" || volume.SourcePV.UID == "" {
			results[index] = domain.NewError(
				domain.ErrorPrecondition,
				verifySourceStoragePhase,
				fmt.Sprintf(
					"source PV reference for PVC %s/%s is incomplete",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
			)

			return
		}

		pvc, err := s.client.CoreV1().
			PersistentVolumeClaims(volume.SourcePVC.Namespace).
			Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
		if err != nil {
			results[index] = domain.WrapError(
				domain.ErrorKubernetes,
				verifySourceStoragePhase,
				fmt.Sprintf(
					"read source PVC %s/%s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
				err,
			)

			return
		}

		if pvc == nil || pvc.Name == "" {
			results[index] = domain.NewError(
				domain.ErrorKubernetes,
				verifySourceStoragePhase,
				fmt.Sprintf(
					"read source PVC %s/%s returned an empty object",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
			)

			return
		}

		if pvc.UID != volume.SourcePVC.UID || pvc.Status.Phase != corev1.ClaimBound ||
			pvc.Spec.VolumeName != volume.SourcePV.Name {
			results[index] = domain.NewError(
				domain.ErrorConflict,
				verifySourceStoragePhase,
				fmt.Sprintf(
					"source PVC %s/%s identity or binding changed",
					pvc.Namespace,
					pvc.Name,
				),
			)

			return
		}

		pv, err := s.client.CoreV1().
			PersistentVolumes().
			Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
		if err != nil {
			results[index] = domain.WrapError(
				domain.ErrorKubernetes,
				verifySourceStoragePhase,
				"read source PV "+volume.SourcePV.Name,
				err,
			)

			return
		}

		if pv == nil || pv.Name == "" {
			results[index] = domain.NewError(
				domain.ErrorKubernetes,
				verifySourceStoragePhase,
				fmt.Sprintf("read source PV %s returned an empty object", volume.SourcePV.Name),
			)

			return
		}

		if pv.UID != volume.SourcePV.UID || pv.Spec.ClaimRef == nil ||
			pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
			pv.Spec.ClaimRef.Name != pvc.Name ||
			pv.Spec.ClaimRef.UID != pvc.UID {
			results[index] = domain.NewError(
				domain.ErrorConflict,
				verifySourceStoragePhase,
				fmt.Sprintf("source PV %s identity or claimRef changed", pv.Name),
			)
		}
	})

	for _, err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) verifyActiveStorage(ctx context.Context, session *domain.Session) error {
	results := make([]error, len(session.Spec.Volumes))
	parallel.For(len(session.Spec.Volumes), func(index int) {
		results[index] = s.verifyActiveStorageVolume(ctx, session, index)
	})

	for _, err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateActivationStorage(ctx context.Context, session *domain.Session) error {
	offline := make([]*domain.VolumeSpec, 0, len(session.Spec.Volumes))
	for index := range session.Spec.Volumes {
		status := &session.Status.Volumes[index]
		if status.Activation.ActivatedAt != nil || status.Activation.ActivePVC.Name != "" {
			if err := s.verifyActiveStorageVolume(ctx, session, index); err != nil {
				return err
			}
			continue
		}

		active, _, err := s.unrecordedActivePVC(ctx, session, index)
		if err != nil {
			return err
		}

		if active != nil {
			if err := s.verifyActiveStorageVolumeWithRef(ctx, session, index, *active); err != nil {
				return err
			}

			volume := session.Spec.Volumes[index]
			volume.SourcePVC = *active
			volume.SourcePV = volume.DestinationPV
			volume.DestinationPVC = *active

			if err := s.verifyVolumesOffline(
				ctx,
				session,
				[]*domain.VolumeSpec{&volume},
			); err != nil {
				return err
			}

			continue
		}

		offline = append(offline, &session.Spec.Volumes[index])
	}

	return s.verifyVolumesOffline(ctx, session, offline)
}

func (s *Service) unrecordedActivePVC(
	ctx context.Context,
	session *domain.Session,
	index int,
) (*domain.ObjectReference, bool, error) {
	volume := &session.Spec.Volumes[index]

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, domain.WrapError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf("read PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return nil, false, domain.NewError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf(
				"read PVC %s/%s returned an empty object",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if pvc.UID == volume.SourcePVC.UID && pvc.Spec.VolumeName == volume.SourcePV.Name {
		return nil, true, nil
	}

	return &domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}, true, nil
}

func (s *Service) validateActivationPVCPolicies(
	ctx context.Context,
	session *domain.Session,
) error {
	groups := make(map[string][]kube.PVCAdmissionChange)
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Activation.ActivatedAt != nil || status.Activation.ActivePVC.Name != "" {
			continue
		}

		requested, err := resource.ParseQuantity(volume.Capacity)
		if err != nil || requested.Sign() <= 0 {
			return domain.NewError(
				domain.ErrorValidation,
				activationPreflightPhase,
				fmt.Sprintf(
					"PVC %s/%s has invalid destination capacity %q",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					volume.Capacity,
				),
			)
		}

		active, found, err := s.unrecordedActivePVC(ctx, session, index)
		if err != nil {
			return err
		}

		if active != nil && active.UID != volume.SourcePVC.UID {
			// The replacement is already present. Storage validation below
			// verifies its identity and session ownership; it must not be
			// projected as another admission request.
			continue
		}

		existing := volume.SourcePVCSpec.Resources.Requests[corev1.ResourceStorage]

		sourceClass := ""
		if volume.SourcePVCSpec.StorageClassName != nil {
			sourceClass = *volume.SourcePVCSpec.StorageClassName
		}

		volumeAttributesClasses := kube.RequestedVolumeAttributesClassNames(volume.SourcePVCSpec)

		groups[volume.SourcePVC.Namespace] = append(
			groups[volume.SourcePVC.Namespace],
			kube.PVCAdmissionChange{
				Namespace:                           volume.SourcePVC.Namespace,
				Name:                                volume.SourcePVC.Name,
				RequestedStorage:                    requested,
				RequestedStorageClass:               volume.StorageClass,
				Existing:                            found && !status.Activation.SourcePVCDeleted,
				ExistingUID:                         volume.SourcePVC.UID,
				ExistingStorage:                     existing,
				ExistingStorageClass:                sourceClass,
				RequestedVolumeAttributesClassNames: volumeAttributesClasses,
				ExistingVolumeAttributesClassNames:  volumeAttributesClasses,
			},
		)
	}

	namespaces := make([]string, 0, len(groups))
	for namespace := range groups {
		namespaces = append(namespaces, namespace)
	}

	sort.Strings(namespaces)

	for _, namespace := range namespaces {
		report, err := kube.CheckPVCAdmissionPolicies(ctx, s.client, groups[namespace])
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				activationPreflightPhase,
				"check application PVC admission in "+namespace,
				err,
			)
		}

		if len(report.QuotaViolations) > 0 {
			return domain.NewError(
				domain.ErrorPrecondition,
				activationPreflightPhase,
				"application PVC quota rejected the replacement: "+strings.Join(
					report.QuotaViolations,
					"; ",
				),
			)
		}

		if len(report.LimitRangeViolations) > 0 {
			return domain.NewError(
				domain.ErrorPrecondition,
				activationPreflightPhase,
				"application PVC LimitRange rejected the replacement: "+strings.Join(
					report.LimitRangeViolations,
					"; ",
				),
			)
		}
	}

	return nil
}

func (s *Service) validateRollbackRecoveryStorage(
	ctx context.Context,
	session *domain.Session,
	rollbackOrigin domain.Phase,
) error {
	offline := make([]*domain.VolumeSpec, 0, len(session.Spec.Volumes))

	originWasActive := rollbackOrigin == domain.PhaseActivated ||
		rollbackOrigin == domain.PhaseResuming ||
		rollbackOrigin == domain.PhaseCompleted
	for index := range session.Spec.Volumes {
		status := &session.Status.Volumes[index]
		if status.Activation.RolledBackAt != nil {
			if err := s.verifyRollbackStorageVolume(ctx, session, index); err != nil {
				return err
			}
			continue
		}

		if originWasActive || status.Activation.ActivatedAt != nil ||
			status.Activation.ActivePVC.Name != "" {
			if err := s.verifyActiveStorageVolume(ctx, session, index); err != nil {
				return err
			}
			continue
		}

		offline = append(offline, &session.Spec.Volumes[index])
	}

	return s.verifyVolumesOffline(ctx, session, offline)
}

func (s *Service) validateRollbackStorage(
	ctx context.Context,
	session *domain.Session,
	phase, rollbackOrigin domain.Phase,
	recoveringRollback, wasRunning bool,
) error {
	if recoveringRollback {
		return s.validateRollbackRecoveryStorage(ctx, session, rollbackOrigin)
	}

	if wasRunning {
		return s.verifyActiveVolumes(ctx, session)
	}

	if phase == domain.PhaseActivated {
		return s.verifyActiveStorage(ctx, session)
	}

	if phase == domain.PhaseActivating ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating) {
		return s.validateActivationStorage(ctx, session)
	}

	return s.validateOfflineVolumes(ctx, session)
}

func (s *Service) verifyRollbackStorage(ctx context.Context, session *domain.Session) error {
	results := make([]error, len(session.Spec.Volumes))
	parallel.For(len(session.Spec.Volumes), func(index int) {
		results[index] = s.verifyRollbackStorageVolume(ctx, session, index)
	})

	for _, err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) verifyRollbackStorageVolume(
	ctx context.Context,
	session *domain.Session,
	index int,
) error {
	volume := &session.Spec.Volumes[index]

	active := session.Status.Volumes[index].Activation.ActivePVC
	if active.Namespace == "" || active.Name == "" || active.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyRollbackPhase,
			fmt.Sprintf(
				"PVC %s/%s has no recorded restored identity",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if active.Namespace != volume.SourcePVC.Namespace || active.Name != volume.SourcePVC.Name {
		return domain.NewError(
			domain.ErrorConflict,
			verifyRollbackPhase,
			fmt.Sprintf(
				"recorded restored PVC %s/%s does not match source PVC %s/%s",
				active.Namespace,
				active.Name,
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if volume.SourcePV.Name == "" || volume.SourcePV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyRollbackPhase,
			fmt.Sprintf(
				"PVC %s/%s has no recorded source PV identity",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(active.Namespace).
		Get(ctx, active.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			fmt.Sprintf("read restored PVC %s/%s", active.Namespace, active.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			fmt.Sprintf(
				"read restored PVC %s/%s returned an empty object",
				active.Namespace,
				active.Name,
			),
		)
	}

	if pvc.UID != active.UID || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != volume.SourcePV.Name {
		return domain.NewError(
			domain.ErrorConflict,
			verifyRollbackPhase,
			fmt.Sprintf("restored PVC %s/%s identity or binding changed", pvc.Namespace, pvc.Name),
		)
	}

	if pvc.UID != volume.SourcePVC.UID && pvc.Annotations[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyRollbackPhase,
			fmt.Sprintf(
				"restored PVC %s/%s is not the original or session-owned PVC",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			"read restored PV "+volume.SourcePV.Name,
			err,
		)
	}

	if pv == nil || pv.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			fmt.Sprintf("read restored PV %s returned an empty object", volume.SourcePV.Name),
		)
	}

	if pv.UID != volume.SourcePV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyRollbackPhase,
			fmt.Sprintf("restored PV %s identity or claimRef changed", pv.Name),
		)
	}

	return nil
}

func (s *Service) verifyActiveStorageVolume(
	ctx context.Context,
	session *domain.Session,
	index int,
) error {
	return s.verifyActiveStorageVolumeWithRef(
		ctx,
		session,
		index,
		session.Status.Volumes[index].Activation.ActivePVC,
	)
}

func (s *Service) verifyActiveStorageVolumeWithRef(
	ctx context.Context,
	session *domain.Session,
	index int,
	active domain.ObjectReference,
) error {
	volume := &session.Spec.Volumes[index]
	if active.Namespace == "" || active.Name == "" || active.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyMigrationPhase,
			fmt.Sprintf(
				"PVC %s/%s has no recorded active identity",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if active.Namespace != volume.SourcePVC.Namespace || active.Name != volume.SourcePVC.Name {
		return domain.NewError(
			domain.ErrorConflict,
			verifyMigrationPhase,
			fmt.Sprintf(
				"recorded active PVC %s/%s does not match application PVC %s/%s",
				active.Namespace,
				active.Name,
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if volume.DestinationPV.Name == "" || volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyMigrationPhase,
			fmt.Sprintf(
				"PVC %s/%s has no recorded destination PV identity",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf("read PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf(
				"read PVC %s/%s returned an empty object",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != volume.DestinationPV.Name ||
		pvc.Annotations[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyMigrationPhase,
			fmt.Sprintf(
				"PVC %s/%s is not active on destination PV %s",
				pvc.Namespace,
				pvc.Name,
				volume.DestinationPV.Name,
			),
		)
	}

	if pvc.UID != active.UID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyMigrationPhase,
			fmt.Sprintf("active PVC %s/%s UID changed", pvc.Namespace, pvc.Name),
		)
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			"read active PV "+volume.DestinationPV.Name,
			err,
		)
	}

	if pv == nil || pv.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf("read active PV %s returned an empty object", volume.DestinationPV.Name),
		)
	}

	if pv.UID != volume.DestinationPV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyMigrationPhase,
			fmt.Sprintf("active PV %s identity or claimRef changed", pv.Name),
		)
	}

	return nil
}

func (s *Service) validateWorkloadResume(ctx context.Context, session *domain.Session) error {
	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if err := s.verifyActiveStorage(ctx, session); err != nil {
		return err
	}

	if err := s.controllers.ValidateResume(ctx, session); err != nil {
		return err
	}

	options := session.Spec.WorkflowOptions()
	// TargetNode is the exact placement contract only for the standalone
	// adapter. Controller-managed workloads keep their own scheduling policy;
	// the tool node can become unavailable while the workload still has a
	// valid placement elsewhere.
	if session.Spec.Workload().Adapter != domain.WorkloadStandalone || options.TargetNode == "" {
		return nil
	}

	node, err := s.client.CoreV1().Nodes().Get(ctx, options.TargetNode, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "resume dry-run", "read target node", err)
	}

	if !kube.NodeReadyAndSchedulable(node) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume dry-run",
			fmt.Sprintf("target node %s must be Ready and schedulable", node.Name),
		)
	}

	return nil
}

func (s *Service) verifyActiveVolumes(ctx context.Context, session *domain.Session) error {
	if err := s.verifyActiveStorage(ctx, session); err != nil {
		return err
	}

	options := session.Spec.WorkflowOptions()
	// TargetNode pins reservation and copy tools for every workload. The
	// standalone adapter also pins the recreated Pod; controller-managed
	// workloads retain their own scheduler policy and may validly land on a
	// different node when the destination volume is topology-independent.
	if session.Spec.Workload().Adapter == domain.WorkloadStandalone && options.TargetNode != "" {
		ref := session.Spec.Workload().Pod

		pod, err := s.client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				verifyMigrationPhase,
				"read resumed Pod",
				err,
			)
		}

		if ref.UID == "" || pod.UID != ref.UID || pod.Annotations[kube.SessionKey] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				verifyMigrationPhase,
				fmt.Sprintf(
					"Pod %s/%s identity or session ownership changed",
					pod.Namespace,
					pod.Name,
				),
			)
		}

		if pod.Spec.NodeName != options.TargetNode {
			return domain.NewError(
				domain.ErrorPrecondition,
				verifyMigrationPhase,
				fmt.Sprintf(
					"Pod %s/%s runs on %s, expected %s",
					pod.Namespace,
					pod.Name,
					pod.Spec.NodeName,
					options.TargetNode,
				),
			)
		}
	}

	return nil
}
