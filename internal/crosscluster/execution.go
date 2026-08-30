package crosscluster

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) CreateSession(
	ctx context.Context,
	options Options,
	plan *Plan,
) (*Session, error) {
	if plan == nil || !plan.Ready {
		return nil, errors.New("cross-cluster plan contains failed checks")
	}

	sourceID, err := kube.Identity(ctx, s.source)
	if err != nil {
		return nil, err
	}

	destID, err := kube.Identity(ctx, s.destination)
	if err != nil {
		return nil, err
	}

	if sourceID.ID != plan.SourceCluster.ID || destID.ID != plan.DestinationCluster.ID {
		return nil, errors.New(
			"cluster identity changed after planning; generate a new cross-cluster plan",
		)
	}

	volumes := make([]VolumeSpec, 0, len(plan.Volumes))
	for _, p := range plan.Volumes {
		pvc, err := s.source.Kubernetes.CoreV1().
			PersistentVolumeClaims(p.SourceNamespace).
			Get(ctx, p.SourcePVC, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		pv, err := s.source.Kubernetes.CoreV1().
			PersistentVolumes().
			Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		if pvc.UID != p.SourcePVCUID || pv.UID != p.SourcePVUID {
			return nil, fmt.Errorf(
				"source PVC/PV identity changed after planning for %s/%s; generate a new cross-cluster plan",
				p.SourceNamespace,
				p.SourcePVC,
			)
		}

		expectedSourceCapacity, parseErr := resource.ParseQuantity(p.SourceCapacity)
		if parseErr != nil {
			return nil, fmt.Errorf(
				"planned source capacity for %s is invalid: %w",
				p.SourcePVC,
				parseErr,
			)
		}

		if current := pv.Spec.Capacity[corev1.ResourceStorage]; current.Cmp(
			expectedSourceCapacity,
		) != 0 {
			return nil, fmt.Errorf(
				"source PV capacity changed after planning for %s; generate a new cross-cluster plan",
				p.SourcePVC,
			)
		}

		mode := corev1.PersistentVolumeFilesystem
		if pvc.Spec.VolumeMode != nil {
			mode = *pvc.Spec.VolumeMode
		}

		storageClass := options.DestinationStorageClass

		destinationClass, err := s.destination.Kubernetes.StorageV1().
			StorageClasses().
			Get(ctx, storageClass, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		if destinationClass.UID != p.StorageClassUID {
			return nil, errors.New(
				"destination StorageClass changed after planning; generate a new cross-cluster plan",
			)
		}

		if err := kube.ValidateDestinationAccessModes(
			destinationClass.Provisioner,
			pvc.Spec.AccessModes,
		); err != nil {
			return nil, fmt.Errorf(
				"destination StorageClass %s cannot provide source PVC %s/%s access modes: %w; generate a new cross-cluster plan with a compatible StorageClass",
				destinationClass.Name,
				pvc.Namespace,
				pvc.Name,
				err,
			)
		}

		volumes = append(volumes, VolumeSpec{
			Source: SourceVolumeSpec{
				PVC: ClusterResourceRef{
					ClusterID:  sourceID.ID,
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Namespace:  pvc.Namespace,
					Name:       pvc.Name,
					UID:        pvc.UID,
				},
				PV: ClusterResourceRef{
					ClusterID:  sourceID.ID,
					APIVersion: "v1",
					Kind:       "PersistentVolume",
					Name:       pv.Name,
					UID:        pv.UID,
				},
				Capacity: p.SourceCapacity,
			},
			Destination: DestinationVolumeSpec{
				PVC: ClusterResourceRef{
					ClusterID:  destID.ID,
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Namespace:  p.DestinationNamespace,
					Name:       p.DestinationPVC,
				},
				Capacity: p.Capacity,
				StorageClass: ClusterResourceRef{
					ClusterID:  destID.ID,
					APIVersion: "storage.k8s.io/v1",
					Kind:       "StorageClass",
					Name:       destinationClass.Name,
					UID:        destinationClass.UID,
				},
				AccessModes: append(
					[]corev1.PersistentVolumeAccessMode(nil),
					pvc.Spec.AccessModes...,
				),
				VolumeMode: mode,
			},
			Transfer: TransferSpec{SourcePath: p.SourcePath, DestinationPath: p.DestinationPath},
		})
	}

	session := NewSession(
		options.SessionID,
		Spec{
			SessionNamespace:     options.SessionNamespace,
			SourceCluster:        sourceID,
			DestinationCluster:   destID,
			SourceNamespace:      options.SourceNamespace,
			DestinationNamespace: options.DestinationNamespace,
			ToolImage:            options.ToolImage,
			Strategies:           normalizeStrategies(options.Strategies),
			Online:               options.Online,
			VerifyChecksum:       options.VerifyChecksum,
			DeleteExtraneous:     options.DeleteExtraneous,
			AllowVolumeShrink:    options.AllowVolumeShrink,
			SkipSourceUsageCheck: options.SkipSourceUsageCheck,
			TargetNode:           plan.TargetNode,
			Volumes:              volumes,
		},
		s.now(),
	)
	if _, err := kube.NormalizeToolImage(session.Spec.ToolImage); err != nil {
		return nil, err
	}

	if err := kube.EnsureNamespace(
		ctx,
		s.source.Kubernetes,
		session.Spec.SessionNamespace,
		session.ID,
		false,
	); err != nil {
		return nil, err
	}

	if err := s.save(ctx, session, true); err != nil {
		return nil, err
	}

	return session, nil
}

func (s *Service) Reserve(ctx context.Context, session *Session) error {
	if s.store != nil {
		return s.withLock(
			ctx,
			session,
			func(locked context.Context) error { return s.reserve(locked, session) },
		)
	}

	return s.reserve(ctx, session)
}

func (s *Service) reserve(ctx context.Context, session *Session) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if session.Status.Phase == PhaseCleaned || session.Status.Phase == PhaseCleaning {
		return errors.New("cross-cluster session is already being cleaned or has been cleaned")
	}

	if session.Status.Phase == PhaseReserved || session.Status.Phase == PhaseTransferring ||
		session.Status.Phase == PhaseCompleted {
		return nil
	}

	session.Status.Phase = PhaseReserving
	session.Status.Message = "creating destination PVCs"
	s.touch(session)

	if err := kube.EnsureNamespace(
		ctx,
		s.destination.Kubernetes,
		session.Spec.DestinationNamespace,
		session.ID,
		false,
	); err != nil {
		return err
	}

	for i := range session.Spec.Volumes {
		if session.Status.Volumes[i].Reservation.PV.UID != "" {
			continue
		}

		if err := s.reserveVolume(ctx, session, i); err != nil {
			return s.fail(ctx, session, err)
		}

		session.Status.Volumes[i].Reservation.PV = session.Spec.Volumes[i].Destination.PV

		session.Status.Volumes[i].Reservation.PVC = session.Spec.Volumes[i].Destination.PVC
		if err := s.save(ctx, session, false); err != nil {
			return err
		}
	}

	session.Status.Phase = PhaseReserved
	session.Status.Message = "destination PVCs are bound"
	s.touch(session)

	return s.save(ctx, session, false)
}

func (s *Service) Copy(ctx context.Context, session *Session, retries int, noCompress bool) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if s.store != nil {
		return s.withLock(
			ctx,
			session,
			func(locked context.Context) error { return s.copy(locked, session, retries, noCompress) },
		)
	}

	return s.copy(ctx, session, retries, noCompress)
}

func (s *Service) copy(ctx context.Context, session *Session, retries int, noCompress bool) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if session.Status.Phase == PhaseCleaned || session.Status.Phase == PhaseCleaning {
		return errors.New("cross-cluster session is already being cleaned or has been cleaned")
	}

	if err := s.reserve(ctx, session); err != nil {
		return err
	}

	if s.copier == nil {
		return errors.New("copy engine is unavailable")
	}

	schedulingValues, err := s.toolSchedulingValues(ctx, session)
	if err != nil {
		return s.fail(ctx, session, err)
	}

	session.Status.Phase = PhaseTransferring
	session.Status.Message = "copying PVC data"
	s.touch(session)

	if err := s.save(ctx, session, false); err != nil {
		return err
	}

	if retries < 1 {
		retries = 1
	}

	for i := range session.Spec.Volumes {
		status := &session.Status.Volumes[i]
		if status.Transfer.CompletedAt != nil {
			if err := s.validateDestinationVolume(ctx, session, i); err != nil {
				status.Transfer.LastError = err.Error()
				session.Status.Phase = PhaseFailed
				session.Status.Message = err.Error()
				s.touch(session)

				if saveErr := s.save(ctx, session, false); saveErr != nil {
					return errors.Join(err, saveErr)
				}

				return err
			}

			continue
		}

		volume := &session.Spec.Volumes[i]
		if err := s.validateTransferVolume(ctx, session, i); err != nil {
			status.Transfer.LastError = err.Error()
			session.Status.Phase = PhaseFailed
			session.Status.Message = err.Error()
			s.touch(session)

			if saveErr := s.save(ctx, session, false); saveErr != nil {
				return errors.Join(err, saveErr)
			}

			return err
		}

		var last error

		previousAttempts := status.Transfer.Attempts
		for retry := 1; retry <= retries; retry++ {
			attempt := previousAttempts + retry
			status.Transfer.Attempts = attempt
			req := copyengine.Request{
				SessionID:                 session.ID + "-" + volume.Source.PVC.Name,
				ToolImage:                 session.Spec.ToolImage,
				Source:                    objectRef(volume.Source.PVC),
				Destination:               objectRef(volume.Destination.PVC),
				SourcePath:                volume.Transfer.SourcePath,
				DestinationPath:           volume.Transfer.DestinationPath,
				Mode:                      copyengine.ModeFinal,
				Attempt:                   attempt,
				KubeconfigPath:            s.sourceKubeconfig,
				Context:                   s.sourceContext,
				DestinationKubeconfigPath: s.destinationKubeconfig,
				DestinationContext:        s.destinationContext,
				Strategies:                session.Spec.Strategies,
				DeleteExtraneousFiles:     session.Spec.DeleteExtraneous,
				VerifyChecksum:            session.Spec.VerifyChecksum,
				IgnoreSizes: capacitySmaller(
					volume.Destination.Capacity,
					volume.Source.Capacity,
				),
				NoCompress:       noCompress,
				HelmTimeout:      s.helmTimeout,
				HelmStringValues: append([]string(nil), schedulingValues...),
				Writer:           s.writer,
				Logger:           s.logger,
			}

			last = s.copier.Copy(ctx, req, nil)
			if last == nil {
				break
			}
		}

		if last != nil {
			status.Transfer.LastError = last.Error()
			return s.fail(ctx, session, last)
		}

		now := metav1.NewTime(s.now().UTC())
		status.Transfer.CompletedAt = &now
		status.Transfer.LastError = ""

		if err := s.save(ctx, session, false); err != nil {
			return err
		}
	}

	now := metav1.NewTime(s.now().UTC())
	session.Status.CompletedAt = &now
	session.Status.Phase = PhaseCompleted
	session.Status.Message = "cross-cluster copy completed"
	s.touch(session)

	return s.save(ctx, session, false)
}

// fail records a recoverable cross-cluster failure and preserves a persistence
// error when the checkpoint itself cannot be written.
func (s *Service) fail(ctx context.Context, session *Session, cause error) error {
	session.Status.Phase = PhaseFailed
	session.Status.Message = cause.Error()
	s.touch(session)

	if err := s.save(ctx, session, false); err != nil {
		return errors.Join(cause, err)
	}

	return cause
}

// toolSchedulingValues carries the node taints that the upstream pv-migrate
// chart must tolerate. Cross-cluster sessions cannot rely on the source
// cluster's scheduler defaults, so the values are assembled from both API
// servers before launching a transfer.
func (s *Service) toolSchedulingValues(
	ctx context.Context,
	session *Session,
) ([]string, error) {
	if session == nil {
		return nil, errors.New("cross-cluster session is required")
	}

	values := kube.ZeroResourceHelmValues()

	targetName := session.Spec.TargetNode
	if targetName == "" || targetName == domain.AutoValue {
		return nil, errors.New("cross-cluster session has no resolved destination target node")
	}

	target, err := s.destination.Kubernetes.CoreV1().Nodes().Get(
		ctx, targetName, metav1.GetOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("read destination target node %s before copy: %w", targetName, err)
	}

	values = append(values,
		kube.ToolComponentTolerationHelmValues(kube.ToolComponentRsync, target)...,
	)

	// A local PV pins the source SSHD to the node(s) allowed by its PV
	// topology. Mirror those nodes' hard-taint tolerations so source-side
	// tools can start even when the source cluster reserves tainted storage
	// nodes for this workload.
	sourceNodes, err := s.source.Kubernetes.CoreV1().Nodes().List(
		ctx, metav1.ListOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("list source nodes before copy: %w", err)
	}

	sshdNodes := make([]*corev1.Node, 0, 1)
	if slices.Contains(session.Spec.Strategies, "local") {
		sshdNodes = append(sshdNodes, target)
	}

	for i := range session.Spec.Volumes {
		pv, getErr := s.source.Kubernetes.CoreV1().PersistentVolumes().Get(
			ctx,
			session.Spec.Volumes[i].Source.PV.Name,
			metav1.GetOptions{},
		)
		if getErr != nil {
			return nil, fmt.Errorf(
				"read source PV %s before copy scheduling: %w",
				session.Spec.Volumes[i].Source.PV.Name,
				getErr,
			)
		}

		if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
			continue
		}

		for nodeIndex := range sourceNodes.Items {
			if !kube.PVSupportsNode(pv, &sourceNodes.Items[nodeIndex]) {
				continue
			}

			sshdNodes = append(sshdNodes, &sourceNodes.Items[nodeIndex])
		}
	}

	values = append(values,
		kube.ToolComponentTolerationHelmValues(kube.ToolComponentSSHD, sshdNodes...)...,
	)

	return values, nil
}
