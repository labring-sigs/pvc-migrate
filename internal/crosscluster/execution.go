package crosscluster

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) CreateCopySession(
	ctx context.Context,
	options CopyOptions,
	plan *Plan,
) (*CopySession, error) {
	return s.createCopySession(ctx, options, plan)
}

func (s *Service) createCopySession(
	ctx context.Context,
	options CopyOptions,
	plan *Plan,
) (*CopySession, error) {
	if plan == nil || !plan.Ready {
		return nil, errors.New("cross-cluster plan contains failed checks")
	}

	if plan.Kind != CopyKind ||
		plan.SessionID != options.SessionID ||
		plan.SessionNamespace != options.SessionNamespace ||
		plan.SourceNamespace != options.SourceNamespace ||
		plan.DestinationNamespace != options.DestinationNamespace ||
		plan.UnusedStoragePolicy != options.UnusedStoragePolicy ||
		plan.AllowVolumeShrink != options.AllowVolumeShrink ||
		plan.SkipSourceUsageCheck != options.SkipSourceUsageCheck ||
		plan.RequestedTargetNode != options.TargetNode ||
		plan.ToolImage != options.ToolImage ||
		plan.Online != options.Online ||
		plan.VerifyChecksum != options.VerifyChecksum ||
		plan.DeleteExtraneous != options.DeleteExtraneous ||
		!slices.Equal(plan.Strategies, normalizeStrategies(options.Strategies)) {
		return nil, errors.New(
			"cross-cluster copy options changed after planning; generate a new plan",
		)
	}

	sourceID, destID, err := s.clusterIdentities(ctx)
	if err != nil {
		return nil, err
	}

	if sourceID.ID != plan.SourceCluster.ID || destID.ID != plan.DestinationCluster.ID {
		return nil, errors.New(
			"cluster identity changed after planning; generate a new cross-cluster plan",
		)
	}

	destinationClass, err := s.destination.Kubernetes.StorageV1().StorageClasses().Get(
		ctx,
		options.DestinationStorageClass,
		metav1.GetOptions{},
	)
	if err != nil {
		return nil, err
	}

	volumes, err := s.buildSessionVolumes(ctx, plan, sourceID, destID, destinationClass)
	if err != nil {
		return nil, err
	}

	session := NewCopySession(
		options.SessionID,
		CopySpec{
			SessionContext: SessionContext{
				UnusedStoragePolicy:  options.UnusedStoragePolicy,
				SessionNamespace:     options.SessionNamespace,
				SourceCluster:        sourceID,
				DestinationCluster:   destID,
				SourceNamespace:      options.SourceNamespace,
				DestinationNamespace: options.DestinationNamespace,
				ToolImage:            options.ToolImage,
				Strategies:           normalizeStrategies(options.Strategies),
				AllowVolumeShrink:    options.AllowVolumeShrink,
				SkipSourceUsageCheck: options.SkipSourceUsageCheck,
				TargetNode:           plan.TargetNode,
				Volumes:              volumes,
			},
			Online:           options.Online,
			VerifyChecksum:   options.VerifyChecksum,
			DeleteExtraneous: options.DeleteExtraneous,
		},
		s.now(),
	)
	if _, err := kube.NormalizeToolImage(session.Spec.ToolImage); err != nil {
		return nil, err
	}

	if err := kube.RequireNamespace(
		ctx,
		s.source.Kubernetes,
		session.Spec.SessionNamespace,
	); err != nil {
		return nil, err
	}

	if err := kube.RequireNamespace(
		ctx,
		s.destination.Kubernetes,
		session.Spec.DestinationNamespace,
	); err != nil {
		return nil, err
	}

	if err := s.save(ctx, session, true); err != nil {
		return nil, err
	}

	return session, nil
}

// CreateReservationSession persists the operation-specific reservation
// session. Reservation options intentionally cannot carry copy-only flags.
func (s *Service) CreateReservationSession(
	ctx context.Context,
	options ReservationOptions,
	plan *Plan,
) (*ReservationSession, error) {
	if plan == nil || !plan.Ready {
		return nil, errors.New("cross-cluster reservation plan contains failed checks")
	}

	if plan.Kind != ReservationKind ||
		plan.SessionID != options.SessionID ||
		plan.SessionNamespace != options.SessionNamespace ||
		plan.SourceNamespace != options.SourceNamespace ||
		plan.DestinationNamespace != options.DestinationNamespace ||
		plan.UnusedStoragePolicy != options.UnusedStoragePolicy ||
		plan.AllowVolumeShrink != options.AllowVolumeShrink ||
		plan.SkipSourceUsageCheck != options.SkipSourceUsageCheck ||
		plan.RequestedTargetNode != options.TargetNode ||
		plan.ToolImage != options.ToolImage ||
		!slices.Equal(plan.Strategies, normalizeStrategies(options.Strategies)) {
		return nil, errors.New(
			"cross-cluster reservation options changed after planning; generate a new plan",
		)
	}

	sourceID, destID, err := s.clusterIdentities(ctx)
	if err != nil {
		return nil, err
	}

	if sourceID.ID != plan.SourceCluster.ID || destID.ID != plan.DestinationCluster.ID {
		return nil, errors.New(
			"cluster identity changed after planning; generate a new cross-cluster plan",
		)
	}

	destinationClass, err := s.destination.Kubernetes.StorageV1().StorageClasses().Get(
		ctx, options.DestinationStorageClass, metav1.GetOptions{},
	)
	if err != nil {
		return nil, err
	}

	volumes, err := s.buildSessionVolumes(ctx, plan, sourceID, destID, destinationClass)
	if err != nil {
		return nil, err
	}

	session := NewReservationSession(
		options.SessionID,
		ReservationSpec{SessionContext: SessionContext{
			UnusedStoragePolicy:  options.UnusedStoragePolicy,
			SessionNamespace:     options.SessionNamespace,
			SourceCluster:        sourceID,
			DestinationCluster:   destID,
			SourceNamespace:      options.SourceNamespace,
			DestinationNamespace: options.DestinationNamespace,
			ToolImage:            options.ToolImage,
			Strategies:           normalizeStrategies(options.Strategies),
			AllowVolumeShrink:    options.AllowVolumeShrink,
			SkipSourceUsageCheck: options.SkipSourceUsageCheck,
			TargetNode:           plan.TargetNode,
			Volumes:              volumes,
		}},
		s.now(),
	)
	if err := kube.RequireNamespace(
		ctx,
		s.source.Kubernetes,
		session.Spec.SessionNamespace,
	); err != nil {
		return nil, err
	}

	if err := kube.RequireNamespace(
		ctx,
		s.destination.Kubernetes,
		session.Spec.DestinationNamespace,
	); err != nil {
		return nil, err
	}

	if err := s.saveReservation(ctx, session, true); err != nil {
		return nil, err
	}

	return session, nil
}

// PromoteReservation changes the persisted operation after destination PVCs
// are bound. The reservation payload is copied into a copy payload once, so
// transfer-only status can never appear on a reservation session.
func (s *Service) PromoteReservation(
	ctx context.Context,
	reservation *ReservationSession,
) (*CopySession, error) {
	if reservation == nil {
		return nil, errors.New("cross-cluster reservation session is required")
	}

	if reservation.Status.Phase != PhaseReserved {
		return nil, fmt.Errorf(
			"cross-cluster reservation session is %s; reserve all destination PVCs before copying",
			reservation.Status.Phase,
		)
	}

	var promoted *CopySession

	convert := func(operationCtx context.Context) error {
		if err := s.validateReservationSession(operationCtx, reservation); err != nil {
			return err
		}

		volumes := make([]CopyVolumeStatus, len(reservation.Status.Volumes))
		for i := range reservation.Status.Volumes {
			volumes[i].ReservationVolumeStatus = reservation.Status.Volumes[i]
		}

		promoted = &CopySession{
			SessionEnvelope: reservation.SessionEnvelope,
			Spec:            CopySpec{SessionContext: reservation.Spec.SessionContext},
			Status: CopyStatus{
				SessionLifecycleStatus: reservation.Status.SessionLifecycleStatus,
				Volumes:                volumes,
			},
		}
		promoted.Kind = CopyKind
		promoted.Status.Message = "destination PVCs reserved; ready to copy"
		s.touch(promoted)

		if err := s.save(operationCtx, promoted, false); err != nil {
			return err
		}

		return nil
	}

	if s.locker != nil {
		if err := s.withReservationLock(ctx, reservation, convert); err != nil {
			return nil, err
		}
	} else if err := convert(ctx); err != nil {
		return nil, err
	}

	return promoted, nil
}

func (s *Service) buildSessionVolumes(
	ctx context.Context,
	plan *Plan,
	sourceID, destinationID kube.ClusterIdentity,
	destinationClass *storagev1.StorageClass,
) ([]VolumeSpec, error) {
	type volumeResult struct {
		volume VolumeSpec
		err    error
	}

	results := make([]volumeResult, len(plan.Volumes))
	parallel.For(len(plan.Volumes), func(index int) {
		results[index].volume, results[index].err = s.buildSessionVolume(
			ctx, plan.Volumes[index], sourceID, destinationID, destinationClass,
		)
	})

	volumes := make([]VolumeSpec, 0, len(results))
	for _, result := range results {
		if result.err != nil {
			return nil, result.err
		}

		volumes = append(volumes, result.volume)
	}

	return volumes, nil
}

func (s *Service) buildSessionVolume(
	ctx context.Context,
	p VolumePlan,
	sourceID, destinationID kube.ClusterIdentity,
	destinationClass *storagev1.StorageClass,
) (VolumeSpec, error) {
	pvc, err := s.source.Kubernetes.CoreV1().
		PersistentVolumeClaims(p.SourceNamespace).
		Get(ctx, p.SourcePVC, metav1.GetOptions{})
	if err != nil {
		return VolumeSpec{}, err
	}

	pv, err := s.source.Kubernetes.CoreV1().PersistentVolumes().Get(
		ctx, pvc.Spec.VolumeName, metav1.GetOptions{},
	)
	if err != nil {
		return VolumeSpec{}, err
	}

	if pvc.UID != p.SourcePVCUID || pv.UID != p.SourcePVUID {
		return VolumeSpec{}, fmt.Errorf(
			"source PVC/PV identity changed after planning for %s/%s; generate a new cross-cluster plan",
			p.SourceNamespace,
			p.SourcePVC,
		)
	}

	expectedSourceCapacity, parseErr := resource.ParseQuantity(p.SourceCapacity)
	if parseErr != nil {
		return VolumeSpec{}, fmt.Errorf(
			"planned source capacity for %s is invalid: %w",
			p.SourcePVC,
			parseErr,
		)
	}

	if current := pv.Spec.Capacity[corev1.ResourceStorage]; current.Cmp(
		expectedSourceCapacity,
	) != 0 {
		return VolumeSpec{}, fmt.Errorf(
			"source PV capacity changed after planning for %s; generate a new cross-cluster plan",
			p.SourcePVC,
		)
	}

	if destinationClass.UID != p.StorageClassUID {
		return VolumeSpec{}, errors.New(
			"destination StorageClass changed after planning; generate a new cross-cluster plan",
		)
	}

	if err := kube.ValidateDestinationAccessModes(
		destinationClass.Provisioner,
		pvc.Spec.AccessModes,
	); err != nil {
		return VolumeSpec{}, fmt.Errorf(
			"destination StorageClass %s cannot provide source PVC %s/%s access modes: %w; generate a new cross-cluster plan with a compatible StorageClass",
			destinationClass.Name,
			pvc.Namespace,
			pvc.Name,
			err,
		)
	}

	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}

	return VolumeSpec{
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
				ClusterID:  destinationID.ID,
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Namespace:  p.DestinationNamespace,
				Name:       p.DestinationPVC,
			},
			Capacity: p.Capacity,
			StorageClass: ClusterResourceRef{
				ClusterID:  destinationID.ID,
				APIVersion: "storage.k8s.io/v1",
				Kind:       "StorageClass",
				Name:       destinationClass.Name,
				UID:        destinationClass.UID,
			},
			AccessModes: append([]corev1.PersistentVolumeAccessMode(nil), pvc.Spec.AccessModes...),
			VolumeMode:  mode,
		},
		Transfer: TransferSpec{SourcePath: p.SourcePath, DestinationPath: p.DestinationPath},
	}, nil
}

func (s *Service) Reserve(ctx context.Context, session *ReservationSession) error {
	if s.locker != nil {
		return s.withReservationLock(
			ctx,
			session,
			func(locked context.Context) error { return s.reserve(locked, session) },
		)
	}

	return s.reserve(ctx, session)
}

func (s *Service) reserve(ctx context.Context, session *ReservationSession) error {
	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	if err := s.validateReservationSession(ctx, session); err != nil {
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
	s.touchReservation(session)

	if err := kube.RequireNamespace(
		ctx,
		s.destination.Kubernetes,
		session.Spec.DestinationNamespace,
	); err != nil {
		return err
	}

	for i := range session.Spec.Volumes {
		if session.Status.Volumes[i].Reservation.PV.UID != "" {
			continue
		}

		state := reservationState{
			ID:     session.ID,
			Spec:   &session.Spec.SessionContext,
			Status: &session.Status.Volumes[i].Reservation,
			Save: func(saveCtx context.Context) error {
				return s.saveReservation(saveCtx, session, false)
			},
		}
		if err := s.reserveVolume(ctx, state, &session.Spec.Volumes[i]); err != nil {
			return s.failReservation(ctx, session, err)
		}

		session.Status.Volumes[i].Reservation.PV = session.Spec.Volumes[i].Destination.PV

		session.Status.Volumes[i].Reservation.PVC = session.Spec.Volumes[i].Destination.PVC
		if err := s.saveReservation(ctx, session, false); err != nil {
			return err
		}
	}

	session.Status.Phase = PhaseReserved
	session.Status.Message = "destination PVCs are bound"
	s.touchReservation(session)

	return s.saveReservation(ctx, session, false)
}

func (s *Service) Copy(
	ctx context.Context,
	session *CopySession,
	retries int,
	noCompress bool,
) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if s.locker != nil {
		return s.withLock(
			ctx,
			session,
			func(locked context.Context) error { return s.copy(locked, session, retries, noCompress) },
		)
	}

	return s.copy(ctx, session, retries, noCompress)
}

func (s *Service) copy(
	ctx context.Context,
	session *CopySession,
	retries int,
	noCompress bool,
) error {
	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if session.Status.Phase == PhaseCleaned || session.Status.Phase == PhaseCleaning {
		return errors.New("cross-cluster session is already being cleaned or has been cleaned")
	}

	if err := s.reserveCopy(ctx, session); err != nil {
		return err
	}

	if s.copier == nil {
		return errors.New("copy engine is unavailable")
	}

	schedulingValues, err := s.toolSchedulingValues(ctx, session)
	if err != nil {
		return s.fail(ctx, session, err)
	}

	// The upstream transfer chart does not expose PodSpec token automount. Use
	// a project-managed no-token account on each cluster side for sshd/rsync;
	// rclone has a separate identity contract for object-store credentials.
	seen := map[string]struct{}{}
	for _, target := range []struct {
		client    *kube.Clients
		namespace string
	}{
		{client: s.source, namespace: session.Spec.SourceNamespace},
		{client: s.destination, namespace: session.Spec.DestinationNamespace},
	} {
		key := fmt.Sprintf("%p/%s", target.client, target.namespace)
		if _, exists := seen[key]; exists {
			continue
		}

		if err := requireSessionLease(ctx); err != nil {
			return err
		}

		if err := kube.EnsureTransferServiceAccount(
			ctx,
			target.client.Kubernetes,
			target.namespace,
		); err != nil {
			return s.fail(ctx, session, err)
		}

		if err := requireSessionLease(ctx); err != nil {
			return err
		}

		seen[key] = struct{}{}
	}

	identityValues := kube.TransferServiceAccountHelmValues()
	schedulingValues = append(schedulingValues, identityValues.StringValues...)

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
			if err := requireSessionLease(ctx); err != nil {
				return err
			}

			attempt := previousAttempts + retry
			status.Transfer.Attempts = attempt
			req := copyengine.CopyRequest{
				AttemptIdentity: copyengine.AttemptIdentity{
					SessionID: session.ID + "-" + volume.Source.PVC.Name,
					Source:    objectRef(volume.Source.PVC),
					Mode:      copyengine.ModeFinal,
					Attempt:   attempt,
				},
				Source: copyengine.CopySource{
					KubeconfigPath: s.sourceKubeconfig,
					Context:        s.sourceContext,
					Path:           volume.Transfer.SourcePath,
				},
				Destination: copyengine.CopyDestination{
					Reference:      objectRef(volume.Destination.PVC),
					KubeconfigPath: s.destinationKubeconfig,
					Context:        s.destinationContext,
					Path:           volume.Transfer.DestinationPath,
				},
				Policy: copyengine.CopyPolicy{
					Strategies:            append([]string(nil), session.Spec.Strategies...),
					DeleteExtraneousFiles: session.Spec.DeleteExtraneous,
					VerifyChecksum:        session.Spec.VerifyChecksum,
					IgnoreSizes: capacitySmaller(
						volume.Destination.Capacity,
						volume.Source.Capacity,
					),
					NoCompress: noCompress,
				},
				Runtime: copyengine.CopyRuntime{
					ToolImage:        session.Spec.ToolImage,
					HelmTimeout:      s.helmTimeout,
					HelmValues:       append([]string(nil), identityValues.Values...),
					HelmStringValues: append([]string(nil), schedulingValues...),
					Writer:           s.writer,
					Logger:           s.logger,
				},
			}

			last = s.copier.Copy(ctx, req, nil)
			if last == nil {
				last = requireSessionLease(ctx)
			}

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

func (s *Service) reserveCopy(ctx context.Context, session *CopySession) error {
	if err := requireSessionLease(ctx); err != nil {
		return err
	}

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

	if err := s.save(ctx, session, false); err != nil {
		return err
	}

	for i := range session.Spec.Volumes {
		if session.Status.Volumes[i].Reservation.PV.UID != "" {
			continue
		}

		state := reservationState{
			ID:     session.ID,
			Spec:   &session.Spec.SessionContext,
			Status: &session.Status.Volumes[i].Reservation,
			Save: func(saveCtx context.Context) error {
				return s.save(saveCtx, session, false)
			},
		}
		if err := s.reserveVolume(ctx, state, &session.Spec.Volumes[i]); err != nil {
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

func (s *Service) failReservation(
	ctx context.Context,
	session *ReservationSession,
	cause error,
) error {
	session.Status.Phase = PhaseFailed
	session.Status.Message = cause.Error()
	s.touchReservation(session)

	if err := s.saveReservation(ctx, session, false); err != nil {
		return errors.Join(cause, err)
	}

	return cause
}

// fail records a recoverable cross-cluster failure and preserves a persistence
// error when the checkpoint itself cannot be written.
func (s *Service) fail(ctx context.Context, session *CopySession, cause error) error {
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
	session *CopySession,
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
	if slices.Contains(session.Spec.Strategies, domain.StrategyLocal) {
		sshdNodes = append(sshdNodes, target)
	}

	pvs := make([]*corev1.PersistentVolume, len(session.Spec.Volumes))
	errors := make([]error, len(session.Spec.Volumes))
	parallel.For(len(session.Spec.Volumes), func(index int) {
		volume := &session.Spec.Volumes[index]

		pv, getErr := s.source.Kubernetes.CoreV1().PersistentVolumes().Get(
			ctx,
			volume.Source.PV.Name,
			metav1.GetOptions{},
		)
		if getErr != nil {
			errors[index] = fmt.Errorf(
				"read source PV %s before copy scheduling: %w",
				volume.Source.PV.Name,
				getErr,
			)

			return
		}

		pvs[index] = pv
	})

	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	for _, pv := range pvs {
		if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
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
