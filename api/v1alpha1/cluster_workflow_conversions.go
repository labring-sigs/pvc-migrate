package v1alpha1

import (
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
)

func clusterVolumeSpecFromDomain(v domain.VolumeSpec) ClusterVolumeSpec {
	return ClusterVolumeSpec{
		SourcePVC:           localRefFromDomain(v.SourcePVC),
		SourcePV:            localRefFromDomain(v.SourcePV),
		DestinationPVC:      localRefFromDomain(v.DestinationPVC),
		SourceReclaimPolicy: v.SourceReclaimPolicy,
		SourcePVCSpec:       *v.SourcePVCSpec.DeepCopy(),
		SourcePVCMetadata:   pvcMetadataFromDomain(v.SourcePVCMetadata),
		Capacity:            v.Capacity,
		SourceCapacity:      v.SourceCapacity,
		SourceUsedBytes:     v.SourceUsedBytes,
		SourceUsageKnown:    v.SourceUsageKnown,
		StorageClass:        v.StorageClass,
		AccessModes:         append([]corev1.PersistentVolumeAccessMode(nil), v.AccessModes...),
		VolumeMode:          v.VolumeMode,
		ConcurrentConsumers: v.ConcurrentConsumers,
		TransferScope:       scopeFromDomain(v.TransferScope),
	}
}

func (v ClusterVolumeSpec) Domain(
	sourceNamespace, destinationNamespace string,
) domain.VolumeSpec {
	return domain.VolumeSpec{
		SourcePVC:           localRefToDomain(v.SourcePVC, sourceNamespace),
		SourcePV:            localRefToDomain(v.SourcePV, ""),
		DestinationPVC:      localRefToDomain(v.DestinationPVC, destinationNamespace),
		SourceReclaimPolicy: v.SourceReclaimPolicy,
		SourcePVCSpec:       *v.SourcePVCSpec.DeepCopy(),
		SourcePVCMetadata:   pvcMetadataToDomain(v.SourcePVCMetadata),
		Capacity:            v.Capacity,
		SourceCapacity:      v.SourceCapacity,
		SourceUsedBytes:     v.SourceUsedBytes,
		SourceUsageKnown:    v.SourceUsageKnown,
		StorageClass:        v.StorageClass,
		AccessModes:         append([]corev1.PersistentVolumeAccessMode(nil), v.AccessModes...),
		VolumeMode:          v.VolumeMode,
		ConcurrentConsumers: v.ConcurrentConsumers,
		TransferScope:       scopeToDomain(v.TransferScope),
	}
}

func clusterVolumesFromDomain(in []domain.VolumeSpec) []ClusterVolumeSpec {
	if in == nil {
		return nil
	}

	out := make([]ClusterVolumeSpec, len(in))
	for i := range in {
		out[i] = clusterVolumeSpecFromDomain(in[i])
	}

	return out
}

func clusterVolumesToDomain(
	in []ClusterVolumeSpec,
	sourceNamespace, destinationNamespace string,
) []domain.VolumeSpec {
	if in == nil {
		return nil
	}

	out := make([]domain.VolumeSpec, len(in))
	for i := range in {
		out[i] = in[i].Domain(sourceNamespace, destinationNamespace)
	}

	return out
}

func clusterCommonSession(
	source, temporary, destination, sessionNamespace NamespaceName,
	volumes []ClusterVolumeSpec,
) domain.SessionCommon {
	return domain.SessionCommon{
		SourceNamespace:      string(source),
		TemporaryNamespace:   string(temporary),
		DestinationNamespace: string(destination),
		SessionNamespace:     string(sessionNamespace),
		Volumes:              clusterVolumesToDomain(volumes, string(source), string(temporary)),
	}
}

func (s ClusterMigrationSpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		SourceNode:           s.SourceNode,
		TargetNode:           s.TargetNode,
		ToolImage:            s.ToolImage,
		Strategies:           append([]string(nil), s.Strategies...),
		VerifyChecksum:       s.VerifyChecksum,
		DeleteExtraneous:     s.DeleteExtraneous,
		SkipSourceUsageCheck: s.SkipSourceUsageCheck,
	}
}

func (s ClusterPodMigrationSpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		SourceNode:           s.SourceNode,
		TargetNode:           s.TargetNode,
		ToolImage:            s.ToolImage,
		Strategies:           append([]string(nil), s.Strategies...),
		VerifyChecksum:       s.VerifyChecksum,
		DeleteExtraneous:     s.DeleteExtraneous,
		SkipSourceUsageCheck: s.SkipSourceUsageCheck,
	}
}

func (s ClusterReservationSpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		TargetNode:           s.TargetNode,
		ToolImage:            s.ToolImage,
		SkipSourceUsageCheck: s.SkipSourceUsageCheck,
	}
}

func (s ClusterCopySpec) workflowOptions() domain.SessionWorkflowOptions {
	return domain.SessionWorkflowOptions{
		SourceNode:           s.SourceNode,
		TargetNode:           s.TargetNode,
		ToolImage:            s.ToolImage,
		Strategies:           append([]string(nil), s.Strategies...),
		VerifyChecksum:       boolValueOrDefault(s.VerifyChecksum, true),
		DeleteExtraneous:     s.DeleteExtraneous,
		SkipSourceUsageCheck: s.SkipSourceUsageCheck,
	}
}

func (s ClusterMigrationSpec) Domain() domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: clusterCommonSession(
			s.SourceNamespace,
			s.TemporaryNamespace,
			s.DestinationNamespace,
			s.SessionNamespace,
			s.Volumes,
		),
		Type:    domain.SessionTypeMigrate,
		Migrate: &domain.MigrateSessionSpec{SessionWorkflowOptions: s.workflowOptions()},
	}
}

func (s ClusterPodMigrationSpec) Domain() domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: clusterCommonSession(
			s.SourceNamespace,
			s.TemporaryNamespace,
			s.SourceNamespace,
			s.SessionNamespace,
			s.Volumes,
		),
		Type: domain.SessionTypeMigratePod,
		MigratePod: &domain.MigratePodSessionSpec{
			SessionWorkflowOptions: s.workflowOptions(),
			Workload: workloadToDomain(
				WorkloadSpec(s.Workload),
				string(s.SourceNamespace),
			),
			PrecopyPasses:          s.PrecopyPasses,
			OpenEBSLVMEnableShared: s.OpenEBSLVMEnableShared,
		},
	}
}

func (s ClusterReservationSpec) Domain() domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: clusterCommonSession(
			s.SourceNamespace,
			s.DestinationNamespace,
			s.DestinationNamespace,
			s.SessionNamespace,
			s.Volumes,
		),
		Type:    domain.SessionTypeReserve,
		Reserve: &domain.ReserveSessionSpec{SessionWorkflowOptions: s.workflowOptions()},
	}
}

func (s ClusterCopySpec) Domain() domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: clusterCommonSession(
			s.SourceNamespace,
			s.DestinationNamespace,
			s.DestinationNamespace,
			s.SessionNamespace,
			s.Volumes,
		),
		Type: domain.SessionTypeCopy,
		Copy: &domain.CopySessionSpec{
			SessionWorkflowOptions: s.workflowOptions(),
			Online:                 s.Online,
		},
	}
}

func (s MoveSpec) Domain() domain.SessionSpec {
	return identitySessionSpec(
		domain.SessionTypeMove,
		string(s.SourceNamespace),
		string(s.DestinationNamespace),
		string(s.SessionNamespace),
		s.Identity.SourcePVC,
		s.Identity.SourcePV,
		s.Identity.DestinationPVC,
		s.Identity.SourceTemplate,
	)
}

func ClusterMigrationSpecFromDomain(s domain.SessionSpec) ClusterMigrationSpec {
	options := s.WorkflowOptions()

	return ClusterMigrationSpec{
		SourceNamespace:      NamespaceName(s.SourceNamespace),
		TemporaryNamespace:   NamespaceName(s.TemporaryNamespace),
		DestinationNamespace: NamespaceName(s.DestinationNamespace),
		SessionNamespace:     NamespaceName(s.SessionNamespace),
		Volumes:              clusterVolumesFromDomain(s.Volumes),
		SourceNode:           options.SourceNode,
		TargetNode:           options.TargetNode,
		ToolImage:            options.ToolImage,
		Strategies:           append([]string(nil), options.Strategies...),
		VerifyChecksum:       options.VerifyChecksum,
		DeleteExtraneous:     options.DeleteExtraneous,
		SkipSourceUsageCheck: options.SkipSourceUsageCheck,
	}
}

func ClusterPodMigrationSpecFromDomain(s domain.SessionSpec) ClusterPodMigrationSpec {
	options := s.WorkflowOptions()

	return ClusterPodMigrationSpec{
		SourceNamespace:        NamespaceName(s.SourceNamespace),
		TemporaryNamespace:     NamespaceName(s.TemporaryNamespace),
		SessionNamespace:       NamespaceName(s.SessionNamespace),
		Volumes:                clusterVolumesFromDomain(s.Volumes),
		SourceNode:             options.SourceNode,
		TargetNode:             options.TargetNode,
		ToolImage:              options.ToolImage,
		Strategies:             append([]string(nil), options.Strategies...),
		VerifyChecksum:         options.VerifyChecksum,
		DeleteExtraneous:       options.DeleteExtraneous,
		SkipSourceUsageCheck:   options.SkipSourceUsageCheck,
		Workload:               ClusterWorkloadSpec(workloadFromDomain(s.Workload())),
		PrecopyPasses:          s.PrecopyPasses(),
		OpenEBSLVMEnableShared: s.OpenEBSLVMSharedMountEnabled(),
	}
}

func ClusterReservationSpecFromDomain(s domain.SessionSpec) ClusterReservationSpec {
	options := s.WorkflowOptions()

	return ClusterReservationSpec{
		SourceNamespace:      NamespaceName(s.SourceNamespace),
		DestinationNamespace: NamespaceName(s.TemporaryNamespace),
		SessionNamespace:     NamespaceName(s.SessionNamespace),
		Volumes:              clusterVolumesFromDomain(s.Volumes),
		TargetNode:           options.TargetNode,
		ToolImage:            options.ToolImage,
		SkipSourceUsageCheck: options.SkipSourceUsageCheck,
	}
}

func ClusterCopySpecFromDomain(s domain.SessionSpec) ClusterCopySpec {
	options := s.WorkflowOptions()

	return ClusterCopySpec{
		SourceNamespace:      NamespaceName(s.SourceNamespace),
		DestinationNamespace: NamespaceName(s.TemporaryNamespace),
		SessionNamespace:     NamespaceName(s.SessionNamespace),
		Volumes:              clusterVolumesFromDomain(s.Volumes),
		SourceNode:           options.SourceNode,
		TargetNode:           options.TargetNode,
		ToolImage:            options.ToolImage,
		Strategies:           append([]string(nil), options.Strategies...),
		VerifyChecksum:       optionalBool(options.VerifyChecksum, true),
		DeleteExtraneous:     options.DeleteExtraneous,
		SkipSourceUsageCheck: options.SkipSourceUsageCheck,
		Online:               s.Online(),
	}
}

func MoveSpecFromDomain(s domain.SessionSpec) MoveSpec {
	volume := firstVolume(s.Volumes)

	return MoveSpec{
		SourceNamespace:      NamespaceName(s.SourceNamespace),
		DestinationNamespace: NamespaceName(s.DestinationNamespace),
		SessionNamespace:     NamespaceName(s.SessionNamespace),
		Identity: MoveIdentity{
			SourcePVC:      localRefFromDomain(volume.SourcePVC),
			SourcePV:       localRefFromDomain(volume.SourcePV),
			DestinationPVC: localRefFromDomain(volume.DestinationPVC),
			SourceTemplate: PVCSourceTemplate{
				Spec:          *volume.SourcePVCSpec.DeepCopy(),
				Metadata:      pvcMetadataFromDomain(volume.SourcePVCMetadata),
				ReclaimPolicy: volume.SourceReclaimPolicy,
			},
		},
	}
}

func clusterActivationFromDomain(in domain.ActivationState) ClusterVolumeActivationStatus {
	return ClusterVolumeActivationStatus{
		TemporaryPVCDeleted: in.TemporaryPVCDeleted,
		SourcePVCDeleted:    in.SourcePVCDeleted,
		DestinationReserved: in.DestinationReserved,
		ActivePVC:           optionalRefFromDomain(in.ActivePVC),
		ActivatedAt:         copyTime(in.ActivatedAt),
		RolledBackAt:        copyTime(in.RolledBackAt),
	}
}

func clusterActivationToDomain(in ClusterVolumeActivationStatus) domain.ActivationState {
	return domain.ActivationState{
		TemporaryPVCDeleted: in.TemporaryPVCDeleted,
		SourcePVCDeleted:    in.SourcePVCDeleted,
		DestinationReserved: in.DestinationReserved,
		ActivePVC:           optionalRefToDomain(in.ActivePVC),
		ActivatedAt:         copyTime(in.ActivatedAt),
		RolledBackAt:        copyTime(in.RolledBackAt),
	}
}

func clusterMigrationVolumeStatusFromDomain(
	v domain.VolumeStatus,
	spec domain.VolumeSpec,
) ClusterMigrationVolumeStatus {
	return ClusterMigrationVolumeStatus{
		SourcePVCName:     v.SourcePVCName,
		DestinationPVC:    optionalRefFromDomain(spec.DestinationPVC),
		DestinationPV:     optionalRefFromDomain(spec.DestinationPV),
		DestinationPolicy: spec.DestinationPolicy,
		Reserved:          v.Reserved,
		Sync: MigrationSyncStatus{
			FinalCompletedAt: copyTime(v.Sync.FinalCompletedAt),
			Attempts:         v.Sync.Attempts,
			BytesCopied:      v.Sync.BytesCopied,
			ChecksumVerified: v.Sync.ChecksumVerified,
			LastError:        domain.BoundWorkflowMessage(v.Sync.LastError),
		},
		Activation: clusterActivationFromDomain(v.Activation),
	}
}

func clusterMigrationVolumeStatusToDomain(
	v ClusterMigrationVolumeStatus,
) domain.VolumeStatus {
	return domain.VolumeStatus{
		SourcePVCName: v.SourcePVCName,
		Reserved:      v.Reserved,
		Sync: domain.SyncState{
			FinalCompletedAt: copyTime(v.Sync.FinalCompletedAt),
			Attempts:         v.Sync.Attempts,
			BytesCopied:      v.Sync.BytesCopied,
			ChecksumVerified: v.Sync.ChecksumVerified,
			LastError:        domain.BoundWorkflowMessage(v.Sync.LastError),
		},
		Activation: clusterActivationToDomain(v.Activation),
	}
}

func clusterPodMigrationVolumeStatusFromDomain(
	v domain.VolumeStatus,
	spec domain.VolumeSpec,
) ClusterPodMigrationVolumeStatus {
	return ClusterPodMigrationVolumeStatus{
		SourcePVCName:     v.SourcePVCName,
		DestinationPVC:    optionalRefFromDomain(spec.DestinationPVC),
		DestinationPV:     optionalRefFromDomain(spec.DestinationPV),
		DestinationPolicy: spec.DestinationPolicy,
		Reserved:          v.Reserved,
		Sync: PodMigrationSyncStatus{
			WarmCompletedAt:  copyTime(v.Sync.WarmCompletedAt),
			FinalCompletedAt: copyTime(v.Sync.FinalCompletedAt),
			Attempts:         v.Sync.Attempts,
			BytesCopied:      v.Sync.BytesCopied,
			ChecksumVerified: v.Sync.ChecksumVerified,
			LastError:        domain.BoundWorkflowMessage(v.Sync.LastError),
		},
		Activation: clusterActivationFromDomain(v.Activation),
	}
}

func clusterPodMigrationVolumeStatusToDomain(
	v ClusterPodMigrationVolumeStatus,
) domain.VolumeStatus {
	return domain.VolumeStatus{
		SourcePVCName: v.SourcePVCName,
		Reserved:      v.Reserved,
		Sync: domain.SyncState{
			WarmCompletedAt:  copyTime(v.Sync.WarmCompletedAt),
			FinalCompletedAt: copyTime(v.Sync.FinalCompletedAt),
			Attempts:         v.Sync.Attempts,
			BytesCopied:      v.Sync.BytesCopied,
			ChecksumVerified: v.Sync.ChecksumVerified,
			LastError:        domain.BoundWorkflowMessage(v.Sync.LastError),
		},
		Activation: clusterActivationToDomain(v.Activation),
	}
}

func clusterReservationVolumeStatusFromDomain(
	v domain.VolumeStatus,
	spec domain.VolumeSpec,
) ClusterReservationVolumeStatus {
	return ClusterReservationVolumeStatus{
		SourcePVCName:     v.SourcePVCName,
		DestinationPVC:    optionalRefFromDomain(spec.DestinationPVC),
		DestinationPV:     optionalRefFromDomain(spec.DestinationPV),
		DestinationPolicy: spec.DestinationPolicy,
		Reserved:          v.Reserved,
	}
}

func clusterCopyVolumeStatusFromDomain(
	v domain.VolumeStatus,
	spec domain.VolumeSpec,
) ClusterCopyVolumeStatus {
	return ClusterCopyVolumeStatus{
		SourcePVCName:     v.SourcePVCName,
		DestinationPVC:    optionalRefFromDomain(spec.DestinationPVC),
		DestinationPV:     optionalRefFromDomain(spec.DestinationPV),
		DestinationPolicy: spec.DestinationPolicy,
		Reserved:          v.Reserved,
		Sync: CopySyncStatus{
			WarmCompletedAt: copyTime(v.Sync.WarmCompletedAt),
			Attempts:        v.Sync.Attempts,
			BytesCopied:     v.Sync.BytesCopied,
			LastError:       domain.BoundWorkflowMessage(v.Sync.LastError),
		},
	}
}

func clusterCopyVolumeStatusToDomain(v ClusterCopyVolumeStatus) domain.VolumeStatus {
	return domain.VolumeStatus{
		SourcePVCName: v.SourcePVCName,
		Reserved:      v.Reserved,
		Sync: domain.SyncState{
			WarmCompletedAt: copyTime(v.Sync.WarmCompletedAt),
			Attempts:        v.Sync.Attempts,
			BytesCopied:     v.Sync.BytesCopied,
			LastError:       domain.BoundWorkflowMessage(v.Sync.LastError),
		},
	}
}

func applyClusterDestinationCheckpoint(
	volume *domain.VolumeSpec,
	destinationPVC, destinationPV *ObjectReference,
	policy PVReclaimPolicy,
) {
	if volume == nil {
		return
	}

	if destinationPVC != nil {
		volume.DestinationPVC = refToDomain(*destinationPVC)
	}

	if destinationPV != nil {
		volume.DestinationPV = refToDomain(*destinationPV)
	}

	if policy != "" {
		volume.DestinationPolicy = policy
	}
}

func (s ClusterMigrationStatus) Domain() domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)

	out.Volumes = make([]domain.VolumeStatus, len(s.Volumes))
	for i := range s.Volumes {
		out.Volumes[i] = clusterMigrationVolumeStatusToDomain(s.Volumes[i])
	}

	return out
}

func (s ClusterMigrationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil {
		return
	}

	for i := range min(len(spec.Volumes), len(s.Volumes)) {
		checkpoint := s.Volumes[i]
		applyClusterDestinationCheckpoint(
			&spec.Volumes[i],
			checkpoint.DestinationPVC,
			checkpoint.DestinationPV,
			checkpoint.DestinationPolicy,
		)
	}
}

func (s ClusterPodMigrationStatus) Domain() domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)
	out.WarmPassesCompleted = s.WarmPassesCompleted
	out.OriginalPodSnapshotHash = s.OriginalPodSnapshotHash

	out.Volumes = make([]domain.VolumeStatus, len(s.Volumes))
	for i := range s.Volumes {
		out.Volumes[i] = clusterPodMigrationVolumeStatusToDomain(s.Volumes[i])
	}

	out.OpenEBSLVMSharedMounts = make([]domain.OpenEBSLVMSharedMount, len(s.OpenEBSLVMSharedMounts))
	for i := range s.OpenEBSLVMSharedMounts {
		mount := s.OpenEBSLVMSharedMounts[i]
		out.OpenEBSLVMSharedMounts[i] = domain.OpenEBSLVMSharedMount{
			SourcePV:          localRefToDomain(mount.SourcePV, ""),
			LVMVolume:         refToDomain(mount.LVMVolume),
			PreviousShared:    mount.PreviousShared,
			PreviousSharedSet: mount.PreviousSharedSet,
		}
	}

	return out
}

func (s ClusterPodMigrationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil {
		return
	}

	for i := range min(len(spec.Volumes), len(s.Volumes)) {
		checkpoint := s.Volumes[i]
		applyClusterDestinationCheckpoint(
			&spec.Volumes[i],
			checkpoint.DestinationPVC,
			checkpoint.DestinationPV,
			checkpoint.DestinationPolicy,
		)
	}

	workload := spec.WorkloadPtr()
	if workload == nil || s.Workload == nil {
		return
	}

	if s.Workload.Pod != nil {
		workload.Pod = refToDomain(*s.Workload.Pod)
	}

	if len(s.Workload.AffectedPods) > 0 {
		workload.AffectedPods = refsToDomain(s.Workload.AffectedPods)
	}
}

func (s ClusterReservationStatus) Domain() domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)

	out.Volumes = make([]domain.VolumeStatus, len(s.Volumes))
	for i := range s.Volumes {
		out.Volumes[i] = domain.VolumeStatus{
			SourcePVCName: s.Volumes[i].SourcePVCName,
			Reserved:      s.Volumes[i].Reserved,
		}
	}

	return out
}

func (s ClusterReservationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil {
		return
	}

	for i := range min(len(spec.Volumes), len(s.Volumes)) {
		checkpoint := s.Volumes[i]
		applyClusterDestinationCheckpoint(
			&spec.Volumes[i],
			checkpoint.DestinationPVC,
			checkpoint.DestinationPV,
			checkpoint.DestinationPolicy,
		)
	}
}

func (s ClusterCopyStatus) Domain() domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)

	out.Volumes = make([]domain.VolumeStatus, len(s.Volumes))
	for i := range s.Volumes {
		out.Volumes[i] = clusterCopyVolumeStatusToDomain(s.Volumes[i])
	}

	return out
}

func (s ClusterCopyStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	if spec == nil {
		return
	}

	for i := range min(len(spec.Volumes), len(s.Volumes)) {
		checkpoint := s.Volumes[i]
		applyClusterDestinationCheckpoint(
			&spec.Volumes[i],
			checkpoint.DestinationPVC,
			checkpoint.DestinationPV,
			checkpoint.DestinationPolicy,
		)
	}
}

func (s MoveStatus) Domain() domain.SessionStatus {
	out := workflowStatusToDomain(s.WorkflowStatus)

	out.Volumes = make([]domain.VolumeStatus, len(s.Volumes))
	for i := range s.Volumes {
		volume := s.Volumes[i]
		out.Volumes[i] = domain.VolumeStatus{
			SourcePVCName: volume.SourcePVCName,
			Activation: domain.ActivationState{
				ActivePVC:    optionalRefToDomain(volume.Activation.ActivePVC),
				ActivatedAt:  copyTime(volume.Activation.ActivatedAt),
				RolledBackAt: copyTime(volume.Activation.RolledBackAt),
			},
		}
	}

	return out
}

func ClusterMigrationStatusFromDomain(
	s domain.SessionStatus,
	volumes []domain.VolumeSpec,
) ClusterMigrationStatus {
	out := ClusterMigrationStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Volumes:        make([]ClusterMigrationVolumeStatus, len(s.Volumes)),
	}
	for i := range s.Volumes {
		out.Volumes[i] = clusterMigrationVolumeStatusFromDomain(
			s.Volumes[i],
			volumeSpecAt(volumes, i),
		)
	}

	return out
}

func ClusterPodMigrationStatusFromDomain(
	s domain.SessionStatus,
	spec domain.SessionSpec,
) ClusterPodMigrationStatus {
	out := ClusterPodMigrationStatus{
		WorkflowStatus:          workflowStatusFromDomain(s),
		WarmPassesCompleted:     s.WarmPassesCompleted,
		OriginalPodSnapshotHash: s.OriginalPodSnapshotHash,
		Volumes:                 make([]ClusterPodMigrationVolumeStatus, len(s.Volumes)),
		OpenEBSLVMSharedMounts:  make([]ClusterSharedMountStatus, len(s.OpenEBSLVMSharedMounts)),
	}
	if spec.MigratePod != nil {
		workload := spec.MigratePod.Workload
		out.Workload = &ClusterPodMigrationWorkloadStatus{
			Pod:          optionalRefFromDomain(workload.Pod),
			AffectedPods: refsFromDomain(workload.AffectedPods),
		}
	}

	for i := range s.Volumes {
		out.Volumes[i] = clusterPodMigrationVolumeStatusFromDomain(
			s.Volumes[i],
			volumeSpecAt(spec.Volumes, i),
		)
	}

	for i := range s.OpenEBSLVMSharedMounts {
		mount := s.OpenEBSLVMSharedMounts[i]
		out.OpenEBSLVMSharedMounts[i] = ClusterSharedMountStatus{
			SourcePV:          localRefFromDomain(mount.SourcePV),
			LVMVolume:         refFromDomain(mount.LVMVolume),
			PreviousShared:    mount.PreviousShared,
			PreviousSharedSet: mount.PreviousSharedSet,
		}
	}

	return out
}

func ClusterReservationStatusFromDomain(
	s domain.SessionStatus,
	volumes []domain.VolumeSpec,
) ClusterReservationStatus {
	out := ClusterReservationStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Volumes:        make([]ClusterReservationVolumeStatus, len(s.Volumes)),
	}
	for i := range s.Volumes {
		out.Volumes[i] = clusterReservationVolumeStatusFromDomain(
			s.Volumes[i],
			volumeSpecAt(volumes, i),
		)
	}

	return out
}

func ClusterCopyStatusFromDomain(
	s domain.SessionStatus,
	volumes []domain.VolumeSpec,
) ClusterCopyStatus {
	out := ClusterCopyStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Volumes:        make([]ClusterCopyVolumeStatus, len(s.Volumes)),
	}
	for i := range s.Volumes {
		out.Volumes[i] = clusterCopyVolumeStatusFromDomain(
			s.Volumes[i],
			volumeSpecAt(volumes, i),
		)
	}

	return out
}

func MoveStatusFromDomain(s domain.SessionStatus) MoveStatus {
	out := MoveStatus{
		WorkflowStatus: workflowStatusFromDomain(s),
		Volumes:        make([]MoveVolumeStatus, len(s.Volumes)),
	}
	for i := range s.Volumes {
		volume := s.Volumes[i]
		out.Volumes[i] = MoveVolumeStatus{
			SourcePVCName: volume.SourcePVCName,
			Activation: MoveActivationStatus{
				ActivePVC:    optionalRefFromDomain(volume.Activation.ActivePVC),
				ActivatedAt:  copyTime(volume.Activation.ActivatedAt),
				RolledBackAt: copyTime(volume.Activation.RolledBackAt),
			},
		}
	}

	return out
}
