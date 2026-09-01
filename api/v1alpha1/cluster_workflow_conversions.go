package v1alpha1

import "github.com/labring-sigs/pvc-migrate/internal/domain"

func (s ClusterMigrationSpec) Domain() domain.SessionSpec {
	return MigrationSpec(s).Domain()
}

func ClusterMigrationSpecFromDomain(s domain.SessionSpec) ClusterMigrationSpec {
	return ClusterMigrationSpec(MigrationSpecFromDomain(s))
}

func (s ClusterMigrationStatus) Domain() domain.SessionStatus {
	return MigrationStatus(s).Domain()
}

func (s ClusterMigrationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	MigrationStatus(s).ApplyToDomainSpec(spec)
}

func ClusterMigrationStatusFromDomain(
	s domain.SessionStatus,
	volumes []domain.VolumeSpec,
) ClusterMigrationStatus {
	return ClusterMigrationStatus(MigrationStatusFromDomain(s, volumes))
}

func (s ClusterPodMigrationSpec) Domain() domain.SessionSpec {
	return PodMigrationSpec(s).Domain()
}

func ClusterPodMigrationSpecFromDomain(s domain.SessionSpec) ClusterPodMigrationSpec {
	return ClusterPodMigrationSpec(PodMigrationSpecFromDomain(s))
}

func (s ClusterPodMigrationStatus) Domain() domain.SessionStatus {
	return PodMigrationStatus(s).Domain()
}

func (s ClusterPodMigrationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	PodMigrationStatus(s).ApplyToDomainSpec(spec)
}

func ClusterPodMigrationStatusFromDomain(
	s domain.SessionStatus,
	spec domain.SessionSpec,
) ClusterPodMigrationStatus {
	return ClusterPodMigrationStatus(PodMigrationStatusFromDomain(s, spec))
}

func (s ClusterReservationSpec) Domain() domain.SessionSpec {
	return ReservationSpec(s).Domain()
}

func ClusterReservationSpecFromDomain(s domain.SessionSpec) ClusterReservationSpec {
	return ClusterReservationSpec(ReservationSpecFromDomain(s))
}

func (s ClusterReservationStatus) Domain() domain.SessionStatus {
	return ReservationStatus(s).Domain()
}

func (s ClusterReservationStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	ReservationStatus(s).ApplyToDomainSpec(spec)
}

func ClusterReservationStatusFromDomain(
	s domain.SessionStatus,
	volumes []domain.VolumeSpec,
) ClusterReservationStatus {
	return ClusterReservationStatus(ReservationStatusFromDomain(s, volumes))
}

func (s ClusterCopySpec) Domain() domain.SessionSpec {
	return CopySpec(s).Domain()
}

func ClusterCopySpecFromDomain(s domain.SessionSpec) ClusterCopySpec {
	return ClusterCopySpec(CopySpecFromDomain(s))
}

func (s ClusterCopyStatus) Domain() domain.SessionStatus {
	return CopyStatus(s).Domain()
}

func (s ClusterCopyStatus) ApplyToDomainSpec(spec *domain.SessionSpec) {
	CopyStatus(s).ApplyToDomainSpec(spec)
}

func ClusterCopyStatusFromDomain(
	s domain.SessionStatus,
	volumes []domain.VolumeSpec,
) ClusterCopyStatus {
	return ClusterCopyStatus(CopyStatusFromDomain(s, volumes))
}

func (s ClusterBackupSpec) Domain() domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: domain.SessionCommon{
			SourceNamespace:  s.SourceNamespace,
			SessionNamespace: s.SessionNamespace,
			CreatedBy:        s.CreatedBy,
		},
		Type: domain.SessionTypeBackup,
		Backup: &domain.BackupSessionSpec{
			SourcePVC:                 refToDomain(s.SourcePVC),
			SourcePV:                  refToDomain(s.SourcePV),
			Path:                      s.Path,
			Name:                      s.Name,
			BackupRepository:          s.RepositoryRef.Name,
			BackupRepositoryNamespace: s.RepositoryRef.Namespace,
			Online:                    s.Online,
			OpenEBSLVMEnableShared:    s.OpenEBSLVMEnableShared,
		},
	}
}

func ClusterBackupSpecFromDomain(s domain.SessionSpec) ClusterBackupSpec {
	p := s.Backup
	if p == nil {
		p = &domain.BackupSessionSpec{}
	}

	return ClusterBackupSpec{
		SourceNamespace:  s.SourceNamespace,
		SessionNamespace: s.SessionNamespace,
		CreatedBy:        s.CreatedBy,
		SourcePVC: refFromDomain(
			p.SourcePVC,
		),
		SourcePV: refFromDomain(p.SourcePV),
		Path:     p.Path,
		Name:     p.Name,
		RepositoryRef: RepositoryReference{
			Name:      p.BackupRepository,
			Namespace: p.BackupRepositoryNamespace,
		},
		Online:                 p.Online,
		OpenEBSLVMEnableShared: p.OpenEBSLVMEnableShared,
		ToolImage:              p.ToolImage,
		DeleteExtraneous:       p.DeleteExtraneous,
	}
}

func (s ClusterBackupStatus) Domain() domain.SessionStatus {
	return BackupStatus(s).Domain()
}

func ClusterBackupStatusFromDomain(s domain.SessionStatus) ClusterBackupStatus {
	return ClusterBackupStatus(BackupStatusFromDomain(s))
}

func (s ClusterRestoreSpec) Domain() domain.SessionSpec {
	return domain.SessionSpec{
		SessionCommon: domain.SessionCommon{
			SourceNamespace:      s.DestinationNamespace,
			DestinationNamespace: s.DestinationNamespace,
			SessionNamespace:     s.SessionNamespace,
			CreatedBy:            s.CreatedBy,
		},
		Type: domain.SessionTypeRestore,
		Restore: &domain.RestoreSessionSpec{
			DestinationPVC: refToDomain(
				s.DestinationPVC,
			),
			Path:                      s.Path,
			Name:                      s.Name,
			BackupRepository:          s.RepositoryRef.Name,
			BackupRepositoryNamespace: s.RepositoryRef.Namespace,
			CreatePVC:                 s.CreatePVC,
			DestinationStorageClass:   s.DestinationStorageClass,
			DestinationAccessMode:     s.DestinationAccessMode,
			DestinationCapacity:       s.DestinationCapacity,
			AllowMounted:              s.AllowMounted,
		},
	}
}

func ClusterRestoreSpecFromDomain(s domain.SessionSpec) ClusterRestoreSpec {
	p := s.Restore
	if p == nil {
		p = &domain.RestoreSessionSpec{}
	}

	return ClusterRestoreSpec{
		DestinationNamespace: s.DestinationNamespace,
		SessionNamespace:     s.SessionNamespace,
		CreatedBy:            s.CreatedBy,
		DestinationPVC:       refFromDomain(p.DestinationPVC),
		Path:                 p.Path,
		Name:                 p.Name,
		RepositoryRef: RepositoryReference{
			Name:      p.BackupRepository,
			Namespace: p.BackupRepositoryNamespace,
		},
		CreatePVC:               p.CreatePVC,
		DestinationStorageClass: p.DestinationStorageClass,
		DestinationAccessMode:   p.DestinationAccessMode,
		DestinationCapacity:     p.DestinationCapacity,
		AllowMounted:            p.AllowMounted,
		TargetNode:              p.TargetNode,
		ToolImage:               p.ToolImage,
		DeleteExtraneous:        p.DeleteExtraneous,
	}
}

func (s ClusterRestoreStatus) Domain() domain.SessionStatus {
	return RestoreStatus(s).Domain()
}

func ClusterRestoreStatusFromDomain(s domain.SessionStatus) ClusterRestoreStatus {
	return ClusterRestoreStatus(RestoreStatusFromDomain(s))
}

func (s ClusterRenameSpec) Domain() domain.SessionSpec {
	return RenameSpec(s).Domain()
}

func ClusterRenameSpecFromDomain(s domain.SessionSpec) ClusterRenameSpec {
	return ClusterRenameSpec(RenameSpecFromDomain(s))
}

func (s ClusterRenameStatus) Domain() domain.SessionStatus {
	return RenameStatus(s).Domain()
}

func ClusterRenameStatusFromDomain(s domain.SessionStatus) ClusterRenameStatus {
	return ClusterRenameStatus(RenameStatusFromDomain(s))
}

func (s ClusterMoveSpec) Domain() domain.SessionSpec {
	return MoveSpec(s).Domain()
}

func ClusterMoveSpecFromDomain(s domain.SessionSpec) ClusterMoveSpec {
	return ClusterMoveSpec(MoveSpecFromDomain(s))
}

func (s ClusterMoveStatus) Domain() domain.SessionStatus {
	return MoveStatus(s).Domain()
}

func ClusterMoveStatusFromDomain(s domain.SessionStatus) ClusterMoveStatus {
	return ClusterMoveStatus(MoveStatusFromDomain(s))
}
