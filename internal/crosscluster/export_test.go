package crosscluster

import (
	"context"

	"k8s.io/client-go/kubernetes"
)

func (s *Service) DestinationClientForTest() kubernetes.Interface {
	return s.destination.Kubernetes
}

func (s *Service) SourceClientForTest() kubernetes.Interface {
	return s.source.Kubernetes
}

func (s *Service) SaveForTest(ctx context.Context, session *CopySession, create bool) error {
	return s.save(ctx, session, create)
}

func (s *Service) DeleteSessionRecordForTest(ctx context.Context, session *CopySession) error {
	return s.delete(ctx, session)
}

func (s *Service) DeleteReservationRecordForTest(
	ctx context.Context,
	session *ReservationSession,
) error {
	return s.deleteReservation(ctx, session)
}

func (s *Service) CleanupDestinationVolumeForTest(
	ctx context.Context,
	session *CopySession,
	index int,
) error {
	return s.cleanupDestinationVolume(ctx, session, index)
}

func ResolveNamesForTest(values, source []string) ([]string, error) {
	return resolveNames(values, source)
}

func ResolveValuesForTest(values, source []string) ([]string, error) {
	return resolveValues(values, source)
}

func ResolvePathsForTest(values, source []string) ([]string, error) {
	return resolvePaths(values, source)
}

func ReservationConsumerNameForTest(sessionID, pvc string) string {
	return reservationConsumerName(sessionID, pvc)
}

func (s *Service) CreateReservationConsumerForTest(
	ctx context.Context,
	session *CopySession,
	volume *VolumeSpec,
) error {
	index := volumeIndexForTest(session, volume.Source.PVC.Name)

	return s.createReservationConsumer(ctx, reservationState{
		ID:     session.ID,
		Spec:   &session.Spec.SessionContext,
		Status: &session.Status.Volumes[index].Reservation,
	}, volume)
}

func volumeIndexForTest(session *CopySession, name string) int {
	for i, volume := range session.Spec.Volumes {
		if volume.Source.PVC.Name == name {
			return i
		}
	}

	return 0
}
