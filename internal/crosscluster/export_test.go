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

func (s *Service) SaveForTest(ctx context.Context, session *Session, create bool) error {
	return s.save(ctx, session, create)
}

func (s *Service) CleanupDestinationVolumeForTest(
	ctx context.Context,
	session *Session,
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
	session *Session,
	volume *VolumeSpec,
) error {
	return s.createReservationConsumer(ctx, session, volume)
}
