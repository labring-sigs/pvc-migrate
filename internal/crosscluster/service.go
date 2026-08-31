package crosscluster

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

type Options struct {
	SessionID               string
	SessionNamespace        string
	SourceNamespace         string
	DestinationNamespace    string
	SourcePVCs              []string
	DestinationPVCs         []string
	DestinationCapacities   []string
	SourcePaths             []string
	DestinationPaths        []string
	DestinationStorageClass string
	AllowVolumeShrink       bool
	SkipSourceUsageCheck    bool
	Online                  bool
	VerifyChecksum          bool
	DeleteExtraneous        bool
	TargetNode              string
	ToolImage               string
	Strategies              []string
}

type Service struct {
	source                                    *kube.Clients
	destination                               *kube.Clients
	copier                                    copyengine.Engine
	sourceKubeconfig, sourceContext           string
	destinationKubeconfig, destinationContext string
	now                                       func() time.Time
	interval                                  time.Duration
	helmTimeout                               time.Duration
	writer                                    io.Writer
	logger                                    *slog.Logger
	store                                     kube.SessionLocker
}

func NewService(source, destination *kube.Clients, copier copyengine.Engine) *Service {
	service := &Service{
		source:      source,
		destination: destination,
		copier:      copier,
		now:         time.Now,
		interval:    time.Second,
		helmTimeout: 10 * time.Minute,
		writer:      kube.NewSynchronizedWriter(io.Discard),
		logger:      slog.Default(),
	}
	if source != nil {
		service.store = kube.NewConfigMapSessionStore(source.Kubernetes)
	}

	return service
}

func (s *Service) WithRuntime(
	writer io.Writer,
	logger *slog.Logger,
	helmTimeout time.Duration,
) *Service {
	if writer != nil {
		s.writer = kube.NewSynchronizedWriter(writer)
	}

	if logger != nil {
		s.logger = logger
	}

	if helmTimeout > 0 {
		s.helmTimeout = helmTimeout
	}

	return s
}

func (s *Service) WithConnections(
	sourceKubeconfig, sourceContext, destinationKubeconfig, destinationContext string,
) *Service {
	s.sourceKubeconfig, s.sourceContext = sourceKubeconfig, sourceContext
	s.destinationKubeconfig, s.destinationContext = destinationKubeconfig, destinationContext
	return s
}

// clusterIdentities reads both API endpoints concurrently. The two reads are
// independent, while returning source errors first preserves the historical
// error precedence for callers and diagnostics.
func (s *Service) clusterIdentities(
	ctx context.Context,
) (source, destination kube.ClusterIdentity, err error) {
	var (
		sourceErr      error
		destinationErr error
		wg             sync.WaitGroup
	)

	wg.Go(func() {
		source, sourceErr = kube.Identity(ctx, s.source)
	})
	wg.Go(func() {
		destination, destinationErr = kube.Identity(ctx, s.destination)
	})
	wg.Wait()

	if sourceErr != nil {
		return kube.ClusterIdentity{}, kube.ClusterIdentity{}, sourceErr
	}

	if destinationErr != nil {
		return kube.ClusterIdentity{}, kube.ClusterIdentity{}, destinationErr
	}

	return source, destination, nil
}
