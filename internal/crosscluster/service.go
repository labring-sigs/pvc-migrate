package crosscluster

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// CopyOptions describes the complete cross-cluster copy workflow. Reservation
// uses its own input type below so copy-only consistency flags cannot leak into
// the reservation command.
type CopyOptions struct {
	UnusedStoragePolicy     v1alpha1.UnusedStoragePolicy
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

// ReservationOptions describes the destination reservation workflow. A
// reservation does not accept copy consistency or online transfer controls.
type ReservationOptions struct {
	UnusedStoragePolicy     v1alpha1.UnusedStoragePolicy
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
	TargetNode              string
	ToolImage               string
	Strategies              []string
}

// Runtime configures the submitting process for both cross-cluster workflows.
type Runtime struct {
	Writer            io.Writer
	Logger            *slog.Logger
	HelmTimeout       time.Duration
	ToolNetworkPolicy string
	CopyTimeout       time.Duration
	RetryBackoff      time.Duration
	StreamToolLogs    bool
	StructuredLogs    bool
}

type Service struct {
	source                                    *kube.Clients
	destination                               *kube.Clients
	copier                                    copyengine.Engine
	sourceKubeconfig, sourceContext           string
	destinationKubeconfig, destinationContext string
	now                                       func() time.Time
	interval                                  time.Duration
	runtime                                   Runtime
	locker                                    kube.SessionLocker
	leases                                    *kube.CRDWorkflowLeaseCleaner
}

func NewService(source, destination *kube.Clients, copier copyengine.Engine) *Service {
	service := &Service{
		source:      source,
		destination: destination,
		copier:      copier,
		now:         time.Now,
		interval:    time.Second,
		runtime: Runtime{
			HelmTimeout: 10 * time.Minute, RetryBackoff: 2 * time.Second,
			Writer: kube.NewSynchronizedWriter(io.Discard), Logger: slog.Default(),
		},
	}
	if source != nil {
		service.locker = kube.NewConfigMapWorkflowLocker(source.Kubernetes)
		service.leases = kube.NewCRDWorkflowLeaseCleaner(source.Kubernetes)
	}

	return service
}

func (s *Service) WithRuntime(options Runtime) *Service {
	if options.HelmTimeout <= 0 {
		options.HelmTimeout = s.runtime.HelmTimeout
	}

	if options.RetryBackoff <= 0 {
		options.RetryBackoff = s.runtime.RetryBackoff
	}

	if options.Logger == nil {
		options.Logger = s.runtime.Logger
	}

	options.Writer = kube.NewSynchronizedWriter(options.Writer)
	s.runtime = options

	return s
}

func (s *Service) toolLogOptions(cluster string) kube.ToolLogOptions {
	return kube.ToolLogOptions{
		Cluster:    cluster,
		Writer:     s.runtime.Writer,
		Logger:     s.runtime.Logger,
		Structured: s.runtime.StructuredLogs,
	}
}

func (s *Service) waitForDestination(
	ctx context.Context,
	description string,
	condition func(context.Context) (bool, error),
) error {
	s.runtime.Logger.InfoContext(
		ctx,
		"waiting for Kubernetes resource",
		"cluster",
		"destination",
		"description",
		description,
	)

	return kube.WaitFor(ctx, s.interval, description, condition)
}

func (s *Service) WithConnections(
	sourceKubeconfig, sourceContext, destinationKubeconfig, destinationContext string,
) *Service {
	s.sourceKubeconfig, s.sourceContext = sourceKubeconfig, sourceContext
	s.destinationKubeconfig, s.destinationContext = destinationKubeconfig, destinationContext
	return s
}

// clusterIdentities reads both API endpoints concurrently. The two reads are
// independent; source errors are returned first for deterministic diagnostics.
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
