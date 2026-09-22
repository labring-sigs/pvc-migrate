package copyengine

import (
	"context"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

type Mode string

const (
	ModeWarm  Mode = "warm"
	ModeFinal Mode = "final"
)

// AttemptIdentity identifies one copy attempt across execution and recovery.
type AttemptIdentity struct {
	SessionID string
	Source    v1alpha1.ObjectReference
	Mode      Mode
	Attempt   int
}

// CopySource contains source-side transport settings. The source identity is
// part of AttemptIdentity because it also participates in the stable attempt
// ID used by cleanup and recovery.
type CopySource struct {
	KubeconfigPath string
	Context        string
	Path           string
	MountReadWrite bool
}

// CopyDestination contains the destination identity and transport settings.
type CopyDestination struct {
	Reference      v1alpha1.ObjectReference
	KubeconfigPath string
	Context        string
	Path           string
}

// CopyPolicy contains data convergence and retry behavior. It is independent
// of Kubernetes connection details and process output.
type CopyPolicy struct {
	Strategies            []string
	DeleteExtraneousFiles bool
	VerifyChecksum        bool
	IgnoreSizes           bool
	// Compress enables rsync/ssh compression for the transfer. Default
	// off; the upstream adapter inverts it into Migration.NoCompress.
	Compress bool
	// BandwidthLimit caps the rsync transfer rate in rsync --bwlimit syntax
	// (KiB/s for a bare number, or a K/M/G suffix). Empty is unlimited.
	BandwidthLimit          string
	RsyncMaxRetries         int
	TolerateLiveSourceChurn bool
}

// CopyRuntime contains the trusted tool and process-level execution options.
type CopyRuntime struct {
	ToolImage        string
	HelmTimeout      time.Duration
	HelmValues       []string
	HelmStringValues []string
	Writer           io.Writer
	Logger           *slog.Logger
}

// CleanupRequest contains release ownership and cluster locations only.
type CleanupRequest struct {
	AttemptIdentity
	DestinationNamespace string
	KubeconfigPath       string
	Context              string
	// Destination connection overrides for cross-cluster cleanup. Empty
	// values reuse the source connection.
	DestinationKubeconfigPath string
	DestinationContext        string
	Strategies                []string
}

// CopyRequest is one copy attempt assembled from focused identity, endpoint,
// policy, and runtime values.
type CopyRequest struct {
	AttemptIdentity
	Source      CopySource
	Destination CopyDestination
	Policy      CopyPolicy
	Runtime     CopyRuntime
}

type Progress struct {
	Mode    Mode   `json:"mode"              yaml:"mode"`
	Attempt int    `json:"attempt"           yaml:"attempt"`
	State   string `json:"state"             yaml:"state"`
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
	Bytes   int64  `json:"bytes,omitempty"   yaml:"bytes,omitempty"`
}

type ProgressFunc func(Progress)

type Engine interface {
	Copy(ctx context.Context, request CopyRequest, progress ProgressFunc) error
	Cleanup(ctx context.Context, request CleanupRequest) error
}
