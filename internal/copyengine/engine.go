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

// Request is one copy attempt: identity, transfer target, transport policy,
// and tool runtime. Presentation (Writer/Logger) stays with the runner that
// owns the process output.
type Request struct {
	AttemptIdentity
	ToolImage       string
	Destination     v1alpha1.ObjectReference
	SourcePath      string
	DestinationPath string
	KubeconfigPath  string
	Context         string
	// Destination connection overrides for cross-cluster transfer. Empty
	// values reuse the source connection.
	DestinationKubeconfigPath string
	DestinationContext        string
	Strategies                []string
	DeleteExtraneousFiles     bool
	VerifyChecksum            bool
	SourceMountReadWrite      bool
	IgnoreSizes               bool
	NoCompress                bool
	// TolerateLiveSourceChurn marks a warm-copy pass whose destination will
	// be converged by a later paused final sync. With it set, a transfer that
	// fails because the live source rewrote files mid-pass (rsync code 23)
	// is treated as done: the pre-copy is best effort by definition. Never
	// set it for final-sync or standalone copies, which must be exact.
	TolerateLiveSourceChurn bool
	HelmTimeout               time.Duration
	HelmValues                []string
	HelmStringValues          []string
	Writer                    io.Writer
	Logger                    *slog.Logger
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
	Copy(ctx context.Context, request Request, progress ProgressFunc) error
	Cleanup(ctx context.Context, request CleanupRequest) error
}
