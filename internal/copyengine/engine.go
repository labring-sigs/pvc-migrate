package copyengine

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

type Mode string

const (
	ModeWarm  Mode = "warm"
	ModeFinal Mode = "final"
)

type Request struct {
	SessionID                 string
	ToolImage                 string
	Source                    domain.ObjectReference
	Destination               domain.ObjectReference
	SourcePath                string
	DestinationPath           string
	Mode                      Mode
	Attempt                   int
	KubeconfigPath            string
	Context                   string
	DestinationKubeconfigPath string
	DestinationContext        string
	Strategies                []string
	DeleteExtraneousFiles     bool
	VerifyChecksum            bool
	SourceMountReadWrite      bool
	IgnoreSizes               bool
	NoCompress                bool
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
}
