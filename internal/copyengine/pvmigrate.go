package copyengine

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

type PVMigrate struct {
	run func(context.Context, pvmigrate.Migration) error
}

func NewPVMigrate() *PVMigrate { return &PVMigrate{run: pvmigrate.Run} }

func (p *PVMigrate) Copy(ctx context.Context, request Request, progress ProgressFunc) error {
	strategies := make([]pvmigrate.Strategy, 0, len(request.Strategies))
	for _, strategy := range request.Strategies {
		converted, err := strategyValue(strategy)
		if err != nil {
			return err
		}
		strategies = append(strategies, converted)
	}
	rsyncArgs := "-HAXS --numeric-ids"
	if request.VerifyChecksum {
		rsyncArgs += " --checksum"
	}
	operationID := OperationID(request)
	imageValues, err := kube.ToolImageHelmValues(request.ToolImage)
	if err != nil {
		return err
	}
	if progress != nil {
		progress(Progress{Mode: request.Mode, Attempt: request.Attempt, State: "running", Message: operationID})
	}
	helmValues := kube.ToolSecurityContextHelmValues()
	if request.IgnoreSizes {
		// A smaller destination can deterministically exhaust its filesystem.
		// Let the session-level retry policy handle transient failures so an
		// ENOSPC result is surfaced without repeated in-Job attempts.
		helmValues = append(helmValues, "rsync.maxRetries=0")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	detector := &destinationNoSpaceDetector{cancel: cancel}
	migration := pvmigrate.Migration{
		ID: operationID,
		Source: pvmigrate.PVC{
			KubeconfigPath: request.KubeconfigPath,
			Context:        request.Context,
			Namespace:      request.Source.Namespace,
			Name:           request.Source.Name,
			Path:           transferEnginePath(request.SourcePath),
		},
		Dest: pvmigrate.PVC{
			KubeconfigPath: request.KubeconfigPath,
			Context:        request.Context,
			Namespace:      request.Destination.Namespace,
			Name:           request.Destination.Name,
			Path:           transferEnginePath(request.DestinationPath),
		},
		DeleteExtraneousFiles: request.DeleteExtraneousFiles,
		IgnoreMounted:         request.Mode == ModeWarm,
		SourceMountReadWrite:  request.SourceMountReadWrite,
		NoCompress:            request.NoCompress,
		NoCleanupOnFailure:    false,
		IgnoreSizes:           request.IgnoreSizes,
		ShowProgressBar:       false,
		RsyncExtraArgs:        rsyncArgs,
		Strategies:            strategies,
		HelmTimeout:           request.HelmTimeout,
		HelmValues:            helmValues,
		HelmStringValues:      append(imageValues, request.HelmStringValues...),
		Writer:                request.Writer,
		Logger:                loggerWithDestinationNoSpaceDetection(request.Logger, detector),
		StructuredLogs:        true,
	}
	if migration.HelmTimeout == 0 {
		migration.HelmTimeout = 10 * time.Minute
	}
	run := p.run
	if run == nil {
		run = pvmigrate.Run
	}
	if err := run(runCtx, migration); err != nil {
		classified := classifyRunError(ctx, operationID, err, detector.Detected())
		if progress != nil {
			progress(Progress{Mode: request.Mode, Attempt: request.Attempt, State: "failed", Message: classified.Error()})
		}
		return classified
	}
	if progress != nil {
		progress(Progress{Mode: request.Mode, Attempt: request.Attempt, State: "completed", Message: operationID})
	}
	return nil
}

func transferEnginePath(value string) string {
	if value == "" || value == domain.VolumeRootPath {
		return ""
	}
	return value + "/."
}

func classifyRunError(ctx context.Context, operationID string, err error, destinationNoSpace bool) error {
	message := fmt.Sprintf("pv-migrate operation %s failed", operationID)
	if destinationNoSpace {
		return domain.WrapError(domain.ErrorCopy, "copy PVC", message+": destination volume ran out of space (ENOSPC)", &destinationNoSpaceError{cause: err})
	}
	contextErr := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded) {
		return domain.WrapError(domain.ErrorTimeout, "copy PVC", message+": deadline exceeded", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled) {
		return domain.WrapError(domain.ErrorTimeout, "copy PVC", message+": canceled", err)
	}
	return domain.WrapError(domain.ErrorCopy, "copy PVC", message, err)
}

type destinationNoSpaceError struct {
	cause error
}

func (e *destinationNoSpaceError) Error() string {
	return "no space left on destination device (ENOSPC): " + e.cause.Error()
}

func (e *destinationNoSpaceError) Unwrap() error { return e.cause }

// IsDestinationNoSpaceError reports an ENOSPC failure observed in the data
// mover logs. It does not infer capacity exhaustion from rsync exit codes.
func IsDestinationNoSpaceError(err error) bool {
	var target *destinationNoSpaceError
	return errors.As(err, &target)
}

type destinationNoSpaceDetector struct {
	detected atomic.Bool
	cancel   context.CancelFunc
}

func (d *destinationNoSpaceDetector) Observe(value string) {
	if d == nil || !containsDestinationNoSpace(value) || !d.detected.CompareAndSwap(false, true) {
		return
	}
	if d.cancel != nil {
		d.cancel()
	}
}

func (d *destinationNoSpaceDetector) Detected() bool {
	return d != nil && d.detected.Load()
}

func containsDestinationNoSpace(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "no space left on device") || strings.Contains(lower, "enospc")
}

type destinationNoSpaceHandler struct {
	delegate slog.Handler
	detector *destinationNoSpaceDetector
}

func loggerWithDestinationNoSpaceDetection(logger *slog.Logger, detector *destinationNoSpaceDetector) *slog.Logger {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return slog.New(&destinationNoSpaceHandler{delegate: logger.Handler(), detector: detector})
}

func (h *destinationNoSpaceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	// Observe warnings even when the configured logger discards that level.
	return level >= slog.LevelWarn || h.delegate.Enabled(ctx, level)
}

func (h *destinationNoSpaceHandler) Handle(ctx context.Context, record slog.Record) error {
	h.detector.Observe(record.Message)
	record.Attrs(func(attr slog.Attr) bool {
		h.observeAttr(attr)
		return true
	})
	if h.detector.Detected() {
		record.Message = strings.Replace(record.Message, ", will try with the remaining strategies", "; destination capacity is exhausted, stopping strategy attempts", 1)
	}
	if !h.delegate.Enabled(ctx, record.Level) {
		return nil
	}
	return h.delegate.Handle(ctx, record)
}

func (h *destinationNoSpaceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	for _, attr := range attrs {
		h.observeAttr(attr)
	}
	return &destinationNoSpaceHandler{delegate: h.delegate.WithAttrs(attrs), detector: h.detector}
}

func (h *destinationNoSpaceHandler) WithGroup(name string) slog.Handler {
	return &destinationNoSpaceHandler{delegate: h.delegate.WithGroup(name), detector: h.detector}
}

func (h *destinationNoSpaceHandler) observeAttr(attr slog.Attr) {
	value := attr.Value.Resolve()
	if value.Kind() == slog.KindGroup {
		for _, child := range value.Group() {
			h.observeAttr(child)
		}
		return
	}
	if attr.Key == "error" || attr.Key == "tail" {
		h.detector.Observe(value.String())
	}
}

func strategyValue(value string) (pvmigrate.Strategy, error) {
	switch value {
	case string(pvmigrate.Mount):
		return pvmigrate.Mount, nil
	case string(pvmigrate.ClusterIP):
		return pvmigrate.ClusterIP, nil
	case string(pvmigrate.LoadBalancer):
		return pvmigrate.LoadBalancer, nil
	case string(pvmigrate.NodePort):
		return pvmigrate.NodePort, nil
	case string(pvmigrate.Local):
		return pvmigrate.Local, nil
	default:
		return "", domain.NewError(domain.ErrorValidation, "copy strategy", fmt.Sprintf("unsupported strategy %q", value))
	}
}

// OperationID returns the stable upstream operation identity used in Helm
// release names and tool Pod labels.
func OperationID(request Request) string {
	value := fmt.Sprintf("%s/%s/%s/%s/%d", request.SessionID, request.Source.Namespace, request.Source.Name, request.Mode, request.Attempt)
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("pm-%x", digest[:8])
}
