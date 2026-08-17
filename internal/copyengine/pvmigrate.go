package copyengine

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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
	migration := pvmigrate.Migration{
		ID: operationID,
		Source: pvmigrate.PVC{
			KubeconfigPath: request.KubeconfigPath,
			Context:        request.Context,
			Namespace:      request.Source.Namespace,
			Name:           request.Source.Name,
		},
		Dest: pvmigrate.PVC{
			KubeconfigPath: request.KubeconfigPath,
			Context:        request.Context,
			Namespace:      request.Destination.Namespace,
			Name:           request.Destination.Name,
		},
		DeleteExtraneousFiles: request.DeleteExtraneousFiles,
		IgnoreMounted:         request.Mode == ModeWarm,
		SourceMountReadWrite:  request.SourceMountReadWrite,
		NoCompress:            request.NoCompress,
		NoCleanupOnFailure:    false,
		ShowProgressBar:       false,
		RsyncExtraArgs:        rsyncArgs,
		Strategies:            strategies,
		HelmTimeout:           request.HelmTimeout,
		HelmValues:            kube.ToolSecurityContextHelmValues(),
		HelmStringValues:      append(imageValues, request.HelmStringValues...),
		Writer:                request.Writer,
		Logger:                request.Logger,
		StructuredLogs:        true,
	}
	if migration.HelmTimeout == 0 {
		migration.HelmTimeout = 10 * time.Minute
	}
	run := p.run
	if run == nil {
		run = pvmigrate.Run
	}
	if err := run(ctx, migration); err != nil {
		if progress != nil {
			progress(Progress{Mode: request.Mode, Attempt: request.Attempt, State: "failed", Message: err.Error()})
		}
		return classifyRunError(ctx, operationID, err)
	}
	if progress != nil {
		progress(Progress{Mode: request.Mode, Attempt: request.Attempt, State: "completed", Message: operationID})
	}
	return nil
}

func classifyRunError(ctx context.Context, operationID string, err error) error {
	message := fmt.Sprintf("pv-migrate operation %s failed", operationID)
	contextErr := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded) {
		return domain.WrapError(domain.ErrorTimeout, "copy PVC", message+": deadline exceeded", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled) {
		return domain.WrapError(domain.ErrorTimeout, "copy PVC", message+": canceled", err)
	}
	return domain.WrapError(domain.ErrorCopy, "copy PVC", message, err)
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
