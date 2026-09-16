package app

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (s *volumeCopyRunner) copyWithRetry(
	ctx context.Context,
	request copyengine.Request,
	sourceNode, targetNode, capacityRecovery string,
	attempts *int,
	lastError *string,
	probeResults []kube.ToolImageProbeResult,
	save func(context.Context) error,
	validate func(context.Context) error,
	sourceMountReadWrite func(context.Context) (bool, error),
) error {
	values, err := s.helmSchedulingValues(
		ctx,
		probedSourceNode(
			sourceNode,
			request.Source,
			probeResults,
		),
		targetNode,
		request.Strategies,
	)
	if err != nil {
		return err
	}

	pullSecretValues, err := kube.ToolImagePullSecretHelmValues(probeResults)
	if err != nil {
		return err
	}

	values = append(values, pullSecretValues...)

	// The upstream transfer chart does not expose PodSpec token automount. Use
	// a namespace-local, project-managed account whose automount setting is
	// explicitly disabled for sshd/rsync transfer Pods.
	seenNamespaces := map[string]struct{}{}
	for _, namespace := range []string{
		request.Source.Namespace,
		request.Destination.Namespace,
	} {
		if _, seen := seenNamespaces[namespace]; seen {
			continue
		}

		if err := kube.EnsureTransferServiceAccount(ctx, s.client, namespace); err != nil {
			return err
		}

		seenNamespaces[namespace] = struct{}{}
	}

	identityValues := kube.TransferServiceAccountHelmValues()
	values = append(values, identityValues.StringValues...)

	request.ToolImage = s.toolImage(request.ToolImage)
	request.KubeconfigPath = s.config.KubeconfigPath
	request.Context = s.config.Context
	request.NoCompress = s.config.NoCompress
	request.HelmTimeout = s.config.HelmTimeout
	request.Writer = s.config.Writer
	request.Logger = s.config.Logger
	request.Strategies = slices.Clone(request.Strategies)
	request.HelmValues = append(slices.Clone(request.HelmValues), identityValues.Values...)
	request.HelmStringValues = append(slices.Clone(request.HelmStringValues), values...)

	var last error

	for retryIndex := range s.config.Retries {
		if err := validate(ctx); err != nil {
			return err
		}

		mountReadWrite, err := sourceMountReadWrite(ctx)
		if err != nil {
			return err
		}

		previousAttempts, previousError := *attempts, *lastError
		*attempts++
		*lastError = ""

		if err := persistCheckpoint(ctx, save); err != nil {
			*attempts, *lastError = previousAttempts, previousError
			return err
		}

		request.Attempt = *attempts
		request.SourceMountReadWrite = mountReadWrite

		s.logInfo(
			"copy started",
			"session",
			request.SessionID,
			"pvc",
			request.Source.Name,
			"mode",
			request.Mode,
			"attempt",
			*attempts,
			"source",
			request.Source.Namespace+"/"+request.Source.Name,
			"sourcePath",
			request.SourcePath,
			"destination",
			request.Destination.Namespace+"/"+request.Destination.Name,
			"destinationPath",
			request.DestinationPath,
		)

		toolLogs := s.startCopyToolLogs(
			ctx,
			request.Source.Namespace,
			request.Destination.Namespace,
			copyengine.OperationID(request.AttemptIdentity),
		)
		attemptRequest := request
		attemptRequest.Strategies = slices.Clone(request.Strategies)
		attemptRequest.HelmValues = slices.Clone(request.HelmValues)
		attemptRequest.HelmStringValues = slices.Clone(request.HelmStringValues)

		// A per-attempt bound turns a hung transfer into a retryable failure
		// instead of burning the whole operation budget. Tool cleanup below
		// still runs on the operation context on purpose.
		attemptCtx, attemptCancel := ctx, func() {}
		if s.config.CopyTimeout > 0 {
			attemptCtx, attemptCancel = context.WithTimeout(ctx, s.config.CopyTimeout)
		}

		copyErr := s.copier.Copy(attemptCtx, attemptRequest, func(progress copyengine.Progress) {
			s.logInfo(
				"copy progress",
				"session",
				request.SessionID,
				"pvc",
				request.Source.Name,
				"mode",
				progress.Mode,
				"attempt",
				progress.Attempt,
				"state",
				progress.State,
				"message",
				progress.Message,
			)
		})

		toolLogs.Stop()
		copyErr = mergeToolLogError(copyErr, toolLogs.ObservedError())

		attemptCancel()

		if copyErr != nil && attemptCtx.Err() != nil && ctx.Err() == nil {
			copyErr = domain.WrapError(
				domain.ErrorTimeout,
				domain.ErrorOperationCopyAttempt,
				fmt.Sprintf(
					"copy attempt %d exceeded --copy-timeout %s",
					*attempts,
					s.config.CopyTimeout,
				),
				copyErr,
			)
		}

		s.logInfo(
			"waiting for copy tool Pods to release PVCs",
			"session",
			request.SessionID,
			"pvc",
			request.Source.Name,
		)

		operationID := copyengine.OperationID(request.AttemptIdentity)

		last = errors.Join(
			copyErr,
			s.cleanupCopyToolPods(ctx, request.Source, request.Destination, operationID),
		)
		if last == nil {
			return nil
		}

		previousError = *lastError

		*lastError = last.Error()
		if err := persistCheckpoint(ctx, save); err != nil {
			// Preserve the copy failure when the operation context was canceled;
			// failContext checkpoints the updated status with an independent context.
			if ctx.Err() != nil {
				return last
			}

			*lastError = previousError

			return err
		}

		if isDestinationNoSpaceError(last) {
			message := fmt.Sprintf(
				"destination PVC %s/%s ran out of space; abort and clean up this session, then create a new session with a larger --destination-capacity",
				request.Destination.Namespace,
				request.Destination.Name,
			)
			if capacityRecovery != "" {
				message = fmt.Sprintf(
					"destination PVC %s/%s ran out of space; %s",
					request.Destination.Namespace,
					request.Destination.Name,
					capacityRecovery,
				)
			}

			return domain.WrapError(
				domain.ErrorConflict,
				domain.ErrorOperationCopyCapacity,
				message,
				last,
			)
		}

		if retryIndex+1 < s.config.Retries {
			delay := time.Duration(math.Pow(2, float64(retryIndex))) * s.config.RetryBackoff
			s.logInfo(
				"copy retry scheduled",
				"session",
				request.SessionID,
				"pvc",
				request.Source.Name,
				"mode",
				request.Mode,
				"attempt",
				*attempts,
				"nextAttempt",
				*attempts+1,
				"backoff",
				delay,
				"error",
				last,
			)

			if err := s.sleep(ctx, delay); err != nil {
				return domain.WrapError(
					domain.ErrorTimeout,
					"copy retry",
					"context ended during retry backoff",
					err,
				)
			}
		}
	}

	return last
}
