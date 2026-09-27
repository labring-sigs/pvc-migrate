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

// retryBackoffDelay returns the exponential backoff before the next copy
// attempt. The multiple saturates instead of overflowing: a float-to-int
// conversion of 2^63 wraps negative and turned late retries into a tight
// loop, and even a correct 2^63 multiple overflows against hour-scale bases.
func retryBackoffDelay(base time.Duration, retryIndex int) time.Duration {
	if base <= 0 {
		return 0
	}

	multiple := uint64(1) << uint(min(retryIndex, 62))
	if uint64(base) > math.MaxInt64/multiple {
		return time.Duration(math.MaxInt64)
	}

	// The guard above bounds the product to math.MaxInt64.
	return base * time.Duration(multiple) //nolint:gosec // bounded by the saturation guard
}

func (s *volumeCopyRunner) copyWithRetry(
	ctx context.Context,
	request copyengine.CopyRequest,
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
			request.AttemptIdentity.Source,
			probeResults,
		),
		targetNode,
		request.Policy.Strategies,
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
		request.AttemptIdentity.Source.Namespace,
		request.Destination.Reference.Namespace,
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

	request.Runtime.ToolImage = s.toolImage(request.Runtime.ToolImage)
	request.Source.KubeconfigPath = s.config.KubeconfigPath
	request.Source.Context = s.config.Context
	request.Policy.Compress = s.config.Compress
	request.Runtime.HelmTimeout = s.config.HelmTimeout
	request.Runtime.Writer = s.config.Writer
	request.Runtime.Logger = s.config.Logger
	request.Policy.Strategies = slices.Clone(request.Policy.Strategies)
	request.Runtime.HelmValues = append(
		slices.Clone(request.Runtime.HelmValues),
		identityValues.Values...,
	)
	request.Runtime.HelmStringValues = append(
		slices.Clone(request.Runtime.HelmStringValues),
		values...,
	)

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
		request.Source.MountReadWrite = mountReadWrite
		request.Policy.RsyncMaxRetries = s.config.RsyncMaxRetries
		request.Policy.BandwidthLimit = s.config.BandwidthLimit

		s.logInfo(
			"copy started",
			"session",
			request.SessionID,
			"pvc",
			request.AttemptIdentity.Source.Name,
			"mode",
			request.Mode,
			"attempt",
			*attempts,
			"source",
			request.AttemptIdentity.Source.Namespace+"/"+request.AttemptIdentity.Source.Name,
			"sourcePath",
			request.Source.Path,
			"destination",
			request.Destination.Reference.Namespace+"/"+request.Destination.Reference.Name,
			"destinationPath",
			request.Destination.Path,
		)

		toolLogs := s.startCopyToolLogs(
			ctx,
			request.AttemptIdentity.Source.Namespace,
			request.Destination.Reference.Namespace,
			copyengine.OperationID(request.AttemptIdentity),
		)
		attemptRequest := request
		attemptRequest.Policy.Strategies = slices.Clone(request.Policy.Strategies)
		attemptRequest.Runtime.HelmValues = slices.Clone(request.Runtime.HelmValues)
		attemptRequest.Runtime.HelmStringValues = slices.Clone(request.Runtime.HelmStringValues)

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
				request.AttemptIdentity.Source.Name,
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
			request.AttemptIdentity.Source.Name,
		)

		operationID := copyengine.OperationID(request.AttemptIdentity)

		last = errors.Join(
			copyErr,
			s.cleanupCopyToolPods(
				ctx,
				request.AttemptIdentity.Source,
				request.Destination.Reference,
				operationID,
			),
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
				request.Destination.Reference.Namespace,
				request.Destination.Reference.Name,
			)
			if capacityRecovery != "" {
				message = fmt.Sprintf(
					"destination PVC %s/%s ran out of space; %s",
					request.Destination.Reference.Namespace,
					request.Destination.Reference.Name,
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
			delay := retryBackoffDelay(s.config.RetryBackoff, retryIndex)
			s.logInfo(
				"copy retry scheduled",
				"session",
				request.SessionID,
				"pvc",
				request.AttemptIdentity.Source.Name,
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
