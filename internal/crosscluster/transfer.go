package crosscluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/client-go/kubernetes"
)

// A recorded attempt may still be running after the previous process exited.
func (s *Service) copyVolume(
	ctx context.Context,
	session *CopySession,
	index int,
	req copyengine.CopyRequest,
	retries int,
	recovering bool,
) error {
	status := &session.Status.Volumes[index].Transfer
	if recovering || status.Attempts > 0 {
		req.Attempt = status.Attempts
		if err := s.waitForTransferRelease(ctx, req, true); err != nil {
			return err
		}
	}

	for retry := range retries {
		if err := requireSessionLease(ctx); err != nil {
			return err
		}

		if err := s.validateTransferVolume(ctx, session, index); err != nil {
			return err
		}

		previous := *status
		status.Attempts++
		status.LastError = ""
		req.Attempt = status.Attempts

		s.touch(session)

		if err := s.save(ctx, session, false); err != nil {
			*status = previous
			return err
		}

		if err := requireSessionLease(ctx); err != nil {
			return err
		}

		err := s.runAttempt(ctx, req)
		if fenceErr := requireSessionLease(ctx); fenceErr != nil {
			return errors.Join(err, fenceErr)
		}

		cleanupErr := s.waitForTransferRelease(ctx, req, err != nil)
		if err == nil {
			return cleanupErr
		}

		if copyengine.IsDestinationNoSpaceError(err) {
			err = domain.WrapError(
				domain.ErrorConflict,
				domain.ErrorOperationCopyCapacity,
				fmt.Sprintf(
					"destination PVC %s/%s ran out of space",
					req.Destination.Reference.Namespace,
					req.Destination.Reference.Name,
				),
				err,
			)

			return errors.Join(err, cleanupErr)
		}

		if cleanupErr != nil || retry+1 == retries {
			return errors.Join(err, cleanupErr)
		}

		status.LastError = err.Error()

		s.touch(session)

		if saveErr := s.save(ctx, session, false); saveErr != nil {
			return errors.Join(err, saveErr)
		}

		delay := copyengine.RetryBackoffDelay(s.runtime.RetryBackoff, retry)
		s.runtime.Logger.InfoContext(
			ctx,
			"copy retry scheduled",
			"session",
			session.ID,
			"pvc",
			req.AttemptIdentity.Source.Name,
			"attempt",
			req.Attempt,
			"nextAttempt",
			req.Attempt+1,
			"backoff",
			delay,
			"error",
			err,
		)

		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(delay):
		}
	}

	return nil
}

func (s *Service) runAttempt(ctx context.Context, req copyengine.CopyRequest) error {
	if !s.runtime.StreamToolLogs {
		return copyengine.RunAttempt(ctx, s.copier, req, s.runtime.CopyTimeout, nil)
	}

	op := copyengine.OperationID(req.AttemptIdentity)
	start := func(side string, client kubernetes.Interface, namespace string) *kube.ToolLogStream {
		options := s.toolLogOptions(side)
		options.OperationID = op
		options.Namespaces = []string{namespace}
		return kube.StartPVMigrateToolLogs(ctx, client, options)
	}

	source := start("source", s.source.Kubernetes, req.AttemptIdentity.Source.Namespace)
	defer source.Stop()
	// Only destination log evidence can attribute a generic engine failure to a full destination.
	destination := start(
		"destination",
		s.destination.Kubernetes,
		req.Destination.Reference.Namespace,
	)

	return copyengine.RunAttempt(ctx, s.copier, req, s.runtime.CopyTimeout, destination)
}

// An upstream uninstall can return while Kubernetes is still removing Pods.
// After failure or interruption also check controllers that could restart them.
// The destination claim has one writer, while an online source can have other readers.
func (s *Service) waitForTransferRelease(
	ctx context.Context,
	req copyengine.CopyRequest,
	checkControllers bool,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	wait := func(side string, client kubernetes.Interface, ns, claim string, check kube.TransferToolCheck) error {
		description := fmt.Sprintf("transfer tools to release %s PVC %s/%s", side, ns, claim)

		var blocking error

		err := kube.WaitFor(
			waitCtx,
			s.interval,
			description,
			func(ctx context.Context) (bool, error) {
				err := check.RequireStopped(ctx, client, ns, claim)
				if errors.Is(err, kube.ErrTransferToolStillExists) {
					if blocking == nil {
						s.runtime.Logger.InfoContext(
							ctx,
							"waiting for copy tools to release PVC",
							"cluster",
							side,
							"namespace",
							ns,
							"pvc",
							claim,
							"detail",
							err,
						)
					}

					blocking = err

					return false, nil
				}

				return err == nil, err
			},
		)
		if err != nil {
			return fmt.Errorf("%s: %w", description, errors.Join(err, blocking))
		}

		return nil
	}
	if req.Attempt > 0 {
		src := req.AttemptIdentity.Source

		check := kube.TransferToolCheck{
			OperationID: copyengine.OperationID(req.AttemptIdentity),
			Deployments: checkControllers,
		}
		if err := wait("source", s.source.Kubernetes, src.Namespace, src.Name, check); err != nil {
			return err
		}
	}

	dst := req.Destination.Reference

	return wait(
		"destination",
		s.destination.Kubernetes,
		dst.Namespace,
		dst.Name,
		kube.TransferToolCheck{
			Deployments: checkControllers &&
				slices.Contains(req.Policy.Strategies, domain.StrategyLocal),
			Jobs: checkControllers &&
				slices.ContainsFunc(
					req.Policy.Strategies,
					func(strategy string) bool { return strategy != domain.StrategyLocal },
				),
		},
	)
}
