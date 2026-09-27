package backup

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	restoreLockAnnotation       = kube.MetadataDomain + "/backup-restore-lock"
	restoreLockExpiryAnnotation = kube.MetadataDomain + "/backup-restore-lock-expires-at"
)

func restoreToolProbeTarget(
	destination v1alpha1.ObjectReference,
	path string,
	nodeName string,
) kube.ToolProbeTarget {
	pvcName := ""
	if nodeName == "" || path != "" {
		pvcName = destination.Name
	}

	return kube.ToolProbeTarget{
		Namespace: destination.Namespace, NodeName: nodeName, PVCName: pvcName,
		RequiredPath: path, CreatePath: path != "",
		Components: []string{kube.ToolComponentRclone},
	}
}

func acquireRestoreLock(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, holder string,
	ttl time.Duration,
	expectedPVCUID string,
) (func(context.Context) error, string, error) {
	if expectedPVCUID == "" {
		return nil, "", domain.NewError(
			domain.ErrorPrecondition,
			restoreLockPhase,
			"expected PVC identity is required",
		)
	}

	annotation := restoreLockAnnotation

	var originalUID string

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		originalUID = string(pvc.UID)
		if originalUID != expectedPVCUID {
			return domain.NewError(
				domain.ErrorConflict,
				restoreLockPhase,
				"destination PVC identity changed since preflight",
			)
		}

		if owner := pvc.Annotations[annotation]; owner != "" && owner != holder {
			expiresAt, parseErr := time.Parse(
				time.RFC3339Nano,
				pvc.Annotations[restoreLockExpiryAnnotation],
			)
			if parseErr != nil || time.Now().UTC().Before(expiresAt) {
				return domain.NewError(
					domain.ErrorConflict,
					restoreLockPhase,
					"PVC is locked by "+owner,
				)
			}
		}

		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}

		pvc.Annotations[annotation] = holder
		pvc.Annotations[restoreLockExpiryAnnotation] = time.Now().
			UTC().
			Add(ttl).
			Format(time.RFC3339Nano)
		_, err = client.CoreV1().
			PersistentVolumeClaims(namespace).
			Update(ctx, pvc, metav1.UpdateOptions{})

		return err
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
			errors.Is(ctx.Err(), context.DeadlineExceeded) ||
			errors.Is(ctx.Err(), context.Canceled) {
			return nil, "", domain.WrapError(
				domain.ErrorTimeout,
				restoreLockPhase,
				"acquire PVC restore lock timed out",
				err,
			)
		}

		if _, ok := errors.AsType[*domain.Error](err); ok {
			return nil, "", err
		}

		if apierrors.IsConflict(err) {
			return nil, "", domain.WrapError(
				domain.ErrorConflict,
				restoreLockPhase,
				"PVC changed while acquiring lock",
				err,
			)
		}

		return nil, "", domain.WrapError(
			domain.ErrorKubernetes,
			restoreLockPhase,
			"acquire PVC lock",
			err,
		)
	}

	return func(releaseCtx context.Context) error {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			pvc, err := client.CoreV1().
				PersistentVolumeClaims(namespace).
				Get(releaseCtx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return nil
			}

			if err != nil {
				return err
			}

			if string(pvc.UID) != originalUID || pvc.Annotations[annotation] != holder {
				return nil
			}

			delete(pvc.Annotations, annotation)
			delete(pvc.Annotations, restoreLockExpiryAnnotation)
			_, err = client.CoreV1().
				PersistentVolumeClaims(namespace).
				Update(releaseCtx, pvc, metav1.UpdateOptions{})

			return err
		})
	}, originalUID, nil
}

func renewRestoreLock(
	ctx context.Context,
	cancel context.CancelFunc,
	client kubernetes.Interface,
	namespace, name, holder, pvcUID string,
	ttl time.Duration,
	leaseErrors chan<- error,
	done chan<- struct{},
) {
	defer close(done)

	interval := max(ttl/3, 30*time.Second)

	interval = min(interval, time.Minute)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Ownership loss (a rewritten annotation or a replaced PVC) aborts
	// immediately; transient API errors retry inside the TTL budget, so one
	// API-server blip cannot abandon a healthy restore. Past half the TTL a
	// successor may legally hold the lock, so writing on would be unsafe.
	lastRenewed := time.Now()

	const retryInterval = 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := renewRestoreLockOnce(ctx, client, namespace, name, holder, pvcUID, ttl)
			if err == nil {
				lastRenewed = time.Now()

				ticker.Reset(interval)

				continue
			}

			if lockRenewalAborts(err, lastRenewed, ttl) {
				select {
				case leaseErrors <- classifyRestoreLockError(ctx, err):
				default:
				}

				cancel()

				return
			}

			ticker.Reset(retryInterval)
		}
	}
}

func renewRestoreLockOnce(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, holder, pvcUID string,
	ttl time.Duration,
) error {
	if pvcUID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			restoreLockPhase,
			"PVC identity is required for lock renewal",
		)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if string(pvc.UID) != pvcUID || pvc.Annotations[restoreLockAnnotation] != holder {
			return domain.NewError(
				domain.ErrorConflict,
				restoreLockPhase,
				"PVC lock ownership changed during renewal",
			)
		}

		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}

		pvc.Annotations[restoreLockExpiryAnnotation] = time.Now().
			UTC().
			Add(ttl).
			Format(time.RFC3339Nano)
		_, err = client.CoreV1().
			PersistentVolumeClaims(namespace).
			Update(ctx, pvc, metav1.UpdateOptions{})

		return err
	})
}

func classifyRestoreLockError(ctx context.Context, err error) error {
	if domain.CategoryOf(err) == domain.ErrorTimeout || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.Canceled) {
		return domain.WrapError(
			domain.ErrorTimeout,
			restoreLockPhase,
			"renew PVC restore lock timed out",
			err,
		)
	}

	return domain.WrapError(domain.ErrorConflict, restoreLockPhase, "renew PVC restore lock", err)
}
