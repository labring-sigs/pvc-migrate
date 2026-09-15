package kube

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Switcher struct {
	client kubernetes.Interface
	poll   time.Duration
	now    func() time.Time
	logger *slog.Logger
}

type ProgressFunc func() error

func NewSwitcher(client kubernetes.Interface) *Switcher {
	return &Switcher{client: client, poll: time.Second, now: time.Now}
}

// WithLogger enables progress logs for volume switch waits.
func (s *Switcher) WithLogger(logger *slog.Logger) *Switcher {
	s.logger = logger
	return s
}

func (s *Switcher) waitFor(
	ctx context.Context,
	description string,
	condition func(context.Context) (bool, error),
) error {
	if s.logger != nil {
		s.logger.Info(
			"waiting for Kubernetes resource",
			"operation",
			"volume switch",
			"description",
			description,
		)
	}

	return WaitFor(ctx, s.poll, description, condition)
}

// PVCTransferBindings contains only the storage identities needed to verify
// consumers, ownership and a retained-volume cutover.
type PVCTransferBindings struct {
	SourcePVC, SourcePV, DestinationPVC, DestinationPV v1alpha1.ObjectReference
}

func (s *Switcher) VerifyVolumeOffline(ctx context.Context, bindings PVCTransferBindings) error {
	return s.verifyOfflineBindings(ctx, "", []PVCTransferBindings{bindings}, false)
}

type offlineIdentityKey struct {
	pvc v1alpha1.ObjectReference
	pv  v1alpha1.ObjectReference
}

type offlineIdentityRead struct {
	pvc           v1alpha1.ObjectReference
	pv            v1alpha1.ObjectReference
	role          string
	err           error
	recoveryClaim *v1alpha1.ObjectReference
}

type podListResult struct {
	pods []corev1.Pod
	err  error
}

// VerifyVolumesOffline shares namespace and cluster-wide inventory reads
// across volumes while preserving source-first, input-order validation.
func (s *Switcher) VerifyVolumesOffline(ctx context.Context, volumes []PVCTransferBindings) error {
	return s.verifyOfflineBindings(ctx, "", volumes, false)
}

// VerifyVolumesOfflineForSession also fences live PVC/PV ownership. This
// prevents a stale or concurrently claimed volume from reaching a destructive
// activation, rollback, rename, or move step.
func (s *Switcher) VerifyVolumesOfflineForSession(
	ctx context.Context,
	sessionID string,
	volumes []PVCTransferBindings,
) error {
	if strings.TrimSpace(sessionID) == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"verify PVC offline",
			"session ID is required",
		)
	}

	return s.verifyOfflineBindings(ctx, sessionID, volumes, false)
}

// VerifyActivationRecovery accepts missing claims only when their retained PVs
// still prove session ownership and an expected cutover binding.
func (s *Switcher) VerifyActivationRecovery(
	ctx context.Context,
	sessionID string,
	volumes []PVCTransferBindings,
) error {
	if strings.TrimSpace(sessionID) == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"verify activation recovery",
			"session ID is required",
		)
	}

	return s.verifyOfflineBindings(ctx, sessionID, volumes, true)
}

// VerifyPVCOffline checks the exact binding and its consumers without any
// transfer plan, destination capacity, or activation policy.
func (s *Switcher) VerifyPVCOffline(
	ctx context.Context,
	sessionID string,
	pvc, pv v1alpha1.ObjectReference,
) error {
	return s.verifyOfflineBindings(ctx, sessionID, []PVCTransferBindings{{pvc, pv, pvc, pv}}, false)
}

func (s *Switcher) verifyOfflineBindings(
	ctx context.Context,
	sessionID string,
	volumes []PVCTransferBindings,
	recovering bool,
) error {
	identities := make([]offlineIdentityRead, 0, 2*len(volumes))
	identityIndexes := make([]int, 0, 2*len(volumes))
	identityByKey := make(map[offlineIdentityKey]int, 2*len(volumes))

	addIdentity := func(pvc, pv v1alpha1.ObjectReference, role string, recoveryClaim v1alpha1.ObjectReference) {
		key := offlineIdentityKey{pvc: pvc, pv: pv}

		index, exists := identityByKey[key]
		if !exists {
			index = len(identities)
			identityByKey[key] = index

			identities = append(identities, offlineIdentityRead{pvc: pvc, pv: pv, role: role})
			if recovering {
				identities[index].recoveryClaim = &recoveryClaim
			}
		}

		identityIndexes = append(identityIndexes, index)
	}
	for _, volume := range volumes {
		addIdentity(
			volume.SourcePVC,
			volume.SourcePV,
			ResourceRoleSource,
			v1alpha1.ObjectReference{},
		)
		addIdentity(
			volume.DestinationPVC,
			volume.DestinationPV,
			ResourceRoleDestination,
			volume.SourcePVC,
		)
	}

	parallel.For(len(identities), func(index int) {
		identity := &identities[index]
		identity.err = s.verifyPVCAndPVIdentity(
			ctx,
			identity.pvc,
			identity.pv,
			identity.role,
			sessionID,
			identity.recoveryClaim,
		)
	})

	for _, index := range identityIndexes {
		if err := identities[index].err; err != nil {
			return err
		}
	}

	namespaces := make([]string, 0)
	namespaceIndexes := make(map[string]int)
	pvNames := make([]string, 0, 2*len(volumes))

	seenPVs := make(map[string]struct{}, 2*len(volumes))
	for _, volume := range volumes {
		for _, ref := range []v1alpha1.ObjectReference{volume.SourcePVC, volume.DestinationPVC} {
			if _, exists := namespaceIndexes[ref.Namespace]; !exists {
				namespaceIndexes[ref.Namespace] = len(namespaces)
				namespaces = append(namespaces, ref.Namespace)
			}
		}

		for _, ref := range []v1alpha1.ObjectReference{volume.SourcePV, volume.DestinationPV} {
			if ref.Name == "" {
				continue
			}

			if _, exists := seenPVs[ref.Name]; exists {
				continue
			}

			seenPVs[ref.Name] = struct{}{}
			pvNames = append(pvNames, ref.Name)
		}
	}

	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	podLists := make([]podListResult, len(namespaces))
	podCheckDone := make(chan error, 1)
	detachDone := make(chan error, 1)
	checkPods := func() error {
		for _, volume := range volumes {
			for _, ref := range []v1alpha1.ObjectReference{volume.SourcePVC, volume.DestinationPVC} {
				result := podLists[namespaceIndexes[ref.Namespace]]
				if result.err != nil {
					return domain.WrapError(
						domain.ErrorKubernetes,
						"verify PVC offline",
						"list Pods in "+ref.Namespace,
						result.err,
					)
				}

				if err := ensureNoConsumerInPods(result.pods, ref.Namespace, ref.Name); err != nil {
					return err
				}
			}
		}

		return nil
	}

	var offlineWG sync.WaitGroup
	offlineWG.Go(func() {
		parallel.For(len(namespaces), func(index int) {
			pods, err := s.client.CoreV1().
				Pods(namespaces[index]).
				List(checkCtx, metav1.ListOptions{})
			if err == nil && pods == nil {
				err = fmt.Errorf("list Pods in %s returned an empty object", namespaces[index])
			}

			if pods != nil {
				podLists[index].pods = pods.Items
			}

			podLists[index].err = err
		})

		podCheckDone <- checkPods()
	})
	offlineWG.Go(func() {
		detachDone <- s.ensureVolumesDetached(checkCtx, pvNames)
	})

	select {
	case podErr := <-podCheckDone:
		if podErr != nil {
			cancel()
			<-detachDone
			offlineWG.Wait()
			return podErr
		}

		detachErr := <-detachDone

		offlineWG.Wait()

		return detachErr
	case detachErr := <-detachDone:
		if podErr := <-podCheckDone; podErr != nil {
			offlineWG.Wait()
			return podErr
		}

		offlineWG.Wait()

		return detachErr
	}
}

func ensureNoConsumerInPods(pods []corev1.Pod, namespace, claim string) error {
	for i := range pods {
		if PodPreventsSafePVCDeletion(&pods[i], claim) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"verify PVC offline",
				fmt.Sprintf("PVC %s/%s is referenced by Pod %s", namespace, claim, pods[i].Name),
			)
		}
	}

	return nil
}

func (s *Switcher) ensureVolumesDetached(ctx context.Context, pvNames []string) error {
	if len(pvNames) == 0 {
		return nil
	}

	wanted := make(map[string]struct{}, len(pvNames))
	for _, name := range pvNames {
		wanted[name] = struct{}{}
	}

	pvDescription := strings.Join(pvNames, ",")

	return s.waitFor(
		ctx,
		"VolumeAttachment detach for PV(s) "+pvDescription,
		func(waitCtx context.Context) (bool, error) {
			attachments, err := s.client.StorageV1().
				VolumeAttachments().
				List(waitCtx, metav1.ListOptions{})
			if err != nil {
				return false, domain.WrapError(
					domain.ErrorKubernetes,
					"verify PV offline",
					"list VolumeAttachments for PV(s) "+pvDescription,
					err,
				)
			}

			if attachments == nil {
				return false, domain.NewError(
					domain.ErrorKubernetes,
					"verify PV offline",
					fmt.Sprintf(
						"list VolumeAttachments for PV(s) %s returned an empty object",
						pvDescription,
					),
				)
			}

			for _, attachment := range attachments.Items {
				if attachment.Spec.Source.PersistentVolumeName == nil ||
					!attachment.Status.Attached {
					continue
				}

				if _, exists := wanted[*attachment.Spec.Source.PersistentVolumeName]; exists {
					return false, nil
				}
			}

			return true, nil
		},
	)
}

func (s *Switcher) ensureNoConsumers(ctx context.Context, namespace, claim string) error {
	pods, err := s.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify PVC offline",
			"list Pods in "+namespace,
			err,
		)
	}

	if pods == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"verify PVC offline",
			fmt.Sprintf("list Pods in %s returned an empty object", namespace),
		)
	}

	return ensureNoConsumerInPods(pods.Items, namespace, claim)
}

func (s *Switcher) ensureDetached(ctx context.Context, pvName string) error {
	if pvName == "" {
		return nil
	}
	return s.ensureVolumesDetached(ctx, []string{pvName})
}

func callProgress(progress ProgressFunc) error {
	if progress == nil {
		return nil
	}
	return progress()
}
