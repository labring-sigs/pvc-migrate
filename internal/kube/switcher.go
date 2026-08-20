package kube

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
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

func (s *Switcher) VerifyVolumeOffline(ctx context.Context, volume *domain.VolumeSpec) error {
	if volume == nil {
		return domain.NewError(domain.ErrorValidation, "verify PVC offline", "volume is nil")
	}
	return s.verifyVolumesOffline(ctx, "", []*domain.VolumeSpec{volume})
}

type offlineIdentityKey struct {
	pvc domain.ObjectReference
	pv  domain.ObjectReference
}

type offlineIdentityRead struct {
	pvc  domain.ObjectReference
	pv   domain.ObjectReference
	role string
	err  error
}

type podListResult struct {
	pods []corev1.Pod
	err  error
}

// VerifyVolumesOffline shares namespace and cluster-wide inventory reads
// across volumes while preserving source-first, input-order validation.
func (s *Switcher) VerifyVolumesOffline(ctx context.Context, volumes []*domain.VolumeSpec) error {
	return s.verifyVolumesOffline(ctx, "", volumes)
}

// VerifyVolumesOfflineForSession also fences live PVC/PV ownership. This
// prevents a stale or concurrently claimed volume from reaching a destructive
// activation, rollback, rename, or move step.
func (s *Switcher) VerifyVolumesOfflineForSession(
	ctx context.Context,
	sessionID string,
	volumes []*domain.VolumeSpec,
) error {
	if strings.TrimSpace(sessionID) == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"verify PVC offline",
			"session ID is required",
		)
	}

	return s.verifyVolumesOffline(ctx, sessionID, volumes)
}

func (s *Switcher) verifyVolumesOffline(
	ctx context.Context,
	sessionID string,
	volumes []*domain.VolumeSpec,
) error {
	identities := make([]offlineIdentityRead, 0, 2*len(volumes))
	identityIndexes := make([]int, 0, 2*len(volumes))
	identityByKey := make(map[offlineIdentityKey]int, 2*len(volumes))

	addIdentity := func(pvc, pv domain.ObjectReference, role string) {
		key := offlineIdentityKey{pvc: pvc, pv: pv}

		index, exists := identityByKey[key]
		if !exists {
			index = len(identities)
			identityByKey[key] = index

			identities = append(identities, offlineIdentityRead{pvc: pvc, pv: pv, role: role})
		}

		identityIndexes = append(identityIndexes, index)
	}
	for _, volume := range volumes {
		if volume == nil {
			return domain.NewError(domain.ErrorValidation, "verify PVC offline", "volume is nil")
		}

		addIdentity(volume.SourcePVC, volume.SourcePV, ResourceRoleSource)
		addIdentity(volume.DestinationPVC, volume.DestinationPV, ResourceRoleDestination)
	}

	parallel.For(len(identities), func(index int) {
		identity := &identities[index]
		identity.err = s.verifyPVCAndPVIdentity(
			ctx,
			identity.pvc,
			identity.pv,
			identity.role,
			sessionID,
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
		for _, ref := range []domain.ObjectReference{volume.SourcePVC, volume.DestinationPVC} {
			if _, exists := namespaceIndexes[ref.Namespace]; !exists {
				namespaceIndexes[ref.Namespace] = len(namespaces)
				namespaces = append(namespaces, ref.Namespace)
			}
		}

		for _, ref := range []domain.ObjectReference{volume.SourcePV, volume.DestinationPV} {
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
			for _, ref := range []domain.ObjectReference{volume.SourcePVC, volume.DestinationPVC} {
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

func (s *Switcher) ActivateVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if err := validateActivationInputs(session, volume, status); err != nil {
		return err
	}

	temporaryPVC, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"activate volume",
			"read temporary destination PVC",
			err,
		)
	}

	if err == nil {
		if err := s.validateTemporaryActivationPVC(ctx, temporaryPVC, volume, status); err != nil {
			return err
		}
	}

	return s.activateVolumeResources(ctx, session, volume, status, progress)
}

func validateActivationInputs(
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	if session == nil || volume == nil || status == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"activate volume",
			"session, volume, and volume status are required",
		)
	}

	if volume.SourcePVC.Namespace == "" ||
		volume.SourcePVC.Name == "" ||
		volume.SourcePVC.UID == "" ||
		volume.SourcePV.Name == "" ||
		volume.SourcePV.UID == "" ||
		volume.DestinationPVC.Namespace == "" ||
		volume.DestinationPVC.Name == "" ||
		volume.DestinationPVC.UID == "" ||
		volume.DestinationPV.Name == "" ||
		volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate volume",
			"source and destination PVC/PV identities are required",
		)
	}

	if status.Sync.FinalCompletedAt == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate volume",
			fmt.Sprintf("PVC %s has no completed final sync", volume.SourcePVC.Name),
		)
	}

	return nil
}

func (s *Switcher) validateTemporaryActivationPVC(
	ctx context.Context,
	temporaryPVC *corev1.PersistentVolumeClaim,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	if status.Activation.TemporaryPVCDeleted {
		return domain.NewError(
			domain.ErrorConflict,
			"activate volume",
			fmt.Sprintf(
				"temporary destination PVC %s/%s reappeared after its deletion checkpoint",
				temporaryPVC.Namespace,
				temporaryPVC.Name,
			),
		)
	}

	if temporaryPVC.UID != volume.DestinationPVC.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"activate volume",
			fmt.Sprintf(
				"temporary destination PVC %s/%s UID changed",
				temporaryPVC.Namespace,
				temporaryPVC.Name,
			),
		)
	}

	return s.verifyBinding(ctx, temporaryPVC, volume.DestinationPV)
}

func (s *Switcher) activateVolumeResources(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if err := s.ensureNoConsumers(
		ctx,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
	); err != nil {
		return err
	}

	if err := s.ensureNoConsumers(
		ctx,
		volume.DestinationPVC.Namespace,
		volume.DestinationPVC.Name,
	); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.SourcePV.Name); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.DestinationPV.Name); err != nil {
		return err
	}

	if active, err := s.activePVC(ctx, session, volume); err != nil {
		return err
	} else if active != nil {
		return s.completeActivation(ctx, session, volume, status, active, progress)
	}

	if err := s.ensureRetain(ctx, volume.SourcePV, session.ID, ResourceRoleSource); err != nil {
		return err
	}

	if err := s.ensureRetain(
		ctx,
		volume.DestinationPV,
		session.ID,
		ResourceRoleDestination,
	); err != nil {
		return err
	}

	if err := s.deleteTemporaryActivationPVC(ctx, volume, status, progress); err != nil {
		return err
	}

	if err := s.deleteSourceActivationPVC(ctx, volume, status, progress); err != nil {
		return err
	}

	if err := s.reserveActivationDestination(ctx, session, volume, status, progress); err != nil {
		return err
	}

	created, err := s.createActivePVC(
		ctx,
		session,
		volume,
		volume.DestinationPV,
		volume.StorageClass,
	)
	if err != nil {
		return err
	}

	return s.completeActivation(ctx, session, volume, status, created, progress)
}

func (s *Switcher) deleteTemporaryActivationPVC(
	ctx context.Context,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if status.Activation.TemporaryPVCDeleted {
		return nil
	}

	if err := s.deletePVC(ctx, volume.DestinationPVC); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.DestinationPV.Name); err != nil {
		return err
	}

	status.Activation.TemporaryPVCDeleted = true

	return callProgress(progress)
}

func (s *Switcher) deleteSourceActivationPVC(
	ctx context.Context,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if status.Activation.SourcePVCDeleted {
		return nil
	}

	if err := s.deletePVC(ctx, volume.SourcePVC); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.SourcePV.Name); err != nil {
		return err
	}

	status.Activation.SourcePVCDeleted = true

	return callProgress(progress)
}

func (s *Switcher) reserveActivationDestination(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if status.Activation.DestinationReserved {
		return nil
	}

	if err := s.validateActivePVC(
		ctx,
		session,
		volume,
		volume.DestinationPV,
		volume.StorageClass,
	); err != nil {
		return err
	}

	if err := s.reservePV(
		ctx,
		volume.DestinationPV,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
		session.ID,
	); err != nil {
		return err
	}

	status.Activation.DestinationReserved = true

	return callProgress(progress)
}

func (s *Switcher) RollbackVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if err := validateRollbackVolume(session, volume, status); err != nil {
		return err
	}

	if err := s.ensureNoConsumers(
		ctx,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
	); err != nil {
		return err
	}

	current, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err == nil && current.Spec.VolumeName == volume.SourcePV.Name {
		original := current.UID == volume.SourcePVC.UID

		recovered := current.Annotations[SessionKey] == session.ID
		if !original && !recovered {
			return domain.NewError(
				domain.ErrorConflict,
				"rollback volume",
				fmt.Sprintf(
					"PVC %s/%s is not the original or session-owned source PVC",
					current.Namespace,
					current.Name,
				),
			)
		}

		return s.completeRollback(ctx, session, volume, status, current, progress)
	}

	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "rollback volume", "read active PVC", err)
	}

	if err == nil {
		if err := s.removeActiveDestination(ctx, session, volume, current); err != nil {
			return err
		}
	}

	if err := s.ensureRetain(ctx, volume.SourcePV, session.ID, ResourceRoleSource); err != nil {
		return err
	}

	if err := s.ensureRetain(
		ctx,
		volume.DestinationPV,
		session.ID,
		ResourceRoleDestination,
	); err != nil {
		return err
	}

	sourceClass := ""
	if volume.SourcePVCSpec.StorageClassName != nil {
		sourceClass = *volume.SourcePVCSpec.StorageClassName
	}

	if err := s.validateActivePVC(ctx, session, volume, volume.SourcePV, sourceClass); err != nil {
		return err
	}

	if err := s.reservePV(
		ctx,
		volume.SourcePV,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
		session.ID,
	); err != nil {
		return err
	}

	recreated, err := s.createActivePVC(ctx, session, volume, volume.SourcePV, sourceClass)
	if err != nil {
		return err
	}

	return s.completeRollback(ctx, session, volume, status, recreated, progress)
}

func validateRollbackVolume(
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	if session == nil || volume == nil || status == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"rollback volume",
			"session, volume, and volume status are required",
		)
	}

	if volume.SourcePVC.Namespace == "" || volume.SourcePVC.Name == "" ||
		volume.SourcePVC.UID == "" || volume.SourcePV.Name == "" || volume.SourcePV.UID == "" ||
		volume.DestinationPV.Name == "" || volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback volume",
			"source PVC and source/destination PV identities are required",
		)
	}

	return nil
}

func (s *Switcher) removeActiveDestination(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	current *corev1.PersistentVolumeClaim,
) error {
	if current.Spec.VolumeName != volume.DestinationPV.Name ||
		current.Annotations[SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"rollback volume",
			fmt.Sprintf(
				"PVC %s/%s is not the session's active destination",
				current.Namespace,
				current.Name,
			),
		)
	}

	if err := s.verifyBinding(ctx, current, volume.DestinationPV); err != nil {
		return err
	}
	// The storage provisioner may restore the destination PV's original reclaim
	// policy after activation, so retain it before deleting the active PVC.
	if err := s.ensureRetain(
		ctx,
		volume.DestinationPV,
		session.ID,
		ResourceRoleDestination,
	); err != nil {
		return err
	}

	ref := domain.ObjectReference{
		Namespace: current.Namespace, Name: current.Name, UID: current.UID,
		ResourceVersion: current.ResourceVersion,
	}
	if err := s.deletePVC(ctx, ref); err != nil {
		return err
	}

	return s.ensureDetached(ctx, volume.DestinationPV.Name)
}

func (s *Switcher) RenamePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	progress ProgressFunc,
) (*corev1.PersistentVolumeClaim, error) {
	if err := validateRenamePVCRequest(session, volume); err != nil {
		return nil, err
	}

	if err := s.ensureNoConsumers(
		ctx,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
	); err != nil {
		return nil, err
	}

	source, sourceErr := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if sourceErr != nil && !apierrors.IsNotFound(sourceErr) {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"rename PVC",
			"read source PVC",
			sourceErr,
		)
	}

	if sourceErr == nil {
		if source.UID != volume.SourcePVC.UID {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"rename PVC",
				fmt.Sprintf("source PVC %s/%s UID changed", source.Namespace, source.Name),
			)
		}

		if err := s.verifyBinding(ctx, source, volume.SourcePV); err != nil {
			return nil, err
		}
	}

	if existing, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{}); err == nil {
		if sourceErr == nil &&
			(source.Namespace != existing.Namespace || source.Name != existing.Name) {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"rename PVC",
				fmt.Sprintf(
					"both source PVC %s/%s and destination PVC %s/%s exist",
					source.Namespace,
					source.Name,
					existing.Namespace,
					existing.Name,
				),
			)
		}

		if existing.Spec.VolumeName == volume.SourcePV.Name &&
			existing.Annotations[SessionKey] == session.ID {
			if err := s.ensureNoConsumers(ctx, existing.Namespace, existing.Name); err != nil {
				return nil, err
			}

			if err := s.verifyBinding(ctx, existing, volume.SourcePV); err != nil {
				return nil, err
			}

			if err := s.ensureRetain(
				ctx,
				volume.SourcePV,
				session.ID,
				ResourceRoleActive,
			); err != nil {
				return nil, err
			}

			return existing, nil
		}

		return nil, domain.NewError(
			domain.ErrorConflict,
			"rename PVC",
			fmt.Sprintf("destination PVC %s/%s already exists", existing.Namespace, existing.Name),
		)
	} else if !apierrors.IsNotFound(
		err,
	) {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"rename PVC",
			"read destination PVC",
			err,
		)
	}

	if err := s.ensureRetain(ctx, volume.SourcePV, session.ID, ResourceRoleRename); err != nil {
		return nil, err
	}

	if err := s.deletePVC(ctx, volume.SourcePVC); err != nil {
		return nil, err
	}

	if err := s.ensureDetached(ctx, volume.SourcePV.Name); err != nil {
		return nil, err
	}

	if err := callProgress(progress); err != nil {
		return nil, err
	}

	cloned := *volume
	cloned.SourcePVC.Namespace = volume.DestinationPVC.Namespace
	cloned.SourcePVC.Name = volume.DestinationPVC.Name

	sourceClass := ""
	if volume.SourcePVCSpec.StorageClassName != nil {
		sourceClass = *volume.SourcePVCSpec.StorageClassName
	}

	if err := s.validateActivePVC(ctx, session, &cloned, volume.SourcePV, sourceClass); err != nil {
		return nil, err
	}

	if err := s.reservePV(
		ctx,
		volume.SourcePV,
		volume.DestinationPVC.Namespace,
		volume.DestinationPVC.Name,
		session.ID,
	); err != nil {
		return nil, err
	}

	created, err := s.createActivePVC(ctx, session, &cloned, volume.SourcePV, sourceClass)
	if err != nil {
		return nil, err
	}

	if err := s.verifyBinding(ctx, created, volume.SourcePV); err != nil {
		return nil, err
	}

	if err := s.ensureRetain(ctx, volume.SourcePV, session.ID, ResourceRoleActive); err != nil {
		return nil, err
	}

	return created, nil
}

func validateRenamePVCRequest(session *domain.Session, volume *domain.VolumeSpec) error {
	if session == nil || volume == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"rename PVC",
			"session and volume are required",
		)
	}

	if volume.SourcePVC.Namespace == "" || volume.SourcePVC.Name == "" ||
		volume.SourcePVC.UID == "" ||
		volume.SourcePV.Name == "" ||
		volume.SourcePV.UID == "" ||
		volume.DestinationPVC.Namespace == "" ||
		volume.DestinationPVC.Name == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rename PVC",
			"source PVC/PV identity and destination PVC name are required",
		)
	}

	return nil
}

func (s *Switcher) activePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"activate volume",
			"read application PVC",
			err,
		)
	}

	if pvc.UID == volume.SourcePVC.UID && pvc.Spec.VolumeName == volume.SourcePV.Name {
		if err := s.verifyBinding(ctx, pvc, volume.SourcePV); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if pvc.Spec.VolumeName == volume.DestinationPV.Name &&
		pvc.Annotations[SessionKey] == session.ID {
		return pvc, nil
	}

	return nil, domain.NewError(
		domain.ErrorConflict,
		"activate volume",
		fmt.Sprintf("PVC %s/%s has unexpected UID or binding", pvc.Namespace, pvc.Name),
	)
}

func (s *Switcher) createActivePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	pvRef domain.ObjectReference,
	storageClass string,
) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := activePVCManifest(session, volume, pvRef, storageClass)
	if err != nil {
		return nil, err
	}

	created, err := s.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Create(ctx, pvc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = s.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Get(ctx, pvc.Name, metav1.GetOptions{})
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"create active PVC",
			fmt.Sprintf("create %s/%s", pvc.Namespace, pvc.Name),
			err,
		)
	}

	if created == nil || created.UID == "" {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"create active PVC",
			fmt.Sprintf("create %s/%s returned an object without a UID", pvc.Namespace, pvc.Name),
		)
	}

	if created.Spec.VolumeName != pvRef.Name || created.Labels[ManagedByLabel] != ManagedByValue ||
		created.Labels[SessionKey] != session.ID ||
		created.Annotations[SessionKey] != session.ID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"create active PVC",
			fmt.Sprintf(
				"PVC %s/%s exists with an unexpected binding",
				created.Namespace,
				created.Name,
			),
		)
	}

	bound := created
	if err := s.waitFor(
		ctx,
		fmt.Sprintf("PVC %s/%s binding to PV %s", created.Namespace, created.Name, pvRef.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := s.client.CoreV1().
				PersistentVolumeClaims(created.Namespace).
				Get(waitCtx, created.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}

			if current.UID != created.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"create active PVC",
					fmt.Sprintf(
						"PVC %s/%s was replaced while waiting for binding",
						current.Namespace,
						current.Name,
					),
				)
			}

			if current.Labels[ManagedByLabel] != ManagedByValue ||
				current.Labels[SessionKey] != session.ID ||
				current.Annotations[SessionKey] != session.ID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"create active PVC",
					fmt.Sprintf(
						"PVC %s/%s ownership changed while waiting for binding",
						current.Namespace,
						current.Name,
					),
				)
			}

			if current.Spec.VolumeName != pvRef.Name {
				return false, domain.NewError(
					domain.ErrorConflict,
					"create active PVC",
					"PVC bound to PV "+current.Spec.VolumeName,
				)
			}

			bound = current

			return current.Status.Phase == corev1.ClaimBound, nil
		},
	); err != nil {
		return nil, err
	}

	return bound, nil
}

func (s *Switcher) validateActivePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	pvRef domain.ObjectReference,
	storageClass string,
) error {
	pvc, err := activePVCManifest(session, volume, pvRef, storageClass)
	if err != nil {
		return err
	}

	if _, err := s.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Create(ctx, pvc, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"validate active PVC",
			fmt.Sprintf("server-side dry-run rejected %s/%s", pvc.Namespace, pvc.Name),
			err,
		)
	}

	return nil
}

func activePVCManifest(
	session *domain.Session,
	volume *domain.VolumeSpec,
	pvRef domain.ObjectReference,
	storageClass string,
) (*corev1.PersistentVolumeClaim, error) {
	spec := *volume.SourcePVCSpec.DeepCopy()
	if pvRef.Name == volume.DestinationPV.Name {
		capacity, err := resource.ParseQuantity(volume.Capacity)
		if err != nil || capacity.Sign() <= 0 {
			if err == nil {
				err = errors.New("capacity must be positive")
			}

			return nil, domain.NewError(
				domain.ErrorValidation,
				"active PVC",
				fmt.Sprintf("destination capacity %q is invalid: %v", volume.Capacity, err),
			)
		}

		if spec.Resources.Requests == nil {
			spec.Resources.Requests = corev1.ResourceList{}
		}

		spec.Resources.Requests[corev1.ResourceStorage] = capacity
	}

	spec.VolumeName = pvRef.Name
	spec.Selector = nil
	spec.DataSource = nil
	spec.DataSourceRef = nil
	spec.StorageClassName = &storageClass
	metadata := volume.SourcePVCMetadata

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            volume.SourcePVC.Name,
			Namespace:       volume.SourcePVC.Namespace,
			Labels:          maps.Clone(metadata.Labels),
			Annotations:     maps.Clone(metadata.Annotations),
			OwnerReferences: append([]metav1.OwnerReference(nil), metadata.OwnerReferences...),
		},
		Spec: spec,
	}
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}

	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}

	for _, key := range []string{
		"pv.kubernetes.io/bind-completed",
		"pv.kubernetes.io/bound-by-controller",
		"volume.beta.kubernetes.io/storage-provisioner",
		"volume.kubernetes.io/selected-node",
		"volume.kubernetes.io/storage-provisioner",
		PVCStorageResizerAnnotation,
	} {
		delete(pvc.Annotations, key)
	}

	pvc.Labels[ManagedByLabel] = ManagedByValue
	pvc.Labels[SessionKey] = session.ID
	pvc.Annotations[SessionKey] = session.ID

	rollbackPV := volume.SourcePV.Name
	if pvRef.Name == volume.SourcePV.Name {
		rollbackPV = volume.DestinationPV.Name
	}

	pvc.Annotations[RollbackPVAnnotation] = rollbackPV

	return pvc, nil
}

func (s *Switcher) completeActivation(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	pvc *corev1.PersistentVolumeClaim,
	progress ProgressFunc,
) error {
	if err := validateActivePVCRequest(pvc, volume); err != nil {
		return err
	}

	if err := s.verifyBinding(ctx, pvc, volume.DestinationPV); err != nil {
		return err
	}

	if err := s.markPVPair(ctx, session.ID, volume, false); err != nil {
		return err
	}

	now := metav1.NewTime(s.now().UTC())
	status.Activation.ActivePVC = domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.Activation.ActivatedAt = &now
	status.Activation.TemporaryPVCDeleted = true
	status.Activation.SourcePVCDeleted = true
	status.Activation.DestinationReserved = true

	return callProgress(progress)
}

func validateActivePVCRequest(pvc *corev1.PersistentVolumeClaim, volume *domain.VolumeSpec) error {
	if pvc == nil || volume == nil {
		return domain.NewError(domain.ErrorValidation, "active PVC", "PVC and volume are required")
	}

	capacity, err := resource.ParseQuantity(volume.Capacity)
	if err != nil || capacity.Sign() <= 0 {
		if err == nil {
			err = errors.New("capacity must be positive")
		}

		return domain.NewError(
			domain.ErrorValidation,
			"active PVC",
			fmt.Sprintf("destination capacity %q is invalid: %v", volume.Capacity, err),
		)
	}

	requested, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || requested.Cmp(capacity) != 0 {
		actual := "missing"
		if ok {
			actual = requested.String()
		}

		return domain.NewError(
			domain.ErrorConflict,
			"active PVC",
			fmt.Sprintf(
				"PVC %s/%s requests %s, session requires %s",
				pvc.Namespace,
				pvc.Name,
				actual,
				capacity.String(),
			),
		)
	}

	return nil
}

func (s *Switcher) completeRollback(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	pvc *corev1.PersistentVolumeClaim,
	progress ProgressFunc,
) error {
	if err := s.verifyBinding(ctx, pvc, volume.SourcePV); err != nil {
		return err
	}

	if err := s.markPVPair(ctx, session.ID, volume, true); err != nil {
		return err
	}

	now := metav1.NewTime(s.now().UTC())
	status.Activation.ActivePVC = domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.Activation.RolledBackAt = &now

	return callProgress(progress)
}

func (s *Switcher) deletePVC(ctx context.Context, ref domain.ObjectReference) error {
	if ref.Namespace == "" || ref.Name == "" || ref.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"delete PVC",
			"PVC namespace, name, and UID are required",
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete PVC",
			fmt.Sprintf("read %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	if pvc.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"delete PVC",
			fmt.Sprintf(
				"PVC %s/%s UID changed from %s to %s",
				ref.Namespace,
				ref.Name,
				ref.UID,
				pvc.UID,
			),
		)
	}

	preconditions := &metav1.Preconditions{UID: &pvc.UID, ResourceVersion: &pvc.ResourceVersion}
	if err := s.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: preconditions}); err != nil &&
		!apierrors.IsNotFound(err) {
		if apierrors.IsConflict(err) {
			return domain.WrapError(
				domain.ErrorConflict,
				"delete PVC",
				fmt.Sprintf("PVC %s/%s changed concurrently", ref.Namespace, ref.Name),
				err,
			)
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete PVC",
			fmt.Sprintf("delete %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	return s.waitFor(
		ctx,
		fmt.Sprintf("PVC %s/%s deletion", ref.Namespace, ref.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := s.client.CoreV1().
				PersistentVolumeClaims(ref.Namespace).
				Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}

			if getErr != nil {
				return false, getErr
			}

			if current.UID != pvc.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"delete PVC",
					fmt.Sprintf("PVC %s/%s name was reused", ref.Namespace, ref.Name),
				)
			}

			return false, nil
		},
	)
}

func (s *Switcher) reservePV(
	ctx context.Context,
	ref domain.ObjectReference,
	namespace, claim, sessionID string,
) error {
	if ref.Name == "" || ref.UID == "" || namespace == "" || claim == "" || sessionID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"reserve PV",
			"PV name/UID, claim namespace/name, and session ID are required",
		)
	}

	if err := s.waitForPVReservation(ctx, ref, namespace, claim); err != nil {
		return err
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return s.updatePVReservation(ctx, ref, namespace, claim, sessionID)
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve PV",
			fmt.Sprintf("update PV %s claimRef", ref.Name),
			err,
		)
	}

	return nil
}

func (s *Switcher) waitForPVReservation(
	ctx context.Context,
	ref domain.ObjectReference,
	namespace, claim string,
) error {
	return s.waitFor(
		ctx,
		fmt.Sprintf("PV %s release before reservation", ref.Name),
		func(waitCtx context.Context) (bool, error) {
			return s.pvReadyForReservation(waitCtx, ref, namespace, claim)
		},
	)
}

func (s *Switcher) pvReadyForReservation(
	ctx context.Context,
	ref domain.ObjectReference,
	namespace, claim string,
) (bool, error) {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	if pv.UID != ref.UID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"reserve PV",
			fmt.Sprintf("PV %s UID changed", ref.Name),
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != namespace ||
		pv.Spec.ClaimRef.Name != claim {
		return pv.Status.Phase == corev1.VolumeReleased ||
			pv.Status.Phase == corev1.VolumeAvailable, nil
	}

	current, getErr := s.client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, claim, metav1.GetOptions{})
	if getErr == nil {
		if pv.Spec.ClaimRef.UID != "" && current.UID == pv.Spec.ClaimRef.UID &&
			current.Spec.VolumeName == pv.Name {
			return true, nil
		}

		return false, domain.NewError(
			domain.ErrorConflict,
			"reserve PV",
			fmt.Sprintf("PV %s claimRef points to a different PVC identity", pv.Name),
		)
	}

	if !apierrors.IsNotFound(getErr) {
		return false, getErr
	}
	// The recorded claim is stale after its PVC was deleted. The mutation below
	// can safely clear it before creating the new claim.
	return true, nil
}

func (s *Switcher) updatePVReservation(
	ctx context.Context,
	ref domain.ObjectReference,
	namespace, claim, sessionID string,
) error {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve PV",
			fmt.Sprintf("PV %s UID changed", ref.Name),
		)
	}

	if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve PV",
			fmt.Sprintf("PV %s belongs to session %s", ref.Name, owner),
		)
	}

	if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == namespace &&
		pv.Spec.ClaimRef.Name == claim {
		current, getErr := s.client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, claim, metav1.GetOptions{})
		if getErr == nil {
			if pv.Spec.ClaimRef.UID != "" && current.UID == pv.Spec.ClaimRef.UID &&
				current.Spec.VolumeName == pv.Name {
				return nil
			}

			return domain.NewError(
				domain.ErrorConflict,
				"reserve PV",
				fmt.Sprintf("PV %s claimRef points to a different PVC identity", pv.Name),
			)
		}

		if getErr != nil && !apierrors.IsNotFound(getErr) {
			return getErr
		}

		if pv.Spec.ClaimRef.UID == "" {
			return nil
		}
	}

	pv.Spec.ClaimRef = &corev1.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  namespace,
		Name:       claim,
	}
	_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

	return err
}

func (s *Switcher) ensureRetain(
	ctx context.Context,
	ref domain.ObjectReference,
	sessionID, role string,
) error {
	if ref.Name == "" || ref.UID == "" || sessionID == "" || role == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"retain PV",
			"PV name/UID, session ID, and role are required",
		)
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if pv.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"retain PV",
				fmt.Sprintf("PV %s UID changed", ref.Name),
			)
		}

		if pv.Labels == nil {
			pv.Labels = map[string]string{}
		}

		if pv.Annotations == nil {
			pv.Annotations = map[string]string{}
		}

		if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
			return domain.NewError(
				domain.ErrorConflict,
				"retain PV",
				fmt.Sprintf("PV %s belongs to session %s", ref.Name, owner),
			)
		}

		changed := markPVSession(pv.Labels, sessionID, role)
		if pv.Annotations[OriginalPolicyAnnotation] == "" {
			pv.Annotations[OriginalPolicyAnnotation] = string(pv.Spec.PersistentVolumeReclaimPolicy)
			changed = true
		}

		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
			changed = true
		}

		if !changed {
			return nil
		}

		_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

		return err
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}
		return domain.WrapError(domain.ErrorKubernetes, "retain PV", "update PV "+ref.Name, err)
	}

	return nil
}

func (s *Switcher) verifyBinding(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	ref domain.ObjectReference,
) error {
	if pvc == nil || pvc.Namespace == "" || pvc.Name == "" || pvc.UID == "" || ref.Name == "" ||
		ref.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"verify binding",
			"PVC and PV identities are required",
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != ref.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify binding",
			fmt.Sprintf("PVC %s/%s is not Bound to %s", pvc.Namespace, pvc.Name, ref.Name),
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify binding", "read PV "+ref.Name, err)
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify binding",
			fmt.Sprintf("PV %s UID changed", ref.Name),
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify binding",
			fmt.Sprintf("PV %s claimRef does not match PVC UID %s", ref.Name, pvc.UID),
		)
	}

	return nil
}

// verifyPVCAndPVIdentity re-reads both objects before a copy and validates the
// persisted identities and the two-sided Kubernetes binding relationship.
func (s *Switcher) verifyPVCAndPVIdentity(
	ctx context.Context,
	pvcRef, pvRef domain.ObjectReference,
	role, sessionID string,
) error {
	if pvcRef.Namespace == "" || pvcRef.Name == "" || pvcRef.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify PVC offline",
			role+" PVC reference is incomplete",
		)
	}

	if pvRef.Name == "" || pvRef.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify PVC offline",
			role+" PV reference is incomplete",
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(pvcRef.Namespace).
		Get(ctx, pvcRef.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify PVC offline",
			fmt.Sprintf("read %s PVC %s/%s", role, pvcRef.Namespace, pvcRef.Name),
			err,
		)
	}

	if pvc.UID != pvcRef.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			fmt.Sprintf(
				"%s PVC %s/%s UID changed from %s to %s",
				role,
				pvc.Namespace,
				pvc.Name,
				pvcRef.UID,
				pvc.UID,
			),
		)
	}

	if sessionID != "" {
		for _, owner := range []string{pvc.Annotations[SessionKey], pvc.Labels[SessionKey]} {
			if owner != "" && owner != sessionID {
				return domain.NewError(
					domain.ErrorConflict,
					"verify PVC offline",
					fmt.Sprintf(
						"%s PVC %s/%s belongs to session %s",
						role,
						pvc.Namespace,
						pvc.Name,
						owner,
					),
				)
			}
		}
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify PVC offline",
			fmt.Sprintf("%s PVC %s/%s is %s", role, pvc.Namespace, pvc.Name, pvc.Status.Phase),
		)
	}

	if pvc.Spec.VolumeName != pvRef.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			fmt.Sprintf(
				"%s PVC %s/%s points to PV %s, expected %s",
				role,
				pvc.Namespace,
				pvc.Name,
				pvc.Spec.VolumeName,
				pvRef.Name,
			),
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, pvRef.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify PVC offline",
			fmt.Sprintf("read %s PV %s", role, pvRef.Name),
			err,
		)
	}

	if pv.UID != pvRef.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			fmt.Sprintf("%s PV %s UID changed from %s to %s", role, pv.Name, pvRef.UID, pv.UID),
		)
	}

	if sessionID != "" {
		if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
			return domain.NewError(
				domain.ErrorConflict,
				"verify PVC offline",
				fmt.Sprintf("%s PV %s belongs to session %s", role, pv.Name, owner),
			)
		}
	}

	claimRef := pv.Spec.ClaimRef
	if claimRef == nil || claimRef.Namespace != pvc.Namespace || claimRef.Name != pvc.Name ||
		claimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			fmt.Sprintf(
				"%s PV %s claimRef does not match PVC %s/%s UID %s",
				role,
				pv.Name,
				pvc.Namespace,
				pvc.Name,
				pvc.UID,
			),
		)
	}

	return nil
}

func (s *Switcher) markPVPair(
	ctx context.Context,
	sessionID string,
	volume *domain.VolumeSpec,
	rolledBack bool,
) error {
	if sessionID == "" || volume == nil || volume.SourcePV.Name == "" ||
		volume.SourcePV.UID == "" ||
		volume.DestinationPV.Name == "" ||
		volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"mark PV pair",
			"session ID and source/destination PV identities are required",
		)
	}

	active := volume.DestinationPV

	rollback := volume.SourcePV
	if rolledBack {
		active, rollback = rollback, active
	}

	for _, item := range []struct {
		ref   domain.ObjectReference
		role  string
		other string
	}{
		{ref: active, role: ResourceRoleActive, other: rollback.Name},
		{ref: rollback, role: ResourceRoleRollback, other: active.Name},
	} {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			pv, err := s.client.CoreV1().
				PersistentVolumes().
				Get(ctx, item.ref.Name, metav1.GetOptions{})
			if err != nil {
				// A rollback PV is disposable once the source PV is active again.
				// Provisioners with a Delete reclaim policy can remove it while the
				// active PVC is being deleted; keep validating and marking the active
				// PV without turning a completed rollback into a failure.
				if rolledBack && item.role == ResourceRoleRollback && apierrors.IsNotFound(err) {
					return nil
				}
				return err
			}

			if pv.UID != item.ref.UID {
				return domain.NewError(
					domain.ErrorConflict,
					"mark PV pair",
					fmt.Sprintf("PV %s UID changed", item.ref.Name),
				)
			}

			if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
				return domain.NewError(
					domain.ErrorConflict,
					"mark PV pair",
					fmt.Sprintf("PV %s belongs to session %s", item.ref.Name, owner),
				)
			}

			if pv.Labels == nil {
				pv.Labels = map[string]string{}
			}

			if pv.Annotations == nil {
				pv.Annotations = map[string]string{}
			}

			changed := markPVSession(pv.Labels, sessionID, item.role)
			if pv.Annotations[PairedPVAnnotation] != item.other {
				pv.Annotations[PairedPVAnnotation] = item.other
				changed = true
			}

			if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
				changed = true
			}

			if !changed {
				return nil
			}

			_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

			return err
		})
		if err != nil {
			if domain.CategoryOf(err) == domain.ErrorConflict {
				return err
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"mark PV pair",
				"update PV "+item.ref.Name,
				err,
			)
		}
	}

	return nil
}

func callProgress(progress ProgressFunc) error {
	if progress == nil {
		return nil
	}
	return progress()
}
