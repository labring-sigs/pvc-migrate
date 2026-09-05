package crosscluster

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func ensureNoActiveConsumers(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, pvcName string,
) error {
	consumers, err := activeConsumers(ctx, client, namespace, pvcName)
	if err != nil {
		return fmt.Errorf("check destination PVC %s/%s consumers: %w", namespace, pvcName, err)
	}

	if len(consumers) > 0 {
		return fmt.Errorf(
			"destination PVC %s/%s has active consumers: %s; stop them before cleanup",
			namespace,
			pvcName,
			strings.Join(consumers, ", "),
		)
	}

	return nil
}

func (s *Service) reserveVolume(ctx context.Context, session *Session, index int) error {
	v := &session.Spec.Volumes[index]

	capacity, err := resource.ParseQuantity(v.Destination.Capacity)
	if err != nil {
		return err
	}

	clients := s.destination.Kubernetes

	pvc, err := clients.CoreV1().
		PersistentVolumeClaims(v.Destination.PVC.Namespace).
		Get(ctx, v.Destination.PVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		storageClass := v.Destination.StorageClass.Name
		pvc = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        v.Destination.PVC.Name,
				Namespace:   v.Destination.PVC.Namespace,
				Labels:      map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
				Annotations: map[string]string{SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: append(
					[]corev1.PersistentVolumeAccessMode(nil),
					v.Destination.AccessModes...,
				),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: capacity},
				},
				StorageClassName: &storageClass,
				VolumeMode:       &v.Destination.VolumeMode,
			},
		}
		pvc, err = clients.CoreV1().
			PersistentVolumeClaims(v.Destination.PVC.Namespace).
			Create(ctx, pvc, metav1.CreateOptions{})
	}

	if err != nil {
		return fmt.Errorf(
			"create destination PVC %s/%s: %w",
			v.Destination.PVC.Namespace,
			v.Destination.PVC.Name,
			err,
		)
	}

	if pvc.Labels[ManagedByLabel] != ManagedBy || pvc.Labels[SessionKey] != session.ID {
		return fmt.Errorf("destination PVC %s/%s is not owned by session", pvc.Namespace, pvc.Name)
	}

	if v.Destination.PVC.UID != "" && pvc.UID != v.Destination.PVC.UID {
		return fmt.Errorf("destination PVC %s/%s UID changed", pvc.Namespace, pvc.Name)
	}

	if err := validateDestinationPVCSpec(pvc, v); err != nil {
		return err
	}

	v.Destination.PVC.UID = pvc.UID

	storageClass, err := clients.StorageV1().
		StorageClasses().
		Get(ctx, v.Destination.StorageClass.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if storageClass.UID != v.Destination.StorageClass.UID {
		return fmt.Errorf("destination StorageClass %s UID changed", storageClass.Name)
	}

	if storageClass.VolumeBindingMode != nil &&
		*storageClass.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer &&
		v.Destination.PV.UID == "" {
		if err := s.createReservationConsumer(ctx, session, v); err != nil {
			return err
		}

		if err := s.save(ctx, session, false); err != nil {
			return fmt.Errorf("persist reservation Pod ownership: %w", err)
		}
	}

	err = kube.WaitFor(
		ctx,
		s.interval,
		"destination PVC "+pvc.Namespace+"/"+pvc.Name+" binding",
		func(waitCtx context.Context) (bool, error) {
			current, e := clients.CoreV1().
				PersistentVolumeClaims(pvc.Namespace).
				Get(waitCtx, pvc.Name, metav1.GetOptions{})
			if e != nil {
				return false, e
			}

			if current.UID != pvc.UID {
				return false, errors.New("destination PVC UID changed")
			}

			if current.Status.Phase != corev1.ClaimBound {
				return false, nil
			}

			if current.Spec.VolumeName == "" {
				return false, nil
			}

			v.Destination.PVC.UID = current.UID
			v.Destination.PVC.ResourceVersion = current.ResourceVersion

			pv, pvErr := clients.CoreV1().
				PersistentVolumes().
				Get(waitCtx, current.Spec.VolumeName, metav1.GetOptions{})
			if pvErr != nil {
				return false, pvErr
			}

			v.Destination.PV = ClusterResourceRef{
				ClusterID:  session.Spec.DestinationCluster.ID,
				APIVersion: "v1",
				Kind:       "PersistentVolume",
				Name:       pv.Name,
				UID:        pv.UID,
			}

			status := &session.Status.Volumes[index]
			if ref := status.Reservation.ConsumerPod; ref.UID != "" {
				if deleteErr := s.deleteReservationConsumer(
					waitCtx,
					session,
					index,
				); deleteErr != nil {
					return false, deleteErr
				}

				now := metav1.NewTime(s.now().UTC())
				status.Reservation.CompletedAt = &now
			}

			return true, nil
		},
	)

	return err
}

func validateDestinationPVCSpec(pvc *corev1.PersistentVolumeClaim, volume *VolumeSpec) error {
	if pvc == nil || volume == nil {
		return errors.New("destination PVC specification is unavailable")
	}

	if pvc.Spec.StorageClassName == nil ||
		*pvc.Spec.StorageClassName != volume.Destination.StorageClass.Name {
		return fmt.Errorf(
			"destination PVC %s/%s StorageClass does not match the plan",
			pvc.Namespace,
			pvc.Name,
		)
	}

	if pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode != volume.Destination.VolumeMode {
		return fmt.Errorf(
			"destination PVC %s/%s volume mode does not match the plan",
			pvc.Namespace,
			pvc.Name,
		)
	}

	requested, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok {
		return fmt.Errorf("destination PVC %s/%s has no storage request", pvc.Namespace, pvc.Name)
	}

	expected, err := resource.ParseQuantity(volume.Destination.Capacity)
	if err != nil || requested.Cmp(expected) != 0 {
		return fmt.Errorf(
			"destination PVC %s/%s storage request does not match the plan",
			pvc.Namespace,
			pvc.Name,
		)
	}

	if !sameAccessModes(pvc.Spec.AccessModes, volume.Destination.AccessModes) {
		return fmt.Errorf(
			"destination PVC %s/%s access modes do not match the plan",
			pvc.Namespace,
			pvc.Name,
		)
	}

	return nil
}

func sameAccessModes(left, right []corev1.PersistentVolumeAccessMode) bool {
	if len(left) != len(right) {
		return false
	}

	set := make(map[corev1.PersistentVolumeAccessMode]struct{}, len(left))
	for _, mode := range left {
		set[mode] = struct{}{}
	}

	for _, mode := range right {
		if _, ok := set[mode]; !ok {
			return false
		}
	}

	return true
}

func (s *Service) createReservationConsumer(
	ctx context.Context,
	session *Session,
	volume *VolumeSpec,
) error {
	name := reservationConsumerName(session.ID, volume.Source.PVC.Name)

	node := session.Spec.TargetNode
	if node == "" || node == domain.AutoValue {
		return fmt.Errorf(
			"WFFC destination PVC %s/%s requires a resolved target node",
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
		)
	}

	target, err := s.destination.Kubernetes.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read destination node %s: %w", node, err)
	}

	hostname := target.Labels[corev1.LabelHostname]
	if hostname == "" {
		return fmt.Errorf("destination node %s lacks %s", node, corev1.LabelHostname)
	}

	runAs := int64(0)
	automountServiceAccountToken := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: volume.Destination.PVC.Namespace,
			Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: &automountServiceAccountToken,
			NodeSelector:                 map[string]string{corev1.LabelHostname: hostname},
			Tolerations:                  nodeTolerations(target),
			Containers: []corev1.Container{
				{
					Name:            "reserve",
					Image:           session.Spec.ToolImage,
					Command:         []string{"sh", "-c", "test -d /data && sleep 3600"},
					Resources:       kube.ZeroResourceRequirements(),
					SecurityContext: &corev1.SecurityContext{RunAsUser: &runAs, RunAsGroup: &runAs},
					VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: volume.Destination.PVC.Name,
						},
					},
				},
			},
		},
	}
	client := s.destination.Kubernetes.CoreV1().Pods(pod.Namespace)

	existing, err := client.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		existing, err = client.Create(ctx, pod, metav1.CreateOptions{})
	}

	if err != nil {
		return fmt.Errorf("create reservation Pod %s/%s: %w", pod.Namespace, name, err)
	}

	if existing.Labels[ManagedByLabel] != ManagedBy || existing.Labels[SessionKey] != session.ID {
		return fmt.Errorf(
			"reservation Pod %s/%s is not owned by session",
			existing.Namespace,
			existing.Name,
		)
	}

	volumeStatus := &session.Status.Volumes[volumeIndex(session, volume.Source.PVC.Name)]
	volumeStatus.Reservation.ConsumerPod = ClusterResourceRef{
		ClusterID:  session.Spec.DestinationCluster.ID,
		APIVersion: "v1",
		Kind:       "Pod",
		Namespace:  existing.Namespace,
		Name:       existing.Name,
		UID:        existing.UID,
	}

	return kube.WaitFor(
		ctx,
		s.interval,
		"reservation Pod "+existing.Namespace+"/"+existing.Name+" scheduling",
		func(waitCtx context.Context) (bool, error) {
			current, e := client.Get(waitCtx, name, metav1.GetOptions{})
			if e != nil {
				return false, e
			}

			if current.UID != existing.UID {
				return false, errors.New("reservation Pod UID changed")
			}

			if current.Status.Phase == corev1.PodFailed {
				return false, fmt.Errorf(
					"reservation Pod %s/%s failed",
					current.Namespace,
					current.Name,
				)
			}

			for _, condition := range current.Status.Conditions {
				if condition.Type == corev1.PodScheduled &&
					condition.Status == corev1.ConditionTrue {
					return true, nil
				}
			}

			return current.Status.Phase == corev1.PodRunning, nil
		},
	)
}

func reservationConsumerName(sessionID, pvc string) string {
	name := sessionID + "-reserve-" + pvc
	if len(name) <= 63 {
		return name
	}

	digest := sha256.Sum256([]byte(name))
	suffix := fmt.Sprintf("-%x", digest[:4])
	prefix := strings.TrimRight(name[:63-len(suffix)], "-")

	return prefix + suffix
}

func volumeIndex(session *Session, name string) int {
	for i, v := range session.Spec.Volumes {
		if v.Source.PVC.Name == name {
			return i
		}
	}

	return 0
}

func nodeTolerations(node *corev1.Node) []corev1.Toleration {
	var out []corev1.Toleration
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule ||
			taint.Effect == corev1.TaintEffectNoExecute {
			out = append(
				out,
				corev1.Toleration{
					Key:      taint.Key,
					Operator: corev1.TolerationOpEqual,
					Value:    taint.Value,
					Effect:   taint.Effect,
				},
			)
		}
	}

	return out
}

func (s *Service) selectTargetNode(
	ctx context.Context,
	requested string,
	sc *storagev1.StorageClass,
) (*corev1.Node, error) {
	nodes, err := s.destination.Kubernetes.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list destination nodes: %w", err)
	}

	for i := range nodes.Items {
		node := &nodes.Items[i]
		if requested != "" && requested != domain.AutoValue && requested != node.Name {
			continue
		}

		if !kube.NodeReadyAndSchedulable(node) || !kube.StorageClassAllowsNode(sc, node) {
			continue
		}

		return node, nil
	}

	if requested != "" && requested != domain.AutoValue {
		return nil, fmt.Errorf(
			"destination node %s is not Ready, schedulable, or allowed by StorageClass topology",
			requested,
		)
	}

	return nil, fmt.Errorf(
		"no Ready, schedulable destination node is compatible with StorageClass %s",
		sc.Name,
	)
}
