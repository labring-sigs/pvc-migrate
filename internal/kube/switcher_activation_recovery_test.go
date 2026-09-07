package kube

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestActivationRecoveryFencesMissingPVC(t *testing.T) {
	for _, scenario := range []string{"retained", "uid", "owner", "unowned", "policy", "claim", "nil-claim", "deleting", "consumer", "recreated"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			switcher, session, volume, _ := switcherFixture(t)

			claims := switcher.client.CoreV1().
				PersistentVolumeClaims(volume.DestinationPVC.Namespace)
			if err := claims.Delete(
				ctx,
				volume.DestinationPVC.Name,
				metav1.DeleteOptions{},
			); err != nil {
				t.Fatal(err)
			}

			pv, err := switcher.client.CoreV1().
				PersistentVolumes().
				Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			switch scenario {
			case "uid":
				pv.UID = "replacement"
			case "owner":
				pv.Labels[SessionKey] = "other"
			case "unowned":
				delete(pv.Labels, SessionKey)
			case "policy":
				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			case "claim":
				pv.Spec.ClaimRef.UID = "replacement"
			case "nil-claim":
				pv.Spec.ClaimRef = nil
			case "deleting":
				now := metav1.Now()
				pv.DeletionTimestamp = &now
			case "consumer":
				_, err = switcher.client.CoreV1().
					Pods(volume.DestinationPVC.Namespace).
					Create(ctx, &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "consumer",
							Namespace: volume.DestinationPVC.Namespace,
						},
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{
								{
									Name: "data",
									VolumeSource: corev1.VolumeSource{
										PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
											ClaimName: volume.DestinationPVC.Name,
										},
									},
								},
							},
						},
					}, metav1.CreateOptions{})
			case "recreated":
				_, err = claims.Create(
					ctx,
					&corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      volume.DestinationPVC.Name,
							Namespace: volume.DestinationPVC.Namespace,
							UID:       "replacement",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: volume.DestinationPV.Name,
						},
					},
					metav1.CreateOptions{},
				)
			}

			if err != nil {
				t.Fatal(err)
			}

			if _, err := switcher.client.CoreV1().
				PersistentVolumes().
				Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			if err := switcher.VerifyVolumesOfflineForSession(
				ctx,
				session.ID,
				[]*domain.VolumeSpec{volume},
			); err == nil {
				t.Fatal("ordinary validation accepted missing/replaced PVC")
			}

			err = switcher.VerifyActivationRecovery(ctx, session.ID, []*domain.VolumeSpec{volume})
			if scenario == "retained" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("recovery accepted unsafe storage")
			}
		})
	}
}
