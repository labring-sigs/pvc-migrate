package kube

import (
	"context"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPVCRebindRecoveryFencesInterruptedIdentity(t *testing.T) {
	for _, scenario := range []string{"deleted", "reserved", "created", "source-recreated", "pv-uid", "owner", "policy", "claim", "nil-claim", "deleting", "foreign-target", "target-uid", "target-binding", "target-deleting", "source-consumer", "target-consumer", "attached"} {
		t.Run(scenario, func(t *testing.T) {
			from := domain.ObjectReference{Namespace: "source", Name: "data", UID: "source-uid"}
			to := domain.ObjectReference{Namespace: "target", Name: "data"}
			pvRef := domain.ObjectReference{Name: "pv", UID: "pv-uid"}
			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   pvRef.Name,
					UID:    pvRef.UID,
					Labels: map[string]string{SessionKey: "session"},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
					ClaimRef: &corev1.ObjectReference{
						Namespace: from.Namespace,
						Name:      from.Name,
						UID:       from.UID,
					},
				},
			}

			objects := []runtime.Object{pv}
			switch scenario {
			case "reserved":
				pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: to.Namespace, Name: to.Name}
			case "source-recreated":
				objects = append(
					objects,
					&corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: from.Namespace,
							Name:      from.Name,
							UID:       "replacement",
						},
					},
				)
			case "pv-uid":
				pv.UID = "replacement"
			case "owner":
				pv.Labels[SessionKey] = "other"
			case "policy":
				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			case "claim":
				pv.Spec.ClaimRef.UID = "other"
			case "nil-claim":
				pv.Spec.ClaimRef = nil
			case "deleting":
				now := metav1.Now()
				pv.DeletionTimestamp = &now
			case "source-consumer", "target-consumer":
				ref := from
				if scenario == "target-consumer" {
					ref = to
				}

				objects = append(
					objects,
					&corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: "consumer"},
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{
								{
									Name: "data",
									VolumeSource: corev1.VolumeSource{
										PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
											ClaimName: ref.Name,
										},
									},
								},
							},
						},
					},
				)
			case "attached":
				objects = append(
					objects,
					&storagev1.VolumeAttachment{
						ObjectMeta: metav1.ObjectMeta{Name: "attachment"},
						Spec: storagev1.VolumeAttachmentSpec{
							Source: storagev1.VolumeAttachmentSource{
								PersistentVolumeName: &pvRef.Name,
							},
						},
						Status: storagev1.VolumeAttachmentStatus{Attached: true},
					},
				)
			default:
				if scenario != "deleted" {
					target := &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Namespace:   to.Namespace,
							Name:        to.Name,
							UID:         "target-uid",
							Annotations: map[string]string{SessionKey: "session"},
						},
						Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: pvRef.Name},
						Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
					}

					pv.Spec.ClaimRef = &corev1.ObjectReference{
						Namespace: to.Namespace,
						Name:      to.Name,
						UID:       target.UID,
					}
					switch scenario {
					case "foreign-target":
						target.Annotations[SessionKey] = "other"
					case "target-uid":
						to.UID = "expected"
					case "target-binding":
						pv.Spec.ClaimRef.UID = "other"
					case "target-deleting":
						now := metav1.Now()
						target.DeletionTimestamp = &now
					}

					objects = append(objects, target)
				}
			}

			client := fake.NewClientset(objects...)
			switcher := NewSwitcher(client)
			switcher.poll = time.Millisecond

			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()

			err := switcher.VerifyPVCRebindRecovery(ctx, "session", from, to, pvRef)

			wantSuccess := scenario == "deleted" || scenario == "reserved" || scenario == "created"
			if (err == nil) != wantSuccess {
				t.Fatalf("error=%v wantSuccess=%v", err, wantSuccess)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("validation mutated storage: %v", action)
				}
			}
		})
	}
}
