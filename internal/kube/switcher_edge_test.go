package kube

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	coretyped "k8s.io/client-go/kubernetes/typed/core/v1"
	storagetyped "k8s.io/client-go/kubernetes/typed/storage/v1"
	clienttesting "k8s.io/client-go/testing"
)

func TestVerifyVolumeOfflineChecksSourceAndDestinationConsumers(t *testing.T) {
	for _, test := range []struct {
		name      string
		namespace string
		claim     string
	}{
		{name: "source consumer", namespace: "app", claim: "data"},
		{name: "destination consumer", namespace: "system", claim: "data-migrated"},
	} {
		t.Run(test.name, func(t *testing.T) {
			switcher, _, volume, _ := switcherFixture(t)

			client := switcher.client
			if _, err := client.CoreV1().
				Pods(test.namespace).
				Create(context.Background(), &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: test.namespace, Name: "consumer"},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: test.claim,
								},
							},
						},
					}},
				}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}

			err := switcher.VerifyVolumeOffline(context.Background(), volume)
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}

	_, _, volume, _ := switcherFixture(t)
	if err := NewSwitcher(
		fake.NewClientset(),
	).VerifyVolumeOffline(context.Background(), volume); err == nil {
		t.Fatal("offline verification unexpectedly passed without recorded resources")
	}

	switcher, _, volume, _ := switcherFixture(t)
	if err := switcher.VerifyVolumeOffline(context.Background(), volume); err != nil {
		t.Fatalf("offline volume: %v", err)
	}
}

func TestVerifyVolumeOfflineRejectsCustomPVCFinalizer(t *testing.T) {
	switcher, _, volume, _ := switcherFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(context.Background(), volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pvc.Finalizers = []string{
		PVCProtectionFinalizer,
		"apps.victoriametrics.com/finalizer",
	}
	if _, err := client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = switcher.VerifyVolumeOffline(context.Background(), volume)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "apps.victoriametrics.com/finalizer") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestDeletePVCRejectsCustomFinalizerBeforeMutation(t *testing.T) {
	switcher, _, volume, _ := switcherFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(context.Background(), volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pvc.Finalizers = []string{"storage.example/protect"}
	if _, err := client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = switcher.deletePVC(context.Background(), volume.SourcePVC)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "storage.example/protect") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" &&
			action.GetResource().Resource == "persistentvolumeclaims" {
			t.Fatalf("PVC delete was issued: %#v", action)
		}
	}
}

func TestVerifyVolumeOfflineReturnsConsumerBeforeAttachmentTimeout(t *testing.T) {
	switcher, _, volume, _ := switcherFixture(t)

	client := testutil.MustType[*fake.Clientset](t, switcher.client)
	if _, err := client.CoreV1().
		Pods(volume.SourcePVC.Namespace).
		Create(context.Background(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: volume.SourcePVC.Namespace, Name: "consumer"},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: volume.SourcePVC.Name,
						},
					},
				},
			}},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.StorageV1().
		VolumeAttachments().
		Create(context.Background(), &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "attached-source"},
			Spec: storagev1.VolumeAttachmentSpec{
				Source: storagev1.VolumeAttachmentSource{
					PersistentVolumeName: &volume.SourcePV.Name,
				},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	switcher.poll = 100 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := time.Now()

	err := switcher.VerifyVolumeOffline(ctx, volume)
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("consumer check waited for attachment timeout: %s", elapsed)
	}
}

func TestVerifyVolumeOfflineRechecksPVCAndPVIdentity(t *testing.T) {
	tests := []struct {
		name     string
		category domain.ErrorCategory
		mutate   func(context.Context, *Switcher, *domain.VolumeSpec) error
	}{
		{
			name:     "source PVC UID changed",
			category: domain.ErrorConflict,
			mutate: func(ctx context.Context, switcher *Switcher, volume *domain.VolumeSpec) error {
				pvc, err := switcher.client.CoreV1().
					PersistentVolumeClaims(volume.SourcePVC.Namespace).
					Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}

				pvc.UID = "replacement-pvc"
				_, err = switcher.client.CoreV1().
					PersistentVolumeClaims(pvc.Namespace).
					Update(ctx, pvc, metav1.UpdateOptions{})

				return err
			},
		},
		{
			name:     "destination PVC UID changed",
			category: domain.ErrorConflict,
			mutate: func(ctx context.Context, switcher *Switcher, volume *domain.VolumeSpec) error {
				pvc, err := switcher.client.CoreV1().
					PersistentVolumeClaims(volume.DestinationPVC.Namespace).
					Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}

				pvc.UID = "replacement-pvc"
				_, err = switcher.client.CoreV1().
					PersistentVolumeClaims(pvc.Namespace).
					Update(ctx, pvc, metav1.UpdateOptions{})

				return err
			},
		},
		{
			name:     "source PVC pending",
			category: domain.ErrorPrecondition,
			mutate: func(ctx context.Context, switcher *Switcher, volume *domain.VolumeSpec) error {
				pvc, err := switcher.client.CoreV1().
					PersistentVolumeClaims(volume.SourcePVC.Namespace).
					Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}

				pvc.Status.Phase = corev1.ClaimPending
				_, err = switcher.client.CoreV1().
					PersistentVolumeClaims(pvc.Namespace).
					UpdateStatus(ctx, pvc, metav1.UpdateOptions{})

				return err
			},
		},
		{
			name:     "destination PVC points to another PV",
			category: domain.ErrorConflict,
			mutate: func(ctx context.Context, switcher *Switcher, volume *domain.VolumeSpec) error {
				pvc, err := switcher.client.CoreV1().
					PersistentVolumeClaims(volume.DestinationPVC.Namespace).
					Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}

				pvc.Spec.VolumeName = "other-pv"
				_, err = switcher.client.CoreV1().
					PersistentVolumeClaims(pvc.Namespace).
					Update(ctx, pvc, metav1.UpdateOptions{})

				return err
			},
		},
		{
			name:     "source PV UID changed",
			category: domain.ErrorConflict,
			mutate: func(ctx context.Context, switcher *Switcher, volume *domain.VolumeSpec) error {
				pv, err := switcher.client.CoreV1().
					PersistentVolumes().
					Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}

				pv.UID = "replacement-pv"
				_, err = switcher.client.CoreV1().
					PersistentVolumes().
					Update(ctx, pv, metav1.UpdateOptions{})

				return err
			},
		},
		{
			name:     "destination PV claimRef UID changed",
			category: domain.ErrorConflict,
			mutate: func(ctx context.Context, switcher *Switcher, volume *domain.VolumeSpec) error {
				pv, err := switcher.client.CoreV1().
					PersistentVolumes().
					Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}

				pv.Spec.ClaimRef.UID = "replacement-pvc"
				_, err = switcher.client.CoreV1().
					PersistentVolumes().
					Update(ctx, pv, metav1.UpdateOptions{})

				return err
			},
		},
		{
			name:     "source PV claimRef name changed",
			category: domain.ErrorConflict,
			mutate: func(ctx context.Context, switcher *Switcher, volume *domain.VolumeSpec) error {
				pv, err := switcher.client.CoreV1().
					PersistentVolumes().
					Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}

				pv.Spec.ClaimRef.Name = "other-pvc"
				_, err = switcher.client.CoreV1().
					PersistentVolumes().
					Update(ctx, pv, metav1.UpdateOptions{})

				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			switcher, _, volume, _ := switcherFixture(t)
			if err := test.mutate(ctx, switcher, volume); err != nil {
				t.Fatal(err)
			}

			err := switcher.VerifyVolumeOffline(ctx, volume)
			if domain.CategoryOf(err) != test.category {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestVerifyVolumesOfflineSharesInventoryAcrossVolumes(t *testing.T) {
	switcher, _, volume, _ := switcherFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)

	var pvcGets, pvGets, podLists, attachmentLists atomic.Int32
	client.PrependReactor(
		"get",
		"persistentvolumeclaims",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			pvcGets.Add(1)
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"persistentvolumes",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			pvGets.Add(1)
			return false, nil, nil
		},
	)
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		podLists.Add(1)
		return false, nil, nil
	})
	client.PrependReactor(
		"list",
		"volumeattachments",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			attachmentLists.Add(1)
			return false, nil, nil
		},
	)

	if err := switcher.VerifyVolumesOffline(
		context.Background(),
		[]*domain.VolumeSpec{volume, volume},
	); err != nil {
		t.Fatal(err)
	}

	if got := pvcGets.Load(); got != 2 {
		t.Fatalf("PVC GETs = %d, want 2", got)
	}

	if got := pvGets.Load(); got != 2 {
		t.Fatalf("PV GETs = %d, want 2", got)
	}

	if got := podLists.Load(); got != 2 {
		t.Fatalf("Pod Lists = %d, want 2 unique namespaces", got)
	}

	if got := attachmentLists.Load(); got != 1 {
		t.Fatalf("VolumeAttachment Lists = %d, want 1", got)
	}
}

func TestVerifyVolumesOfflineForSessionRejectsForeignOwnership(t *testing.T) {
	switcher, _, volume, _ := switcherFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), volume.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Labels = map[string]string{SessionKey: "foreign-session"}
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = switcher.VerifyVolumesOfflineForSession(
		context.Background(),
		"session",
		[]*domain.VolumeSpec{volume},
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestVerifyVolumeOfflineDeduplicatesRebindReferences(t *testing.T) {
	switcher, _, volume, _ := switcherFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)
	rebind := *volume
	rebind.DestinationPVC = volume.SourcePVC
	rebind.DestinationPV = volume.SourcePV

	var pvcGets, pvGets, podLists, attachmentLists atomic.Int32
	client.PrependReactor(
		"get",
		"persistentvolumeclaims",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			pvcGets.Add(1)
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"persistentvolumes",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			pvGets.Add(1)
			return false, nil, nil
		},
	)
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		podLists.Add(1)
		return false, nil, nil
	})
	client.PrependReactor(
		"list",
		"volumeattachments",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			attachmentLists.Add(1)
			return false, nil, nil
		},
	)

	if err := switcher.VerifyVolumeOffline(context.Background(), &rebind); err != nil {
		t.Fatal(err)
	}

	if pvcGets.Load() != 1 || pvGets.Load() != 1 || podLists.Load() != 1 ||
		attachmentLists.Load() != 1 {
		t.Fatalf(
			"deduplicated reads: PVC=%d PV=%d Pods=%d Attachments=%d",
			pvcGets.Load(),
			pvGets.Load(),
			podLists.Load(),
			attachmentLists.Load(),
		)
	}
}

func TestEnsureDetachedWaitsForCSIAttachmentState(t *testing.T) {
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "attachment"},
		Spec: storagev1.VolumeAttachmentSpec{
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: new("pv-data")},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: false},
	}
	switcher := NewSwitcher(fake.NewClientset(attachment))

	switcher.poll = time.Millisecond
	if err := switcher.ensureDetached(context.Background(), "pv-data"); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureDetachedFailsOnEmptyVolumeAttachmentList(t *testing.T) {
	base := fake.NewClientset()
	switcher := NewSwitcher(&nilVolumeAttachmentClient{Interface: base})
	switcher.poll = time.Millisecond

	err := switcher.ensureVolumesDetached(context.Background(), []string{"pv-data", "pv-logs"})
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if !strings.Contains(err.Error(), "pv-data,pv-logs") ||
		!strings.Contains(err.Error(), "empty object") {
		t.Fatalf("error=%v", err)
	}
}

func TestEnsureNoConsumersFailsOnEmptyPodList(t *testing.T) {
	client := &nilPodListClient{Interface: fake.NewClientset()}

	err := NewSwitcher(client).ensureNoConsumers(context.Background(), "app", "data")
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if !strings.Contains(err.Error(), "list Pods in app returned an empty object") {
		t.Fatalf("error=%v", err)
	}
}

func TestEnsureNoConsumersUsesPVCProtectionBoundaryForTerminalPods(t *testing.T) {
	for _, test := range []struct {
		name     string
		nodeName string
		wantErr  bool
	}{
		{name: "scheduled", nodeName: "node-a", wantErr: true},
		{name: "unscheduled", wantErr: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "terminal"},
				Spec: corev1.PodSpec{NodeName: test.nodeName, Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				}},
				Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
			}

			err := NewSwitcher(
				fake.NewClientset(pod),
			).ensureNoConsumers(context.Background(), "app", "data")
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v, wantErr=%t", err, test.wantErr)
			}
		})
	}
}

type nilPodListClient struct {
	kubernetes.Interface
}

func (c *nilPodListClient) CoreV1() coretyped.CoreV1Interface {
	return &nilPodListCore{CoreV1Interface: c.Interface.CoreV1()}
}

type nilPodListCore struct {
	coretyped.CoreV1Interface
}

func (c *nilPodListCore) Pods(namespace string) coretyped.PodInterface {
	return &nilPodInterface{PodInterface: c.CoreV1Interface.Pods(namespace)}
}

type nilPodInterface struct {
	coretyped.PodInterface
}

func (c *nilPodInterface) List(context.Context, metav1.ListOptions) (*corev1.PodList, error) {
	return nil, nil
}

type nilVolumeAttachmentClient struct {
	kubernetes.Interface
}

func (c *nilVolumeAttachmentClient) StorageV1() storagetyped.StorageV1Interface {
	return &nilVolumeAttachmentStorage{StorageV1Interface: c.Interface.StorageV1()}
}

type nilVolumeAttachmentStorage struct {
	storagetyped.StorageV1Interface
}

func (c *nilVolumeAttachmentStorage) VolumeAttachments() storagetyped.VolumeAttachmentInterface {
	return &nilVolumeAttachmentInterface{
		VolumeAttachmentInterface: c.StorageV1Interface.VolumeAttachments(),
	}
}

type nilVolumeAttachmentInterface struct {
	storagetyped.VolumeAttachmentInterface
}

func (c *nilVolumeAttachmentInterface) List(
	context.Context,
	metav1.ListOptions,
) (*storagev1.VolumeAttachmentList, error) {
	return nil, nil
}

//go:fix inline
func TestDeletePVCClassifiesPreconditionConflict(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data", UID: types.UID("pvc-uid"), ResourceVersion: "10",
	}}
	client := fake.NewClientset(pvc)
	client.PrependReactor(
		"delete",
		"persistentvolumeclaims",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "persistentvolumeclaims"},
				pvc.Name,
				errors.New("resource version changed"),
			)
		},
	)

	err := NewSwitcher(
		client,
	).deletePVC(context.Background(), domain.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID})
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestMarkPVPairRejectsForeignSessionOwnership(t *testing.T) {
	volume := &domain.VolumeSpec{
		SourcePV: domain.ObjectReference{Name: "source", UID: types.UID("source-uid")},
		DestinationPV: domain.ObjectReference{
			Name: "destination",
			UID:  types.UID("destination-uid"),
		},
	}
	client := fake.NewClientset(
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "source",
				UID:    volume.SourcePV.UID,
				Labels: map[string]string{SessionKey: "foreign"},
			},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "destination", UID: volume.DestinationPV.UID},
		},
	)

	err := NewSwitcher(client).markPVPair(context.Background(), "session", volume, false)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	source, getErr := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "source", metav1.GetOptions{})
	if getErr != nil {
		t.Fatal(getErr)
	}

	if source.Labels[SessionKey] != "foreign" {
		t.Fatalf("foreign ownership changed: labels=%v", source.Labels)
	}
}

func TestMarkPVPairAllowsMissingRollbackPV(t *testing.T) {
	volume := &domain.VolumeSpec{
		SourcePV: domain.ObjectReference{Name: "source", UID: types.UID("source-uid")},
		DestinationPV: domain.ObjectReference{
			Name: "destination",
			UID:  types.UID("destination-uid"),
		},
	}

	client := fake.NewClientset(&corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "source", UID: volume.SourcePV.UID},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		},
	})
	if err := NewSwitcher(
		client,
	).markPVPair(context.Background(), "session", volume, true); err != nil {
		t.Fatal(err)
	}

	source, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if source.Labels[SessionKey] != "session" || source.Labels[ResourceRoleLabel] != "active" ||
		source.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("active PV was not marked: %#v", source)
	}
}

func TestMarkPVPairRejectsMissingSourceDuringActivation(t *testing.T) {
	volume := &domain.VolumeSpec{
		SourcePV: domain.ObjectReference{Name: "source", UID: types.UID("source-uid")},
		DestinationPV: domain.ObjectReference{
			Name: "destination",
			UID:  types.UID("destination-uid"),
		},
	}

	client := fake.NewClientset(&corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "destination", UID: volume.DestinationPV.UID},
	})
	if err := NewSwitcher(
		client,
	).markPVPair(context.Background(), "session", volume, false); err == nil {
		t.Fatal("activation unexpectedly ignored a missing rollback PV")
	}
}

func TestRenamePVCIdempotentDestinationKeepsActiveRole(t *testing.T) {
	volume := &domain.VolumeSpec{
		SourcePVC: domain.ObjectReference{
			Namespace: "app",
			Name:      "old",
			UID:       types.UID("old-pvc-uid"),
		},
		DestinationPVC: domain.ObjectReference{Namespace: "app", Name: "new"},
		SourcePV:       domain.ObjectReference{Name: "pv", UID: types.UID("pv-uid")},
	}
	session := domain.NewSession("session", domain.SessionSpec{}, time.Unix(100, 0))

	client := fake.NewClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
			Name: "pv", UID: volume.SourcePV.UID,
			Labels: map[string]string{SessionKey: session.ID, ResourceRoleLabel: "active"},
		}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, ClaimRef: &corev1.ObjectReference{Namespace: "app", Name: "new", UID: types.UID("new-pvc-uid")}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace:   "app",
			Name:        "new",
			UID:         types.UID("new-pvc-uid"),
			Annotations: map[string]string{SessionKey: session.ID},
		}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
	)
	if _, err := NewSwitcher(
		client,
	).RenamePVC(context.Background(), session, volume, nil); err != nil {
		t.Fatal(err)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.Labels[ResourceRoleLabel] != "active" {
		t.Fatalf("role=%q labels=%v", pv.Labels[ResourceRoleLabel], pv.Labels)
	}
}

func TestRenamePVCIdempotentDestinationRejectsBindingDrift(t *testing.T) {
	volume := &domain.VolumeSpec{
		SourcePVC: domain.ObjectReference{
			Namespace: "app",
			Name:      "old",
			UID:       types.UID("old-pvc-uid"),
		},
		DestinationPVC: domain.ObjectReference{Namespace: "app", Name: "new"},
		SourcePV:       domain.ObjectReference{Name: "pv", UID: types.UID("pv-uid")},
	}
	session := domain.NewSession("session", domain.SessionSpec{}, time.Unix(100, 0))
	client := fake.NewClientset(
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: volume.SourcePV.UID},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: "app", Name: "new", UID: types.UID("replacement-pvc-uid"),
			}},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "new",
				UID:         types.UID("new-pvc-uid"),
				Annotations: map[string]string{SessionKey: session.ID},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv"},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
	)

	_, err := NewSwitcher(client).RenamePVC(context.Background(), session, volume, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRenamePVCIdempotentDestinationRejectsConsumers(t *testing.T) {
	volume := &domain.VolumeSpec{
		SourcePVC: domain.ObjectReference{
			Namespace: "app",
			Name:      "old",
			UID:       types.UID("old-pvc-uid"),
		},
		DestinationPVC: domain.ObjectReference{Namespace: "app", Name: "new"},
		SourcePV:       domain.ObjectReference{Name: "pv", UID: types.UID("pv-uid")},
	}
	session := domain.NewSession("session", domain.SessionSpec{}, time.Unix(100, 0))
	client := fake.NewClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
			Name: "pv", UID: volume.SourcePV.UID,
			Labels: map[string]string{SessionKey: session.ID, ResourceRoleLabel: "active"},
		}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, ClaimRef: &corev1.ObjectReference{Namespace: "app", Name: "new", UID: types.UID("new-pvc-uid")}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace:   "app",
			Name:        "new",
			UID:         types.UID("new-pvc-uid"),
			Annotations: map[string]string{SessionKey: session.ID},
		}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "consumer"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "new",
							},
						},
					},
				},
			},
		},
	)

	_, err := NewSwitcher(client).RenamePVC(context.Background(), session, volume, nil)
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestActivateVolumeValidatesPreconditionsBeforeMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domain.VolumeSpec, *domain.VolumeStatus)
	}{
		{
			name:   "final sync missing",
			mutate: func(_ *domain.VolumeSpec, status *domain.VolumeStatus) { status.Sync.FinalCompletedAt = nil },
		},
		{
			name:   "destination PV name missing",
			mutate: func(volume *domain.VolumeSpec, _ *domain.VolumeStatus) { volume.DestinationPV.Name = "" },
		},
		{
			name:   "destination PV UID missing",
			mutate: func(volume *domain.VolumeSpec, _ *domain.VolumeStatus) { volume.DestinationPV.UID = "" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			switcher, session, volume, status := switcherFixture(t)
			test.mutate(volume, status)

			err := switcher.ActivateVolume(context.Background(), session, volume, status, nil)
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			if _, getErr := switcher.client.CoreV1().
				PersistentVolumeClaims("app").
				Get(context.Background(), "data", metav1.GetOptions{}); getErr != nil {
				t.Fatalf("source PVC was mutated: %v", getErr)
			}
		})
	}
}

func TestActivateVolumeRejectsSourceBindingDriftBeforeDeletion(t *testing.T) {
	ctx := context.Background()
	switcher, session, volume, status := switcherFixture(t)

	pv, err := switcher.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.ClaimRef.UID = types.UID("replacement-pvc-uid")
	if _, err := switcher.client.CoreV1().
		PersistentVolumes().
		Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = switcher.ActivateVolume(ctx, session, volume, status, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := switcher.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("source PVC was deleted: %v", err)
	}
}

func TestActivateVolumeRejectsDestinationBindingDriftBeforeDeletion(t *testing.T) {
	ctx := context.Background()
	switcher, session, volume, status := switcherFixture(t)

	pv, err := switcher.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.ClaimRef.UID = types.UID("replacement-pvc-uid")
	if _, err := switcher.client.CoreV1().
		PersistentVolumes().
		Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = switcher.ActivateVolume(ctx, session, volume, status, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := switcher.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("temporary destination PVC was deleted: %v", err)
	}
}

func TestActivateVolumeResumesAtEveryProgressBoundary(t *testing.T) {
	for failurePoint := 1; failurePoint <= 3; failurePoint++ {
		t.Run(fmt.Sprintf("progress-%d", failurePoint), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			switcher, session, volume, status := switcherFixture(t)
			persisted := status.Activation
			calls := 0
			sentinel := errors.New("injected progress failure")

			err := switcher.ActivateVolume(ctx, session, volume, status, func() error {
				calls++
				if calls == failurePoint {
					return sentinel
				}

				persisted = status.Activation

				return nil
			})
			if !errors.Is(err, sentinel) {
				t.Fatalf("activation error=%v", err)
			}

			status.Activation = persisted
			if err := switcher.ActivateVolume(ctx, session, volume, status, nil); err != nil {
				t.Fatalf("resume activation: %v", err)
			}

			active, err := switcher.client.CoreV1().
				PersistentVolumeClaims("app").
				Get(ctx, "data", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if active.Spec.VolumeName != volume.DestinationPV.Name ||
				status.Activation.ActivatedAt == nil {
				t.Fatalf("active PVC/status: pvc=%#v status=%#v", active, status.Activation)
			}
		})
	}
}

func TestRollbackVolumeResumesAfterProgressFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	switcher, session, volume, status := switcherFixture(t)
	if err := switcher.ActivateVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatal(err)
	}

	persisted := status.Activation

	sentinel := errors.New("injected rollback progress failure")
	if err := switcher.RollbackVolume(
		ctx,
		session,
		volume,
		status,
		func() error { return sentinel },
	); !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf("rollback error=%v", err)
	}

	status.Activation = persisted
	if err := switcher.RollbackVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatalf("resume rollback: %v", err)
	}

	active, err := switcher.client.CoreV1().
		PersistentVolumeClaims("app").
		Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if active.Spec.VolumeName != volume.SourcePV.Name || status.Activation.RolledBackAt == nil {
		t.Fatalf("rollback PVC/status: pvc=%#v status=%#v", active, status.Activation)
	}
}

func TestRollbackVolumeRejectsForeignActivePVC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	switcher, session, volume, status := switcherFixture(t)
	if err := switcher.ActivateVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatal(err)
	}

	active, _ := switcher.client.CoreV1().
		PersistentVolumeClaims("app").
		Get(ctx, "data", metav1.GetOptions{})

	active.Annotations[SessionKey] = "other-session"
	if _, err := switcher.client.CoreV1().
		PersistentVolumeClaims("app").
		Update(ctx, active, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err := switcher.RollbackVolume(ctx, session, volume, status, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRollbackVolumeRejectsDestinationBindingDriftBeforeDeletion(t *testing.T) {
	ctx := context.Background()

	switcher, session, volume, status := switcherFixture(t)
	if err := switcher.ActivateVolume(ctx, session, volume, status, nil); err != nil {
		t.Fatal(err)
	}

	pv, err := switcher.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.ClaimRef.UID = types.UID("replacement-pvc-uid")
	if _, err := switcher.client.CoreV1().
		PersistentVolumes().
		Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = switcher.RollbackVolume(ctx, session, volume, status, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	active, err := switcher.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil || active.Spec.VolumeName != volume.DestinationPV.Name {
		t.Fatalf("active destination PVC changed: pvc=%#v error=%v", active, err)
	}
}

func TestRollbackVolumeFastPathRequiresSourcePVCIdentityOrSessionOwnership(t *testing.T) {
	tests := []struct {
		name      string
		uid       types.UID
		annotate  bool
		wantError domain.ErrorCategory
	}{
		{name: "original PVC UID", uid: types.UID("source-pvc-uid")},
		{
			name:     "recovered PVC owned by session",
			uid:      types.UID("recovered-pvc-uid"),
			annotate: true,
		},
		{
			name:      "replacement PVC without ownership",
			uid:       types.UID("foreign-pvc-uid"),
			wantError: domain.ErrorConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			switcher, session, volume, status := switcherFixture(t)

			pvc, err := switcher.client.CoreV1().
				PersistentVolumeClaims(volume.SourcePVC.Namespace).
				Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			pvc.UID = test.uid
			if test.annotate {
				pvc.Annotations = map[string]string{SessionKey: session.ID}
			} else {
				pvc.Annotations = nil
			}

			if _, err := switcher.client.CoreV1().
				PersistentVolumeClaims(pvc.Namespace).
				Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			pv, err := switcher.client.CoreV1().
				PersistentVolumes().
				Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			pv.Spec.ClaimRef.UID = test.uid
			if _, err := switcher.client.CoreV1().
				PersistentVolumes().
				Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			err = switcher.RollbackVolume(ctx, session, volume, status, nil)
			if test.wantError != "" {
				if domain.CategoryOf(err) != test.wantError {
					t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if status.Activation.RolledBackAt == nil {
				t.Fatalf("rollback status: %#v", status.Activation)
			}
		})
	}
}

func TestRollbackVolumeRejectsSourcePVCBoundToUnexpectedPV(t *testing.T) {
	ctx := context.Background()
	switcher, session, volume, status := switcherFixture(t)

	pvc, err := switcher.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pvc.Spec.VolumeName = "unexpected-pv"

	pvc.Annotations = map[string]string{SessionKey: session.ID}
	if _, err := switcher.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = switcher.RollbackVolume(ctx, session, volume, status, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestDeletePVCUsesUIDAndResourceVersionPreconditions(t *testing.T) {
	ctx := context.Background()
	uid := types.UID("pvc-uid")
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data", UID: uid, ResourceVersion: "17",
	}})

	var preconditions *metav1.Preconditions
	client.PrependReactor(
		"delete",
		"persistentvolumeclaims",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			preconditions = testutil.MustType[clienttesting.DeleteAction](t, action).
				GetDeleteOptions().Preconditions
			return false, nil, nil
		},
	)
	switcher := NewSwitcher(client)

	switcher.poll = time.Millisecond
	if err := switcher.deletePVC(
		ctx,
		domain.ObjectReference{Namespace: "app", Name: "data", UID: uid},
	); err != nil {
		t.Fatal(err)
	}

	if preconditions == nil || preconditions.UID == nil || *preconditions.UID != uid ||
		preconditions.ResourceVersion == nil ||
		*preconditions.ResourceVersion != "17" {
		t.Fatalf("delete preconditions: %#v", preconditions)
	}
}

func TestDeletePVCDetectsNameReuseDuringDeletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	uid := types.UID("original")
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data", UID: uid, ResourceVersion: "17",
	}})
	client.PrependReactor(
		"delete",
		"persistentvolumeclaims",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			resource := corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
			if err := client.Tracker().Delete(resource, "app", "data"); err != nil {
				return true, nil, err
			}

			replacement := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "app",
				Name:            "data",
				UID:             types.UID("replacement"),
				ResourceVersion: "18",
			}}
			if err := client.Tracker().Create(resource, replacement, "app"); err != nil {
				return true, nil, err
			}

			return true, nil, nil
		},
	)
	switcher := NewSwitcher(client)
	switcher.poll = time.Millisecond

	err := switcher.deletePVC(ctx, domain.ObjectReference{Namespace: "app", Name: "data", UID: uid})
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestReservePVIdempotencyAndConflictChecks(t *testing.T) {
	ctx := context.Background()
	ref := domain.ObjectReference{Name: "pv", UID: types.UID("pv-uid")}
	t.Run("bound claim is idempotent", func(t *testing.T) {
		pvcUID := types.UID("pvc-uid")
		client := fake.NewClientset(
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: pvcUID},
				Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv"},
			},
			&corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "pv",
					UID:    ref.UID,
					Labels: map[string]string{SessionKey: "session"},
				},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef: &corev1.ObjectReference{Namespace: "app", Name: "data", UID: pvcUID},
				},
				Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
			},
		)
		switcher := NewSwitcher(client)

		switcher.poll = time.Millisecond
		if err := switcher.reservePV(ctx, ref, "app", "data", "session"); err != nil {
			t.Fatal(err)
		}

		updates := 0
		for _, action := range client.Actions() {
			if action.GetVerb() == "update" &&
				action.GetResource().Resource == "persistentvolumes" {
				updates++
			}
		}

		if updates != 0 {
			t.Fatalf("idempotent reservation updates=%d", updates)
		}
	})
	t.Run("stale claim UID is cleared", func(t *testing.T) {
		client := fake.NewClientset(&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv",
				UID:    ref.UID,
				Labels: map[string]string{SessionKey: "session"},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       types.UID("deleted-pvc"),
				},
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		})
		switcher := NewSwitcher(client)

		switcher.poll = time.Millisecond
		if err := switcher.reservePV(ctx, ref, "app", "data", "session"); err != nil {
			t.Fatal(err)
		}

		pv, _ := client.CoreV1().PersistentVolumes().Get(ctx, "pv", metav1.GetOptions{})
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != "" ||
			pv.Spec.ClaimRef.Namespace != "app" ||
			pv.Spec.ClaimRef.Name != "data" {
			t.Fatalf("reserved claimRef: %#v", pv.Spec.ClaimRef)
		}
	})

	for _, test := range []struct {
		name        string
		claimRefUID types.UID
	}{
		{name: "stale claim UID", claimRefUID: types.UID("deleted-pvc")},
		{name: "missing claim UID"},
	} {
		t.Run(test.name+" with replacement PVC", func(t *testing.T) {
			client := fake.NewClientset(
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "app",
						Name:      "data",
						UID:       types.UID("replacement-pvc"),
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "pv",
						UID:    ref.UID,
						Labels: map[string]string{SessionKey: "session"},
					},
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Namespace: "app",
							Name:      "data",
							UID:       test.claimRefUID,
						},
					},
					Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
				},
			)
			switcher := NewSwitcher(client)
			switcher.poll = time.Millisecond

			err := switcher.reservePV(ctx, ref, "app", "data", "session")
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			pv, getErr := client.CoreV1().PersistentVolumes().Get(ctx, "pv", metav1.GetOptions{})
			if getErr != nil {
				t.Fatal(getErr)
			}

			if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != test.claimRefUID {
				t.Fatalf("PV claimRef was changed: %#v", pv.Spec.ClaimRef)
			}
		})
	}

	for _, test := range []struct {
		name string
		pv   *corev1.PersistentVolume
		ref  domain.ObjectReference
	}{
		{
			name: "foreign owner",
			ref:  ref,
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: ref.UID, Labels: map[string]string{SessionKey: "other"}},
				Status:     corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
			},
		},
		{
			name: "UID changed",
			ref:  ref,
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: types.UID("replacement")},
				Status:     corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			switcher := NewSwitcher(fake.NewClientset(test.pv))
			switcher.poll = time.Millisecond

			err := switcher.reservePV(ctx, test.ref, "app", "data", "session")
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestEnsureRetainPreservesPolicyAndRejectsIdentityConflicts(t *testing.T) {
	ctx := context.Background()
	ref := domain.ObjectReference{Name: "pv", UID: types.UID("pv-uid")}
	client := fake.NewClientset(&corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: ref.Name, UID: ref.UID},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		},
	})

	switcher := NewSwitcher(client)
	if err := switcher.ensureRetain(ctx, ref, "session", "source"); err != nil {
		t.Fatal(err)
	}

	updatesAfterFirstRole := pvUpdateCount(client)

	if err := switcher.ensureRetain(ctx, ref, "session", "active"); err != nil {
		t.Fatalf("idempotent retain: %v", err)
	}

	if pvUpdateCount(client) != updatesAfterFirstRole+1 {
		t.Fatalf("role change did not update PV exactly once: updates=%d", pvUpdateCount(client))
	}

	updatesAfterRoleChange := pvUpdateCount(client)

	if err := switcher.ensureRetain(ctx, ref, "session", "active"); err != nil {
		t.Fatalf("repeat retain: %v", err)
	}

	if pvUpdateCount(client) != updatesAfterRoleChange {
		t.Fatalf("unchanged retain issued update: updates=%d", pvUpdateCount(client))
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain ||
		pv.Annotations[OriginalPolicyAnnotation] != string(corev1.PersistentVolumeReclaimDelete) ||
		pv.Labels[ResourceRoleLabel] != "active" {
		t.Fatalf("retained PV: %#v", pv)
	}

	foreign := pv.DeepCopy()
	foreign.Name = "foreign"

	foreign.Labels[SessionKey] = "other-session"
	if _, err := client.CoreV1().
		PersistentVolumes().
		Create(ctx, foreign, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = switcher.ensureRetain(
		ctx,
		domain.ObjectReference{Name: foreign.Name, UID: foreign.UID},
		"session",
		"source",
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("foreign owner category=%s error=%v", domain.CategoryOf(err), err)
	}

	err = switcher.ensureRetain(
		ctx,
		domain.ObjectReference{Name: ref.Name, UID: types.UID("replacement")},
		"session",
		"source",
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("UID conflict category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestVerifyBindingValidatesBothSidesOfBinding(t *testing.T) {
	base := func() (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, domain.ObjectReference) {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "data",
				UID:       types.UID("pvc-uid"),
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv"},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: types.UID("pv-uid")},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID,
			}},
		}

		return pvc, pv, domain.ObjectReference{Name: pv.Name, UID: pv.UID}
	}

	pvc, pv, ref := base()
	if err := NewSwitcher(
		fake.NewClientset(pv),
	).verifyBinding(context.Background(), pvc, ref); err != nil {
		t.Fatalf("valid binding: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *domain.ObjectReference)
	}{
		{
			"PVC pending",
			func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *domain.ObjectReference) {
				pvc.Status.Phase = corev1.ClaimPending
			},
		},
		{
			"PVC bound elsewhere",
			func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *domain.ObjectReference) {
				pvc.Spec.VolumeName = "other"
			},
		},
		{
			"PV UID changed",
			func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *domain.ObjectReference) {
				pv.UID = "replacement"
			},
		},
		{
			"claimRef absent",
			func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *domain.ObjectReference) {
				pv.Spec.ClaimRef = nil
			},
		},
		{
			"claimRef PVC UID changed",
			func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *domain.ObjectReference) {
				pv.Spec.ClaimRef.UID = "replacement"
			},
		},
		{
			"claimRef namespace changed",
			func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *domain.ObjectReference) {
				pv.Spec.ClaimRef.Namespace = "other"
			},
		},
		{
			"claimRef name changed",
			func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *domain.ObjectReference) {
				pv.Spec.ClaimRef.Name = "other"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pvc, pv, ref := base()
			test.mutate(pvc, pv, &ref)

			err := NewSwitcher(fake.NewClientset(pv)).verifyBinding(context.Background(), pvc, ref)
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestCreateAndValidateActivePVCRejectUnexpectedObjects(t *testing.T) {
	switcher, session, volume, _ := switcherFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)
	existing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: volume.SourcePVC.Namespace,
			Name:      volume.SourcePVC.Name,
			UID:       types.UID("foreign"),
			Annotations: map[string]string{
				SessionKey: "other-session",
			},
		},
		Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "other-pv"},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	resource := corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
	if err := client.Tracker().Delete(resource, existing.Namespace, existing.Name); err != nil {
		t.Fatal(err)
	}

	if err := client.Tracker().Create(resource, existing, existing.Namespace); err != nil {
		t.Fatal(err)
	}

	_, err := switcher.createActivePVC(
		context.Background(),
		session,
		volume,
		volume.DestinationPV,
		volume.StorageClass,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("existing PVC category=%s error=%v", domain.CategoryOf(err), err)
	}

	validationClient := fake.NewClientset()
	validationClient.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			options := testutil.MustType[interface {
				GetCreateOptions() metav1.CreateOptions
			}](t, action).GetCreateOptions()
			if len(options.DryRun) > 0 {
				return true, nil, errors.New("admission rejected PVC")
			}

			return false, nil, nil
		},
	)

	err = NewSwitcher(
		validationClient,
	).validateActivePVC(context.Background(), session, volume, volume.DestinationPV, volume.StorageClass)
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("validation category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCreateActivePVCRejectsReplacementWhileWaiting(t *testing.T) {
	switcher, session, volume, _ := switcherFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)

	resource := corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
	if err := client.Tracker().
		Delete(resource, volume.SourcePVC.Namespace, volume.SourcePVC.Name); err != nil {
		t.Fatal(err)
	}

	var created *corev1.PersistentVolumeClaim
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			created = testutil.MustActionObject[*corev1.PersistentVolumeClaim](t, action)
			created.UID = "created-uid"
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"persistentvolumeclaims",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			if created == nil {
				return false, nil, nil
			}

			replacement := created.DeepCopy()
			replacement.UID = "replacement-uid"
			replacement.Status.Phase = corev1.ClaimBound

			return true, replacement, nil
		},
	)

	switcher.poll = time.Millisecond

	_, err := switcher.createActivePVC(
		context.Background(),
		session,
		volume,
		volume.DestinationPV,
		volume.StorageClass,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replaced while waiting") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func configureRenameFixture(t *testing.T) (*Switcher, *domain.Session, *domain.VolumeSpec) {
	t.Helper()
	switcher, session, volume, _ := switcherFixture(t)
	volume.DestinationPVC = domain.ObjectReference{Namespace: "app", Name: "data-renamed"}
	return switcher, session, volume
}

func TestRenamePVCRejectsSourceBindingDriftBeforeDeletion(t *testing.T) {
	ctx := context.Background()
	switcher, session, volume := configureRenameFixture(t)

	pv, err := switcher.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.ClaimRef.UID = types.UID("replacement-pvc-uid")
	if _, err := switcher.client.CoreV1().
		PersistentVolumes().
		Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	_, err = switcher.RenamePVC(ctx, session, volume, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := switcher.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("source PVC was deleted: %v", err)
	}
}

func TestRenamePVCSuccessIdempotencyAndRecovery(t *testing.T) {
	for _, test := range []struct {
		name         string
		progressFail bool
	}{
		{name: "success"},
		{name: "resume after source deletion", progressFail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			switcher, session, volume := configureRenameFixture(t)
			if test.progressFail {
				sentinel := errors.New("injected rename progress failure")
				if _, err := switcher.RenamePVC(
					ctx,
					session,
					volume,
					func() error { return sentinel },
				); !errors.Is(
					err,
					sentinel,
				) {
					t.Fatalf("rename error=%v", err)
				}
			}

			renamed, err := switcher.RenamePVC(ctx, session, volume, nil)
			if err != nil {
				t.Fatal(err)
			}

			if renamed.Namespace != "app" || renamed.Name != "data-renamed" ||
				renamed.Spec.VolumeName != volume.SourcePV.Name ||
				renamed.Annotations[SessionKey] != session.ID {
				t.Fatalf("renamed PVC: %#v", renamed)
			}

			pv, _ := switcher.client.CoreV1().
				PersistentVolumes().
				Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
			if pv.Labels[ResourceRoleLabel] != "active" || pv.Spec.ClaimRef == nil ||
				pv.Spec.ClaimRef.UID != renamed.UID {
				t.Fatalf("renamed PV: %#v", pv)
			}

			again, err := switcher.RenamePVC(ctx, session, volume, nil)
			if err != nil || again.UID != renamed.UID {
				t.Fatalf("idempotent rename: pvc=%#v error=%v", again, err)
			}
		})
	}
}

func TestRenamePVCRejectsExistingDestination(t *testing.T) {
	switcher, session, volume := configureRenameFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)

	err := client.Tracker().
		Create(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "data-renamed",
				UID:       types.UID("foreign"),
			},
			Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "other-pv"},
		}, "app")
	if err != nil {
		t.Fatal(err)
	}

	_, err = switcher.RenamePVC(context.Background(), session, volume, nil)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}
