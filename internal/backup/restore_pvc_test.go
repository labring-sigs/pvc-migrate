package backup

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRestorePreflightRequiresPublishedManifestCapacityAndMode(t *testing.T) {
	client, request := preflightFixture(
		t,
		&preflightObjectStore{
			manifest: []byte(
				`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"2Gi","volumeMode":"Filesystem","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`,
			),
		},
	)
	if _, err := Preflight(
		context.Background(),
		client,
		request,
		true,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("capacity category=%s error=%v", domain.CategoryOf(err), err)
	}

	request.Store, _ = objectstore.NewWithClient(
		&preflightObjectStore{
			manifest: []byte(
				`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"1Gi","volumeMode":"Block","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`,
			),
		},
		objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		objectstore.Credentials{},
	)
	if _, err := Preflight(
		context.Background(),
		client,
		request,
		true,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("mode category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestorePVCCreationRequiresStorageClassAndAccessMode(t *testing.T) {
	for _, test := range []struct {
		name         string
		storageClass string
		accessMode   string
	}{
		{name: "missing storage class", accessMode: string(corev1.ReadWriteOnce)},
		{name: "missing access mode", storageClass: "restore-sc"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, request := restorePVCCreationFixture(t, nil)
			request.DestinationStorageClass = test.storageClass
			request.DestinationAccessMode = test.accessMode

			_, err := Preflight(t.Context(), client, request, true)
			if domain.CategoryOf(err) != domain.ErrorValidation ||
				!strings.Contains(err.Error(), "requires --destination-storage-class") {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestRestorePVCCreationCapacity(t *testing.T) {
	for _, test := range []struct {
		name      string
		requested string
		want      string
	}{
		{name: "manifest default", want: "1Gi"},
		{name: "explicit expansion", requested: "2Gi", want: "2Gi"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, request := restorePVCCreationFixture(t, nil)
			request.DestinationCapacity = test.requested

			plan, err := Preflight(t.Context(), client, request, true)
			if err != nil {
				t.Fatal(err)
			}

			if !plan.CreatePVC || plan.Capacity != test.want {
				t.Fatalf("plan=%#v", plan)
			}
		})
	}

	client, request := restorePVCCreationFixture(t, nil)

	request.DestinationCapacity = "512Mi"
	if _, err := Preflight(
		t.Context(),
		client,
		request,
		true,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "below backup capacity") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestorePVCCreationRejectsInvalidAccessModeAndStorageClass(t *testing.T) {
	t.Run("invalid access mode", func(t *testing.T) {
		client, request := restorePVCCreationFixture(t, nil)
		request.DestinationAccessMode = string(corev1.ReadOnlyMany)

		_, err := Preflight(t.Context(), client, request, true)
		if domain.CategoryOf(err) != domain.ErrorValidation ||
			!strings.Contains(err.Error(), "unsupported --destination-access-mode") {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("missing storage class", func(t *testing.T) {
		client, request := restorePVCCreationFixture(t, nil)
		request.DestinationStorageClass = "missing-sc"

		_, err := Preflight(t.Context(), client, request, true)
		if domain.CategoryOf(err) != domain.ErrorPrecondition ||
			!strings.Contains(err.Error(), "read destination StorageClass missing-sc") {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestRestorePVCCreationRejectsForeignExistingPVC(t *testing.T) {
	client, request := restorePVCCreationFixture(t, nil)
	pvc := ownedRestorePVC(request, corev1.ClaimPending)

	pvc.Annotations[restoreNameAnnotation] = "another-recovery-point"
	if _, err := client.CoreV1().PersistentVolumeClaims(request.Namespace).
		Create(t.Context(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	request.DestinationStorageClass = ""
	request.DestinationAccessMode = ""

	_, err := Preflight(t.Context(), client, request, true)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "not owned by this restore") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestorePVCCreationRetriesOwnedPendingPVC(t *testing.T) {
	storeAPI := &preflightObjectStore{manifest: emptyBackupManifestForPath()}
	client, request := restorePVCCreationFixture(t, storeAPI)
	request.Path = "./mysql//current/"
	request.TargetNode = "worker-a"

	pvc := ownedRestorePVC(request, corev1.ClaimPending)
	if _, err := client.CoreV1().PersistentVolumeClaims(request.Namespace).
		Create(t.Context(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	stopErr := domain.NewError(
		domain.ErrorPrecondition,
		"tool image probe",
		"stop after retry assertions",
	)

	var prober *recordingBackupToolProber

	prober = &recordingBackupToolProber{onProbe: func(kube.ToolImageProbeOptions) {
		if len(prober.calls) == 1 {
			bindRestoreTestPVC(t, client, request, pvc.UID, pvc.UID)
			return
		}

		prober.err = stopErr
	}}
	request.ToolImageProber = prober

	if err := Run(t.Context(), client, request, true); !errors.Is(err, stopErr) {
		t.Fatalf("Run() error=%v", err)
	}

	if len(prober.calls) != 2 {
		t.Fatalf("probe calls=%d", len(prober.calls))
	}

	retryTarget := prober.calls[0].Targets[0]
	if retryTarget.NodeName != request.TargetNode || retryTarget.PVCName != request.PVCName ||
		retryTarget.RequiredPath != "mysql/current" || !retryTarget.CreatePath ||
		!retryTarget.WritablePVCMount ||
		!slices.Equal(retryTarget.Components, []string{kube.ToolComponentRclone}) {
		t.Fatalf("retry target=%#v", retryTarget)
	}

	if finalTarget := prober.calls[1].Targets[0]; finalTarget.NodeName != request.TargetNode {
		t.Fatalf("final target=%#v", finalTarget)
	}
}

func TestRestorePVCCreationProbeFailureRetainsPVC(t *testing.T) {
	client, request := restorePVCCreationFixture(t, nil)
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			create := testutil.MustType[ktesting.CreateAction](t, action)
			pvc := testutil.MustType[*corev1.PersistentVolumeClaim](t, create.GetObject())
			pvc.UID = types.UID("created-pvc")
			pvc.Status.Phase = corev1.ClaimPending

			return false, nil, nil
		},
	)

	probeErr := errors.New("probe failed")

	request.ToolImageProber = &recordingBackupToolProber{err: probeErr}
	if err := Run(t.Context(), client, request, true); !errors.Is(err, probeErr) {
		t.Fatalf("Run() error=%v", err)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims(request.Namespace).
		Get(t.Context(), request.PVCName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pvc.UID != types.UID("created-pvc") ||
		pvc.Annotations[restoreBucketAnnotation] != request.Store.Config().Bucket ||
		pvc.Annotations[restorePrefixAnnotation] != request.Store.Config().Prefix ||
		pvc.Annotations[restoreNameAnnotation] != request.Store.Config().Name {
		t.Fatalf("retained PVC=%#v", pvc)
	}
}

func TestRestorePVCCreationRejectsUIDChangeWhileBinding(t *testing.T) {
	client, request := restorePVCCreationFixture(t, nil)

	pvc := ownedRestorePVC(request, corev1.ClaimPending)
	if _, err := client.CoreV1().PersistentVolumeClaims(request.Namespace).
		Create(t.Context(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	request.ToolImageProber = &recordingBackupToolProber{onProbe: func(kube.ToolImageProbeOptions) {
		current, err := client.CoreV1().PersistentVolumeClaims(request.Namespace).
			Get(t.Context(), request.PVCName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		current.UID = types.UID("replacement-pvc")
		if _, err := client.CoreV1().PersistentVolumeClaims(request.Namespace).
			Update(t.Context(), current, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}}

	err := createRestorePVC(t.Context(), client, request, objectstore.Manifest{Capacity: "1Gi"})
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "identity changed while waiting") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestorePVCCreationRequiresProberForPendingPVC(t *testing.T) {
	client, request := restorePVCCreationFixture(t, nil)

	pvc := ownedRestorePVC(request, corev1.ClaimPending)
	if _, err := client.CoreV1().PersistentVolumeClaims(request.Namespace).
		Create(t.Context(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	err := createRestorePVC(t.Context(), client, request, objectstore.Manifest{Capacity: "1Gi"})
	if domain.CategoryOf(err) != domain.ErrorInternal ||
		!strings.Contains(err.Error(), "prober is required") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestorePVCCreationConcurrentAlreadyExistsValidatesPVC(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim)
	}{
		{
			name: "foreign ownership",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Annotations[restoreNameAnnotation] = "other"
			},
		},
		{
			name: "storage class mismatch",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				storageClass := "other-sc"
				pvc.Spec.StorageClassName = &storageClass
			},
		},
		{
			name: "access mode mismatch",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
			},
		},
		{
			name: "extra access mode",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = append(pvc.Spec.AccessModes, corev1.ReadWriteMany)
			},
		},
		{
			name: "volume mode mismatch",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				mode := corev1.PersistentVolumeBlock
				pvc.Spec.VolumeMode = &mode
			},
		},
		{
			name: "capacity mismatch",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("512Mi")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, request := restorePVCCreationFixture(t, nil)
			client.PrependReactor(
				"create",
				"persistentvolumeclaims",
				func(ktesting.Action) (bool, runtime.Object, error) {
					appeared := ownedRestorePVC(request, corev1.ClaimBound)
					appeared.Spec.VolumeName = "pv-concurrent"
					test.mutate(appeared)

					if err := client.Tracker().Create(
						corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"),
						appeared,
						request.Namespace,
					); err != nil {
						return true, nil, err
					}

					return true, nil, apierrors.NewAlreadyExists(
						schema.GroupResource{Resource: "persistentvolumeclaims"},
						request.PVCName,
					)
				},
			)

			err := createRestorePVC(
				t.Context(),
				client,
				request,
				objectstore.Manifest{Capacity: "1Gi"},
			)
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestRestoreTargetNodeConflictsWithMountedRWOConsumer(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{manifest: emptyBackupManifest()})
	request.AllowMounted = true

	request.TargetNode = "worker-b"
	if _, err := client.CoreV1().Pods(request.Namespace).Create(
		t.Context(),
		mountedConsumerPod("consumer", "worker-a"),
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	fakeClient, ok := client.(*fake.Clientset)
	if !ok {
		t.Fatalf("client type=%T, want *fake.Clientset", client)
	}

	fakeClient.PrependReactor(
		"list",
		"nodes",
		func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Resource: "nodes"},
				"",
				errors.New("node list is unavailable"),
			)
		},
	)

	_, err := Preflight(t.Context(), client, request, true)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "mounted RWO consumer requires node worker-a") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestoreTargetNodeConflictsWithPVTopology(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{manifest: emptyBackupManifest()})
	addTransferTestNode(t, client, "worker-a")
	addTransferTestNode(t, client, "worker-b")

	pv, err := client.CoreV1().PersistentVolumes().Get(t.Context(), "pv-data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.NodeAffinity = probePVNodeAffinity("worker-a")
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(t.Context(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	request.TargetNode = "worker-b"

	_, err = Preflight(t.Context(), client, request, true)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "PV topology requires node worker-a") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestoreTargetNodeAppearsInPlanAndExecutionProbe(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{manifest: emptyBackupManifest()})
	request.TargetNode = "worker-a"

	plan, err := Preflight(t.Context(), client, request, true)
	if err != nil {
		t.Fatal(err)
	}

	if plan.ToolNode != request.TargetNode {
		t.Fatalf("tool node=%q", plan.ToolNode)
	}

	probeErr := errors.New("stop after placement assertion")
	prober := &recordingBackupToolProber{err: probeErr}

	request.ToolImageProber = prober
	if err := Run(t.Context(), client, request, true); !errors.Is(err, probeErr) {
		t.Fatalf("Run() error=%v", err)
	}

	if len(prober.calls) != 1 || prober.calls[0].Targets[0].NodeName != request.TargetNode {
		t.Fatalf("probe calls=%#v", prober.calls)
	}
}

func TestRestorePVCCreationRunsPostCreateBindingRevalidation(t *testing.T) {
	client, request := restorePVCCreationFixture(t, nil)
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			create := testutil.MustType[ktesting.CreateAction](t, action)
			pvc := testutil.MustType[*corev1.PersistentVolumeClaim](t, create.GetObject())
			pvc.UID = types.UID("created-pvc")
			pvc.Status.Phase = corev1.ClaimPending

			return false, nil, nil
		},
	)

	prober := &recordingBackupToolProber{onProbe: func(kube.ToolImageProbeOptions) {
		bindRestoreTestPVC(
			t,
			client,
			request,
			types.UID("created-pvc"),
			types.UID("wrong-claim"),
		)
	}}
	request.ToolImageProber = prober

	err := Run(t.Context(), client, request, true)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "claimRef does not match") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(prober.calls) != 1 {
		t.Fatalf("probe calls=%d", len(prober.calls))
	}
}
