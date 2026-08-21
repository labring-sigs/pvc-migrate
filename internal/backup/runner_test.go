package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type preflightObjectStore struct {
	manifest  []byte
	puts      int
	onPut     func()
	getErr    error
	deleteErr error
}

func (f *preflightObjectStore) HeadObject(
	context.Context,
	*s3.HeadObjectInput,
	...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	return nil, preflightMissingObjectError{}
}

func (f *preflightObjectStore) GetObject(
	_ context.Context,
	_ *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}

	if len(f.manifest) == 0 {
		return nil, preflightMissingObjectError{}
	}

	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.manifest))}, nil
}

func (f *preflightObjectStore) PutObject(
	context.Context,
	*s3.PutObjectInput,
	...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	f.puts++
	if f.onPut != nil {
		f.onPut()
	}

	return &s3.PutObjectOutput{ETag: aws.String("etag")}, nil
}

func (f *preflightObjectStore) DeleteObject(
	context.Context,
	*s3.DeleteObjectInput,
	...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (*preflightObjectStore) ListObjectsV2(
	context.Context,
	*s3.ListObjectsV2Input,
	...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

type preflightMissingObjectError struct{}

func (preflightMissingObjectError) Error() string                 { return "missing" }
func (preflightMissingObjectError) ErrorCode() string             { return "NoSuchKey" }
func (preflightMissingObjectError) ErrorMessage() string          { return "missing" }
func (preflightMissingObjectError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func preflightFixture(t *testing.T, client objectstore.API) (kubernetes.Interface, Request) {
	t.Helper()

	mode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "data", UID: types.UID("pvc")},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "pv-data",
			VolumeMode:  &mode,
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv")},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			ClaimRef: &corev1.ObjectReference{Namespace: "default", Name: "data", UID: pvc.UID},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	store, err := objectstore.NewWithClient(
		client,
		objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		objectstore.Credentials{AccessKey: "key", SecretKey: "secret"},
	)
	if err != nil {
		t.Fatal(err)
	}

	clientset := fake.NewClientset(pvc, pv)

	return clientset, Request{
		ID:        "test",
		Namespace: "default",
		PVCName:   "data",
		Store:     store,
	}
}

func TestRunWithCleanupTimeout(t *testing.T) {
	t.Run("deadline bounds cleanup", func(t *testing.T) {
		started := time.Now()

		err := runWithCleanupTimeout(10*time.Millisecond, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		if domain.CategoryOf(err) != domain.ErrorTimeout ||
			!errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
		}

		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("cleanup elapsed=%v", elapsed)
		}
	})
	t.Run("preserves cleanup error", func(t *testing.T) {
		wantErr := domain.NewError(domain.ErrorConflict, "release lock", "ownership changed")
		if err := runWithCleanupTimeout(
			time.Second,
			func(context.Context) error { return wantErr },
		); !errors.Is(
			err,
			wantErr,
		) {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("nil cleanup", func(t *testing.T) {
		if err := runWithCleanupTimeout(time.Second, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("preserves parent values while ignoring cancellation", func(t *testing.T) {
		type contextKey struct{}
		parent, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "value"))
		cancel()
		if err := runWithPreservedCleanupTimeout(parent, time.Second, func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Fatalf("cleanup inherited cancellation: %v", ctx.Err())
			}
			if got := ctx.Value(contextKey{}); got != "value" {
				t.Fatalf("context value=%v", got)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRunBackupPreservesOperationAndLockCleanupErrors(t *testing.T) {
	operationErr := errors.New("manifest read failed")
	cleanupErr := errors.New("lock delete failed")
	storeAPI := &preflightObjectStore{getErr: operationErr, deleteErr: cleanupErr}
	client, request := preflightFixture(t, storeAPI)

	err := runBackup(context.Background(), client, request, "pvc", "pv")
	if !errors.Is(err, operationErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("error=%v", err)
	}
}

func TestBackupTargetLeaseSerializesOneRecoveryPoint(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	fakeClient := client.(*fake.Clientset)
	fakeClient.PrependReactor(
		"create",
		"leases",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			lease, err := testutil.ActionObject[*coordinationv1.Lease](action)
			if err != nil {
				return true, nil, err
			}
			lease.UID = types.UID("lease-" + lease.Name)
			return false, nil, nil
		},
	)
	request.SessionStore = kube.NewConfigMapSessionStore(fakeClient)
	request.SessionNamespace = "sessions"

	firstCtx, first, cancelFirst, err := acquireBackupTargetLock(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelFirst()
	if firstCtx == nil || first == nil {
		t.Fatal("first target Lease was not acquired")
	}

	secondCtx, second, cancelSecond, err := acquireBackupTargetLock(context.Background(), request)
	if secondCtx != nil || second != nil || cancelSecond != nil || domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("second context=%v lock=%v cancel=%v category=%q error=%v", secondCtx, second, cancelSecond, domain.CategoryOf(err), err)
	}

	if err := first.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	secondCtx, second, cancelSecond, err = acquireBackupTargetLock(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	cancelSecond()
	if secondCtx == nil || second == nil {
		t.Fatal("target Lease was not reacquired after deletion")
	}
	if err := second.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}

	leases, err := fakeClient.CoordinationV1().Leases("sessions").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(leases.Items) != 0 {
		t.Fatalf("temporary target Leases remain: %#v", leases.Items)
	}
}

func TestBackupTargetLockIDUsesTheFullRecoveryPoint(t *testing.T) {
	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Prefix: "team", Name: "daily"},
		objectstore.Credentials{AccessKey: "key", SecretKey: "secret"},
	)
	if err != nil {
		t.Fatal(err)
	}
	other, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Prefix: "other", Name: "daily"},
		objectstore.Credentials{AccessKey: "key", SecretKey: "secret"},
	)
	if err != nil {
		t.Fatal(err)
	}

	first := backupTargetLockID(store)
	if first != backupTargetLockID(store) || first == backupTargetLockID(other) {
		t.Fatalf("target lock IDs first=%q other=%q", first, backupTargetLockID(other))
	}
	if !strings.HasPrefix(first, "backup-target-") || len(first) > 63 {
		t.Fatalf("target lock ID is not a DNS label-sized value: %q", first)
	}
}

func TestPreflightLogsStartBeforeValidationFailure(t *testing.T) {
	var logs bytes.Buffer

	request := Request{Logger: slog.New(slog.NewTextHandler(&logs, nil))}
	if _, err := Preflight(context.Background(), nil, request, false); err == nil {
		t.Fatal("expected preflight validation failure")
	}

	if !strings.Contains(logs.String(), "backup preflight started") {
		t.Fatalf("logs=%q", logs.String())
	}
}

func TestExecutionRevalidationLogsItsDistinctPhase(t *testing.T) {
	var logs bytes.Buffer

	request := Request{Logger: slog.New(slog.NewTextHandler(&logs, nil))}
	if _, err := preflight(
		context.Background(),
		nil,
		request,
		false,
		"execution revalidation",
	); err == nil {
		t.Fatal("expected execution revalidation failure")
	}

	if !strings.Contains(logs.String(), "backup execution revalidation started") {
		t.Fatalf("logs=%q", logs.String())
	}
}

func TestPreflightRejectsOfflineMountedAndOnlineRWOP(t *testing.T) {
	mode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "data", UID: "pvc"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "pv-data",
			VolumeMode:  &mode,
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: "pv"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace,
				Name:      pvc.Name,
				UID:       pvc.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "consumer"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				},
			},
		},
	}
	client := fake.NewClientset(pvc, pv, pod)
	store, _ := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Name: "daily"},
		objectstore.Credentials{},
	)

	request := Request{Namespace: "default", PVCName: "data", Store: store}
	if _, err := Preflight(
		context.Background(),
		client,
		request,
		false,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("offline mounted category=%s error=%v", domain.CategoryOf(err), err)
	}

	request.Online = true
	if _, err := Preflight(
		context.Background(),
		client,
		request,
		false,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("online RWOP category=%s error=%v", domain.CategoryOf(err), err)
	}

	request.Online = false

	request.AllowMounted = true
	if _, err := Preflight(
		context.Background(),
		client,
		request,
		true,
	); domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "ReadWriteOncePod") {
		t.Fatalf("mounted restore RWOP category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPreflightRejectsToolObjectQuotaExhaustion(t *testing.T) {
	baseClient, request := preflightFixture(t, &preflightObjectStore{})

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "tool-limit"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName("count/jobs.batch"): resource.MustParse("0"),
		}},
	}
	if _, err := baseClient.CoreV1().
		ResourceQuotas("default").
		Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := Preflight(
		context.Background(),
		baseClient,
		request,
		false,
	); domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "count/jobs.batch") {
		t.Fatalf("quota category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPreflightAccountsForHelmReleaseSecret(t *testing.T) {
	t.Setenv("HELM_DRIVER", "secret")
	client, request := preflightFixture(t, &preflightObjectStore{})

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "secret-limit"},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceName("count/secrets"): resource.MustParse("2"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Used: corev1.ResourceList{
				corev1.ResourceName("count/secrets"): resource.MustParse("1"),
			},
		},
	}
	if _, err := client.CoreV1().
		ResourceQuotas("default").
		Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := Preflight(
		context.Background(),
		client,
		request,
		false,
	); domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "count/secrets") {
		t.Fatalf("quota category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPreflightRejectsEphemeralStorageLimitQuota(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "ephemeral-limit"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceLimitsEphemeralStorage: resource.MustParse("1Gi"),
		}},
	}
	if _, err := client.CoreV1().
		ResourceQuotas("default").
		Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := Preflight(
		context.Background(),
		client,
		request,
		false,
	); domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "limits.ephemeral-storage") {
		t.Fatalf("quota category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestToolQuotaDemandFollowsHelmReleaseDriver(t *testing.T) {
	tests := []struct {
		name       string
		driver     string
		secrets    string
		configMaps string
	}{
		{name: "default secret", driver: "", secrets: "2", configMaps: "0"},
		{name: "configmap", driver: "configmap", secrets: "1", configMaps: "1"},
		{name: "memory", driver: "memory", secrets: "1", configMaps: "0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("HELM_DRIVER", test.driver)

			demand := toolQuotaDemand()

			quantityString := func(name corev1.ResourceName) string {
				quantity, ok := demand[name]
				if !ok {
					return "0"
				}

				return quantity.String()
			}
			if got := quantityString(corev1.ResourceName("count/secrets")); got != test.secrets {
				t.Fatalf("secret demand=%s want=%s", got, test.secrets)
			}

			if got := quantityString(
				corev1.ResourceName("count/configmaps"),
			); got != test.configMaps {
				t.Fatalf("ConfigMap demand=%s want=%s", got, test.configMaps)
			}
		})
	}
}

func TestPreflightRejectsToolLimitRangeMinimum(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})

	limitRange := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "tool-minimum"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Min:  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1m")},
		}}},
	}
	if _, err := client.CoreV1().
		LimitRanges("default").
		Create(context.Background(), limitRange, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := Preflight(
		context.Background(),
		client,
		request,
		false,
	); domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "minimum") {
		t.Fatalf("limit range category=%s error=%v", domain.CategoryOf(err), err)
	}
}

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

func TestWrapBackupErrorPreservesStructuredInventoryDetails(t *testing.T) {
	cause := domain.NewError(
		domain.ErrorConflict,
		"S3 inventory",
		"published backup objects changed: count=2/1",
	)

	err := wrapBackupError(
		domain.ErrorPrecondition,
		"restore preflight",
		"verify S3 backup inventory",
		cause,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "published backup objects changed: count=2/1") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestorePlanDefaultsToKeepingExtraFiles(t *testing.T) {
	client, request := preflightFixture(
		t,
		&preflightObjectStore{
			manifest: []byte(
				`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"1Gi","volumeMode":"Filesystem","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`,
			),
		},
	)

	plan, err := Preflight(context.Background(), client, request, true)
	if err != nil {
		t.Fatal(err)
	}

	if plan.DeleteExtraneous {
		t.Fatal("restore plan enables destructive deletion by default")
	}

	if plan.Compression != "none" {
		t.Fatalf("restore plan compression=%q", plan.Compression)
	}
}

func TestRestorePreflightRejectsPathMismatch(t *testing.T) {
	client, request := preflightFixture(
		t,
		&preflightObjectStore{
			manifest: []byte(
				`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"1Gi","volumeMode":"Filesystem","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","path":"subdir"}`,
			),
		},
	)

	request.Path = "other"
	if _, err := Preflight(
		context.Background(),
		client,
		request,
		true,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("path mismatch category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestBackupPreflightNormalizesPVCPath(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	request.Path = "tenant data//当前's files/"

	plan, err := Preflight(context.Background(), client, request, false)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Path != "tenant data/当前's files" {
		t.Fatalf("normalized plan path=%q", plan.Path)
	}
}

func TestBackupConsistencyRecordsOnlineBoundary(t *testing.T) {
	if got := backupConsistency(true); got != "best-effort crash-consistent file copy" {
		t.Fatalf("online consistency=%q", got)
	}

	if got := backupConsistency(false); got != "offline file-consistent copy" {
		t.Fatalf("offline consistency=%q", got)
	}
}

func TestPVMigrateBackupAndRestoreHonorMountedPolicy(t *testing.T) {
	client := &preflightObjectStore{}

	store, err := objectstore.NewWithClient(
		client,
		objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		objectstore.Credentials{},
	)
	if err != nil {
		t.Fatal(err)
	}

	request := Request{
		ID:                    "operation",
		ToolImage:             "registry.example/pvc-migrate:aio",
		Namespace:             "default",
		PVCName:               "data",
		Store:                 store,
		DeleteExtraneousFiles: true,
		AllowMounted:          true,
		Online:                true,
	}

	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !containsString(
		backupRequest.HelmStringValues,
		"rclone.image.repository=registry.example/pvc-migrate",
	) ||
		!containsString(backupRequest.HelmStringValues, "rclone.image.tag=aio") {
		t.Fatalf("backup tool image values=%v", backupRequest.HelmStringValues)
	}

	for _, expected := range kube.ToolSecurityContextHelmValues() {
		if !containsString(backupRequest.HelmValues, expected) {
			t.Fatalf("backup typed Helm values lack %q: %v", expected, backupRequest.HelmValues)
		}
	}

	if !backupRequest.IgnoreMounted {
		t.Fatal("online backup did not ignore mounted source")
	}

	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !containsString(
		restoreRequest.HelmStringValues,
		"rclone.image.repository=registry.example/pvc-migrate",
	) ||
		!containsString(restoreRequest.HelmStringValues, "rclone.image.tag=aio") {
		t.Fatalf("restore tool image values=%v", restoreRequest.HelmStringValues)
	}

	for _, expected := range kube.ToolSecurityContextHelmValues() {
		if !containsString(restoreRequest.HelmValues, expected) {
			t.Fatalf("restore typed Helm values lack %q: %v", expected, restoreRequest.HelmValues)
		}
	}

	if !restoreRequest.IgnoreMounted || !restoreRequest.DeleteExtraneousFiles {
		t.Fatalf(
			"restore mounted policy=%t delete=%t",
			restoreRequest.IgnoreMounted,
			restoreRequest.DeleteExtraneousFiles,
		)
	}
}

func TestPVMigrateBackupAndRestoreDeferMountedPolicyToPhaseAwarePreflight(t *testing.T) {
	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Name: "daily"},
		objectstore.Credentials{},
	)
	if err != nil {
		t.Fatal(err)
	}

	request := Request{Namespace: "default", PVCName: "data", Store: store}

	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	if !backupRequest.IgnoreMounted || !restoreRequest.IgnoreMounted {
		t.Fatalf(
			"upstream mounted checks backup=%t restore=%t",
			backupRequest.IgnoreMounted,
			restoreRequest.IgnoreMounted,
		)
	}
}

func TestPVMigrateBackupAndRestoreForwardFullRequestContract(t *testing.T) {
	store, err := objectstore.NewWithClient(&preflightObjectStore{}, objectstore.Config{
		Bucket: "bucket", Prefix: "prefix", Name: "recovery-point",
	}, objectstore.Credentials{})
	if err != nil {
		t.Fatal(err)
	}

	writer := &bytes.Buffer{}
	logger := slog.New(slog.DiscardHandler)
	request := Request{
		ID:                    "operation",
		ToolImage:             "registry.example/team/pvc-migrate:contract",
		Namespace:             "source-ns",
		PVCName:               "source-pvc",
		Path:                  "subdir",
		Online:                true,
		AllowMounted:          true,
		DeleteExtraneousFiles: true,
		HelmTimeout:           41 * time.Second,
		KubeconfigPath:        "/tmp/kubeconfig",
		KubeContext:           "cluster-context",
		Store:                 store,
		Writer:                writer,
		Logger:                logger,
	}
	customValues := []string{"rclone.nodeName=source-node"}

	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", customValues)
	if err != nil {
		t.Fatal(err)
	}

	assertBackupRequestContract(
		t,
		backupRequest,
		request,
		store.RemotePath(),
		writer,
		logger,
		customValues[0],
	)

	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	assertRestoreRequestContract(
		t,
		restoreRequest,
		backupRequest,
		request,
		store.RemotePath(),
		writer,
		logger,
	)
}

func assertBackupRequestContract(
	t *testing.T,
	got pvmigrate.Backup,
	request Request,
	remote string,
	writer io.Writer,
	logger *slog.Logger,
	customValue string,
) {
	t.Helper()

	if got.ID != request.ID ||
		got.PVC != (pvmigrate.PVC{KubeconfigPath: request.KubeconfigPath, Context: request.KubeContext, Namespace: request.Namespace, Name: request.PVCName}) ||
		got.Backend != "s3" ||
		got.Bucket != "bucket" ||
		got.Name != "recovery-point" ||
		got.Prefix != "prefix" ||
		got.Path != request.Path ||
		got.RcloneConfigFile != "/tmp/rclone.conf" ||
		got.Remote != remote {
		t.Fatalf("backup upstream request=%#v", got)
	}

	if !got.IgnoreMounted || got.RcloneExtraArgs != rclonePreserveLinksArgs ||
		got.HelmTimeout != request.HelmTimeout ||
		!got.StructuredLogs ||
		got.Writer != writer ||
		got.Logger != logger ||
		got.HelmStringValues[len(got.HelmStringValues)-1] != customValue {
		t.Fatalf("backup execution fields=%#v helmValues=%v", got, got.HelmStringValues)
	}
}

func TestPVMigrateBackupRequestUsesWritableMountForSharedOnlinePVC(t *testing.T) {
	request := Request{
		ID:               "backup-test",
		KubeconfigPath:   "/tmp/kubeconfig",
		KubeContext:      "cluster",
		Namespace:        "source",
		PVCName:          "data",
		Path:             "",
		HelmTimeout:      time.Minute,
		WritablePVCMount: true,
		Store:            testBackupObjectStore(t),
	}
	got, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", []string{"rclone.nodeName=node-a"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(
		got.HelmStringValues,
		"rclone.pvcMounts[0].name=data,rclone.pvcMounts[0].mountPath=/data,rclone.pvcMounts[0].readOnly=false",
	) {
		t.Fatalf("helm values=%v", got.HelmStringValues)
	}
}

func assertRestoreRequestContract(
	t *testing.T,
	got pvmigrate.Restore,
	backup pvmigrate.Backup,
	request Request,
	remote string,
	writer io.Writer,
	logger *slog.Logger,
) {
	t.Helper()

	if got.ID != request.ID || got.PVC != backup.PVC || got.Backend != "s3" ||
		got.Bucket != backup.Bucket ||
		got.Name != backup.Name ||
		got.Prefix != backup.Prefix ||
		got.Path != request.Path ||
		got.RcloneConfigFile != "/tmp/rclone.conf" ||
		got.Remote != remote {
		t.Fatalf("restore upstream request=%#v", got)
	}

	if !got.IgnoreMounted || !got.DeleteExtraneousFiles ||
		got.RcloneExtraArgs != rclonePreserveLinksArgs ||
		got.HelmTimeout != request.HelmTimeout ||
		!got.StructuredLogs ||
		got.Writer != writer ||
		got.Logger != logger {
		t.Fatalf("restore execution fields=%#v", got)
	}
}

func TestPVMigrateBackupAndRestoreForwardLogger(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		objectstore.Credentials{},
	)
	if err != nil {
		t.Fatal(err)
	}

	request := Request{
		ID:        "operation",
		Namespace: "default",
		PVCName:   "data",
		Store:     store,
		Logger:    logger,
	}

	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	if got := backupRequest.Logger; got != logger {
		t.Fatal("backup logger was not forwarded")
	}

	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	if got := restoreRequest.Logger; got != logger {
		t.Fatal("restore logger was not forwarded")
	}
}

func TestPVMigrateBackupAndRestoreUseZeroToolResources(t *testing.T) {
	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Name: "daily"},
		objectstore.Credentials{},
	)
	if err != nil {
		t.Fatal(err)
	}

	request := Request{Namespace: "default", PVCName: "data", Store: store}

	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, values := range [][]string{backupRequest.HelmStringValues, restoreRequest.HelmStringValues} {
		for _, expected := range kube.ZeroResourceHelmValues() {
			if !containsString(values, expected) {
				t.Fatalf("missing tool resource value %q in %v", expected, values)
			}
		}
	}
}

func TestPVMigrateBackupAndRestorePinDefaultToolTimeout(t *testing.T) {
	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Name: "daily"},
		objectstore.Credentials{},
	)
	if err != nil {
		t.Fatal(err)
	}

	request := Request{Namespace: "default", PVCName: "data", Store: store}

	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	if backupRequest.HelmTimeout != 10*time.Minute {
		t.Fatalf("backup default HelmTimeout=%s, want 10m", backupRequest.HelmTimeout)
	}

	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}

	if restoreRequest.HelmTimeout != 10*time.Minute {
		t.Fatalf("restore default HelmTimeout=%s, want 10m", restoreRequest.HelmTimeout)
	}
}

func TestRestoreLockHolderGeneratesUniqueDefaultIdentity(t *testing.T) {
	first, err := operationLockHolder("")
	if err != nil {
		t.Fatal(err)
	}

	second, err := operationLockHolder("")
	if err != nil {
		t.Fatal(err)
	}

	if first == second {
		t.Fatalf("default restore lock holders collided: %q", first)
	}
}

func TestRestoreLockHolderGeneratesUniqueAttemptForExplicitID(t *testing.T) {
	first, err := operationLockHolder("daily")
	if err != nil {
		t.Fatal(err)
	}

	second, err := operationLockHolder("daily")
	if err != nil {
		t.Fatal(err)
	}

	if first == second {
		t.Fatalf("explicit operation lock holders collided: %q", first)
	}

	if toolOperationID(first) == toolOperationID(second) {
		t.Fatalf("explicit operation tool IDs collided: %q", toolOperationID(first))
	}
}

func TestRestoreLockSupportsRetryAfterReleaseAndExpiredAttempt(t *testing.T) {
	const (
		namespace = "default"
		name      = "restore-target"
		pvcUID    = "restore-pvc-uid"
	)
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
		UID:       types.UID(pvcUID),
	}})

	firstRelease, _, err := acquireRestoreLock(
		context.Background(), client, namespace, name, "first-attempt", time.Minute, pvcUID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := acquireRestoreLock(
		context.Background(), client, namespace, name, "concurrent-attempt", time.Minute, pvcUID,
	); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("active lock category=%q error=%v", domain.CategoryOf(err), err)
	}
	if err := firstRelease(context.Background()); err != nil {
		t.Fatal(err)
	}

	staleRelease, _, err := acquireRestoreLock(
		context.Background(), client, namespace, name, "stale-attempt", time.Minute, pvcUID,
	)
	if err != nil {
		t.Fatal(err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(
		context.Background(), name, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	pvc.Annotations[restoreLockExpiryAnnotation] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Update(
		context.Background(), pvc, metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	finalRelease, _, err := acquireRestoreLock(
		context.Background(), client, namespace, name, "retry-attempt", time.Minute, pvcUID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := staleRelease(context.Background()); err != nil {
		t.Fatal(err)
	}
	pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(
		context.Background(), name, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations[restoreLockAnnotation] != "retry-attempt" {
		t.Fatalf("stale release cleared retry owner: %#v", pvc.Annotations)
	}
	if err := finalRelease(context.Background()); err != nil {
		t.Fatal(err)
	}
	pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(
		context.Background(), name, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations[restoreLockAnnotation] != "" || pvc.Annotations[restoreLockExpiryAnnotation] != "" {
		t.Fatalf("restore lock annotations remain after retry: %#v", pvc.Annotations)
	}
}

func TestToolOperationIDIsUniqueSafeAndBounded(t *testing.T) {
	first := toolOperationID("pvc-migrate-first")

	second := toolOperationID("pvc-migrate-second")
	if first == second || len(first) > 24 || len(second) > 24 {
		t.Fatalf("tool IDs first=%q second=%q", first, second)
	}

	if !strings.HasPrefix(first, "pm-") ||
		strings.ContainsAny(first[3:], "ABCDEFGHIJKLMNOPQRSTUVWXYZ_/") {
		t.Fatalf("tool ID %q is not DNS-safe", first)
	}
}

func TestCheckObjectStoreLeaseClassifiesLossAndCancellation(t *testing.T) {
	leaseErrors := make(chan error, 1)
	leaseErrors <- io.ErrUnexpectedEOF

	if category := domain.CategoryOf(
		checkObjectStoreLease(context.Background(), leaseErrors),
	); category != domain.ErrorConflict {
		t.Fatalf("lease loss category=%s, want conflict", category)
	}

	leaseErrors <- domain.NewError(domain.ErrorTimeout, "renew", "deadline")

	if category := domain.CategoryOf(
		checkObjectStoreLease(context.Background(), leaseErrors),
	); category != domain.ErrorTimeout {
		t.Fatalf("lease timeout category=%s, want timeout", category)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if category := domain.CategoryOf(
		checkObjectStoreLease(ctx, make(chan error)),
	); category != domain.ErrorTimeout {
		t.Fatalf("lease cancellation category=%s, want timeout", category)
	}
}

func TestRestoreLockRenewalPreservesTimeoutCategory(t *testing.T) {
	if category := domain.CategoryOf(
		classifyRestoreLockError(
			context.Background(),
			domain.NewError(domain.ErrorTimeout, "renew", "deadline"),
		),
	); category != domain.ErrorTimeout {
		t.Fatalf("renewal timeout category=%s, want timeout", category)
	}

	if category := domain.CategoryOf(
		classifyRestoreLockError(context.Background(), context.DeadlineExceeded),
	); category != domain.ErrorTimeout {
		t.Fatalf("context deadline category=%s, want timeout", category)
	}

	if category := domain.CategoryOf(
		classifyRestoreLockError(context.Background(), io.ErrUnexpectedEOF),
	); category != domain.ErrorConflict {
		t.Fatalf("renewal ownership category=%s, want conflict", category)
	}
}

func TestRestoreLockRenewalRejectsReplacementPVC(t *testing.T) {
	const holder = "pvc-migrate/restore-attempt"

	originalExpiry := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "data",
			UID:       types.UID("replacement-pvc-uid"),
			Annotations: map[string]string{
				restoreLockAnnotation:       holder,
				restoreLockExpiryAnnotation: originalExpiry,
			},
		},
	})

	err := renewRestoreLockOnce(
		context.Background(),
		client,
		"default",
		"data",
		holder,
		"original-pvc-uid",
		time.Hour,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	current, getErr := client.CoreV1().
		PersistentVolumeClaims("default").
		Get(context.Background(), "data", metav1.GetOptions{})
	if getErr != nil {
		t.Fatal(getErr)
	}

	if current.Annotations[restoreLockExpiryAnnotation] != originalExpiry {
		t.Fatalf(
			"replacement PVC lock expiry changed to %q",
			current.Annotations[restoreLockExpiryAnnotation],
		)
	}
}

func TestOfflinePreflightIgnoresTerminalPVCConsumers(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	for _, podState := range []struct {
		name     string
		phase    corev1.PodPhase
		nodeName string
	}{
		{name: "completed-unscheduled", phase: corev1.PodSucceeded},
		{name: "failed-unscheduled", phase: corev1.PodFailed},
		{name: "completed-scheduled", phase: corev1.PodSucceeded, nodeName: "node-a"},
		{name: "failed-scheduled", phase: corev1.PodFailed, nodeName: "node-a"},
	} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: podState.name},
			Spec: corev1.PodSpec{
				NodeName: podState.nodeName,
				Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				}}},
			},
			Status: corev1.PodStatus{Phase: podState.phase},
		}
		if _, err := client.CoreV1().
			Pods("default").
			Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	plan, err := Preflight(context.Background(), client, request, false)
	if err != nil {
		t.Fatal(err)
	}

	if len(plan.MountedPods) != 0 {
		t.Fatalf("terminal Pod consumers=%v, want none", plan.MountedPods)
	}
}

func TestOfflineBackupToolStartRechecksConsumers(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	if err := validateBackupToolStart(context.Background(), client, request); err != nil {
		t.Fatal(err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "consumer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
		}}}},
	}
	if _, err := client.CoreV1().
		Pods("default").
		Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := validateBackupToolStart(
		context.Background(),
		client,
		request,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func containsString(values []string, want string) bool {
	return slices.Contains(values, want)
}

func TestOnlineBackupPinsRWOToolToConsumerNode(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	request.Online = true

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "consumer"},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Volumes: []corev1.Volume{
				{
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				},
			},
		},
	}
	if _, err := client.CoreV1().
		Pods("default").
		Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	node, err := onlineBackupToolNode(context.Background(), client, request)
	if err != nil {
		t.Fatal(err)
	}

	if node != "node-a" {
		t.Fatalf("tool node=%q", node)
	}

	plan, err := Preflight(context.Background(), client, request, false)
	if err != nil {
		t.Fatal(err)
	}

	if plan.ToolNode != "node-a" {
		t.Fatalf("plan toolNode=%q", plan.ToolNode)
	}
}

func TestMountedRestorePinsRWOToolToConsumerNode(t *testing.T) {
	storeAPI := &preflightObjectStore{manifest: emptyBackupManifest()}
	client, request := preflightFixture(t, storeAPI)
	request.AllowMounted = true

	pod := mountedConsumerPod("consumer", "node-a")
	if _, err := client.CoreV1().
		Pods("default").
		Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := Preflight(context.Background(), client, request, true)
	if err != nil {
		t.Fatal(err)
	}

	if plan.ToolNode != "node-a" {
		t.Fatalf("plan toolNode=%q", plan.ToolNode)
	}
}

func TestOfflinePreflightPinsToolToUniquePVNode(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{
		Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{
				{
					Key:      corev1.LabelHostname,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"host-a"},
				},
			},
		}}},
	}
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Nodes().
		Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"},
		}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := Preflight(context.Background(), client, request, false)
	if err != nil {
		t.Fatal(err)
	}

	if plan.ToolNode != "node-a" {
		t.Fatalf("plan toolNode=%q", plan.ToolNode)
	}
}

type recordingBackupToolProber struct {
	calls   []kube.ToolImageProbeOptions
	results []kube.ToolImageProbeResult
	err     error
	onProbe func(kube.ToolImageProbeOptions)
}

func (p *recordingBackupToolProber) Probe(
	_ context.Context,
	options kube.ToolImageProbeOptions,
) ([]kube.ToolImageProbeResult, error) {
	p.calls = append(p.calls, options)
	if p.onProbe != nil {
		p.onProbe(options)
	}

	if p.err != nil {
		return nil, p.err
	}

	if p.results != nil {
		return slices.Clone(p.results), nil
	}

	target := options.Targets[0]

	nodeName := target.NodeName
	if nodeName == "" {
		nodeName = "scheduler-node"
	}

	return []kube.ToolImageProbeResult{{Target: target, NodeName: nodeName}}, nil
}

func TestRunProbesRcloneWhileHoldingTransferLock(t *testing.T) {
	for _, test := range []struct {
		name     string
		online   bool
		wantNode string
	}{
		{name: "offline scheduler selected"},
		{name: "online RWO consumer node", online: true, wantNode: "node-a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			storeAPI := &preflightObjectStore{}
			client, request := preflightFixture(t, storeAPI)

			request.Online = test.online
			if test.online {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "consumer"},
					Spec: corev1.PodSpec{
						NodeName: "node-a",
						Volumes: []corev1.Volume{
							{
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "data",
									},
								},
							},
						},
					},
				}
				if _, err := client.CoreV1().
					Pods("default").
					Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			probeErr := domain.NewError(
				domain.ErrorPrecondition,
				"tool image probe",
				"stopped before transfer",
			)
			prober := &recordingBackupToolProber{err: probeErr}
			request.ToolImage = "registry.example/pvc-migrate:test"
			request.ToolImageProber = prober

			if err := Run(context.Background(), client, request, false); !errors.Is(err, probeErr) {
				t.Fatalf("Run() error=%v", err)
			}

			if len(prober.calls) != 1 {
				t.Fatalf("probe calls=%d", len(prober.calls))
			}

			probe := prober.calls[0]
			if probe.Image != request.ToolImage || len(probe.Targets) != 1 ||
				probe.Targets[0].Namespace != "default" ||
				probe.Targets[0].NodeName != test.wantNode ||
				!slices.Equal(probe.Targets[0].Components, []string{kube.ToolComponentRclone}) {
				t.Fatalf("probe=%#v", probe)
			}

			wantPVC := ""
			if test.wantNode == "" || request.Online {
				wantPVC = request.PVCName
			}

			if probe.Targets[0].PVCName != wantPVC {
				t.Fatalf("probe PVC=%q want=%q", probe.Targets[0].PVCName, wantPVC)
			}

			if storeAPI.puts != 1 {
				t.Fatalf("lock writes before probe=%d", storeAPI.puts)
			}
		})
	}
}

func TestTransferToolProbeValidatesBackupPathAndCreatesRestorePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		restore bool
	}{
		{name: "backup validates selected source"},
		{name: "restore creates selected destination", restore: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			prober := &recordingBackupToolProber{}
			request := Request{
				ID: "partial", Namespace: "default", PVCName: "data", Path: "mysql/current",
				ToolImage: "registry.example/pvc-migrate:test", ToolImageProber: prober,
			}

			result, err := probeTransferToolImage(t.Context(), request, "node-a", test.restore)
			if err != nil {
				t.Fatal(err)
			}

			if result.NodeName != "node-a" || len(prober.calls) != 1 ||
				len(prober.calls[0].Targets) != 1 {
				t.Fatalf("result=%#v calls=%#v", result, prober.calls)
			}

			target := prober.calls[0].Targets[0]
			if target.PVCName != request.PVCName || target.RequiredPath != request.Path ||
				target.CreatePath != test.restore ||
				!slices.Equal(target.Components, []string{kube.ToolComponentRclone}) {
				t.Fatalf("target=%#v", target)
			}
		})
	}
}

func TestRunRecomputesPVToolNodeAfterAcquiringLock(t *testing.T) {
	storeAPI := &preflightObjectStore{}

	client, request := preflightFixture(t, storeAPI)
	for _, node := range []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: "host-b"}}},
	} {
		if _, err := client.CoreV1().
			Nodes().
			Create(context.Background(), node, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.NodeAffinity = probePVNodeAffinity("host-a")
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	storeAPI.onPut = func() {
		current, getErr := client.CoreV1().
			PersistentVolumes().
			Get(context.Background(), "pv-data", metav1.GetOptions{})
		if getErr != nil {
			t.Fatal(getErr)
		}

		current.Spec.NodeAffinity = probePVNodeAffinity("host-b")
		if _, updateErr := client.CoreV1().
			PersistentVolumes().
			Update(context.Background(), current, metav1.UpdateOptions{}); updateErr != nil {
			t.Fatal(updateErr)
		}

		storeAPI.onPut = nil
	}
	probeErr := domain.NewError(
		domain.ErrorPrecondition,
		"tool image probe",
		"stop after placement assertion",
	)
	prober := &recordingBackupToolProber{err: probeErr}
	request.ToolImageProber = prober

	if err := Run(context.Background(), client, request, false); !errors.Is(err, probeErr) {
		t.Fatalf("Run() error=%v", err)
	}

	if len(prober.calls) != 1 || prober.calls[0].Targets[0].NodeName != "node-b" {
		t.Fatalf("probe calls=%#v", prober.calls)
	}
}

func TestRestoreRecomputesMountedRWOConsumerNodeAfterAcquiringLock(t *testing.T) {
	storeAPI := &preflightObjectStore{manifest: emptyBackupManifest()}
	clientAPI, request := preflightFixture(t, storeAPI)
	client := testutil.MustType[*fake.Clientset](t, clientAPI)
	request.AllowMounted = true
	request.ToolImage = "registry.example/pvc-migrate:test"

	if _, err := client.CoreV1().
		Pods("default").
		Create(context.Background(), mountedConsumerPod("consumer", "node-a"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	moved := false
	client.PrependReactor(
		"update",
		"persistentvolumeclaims",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			if moved {
				return false, nil, nil
			}

			update := testutil.MustType[ktesting.UpdateAction](t, action)

			pvc := testutil.MustType[*corev1.PersistentVolumeClaim](t, update.GetObject())
			if pvc.Annotations[restoreLockAnnotation] == "" {
				return false, nil, nil
			}

			pod, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), "default", "consumer")
			if err != nil {
				return true, nil, err
			}

			consumer := testutil.MustType[*corev1.Pod](t, pod).DeepCopy()

			consumer.Spec.NodeName = "node-b"
			if err := client.Tracker().
				Update(corev1.SchemeGroupVersion.WithResource("pods"), consumer, "default"); err != nil {
				return true, nil, err
			}

			moved = true

			return false, nil, nil
		},
	)

	probeErr := domain.NewError(
		domain.ErrorPrecondition,
		"tool image probe",
		"stop after placement assertion",
	)
	prober := &recordingBackupToolProber{err: probeErr}
	request.ToolImageProber = prober

	if err := Run(context.Background(), client, request, true); !errors.Is(err, probeErr) {
		t.Fatalf("Run() error=%v", err)
	}

	if len(prober.calls) != 1 || prober.calls[0].Targets[0].NodeName != "node-b" {
		t.Fatalf("probe calls=%#v", prober.calls)
	}
}

func TestRunRejectsConsumerCreatedDuringToolProbe(t *testing.T) {
	for _, test := range []struct {
		name    string
		restore bool
	}{
		{name: "offline backup"},
		{name: "restore", restore: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			storeAPI := &preflightObjectStore{}
			if test.restore {
				storeAPI.manifest = emptyBackupManifest()
			}

			client, request := preflightFixture(t, storeAPI)
			addTransferTestNode(t, client, "node-a")

			request.ToolImage = "registry.example/pvc-migrate:test"
			request.ToolImageProber = &recordingBackupToolProber{
				results: []kube.ToolImageProbeResult{{NodeName: "node-a"}},
				onProbe: func(kube.ToolImageProbeOptions) {
					if _, err := client.CoreV1().
						Pods("default").
						Create(context.Background(), mountedConsumerPod("late-consumer", "node-a"), metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
				},
			}

			err := Run(context.Background(), client, request, test.restore)
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), "late-consumer") {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestRestoreRejectsRWOConsumerNodeChangeDuringToolProbe(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{manifest: emptyBackupManifest()})
	addTransferTestNode(t, client, "node-a")

	request.AllowMounted = true
	request.ToolImage = "registry.example/pvc-migrate:test"

	if _, err := client.CoreV1().
		Pods("default").
		Create(context.Background(), mountedConsumerPod("consumer", "node-a"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	request.ToolImageProber = &recordingBackupToolProber{onProbe: func(kube.ToolImageProbeOptions) {
		pod, err := client.CoreV1().
			Pods("default").
			Get(context.Background(), "consumer", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		pod.Spec.NodeName = "node-b"
		if _, err := client.CoreV1().
			Pods("default").
			Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}}

	err := Run(context.Background(), client, request, true)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "node-a to node-b") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestBackupRejectsPVCIdentityChangeDuringToolProbe(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	addTransferTestNode(t, client, "node-a")

	request.ToolImage = "registry.example/pvc-migrate:test"
	request.ToolImageProber = &recordingBackupToolProber{
		results: []kube.ToolImageProbeResult{{NodeName: "node-a"}},
		onProbe: func(kube.ToolImageProbeOptions) {
			pvc, err := client.CoreV1().
				PersistentVolumeClaims("default").
				Get(context.Background(), "data", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			pvc.UID = types.UID("replacement-pvc")
			if _, err := client.CoreV1().
				PersistentVolumeClaims("default").
				Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		},
	}

	err := Run(context.Background(), client, request, false)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "binding identity changed") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func addTransferTestNode(t *testing.T, client kubernetes.Interface, name string) {
	t.Helper()

	_, err := client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{corev1.LabelHostname: name},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func probePVNodeAffinity(hostname string) *corev1.VolumeNodeAffinity {
	return &corev1.VolumeNodeAffinity{
		Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{
				{
					Key:      corev1.LabelHostname,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{hostname},
				},
			},
		}}},
	}
}

func TestBackupPreflightDoesNotProbeToolImage(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	prober := &recordingBackupToolProber{}

	request.ToolImageProber = prober
	if _, err := Preflight(context.Background(), client, request, false); err != nil {
		t.Fatal(err)
	}

	if len(prober.calls) != 0 {
		t.Fatalf("preflight probe calls=%d", len(prober.calls))
	}
}

func TestTransferToolHelmValuesUseObservedNodeTaintsAndPullSecrets(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "storage", Value: "true", Effect: corev1.TaintEffectNoSchedule},
			{Key: "draining", Effect: corev1.TaintEffectNoExecute},
		}},
	}
	client := fake.NewClientset(node)
	probe := kube.ToolImageProbeResult{
		Target:           kube.ToolProbeTarget{Components: []string{kube.ToolComponentRclone}},
		NodeName:         "node-a",
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry-pull"}},
	}

	values, err := transferToolHelmValues(context.Background(), client, probe)
	if err != nil {
		t.Fatal(err)
	}

	for _, expected := range []string{
		"rclone.nodeName=node-a",
		"rclone.tolerations[0].key=storage",
		"rclone.tolerations[0].value=true",
		"rclone.tolerations[1].key=draining",
		"rclone.tolerations[1].operator=Exists",
		"rclone.imagePullSecrets[0].name=registry-pull",
	} {
		if !slices.Contains(values, expected) {
			t.Fatalf("missing %q in %v", expected, values)
		}
	}
}

func TestOnlineBackupRejectsRWOConsumersOnDifferentNodes(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	request.Online = true

	for name, node := range map[string]string{"consumer-a": "node-a", "consumer-b": "node-b"} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
			Spec: corev1.PodSpec{
				NodeName: node,
				Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				},
			},
		}
		if _, err := client.CoreV1().
			Pods("default").
			Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := onlineBackupToolNode(
		context.Background(),
		client,
		request,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := Preflight(
		context.Background(),
		client,
		request,
		false,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("preflight category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestMountedRestoreRejectsRWOConsumersOnDifferentNodes(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{manifest: emptyBackupManifest()})
	request.AllowMounted = true

	for name, node := range map[string]string{"consumer-a": "node-a", "consumer-b": "node-b"} {
		if _, err := client.CoreV1().
			Pods("default").
			Create(context.Background(), mountedConsumerPod(name, node), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := Preflight(
		context.Background(),
		client,
		request,
		true,
	); domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "span nodes") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func mountedConsumerPod(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Spec: corev1.PodSpec{
			NodeName: node,
			Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
			}}},
		},
	}
}

func emptyBackupManifest() []byte {
	return []byte(
		`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"1Gi","volumeMode":"Filesystem","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`,
	)
}

func TestVerifyPVCIdentityRejectsNameReuseAndClaimRefMismatch(t *testing.T) {
	mode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "data",
			UID:       types.UID("pvc-new"),
		},
		Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-data", VolumeMode: &mode},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv")},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Namespace: "default",
				Name:      "data",
				UID:       types.UID("pvc-new"),
			},
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	client := fake.NewClientset(pvc, pv)
	if _, _, err := verifyPVCIdentity(
		context.Background(),
		client,
		"default",
		"data",
		"pvc-old",
		"pv",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("PVC name reuse category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, _, err := verifyPVCIdentity(
		context.Background(),
		client,
		"default",
		"data",
		"pvc-new",
		"pv",
	); err != nil {
		t.Fatal(err)
	}

	pv.Spec.ClaimRef.UID = types.UID("foreign")
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := verifyPVCIdentity(
		context.Background(),
		client,
		"default",
		"data",
		"pvc-new",
		"pv",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("claimRef mismatch category=%s error=%v", domain.CategoryOf(err), err)
	}

	pv.Spec.ClaimRef.UID = ""
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := verifyPVCIdentity(
		context.Background(),
		client,
		"default",
		"data",
		"pvc-new",
		"pv",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("missing claimRef UID category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, _, err := verifyPVCIdentity(
		context.Background(),
		client,
		"default",
		"data",
		"",
		"pv",
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("missing expected UID category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPreflightRejectsPVBindingIdentityDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*corev1.PersistentVolume)
	}{
		{name: "claimRef UID changed", mutate: func(pv *corev1.PersistentVolume) { pv.Spec.ClaimRef.UID = "replacement" }},
		{name: "claimRef UID missing", mutate: func(pv *corev1.PersistentVolume) { pv.Spec.ClaimRef.UID = "" }},
		{name: "PV released", mutate: func(pv *corev1.PersistentVolume) { pv.Status.Phase = corev1.VolumeReleased }},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, request := preflightFixture(t, &preflightObjectStore{})

			pv, err := client.CoreV1().
				PersistentVolumes().
				Get(context.Background(), "pv-data", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			test.mutate(pv)

			if _, err := client.CoreV1().
				PersistentVolumes().
				Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			if _, err := Preflight(context.Background(), client, request, false); err == nil {
				t.Fatal("preflight accepted an invalid PVC/PV binding")
			}
		})
	}
}

func TestClassifySyncErrorPreservesTimeoutCategory(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		category domain.ErrorCategory
	}{
		{name: "deadline", err: context.DeadlineExceeded, category: domain.ErrorTimeout},
		{name: "canceled", err: context.Canceled, category: domain.ErrorTimeout},
		{name: "copy", err: io.EOF, category: domain.ErrorCopy},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := classifySyncError(context.Background(), "backup", test.err)
			if domain.CategoryOf(err) != test.category {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestClassifyToolAndLeaseErrorPreservesBothFailures(t *testing.T) {
	toolErr := io.ErrUnexpectedEOF
	for _, test := range []struct {
		name     string
		leaseErr error
		category domain.ErrorCategory
	}{
		{name: "lease conflict", leaseErr: domain.NewError(domain.ErrorConflict, "renew", "ownership changed"), category: domain.ErrorConflict},
		{name: "lease timeout", leaseErr: domain.NewError(domain.ErrorTimeout, "renew", "deadline"), category: domain.ErrorTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			leaseErrors := make(chan error, 1)
			leaseErrors <- test.leaseErr

			err := classifyToolAndLeaseError(context.Background(), "backup", toolErr, leaseErrors)
			if domain.CategoryOf(err) != test.category || !errors.Is(err, test.leaseErr) ||
				!errors.Is(err, toolErr) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}

	err := classifyToolAndLeaseError(context.Background(), "backup", toolErr, make(chan error))
	if domain.CategoryOf(err) != domain.ErrorCopy || !errors.Is(err, toolErr) {
		t.Fatalf("copy-only category=%s error=%v", domain.CategoryOf(err), err)
	}
}
