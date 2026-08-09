package backup

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

type preflightObjectStore struct {
	manifest []byte
}

func (f *preflightObjectStore) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, preflightMissingObject{}
}

func (f *preflightObjectStore) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if len(f.manifest) == 0 {
		return nil, preflightMissingObject{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.manifest))}, nil
}

func (*preflightObjectStore) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{ETag: aws.String("etag")}, nil
}

func (*preflightObjectStore) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func (*preflightObjectStore) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

type preflightMissingObject struct{}

func (preflightMissingObject) Error() string                 { return "missing" }
func (preflightMissingObject) ErrorCode() string             { return "NoSuchKey" }
func (preflightMissingObject) ErrorMessage() string          { return "missing" }
func (preflightMissingObject) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func preflightFixture(t *testing.T, client objectstore.API) (kubernetes.Interface, Request) {
	t.Helper()
	mode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "data", UID: types.UID("pvc")},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-data", VolumeMode: &mode, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv")}, Spec: corev1.PersistentVolumeSpec{Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}
	store, err := objectstore.NewWithClient(client, objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"}, objectstore.Credentials{AccessKey: "key", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	return fake.NewClientset(pvc, pv), Request{ID: "test", Namespace: "default", PVCName: "data", Store: store}
}

func TestPreflightRejectsOfflineMountedAndOnlineRWOP(t *testing.T) {
	mode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "data", UID: "pvc"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-data", VolumeMode: &mode, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-data"}, Spec: corev1.PersistentVolumeSpec{Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "consumer"}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}}
	client := fake.NewClientset(pvc, pv, pod)
	store, _ := objectstore.NewWithClient(&preflightObjectStore{}, objectstore.Config{Bucket: "backups", Name: "daily"}, objectstore.Credentials{})
	request := Request{Namespace: "default", PVCName: "data", Store: store}
	if _, err := Preflight(context.Background(), client, request, false); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("offline mounted category=%s error=%v", domain.CategoryOf(err), err)
	}
	request.Online = true
	if _, err := Preflight(context.Background(), client, request, false); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("online RWOP category=%s error=%v", domain.CategoryOf(err), err)
	}
	request.Online = false
	request.AllowMounted = true
	if _, err := Preflight(context.Background(), client, request, true); domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "ReadWriteOncePod") {
		t.Fatalf("mounted restore RWOP category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPreflightRejectsHelperObjectQuotaExhaustion(t *testing.T) {
	baseClient, request := preflightFixture(t, &preflightObjectStore{})
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "helper-limit"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName("count/jobs.batch"): resource.MustParse("0"),
		}},
	}
	if _, err := baseClient.CoreV1().ResourceQuotas("default").Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Preflight(context.Background(), baseClient, request, false); domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "count/jobs.batch") {
		t.Fatalf("quota category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPreflightAccountsForHelmReleaseSecret(t *testing.T) {
	t.Setenv("HELM_DRIVER", "secret")
	client, request := preflightFixture(t, &preflightObjectStore{})
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "secret-limit"},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{corev1.ResourceName("count/secrets"): resource.MustParse("2")},
		},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{corev1.ResourceName("count/secrets"): resource.MustParse("1")}},
	}
	if _, err := client.CoreV1().ResourceQuotas("default").Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Preflight(context.Background(), client, request, false); domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "count/secrets") {
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
	if _, err := client.CoreV1().ResourceQuotas("default").Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Preflight(context.Background(), client, request, false); domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "limits.ephemeral-storage") {
		t.Fatalf("quota category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestHelperQuotaDemandFollowsHelmReleaseDriver(t *testing.T) {
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
			demand := helperQuotaDemand()
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
			if got := quantityString(corev1.ResourceName("count/configmaps")); got != test.configMaps {
				t.Fatalf("ConfigMap demand=%s want=%s", got, test.configMaps)
			}
		})
	}
}

func TestPreflightRejectsHelperLimitRangeMinimum(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	limitRange := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "helper-minimum"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Min:  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1m")},
		}}},
	}
	if _, err := client.CoreV1().LimitRanges("default").Create(context.Background(), limitRange, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Preflight(context.Background(), client, request, false); domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "minimum") {
		t.Fatalf("limit range category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestorePreflightRequiresPublishedManifestCapacityAndMode(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{manifest: []byte(`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"2Gi","volumeMode":"Filesystem","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`)})
	if _, err := Preflight(context.Background(), client, request, true); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("capacity category=%s error=%v", domain.CategoryOf(err), err)
	}
	request.Store, _ = objectstore.NewWithClient(&preflightObjectStore{manifest: []byte(`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"1Gi","volumeMode":"Block","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`)}, objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"}, objectstore.Credentials{})
	if _, err := Preflight(context.Background(), client, request, true); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("mode category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRestorePlanDefaultsToKeepingExtraFiles(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{manifest: []byte(`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"1Gi","volumeMode":"Filesystem","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`)})
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
	client, request := preflightFixture(t, &preflightObjectStore{manifest: []byte(`{"version":2,"createdAt":"2026-08-07T00:00:00Z","bucket":"backups","prefix":"pv-migrate","name":"daily","sourceNamespace":"default","sourcePVC":"data","sourcePVCUID":"pvc","capacity":"1Gi","volumeMode":"Filesystem","consistency":"offline file-consistent copy","compression":"none","objectCount":0,"totalBytes":0,"inventorySHA256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","path":"subdir"}`)})
	request.Path = "other"
	if _, err := Preflight(context.Background(), client, request, true); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("path mismatch category=%s error=%v", domain.CategoryOf(err), err)
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
	store, err := objectstore.NewWithClient(client, objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"}, objectstore.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{ID: "operation", ToolImage: "registry.example/pvc-migrate:aio", Namespace: "default", PVCName: "data", Store: store, DeleteExtraneousFiles: true, AllowMounted: true, Online: true}
	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(backupRequest.HelmStringValues, "rclone.image.repository=registry.example/pvc-migrate") || !containsString(backupRequest.HelmStringValues, "rclone.image.tag=aio") {
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
	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf")
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(restoreRequest.HelmStringValues, "rclone.image.repository=registry.example/pvc-migrate") || !containsString(restoreRequest.HelmStringValues, "rclone.image.tag=aio") {
		t.Fatalf("restore tool image values=%v", restoreRequest.HelmStringValues)
	}
	for _, expected := range kube.ToolSecurityContextHelmValues() {
		if !containsString(restoreRequest.HelmValues, expected) {
			t.Fatalf("restore typed Helm values lack %q: %v", expected, restoreRequest.HelmValues)
		}
	}
	if !restoreRequest.IgnoreMounted || !restoreRequest.DeleteExtraneousFiles {
		t.Fatalf("restore mounted policy=%t delete=%t", restoreRequest.IgnoreMounted, restoreRequest.DeleteExtraneousFiles)
	}
}

func TestPVMigrateBackupAndRestoreForwardLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := objectstore.NewWithClient(&preflightObjectStore{}, objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"}, objectstore.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{ID: "operation", Namespace: "default", PVCName: "data", Store: store, Logger: logger}
	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := backupRequest.Logger; got != logger {
		t.Fatal("backup logger was not forwarded")
	}
	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf")
	if err != nil {
		t.Fatal(err)
	}
	if got := restoreRequest.Logger; got != logger {
		t.Fatal("restore logger was not forwarded")
	}
}

func TestPVMigrateBackupAndRestoreUseZeroHelperResources(t *testing.T) {
	store, err := objectstore.NewWithClient(&preflightObjectStore{}, objectstore.Config{Bucket: "backups", Name: "daily"}, objectstore.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Namespace: "default", PVCName: "data", Store: store}
	backupRequest, err := pvmigrateBackupRequest(request, "/tmp/rclone.conf", nil)
	if err != nil {
		t.Fatal(err)
	}
	restoreRequest, err := pvmigrateRestoreRequest(request, "/tmp/rclone.conf")
	if err != nil {
		t.Fatal(err)
	}
	for _, values := range [][]string{backupRequest.HelmStringValues, restoreRequest.HelmStringValues} {
		for _, expected := range kube.ZeroResourceHelmValues() {
			if !containsString(values, expected) {
				t.Fatalf("missing helper resource value %q in %v", expected, values)
			}
		}
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
	if helperOperationID(first) == helperOperationID(second) {
		t.Fatalf("explicit operation helper IDs collided: %q", helperOperationID(first))
	}
}

func TestHelperOperationIDIsUniqueSafeAndBounded(t *testing.T) {
	first := helperOperationID("pvc-migrate-first")
	second := helperOperationID("pvc-migrate-second")
	if first == second || len(first) > 24 || len(second) > 24 {
		t.Fatalf("helper IDs first=%q second=%q", first, second)
	}
	if !strings.HasPrefix(first, "pm-") || strings.ContainsAny(first[3:], "ABCDEFGHIJKLMNOPQRSTUVWXYZ_/") {
		t.Fatalf("helper ID %q is not DNS-safe", first)
	}
}

func TestCheckObjectStoreLeaseClassifiesLossAndCancellation(t *testing.T) {
	leaseErrors := make(chan error, 1)
	leaseErrors <- io.ErrUnexpectedEOF
	if category := domain.CategoryOf(checkObjectStoreLease(context.Background(), leaseErrors, "backup")); category != domain.ErrorConflict {
		t.Fatalf("lease loss category=%s, want conflict", category)
	}
	leaseErrors <- domain.NewError(domain.ErrorTimeout, "renew", "deadline")
	if category := domain.CategoryOf(checkObjectStoreLease(context.Background(), leaseErrors, "backup")); category != domain.ErrorTimeout {
		t.Fatalf("lease timeout category=%s, want timeout", category)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if category := domain.CategoryOf(checkObjectStoreLease(ctx, make(chan error), "backup")); category != domain.ErrorTimeout {
		t.Fatalf("lease cancellation category=%s, want timeout", category)
	}
}

func TestRestoreLockRenewalPreservesTimeoutCategory(t *testing.T) {
	if category := domain.CategoryOf(classifyRestoreLockError(context.Background(), domain.NewError(domain.ErrorTimeout, "renew", "deadline"))); category != domain.ErrorTimeout {
		t.Fatalf("renewal timeout category=%s, want timeout", category)
	}
	if category := domain.CategoryOf(classifyRestoreLockError(context.Background(), context.DeadlineExceeded)); category != domain.ErrorTimeout {
		t.Fatalf("context deadline category=%s, want timeout", category)
	}
	if category := domain.CategoryOf(classifyRestoreLockError(context.Background(), io.ErrUnexpectedEOF)); category != domain.ErrorConflict {
		t.Fatalf("renewal ownership category=%s, want conflict", category)
	}
}

func TestOfflinePreflightIgnoresTerminalPVCConsumers(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	for name, phase := range map[string]corev1.PodPhase{
		"completed": corev1.PodSucceeded,
		"failed":    corev1.PodFailed,
	} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
			}}}},
			Status: corev1.PodStatus{Phase: phase},
		}
		if _, err := client.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
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

func TestOfflineBackupHelperStartRechecksConsumers(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	if err := validateBackupHelperStart(context.Background(), client, request); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "consumer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
		}}}},
	}
	if _, err := client.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := validateBackupHelperStart(context.Background(), client, request); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestOnlineBackupPinsRWOHelperToConsumerNode(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	request.Online = true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "consumer"}, Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}}
	if _, err := client.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	values, err := onlineBackupHelmValues(context.Background(), client, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != "rclone.nodeName=node-a" {
		t.Fatalf("Helm scheduling values=%v", values)
	}
}

func TestOnlineBackupRejectsRWOConsumersOnDifferentNodes(t *testing.T) {
	client, request := preflightFixture(t, &preflightObjectStore{})
	request.Online = true
	for name, node := range map[string]string{"consumer-a": "node-a", "consumer-b": "node-b"} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name}, Spec: corev1.PodSpec{NodeName: node, Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}}
		if _, err := client.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := onlineBackupHelmValues(context.Background(), client, request); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := Preflight(context.Background(), client, request, false); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("preflight category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestVerifyPVCIdentityRejectsNameReuseAndClaimRefMismatch(t *testing.T) {
	mode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "data", UID: types.UID("pvc-new")},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-data", VolumeMode: &mode},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv")},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "default", Name: "data", UID: types.UID("pvc-new")},
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	client := fake.NewClientset(pvc, pv)
	if _, _, err := verifyPVCIdentity(context.Background(), client, "default", "data", "pvc-old", "pv"); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("PVC name reuse category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, _, err := verifyPVCIdentity(context.Background(), client, "default", "data", "pvc-new", "pv"); err != nil {
		t.Fatal(err)
	}
	pv.Spec.ClaimRef.UID = types.UID("foreign")
	if _, err := client.CoreV1().PersistentVolumes().Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifyPVCIdentity(context.Background(), client, "default", "data", "pvc-new", "pv"); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("claimRef mismatch category=%s error=%v", domain.CategoryOf(err), err)
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
