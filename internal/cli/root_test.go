package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func executeCLI(t *testing.T, input string, args ...string) (string, string, error) {
	t.Helper()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command := NewRoot(Options{Version: "v1.2.3", In: strings.NewReader(input), Out: &stdout, ErrOut: &stderr})
	command.SetArgs(args)
	err := command.Execute()
	return stdout.String(), stderr.String(), err
}

func executeBackupCLI(t *testing.T, input string, args ...string) (string, string, error) {
	t.Helper()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command := NewRoot(Options{
		Version: "v1.2.3", In: strings.NewReader(input), Out: &stdout, ErrOut: &stderr,
		runtimeFactory: func(_ *rootState) (*commandRuntime, error) {
			mode := corev1.PersistentVolumeFilesystem
			return &commandRuntime{clients: &kube.Clients{Kubernetes: kubernetesfake.NewClientset(
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "data", UID: types.UID("pvc")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-data", VolumeMode: &mode, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv")},
					Spec: corev1.PersistentVolumeSpec{
						Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
						ClaimRef: &corev1.ObjectReference{Namespace: "default", Name: "data", UID: types.UID("pvc")},
					},
					Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
				},
			)}}, nil
		},
		objectStoreFactory: func(_ context.Context, cfg objectstore.Config) (*objectstore.Store, error) {
			return objectstore.NewWithClient(&testObjectStoreClient{}, cfg, objectstore.Credentials{AccessKey: "test", SecretKey: "test"})
		},
	})
	command.SetArgs(args)
	err := command.Execute()
	return stdout.String(), stderr.String(), err
}

type testObjectStoreClient struct{}

func (testObjectStoreClient) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, missingObjectError{}
}

func (testObjectStoreClient) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, missingObjectError{}
}

func (testObjectStoreClient) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{ETag: aws.String("etag")}, nil
}

func (testObjectStoreClient) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func (testObjectStoreClient) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

type missingObjectError struct{}

func (missingObjectError) Error() string                 { return "object not found" }
func (missingObjectError) ErrorCode() string             { return "NoSuchKey" }
func (missingObjectError) ErrorMessage() string          { return "object not found" }
func (missingObjectError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func TestVersionDoesNotInitializeKubernetes(t *testing.T) {
	stdout, _, err := executeCLI(t, "", "version")
	if err != nil {
		t.Fatal(err)
	}
	if stdout != "v1.2.3\n" {
		t.Fatalf("stdout=%q", stdout)
	}
}

func TestMigratePodRequiresPodBeforeClusterAccess(t *testing.T) {
	_, _, err := executeCLI(t, "", "migrate-pod", "--source-pvc", "data", "--target-node", "node-b")
	if domain.CategoryOf(err) != domain.ErrorValidation || !strings.Contains(err.Error(), "--pod") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestMigratePodRejectsSourcePVCOverride(t *testing.T) {
	_, _, err := executeCLI(t, "", "migrate-pod", "--pod", "db-0", "--source-pvc", "data", "--target-node", "node-b")
	if domain.CategoryOf(err) != domain.ErrorValidation || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestBackupDryRunPrintsStructuredPlanWithoutSecrets(t *testing.T) {
	stdout, stderr, err := executeBackupCLI(t, "", "backup", "--dry-run", "--output", "json", "--source-pvc", "data", "--backend", "s3", "--bucket", "backups", "--name", "daily", "--access-key", "visible-key", "--secret-key", "sensitive-secret")
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout)
	}
	if result["operation"] != "backup" || result["pvc"] != "data" || result["mode"] != "offline" {
		t.Fatalf("output=%v", result)
	}
	if result["destination"] != "s3://backups/pv-migrate/daily/" {
		t.Fatalf("destination=%v", result["destination"])
	}
	if strings.Contains(stdout, "visible-key") || strings.Contains(stdout, "sensitive-secret") {
		t.Fatalf("credentials leaked in output: %s", stdout)
	}
	if !strings.Contains(stderr, "dry-run completed") || !strings.Contains(stderr, "--dry-run=false") {
		t.Fatalf("missing follow-up guidance: %s", stderr)
	}
}

func TestLiveBackupDryRunAllowsMountedSourceSemantics(t *testing.T) {
	stdout, _, err := executeBackupCLI(t, "", "live-backup", "--output", "json", "--source-pvc", "data", "--backend", "s3", "--bucket", "backups", "--name", "daily")
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout)
	}
	if result["mode"] != "online" || result["consistency"] != "best-effort crash-consistent file copy" {
		t.Fatalf("online backup output=%v", result)
	}
}

func TestBackupPlanSubcommandIsOperationSpecific(t *testing.T) {
	stdout, _, err := executeBackupCLI(t, "", "backup", "plan", "--output", "json", "--source-pvc", "data", "--backend", "s3", "--bucket", "backups", "--name", "daily")
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout)
	}
	if result["operation"] != "backup" || result["mode"] != "offline" {
		t.Fatalf("backup plan output=%v", result)
	}

	stdout, _, err = executeBackupCLI(t, "", "live-backup", "plan", "--output", "json", "--source-pvc", "data", "--backend", "s3", "--bucket", "backups", "--name", "daily")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode live plan: %v\n%s", err, stdout)
	}
	if result["mode"] != "online" {
		t.Fatalf("live-backup plan output=%v", result)
	}
}

func TestBackupDefaultsToDryRun(t *testing.T) {
	stdout, _, err := executeBackupCLI(t, "", "backup", "--output", "json", "--source-pvc", "data", "--backend", "s3", "--bucket", "backups", "--name", "daily")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout, `"mode": "offline"`) {
		t.Fatalf("default backup did not produce a dry-run plan: %s", stdout)
	}
}

func TestExitCodesAreStable(t *testing.T) {
	cases := map[domain.ErrorCategory]int{
		domain.ErrorValidation:   2,
		domain.ErrorPrecondition: 3,
		domain.ErrorConflict:     4,
		domain.ErrorKubernetes:   5,
		domain.ErrorCopy:         6,
		domain.ErrorTimeout:      7,
	}
	for category, expected := range cases {
		err := domain.NewError(category, "test", "message")
		if actual := domain.ExitCode(err); actual != expected {
			t.Fatalf("category=%s exit=%d want=%d", category, actual, expected)
		}
	}
}
