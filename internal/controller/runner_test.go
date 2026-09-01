package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientfake "k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type runnerSessionStore struct {
	listed     *domain.Session
	latest     *domain.Session
	updates    []*domain.Session
	getErr     error
	acquireErr error
	lock       *runnerSessionLock
}

func (s *runnerSessionStore) Create(context.Context, *domain.Session) error { return nil }

func (s *runnerSessionStore) Get(context.Context, string, string) (*domain.Session, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return cloneRunnerSession(s.latest), nil
}

func (s *runnerSessionStore) Update(_ context.Context, session *domain.Session) error {
	s.updates = append(s.updates, cloneRunnerSession(session))
	s.latest = cloneRunnerSession(session)
	return nil
}

func (s *runnerSessionStore) List(context.Context, string) ([]*domain.Session, error) {
	return []*domain.Session{cloneRunnerSession(s.listed)}, nil
}

func (s *runnerSessionStore) Delete(context.Context, *domain.Session) error { return nil }

func (s *runnerSessionStore) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	if s.acquireErr != nil {
		return nil, s.acquireErr
	}
	return s.lock, nil
}

type runnerSessionLock struct {
	err        error
	releaseErr error
	released   bool
}

func (l *runnerSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

func (l *runnerSessionLock) Err() error { return l.err }

func (l *runnerSessionLock) Release(context.Context) error {
	l.released = true
	return l.releaseErr
}

func (l *runnerSessionLock) Delete(context.Context) error { return nil }

func cloneRunnerSession(in *domain.Session) *domain.Session {
	if in == nil {
		return nil
	}

	out := *in
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)

	return &out
}

func newRunnerSession(id string) *domain.Session {
	spec := domain.NewSessionSpec(
		domain.OperationCopy,
		domain.SessionCommon{
			SourceNamespace:      "system",
			TemporaryNamespace:   "system",
			DestinationNamespace: "system",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace: "system",
						Name:      "data",
						UID:       "pvc-uid",
					},
					DestinationPVC: domain.ObjectReference{Namespace: "system", Name: "data"},
				},
			},
		},
		false,
		domain.SessionWorkflowOptions{},
	)

	return domain.NewSession(id, spec, time.Unix(1, 0))
}

func TestRunnerRejectsTenantControlledToolImage(t *testing.T) {
	session := newRunnerSession("image-policy")
	session.Spec.Copy.ToolImage = "registry.example/pvc-migrate:trusted"

	runner := NewRunner(&recordingWorkflowResumer{}, nil, "system").
		WithTrustedToolImage("registry.example/pvc-migrate:trusted")
	if err := runner.validateTrustedToolImage(session); err != nil {
		t.Fatalf("matching controller image rejected: %v", err)
	}

	session.Spec.Copy.ToolImage = "registry.example/tenant:arbitrary"
	if err := runner.validateTrustedToolImage(session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("mismatched controller image category=%s error=%v", domain.CategoryOf(err), err)
	}

	session.Spec.Copy.ToolImage = "registry.example/tenant"
	if err := runner.validateTrustedToolImage(session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("untagged controller image category=%s error=%v", domain.CategoryOf(err), err)
	}
}

type recordingWorkflowResumer struct {
	called string
}

func TestRunnerObjectStoreProfileScopesTenantAndSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend:              v1alpha1.ObjectStoreBackendS3,
			Bucket:               "backups",
			Prefix:               "tenant-a",
			Endpoint:             "https://s3.example.test",
			ServerSideEncryption: "aws:kms",
			SSEKMSKeyID:          "key-id",
			CredentialsSecret: &v1alpha1.ObjectStoreCredentialsReference{
				Name: "s3-credentials",
			},
			AllowedNamespaces:                       []v1alpha1.ObjectStoreNamespace{"tenant-a"},
			AllowStaticCredentialsInTenantNamespace: true,
		},
	}
	runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
	kubeClient := clientfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-credentials", Namespace: "controller-system"},
		Data:       map[string][]byte{kube.BackupAccessKeyDataKey: []byte("access"), kube.BackupSecretKeyDataKey: []byte("secret")},
	})
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
		SourceNamespace: "tenant-a", SessionNamespace: "tenant-a",
	}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "tenant-store"
	spec.Backup.Bucket = "backups"
	spec.Backup.Prefix = "tenant-a"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant-a"
	runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant-a").
		WithKubernetesClient(kubeClient).
		WithControllerClient(runtimeClient).
		WithControllerNamespace("controller-system")
	config, err := runner.objectStoreConfig(context.Background(), domain.NewSession("backup", spec, time.Now()), "backups", "tenant-a", "daily", "", "", "", false, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if config.Bucket != "backups" || config.Prefix != "tenant-a/namespaces/tenant-a" ||
		config.AccessKey != "access" || config.ServerSideEncryption != "aws:kms" || config.SSEKMSKeyID != "key-id" {
		t.Fatalf("profile config=%#v", config)
	}
	otherNamespace := cloneRunnerSession(domain.NewSession("other", spec, time.Now()))
	otherNamespace.Spec.SessionNamespace = "tenant-b"
	if _, err := runner.objectStoreConfig(context.Background(), otherNamespace, "backups", "tenant-a", "daily", "", "", "", false, "", ""); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("static profile allowed an unlisted namespace: category=%s error=%v", domain.CategoryOf(err), err)
	}

	badBucket := domain.NewSession("backup-bucket", spec, time.Now())
	if _, err := runner.objectStoreConfig(context.Background(), badBucket, "other", "tenant-a", "daily", "", "", "", false, "", ""); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("bucket scope category=%s error=%v", domain.CategoryOf(err), err)
	}
	badNamespace := domain.NewSession("backup-namespace", spec, time.Now())
	badNamespace.Spec.SessionNamespace = "tenant-c"
	if _, err := runner.objectStoreConfig(context.Background(), badNamespace, "backups", "tenant-a", "daily", "", "", "", false, "", ""); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("namespace scope category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRunnerObjectStoreProfileScopesClusterIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend: v1alpha1.ObjectStoreBackendS3, Bucket: "backups", Prefix: "shared",
			Endpoint:          "https://s3.example.test",
			CredentialsSecret: &v1alpha1.ObjectStoreCredentialsReference{Name: "credentials"},
			AllowedNamespaces: []v1alpha1.ObjectStoreNamespace{"tenant"}, AllowStaticCredentialsInTenantNamespace: true,
		},
	}
	runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
	kubeClient := clientfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "controller-system"},
		Data:       map[string][]byte{kube.BackupAccessKeyDataKey: []byte("access"), kube.BackupSecretKeyDataKey: []byte("secret")},
	})
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
		SourceNamespace: "tenant", SessionNamespace: "tenant",
	}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "store"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant"
	session := domain.NewSession("backup", spec, time.Now())

	first := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").
		WithKubernetesClient(kubeClient).WithControllerClient(runtimeClient).
		WithControllerNamespace("controller-system").WithClusterIdentity("cluster-one")
	firstConfig, err := first.objectStoreConfig(context.Background(), session, "backups", "", "daily", "", "", "", false, "", "")
	if err != nil {
		t.Fatal(err)
	}
	wantSegment := clusterScopeSegment("cluster-one")
	if firstConfig.Prefix != "shared/clusters/"+wantSegment+"/namespaces/tenant" {
		t.Fatalf("cluster-scoped prefix=%q, want shared/clusters/%s/namespaces/tenant", firstConfig.Prefix, wantSegment)
	}
	if strings.Contains(firstConfig.Prefix, "cluster-one") {
		t.Fatalf("raw cluster identity leaked into prefix: %q", firstConfig.Prefix)
	}

	second := first.WithClusterIdentity("cluster-two")
	secondConfig, err := second.objectStoreConfig(context.Background(), session, "backups", "", "daily", "", "", "", false, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if firstConfig.Prefix == secondConfig.Prefix {
		t.Fatalf("different cluster identities shared prefix %q", firstConfig.Prefix)
	}
}

func TestRunnerObjectStoreProfileRequiresControllerNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend:           v1alpha1.ObjectStoreBackendS3,
			Bucket:            "backups",
			Endpoint:          "https://s3.example.test",
			AllowedNamespaces: []v1alpha1.ObjectStoreNamespace{"tenant"},
			CredentialsSecret: &v1alpha1.ObjectStoreCredentialsReference{
				Name: "credentials",
			},
			AllowStaticCredentialsInTenantNamespace: true,
		},
	}
	runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
	kubeClient := clientfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "controller-system"},
		Data: map[string][]byte{
			kube.BackupAccessKeyDataKey: []byte("access"),
			kube.BackupSecretKeyDataKey: []byte("secret"),
		},
	})
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
		SourceNamespace: "tenant", SessionNamespace: "tenant",
	}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "store"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant"
	session := domain.NewSession("backup", spec, time.Now())

	runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").
		WithKubernetesClient(kubeClient).
		WithControllerClient(runtimeClient)
	_, err := runner.objectStoreConfig(
		context.Background(), session, "backups", "", "daily", "", "", "", false, "", "",
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v, want precondition", domain.CategoryOf(err), err)
	}
}

func TestRunnerObjectStoreProfileSupportsWorkloadIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: "s3-transfer", Namespace: "tenant", UID: types.UID("sa-uid"),
	}}
	profile := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "ambient-store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend:           v1alpha1.ObjectStoreBackendS3,
			Bucket:            "backups",
			Endpoint:          "https://s3.example.test",
			CredentialsSecret: &v1alpha1.ObjectStoreCredentialsReference{Name: "controller-credentials"},
			ServiceAccountRefs: []v1alpha1.ObjectStoreServiceAccountReference{{
				Name: "s3-transfer", Namespace: "tenant", UID: "sa-uid",
				IdentityFingerprint: kube.ServiceAccountIdentityFingerprint(serviceAccount),
			}},
		},
	}
	runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
	kubeClient := clientfake.NewSimpleClientset(
		serviceAccount,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "controller-credentials", Namespace: "controller-system"},
			Data:       map[string][]byte{kube.BackupAccessKeyDataKey: []byte("access"), kube.BackupSecretKeyDataKey: []byte("secret")},
		},
	)
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
		SourceNamespace: "tenant", SessionNamespace: "tenant",
	}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "ambient-store"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant"
	runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").
		WithKubernetesClient(kubeClient).
		WithControllerClient(runtimeClient).
		WithControllerNamespace("controller-system")
	config, err := runner.objectStoreConfig(context.Background(), domain.NewSession("backup", spec, time.Now()), "backups", "", "daily", "", "", "", false, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !config.UseAmbientCredentials || config.AccessKey != "access" || config.SecretKey != "secret" || config.ServiceAccountName != "s3-transfer" || config.ServiceAccountUID != "sa-uid" {
		t.Fatalf("workload identity config=%#v", config)
	}

	t.Run("rejects disabled token automount", func(t *testing.T) {
		automount := false
		disabledAccount := &corev1.ServiceAccount{
			ObjectMeta:                   metav1.ObjectMeta{Name: "s3-transfer", Namespace: "tenant", UID: types.UID("sa-uid")},
			AutomountServiceAccountToken: &automount,
		}
		disabledProfile := profile.DeepCopy()
		disabledProfile.Spec.ServiceAccountRefs[0].IdentityFingerprint = kube.ServiceAccountIdentityFingerprint(disabledAccount)
		disabledRuntimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(disabledProfile).Build()
		client := clientfake.NewSimpleClientset(
			disabledAccount,
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "controller-credentials", Namespace: "controller-system"},
				Data:       map[string][]byte{kube.BackupAccessKeyDataKey: []byte("access"), kube.BackupSecretKeyDataKey: []byte("secret")},
			},
		)
		runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").
			WithKubernetesClient(client).
			WithControllerClient(disabledRuntimeClient).
			WithControllerNamespace("controller-system")
		_, err := runner.objectStoreConfig(context.Background(), domain.NewSession("backup", spec, time.Now()), "backups", "", "daily", "", "", "", false, "", "")
		if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "automountServiceAccountToken") {
			t.Fatalf("disabled token automount accepted: category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("rejects identity metadata drift", func(t *testing.T) {
		mutated := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Name: "s3-transfer", Namespace: "tenant", UID: types.UID("sa-uid"),
			Annotations: map[string]string{"iam.example/role": "elevated"},
		}}
		client := clientfake.NewSimpleClientset(
			mutated,
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "controller-credentials", Namespace: "controller-system"},
				Data:       map[string][]byte{kube.BackupAccessKeyDataKey: []byte("access"), kube.BackupSecretKeyDataKey: []byte("secret")},
			},
		)
		runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").
			WithKubernetesClient(client).
			WithControllerClient(runtimeClient).
			WithControllerNamespace("controller-system")
		_, err := runner.objectStoreConfig(context.Background(), domain.NewSession("backup", spec, time.Now()), "backups", "", "daily", "", "", "", false, "", "")
		if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "identity fingerprint") {
			t.Fatalf("identity metadata drift accepted: category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("rejects replacement ServiceAccount", func(t *testing.T) {
		replacement := &v1alpha1.ObjectStoreProfile{
			ObjectMeta: metav1.ObjectMeta{Name: "ambient-store"},
			Spec: v1alpha1.ObjectStoreProfileSpec{
				Backend: v1alpha1.ObjectStoreBackendS3, Bucket: "backups", Endpoint: "https://s3.example.test",
				CredentialsSecret:  &v1alpha1.ObjectStoreCredentialsReference{Name: "controller-credentials"},
				ServiceAccountRefs: []v1alpha1.ObjectStoreServiceAccountReference{{Name: "s3-transfer", Namespace: "tenant", UID: "different-uid", IdentityFingerprint: strings.Repeat("a", 64)}},
			},
		}
		runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(replacement).Build()
		runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").WithKubernetesClient(kubeClient).WithControllerClient(runtimeClient).WithControllerNamespace("controller-system")
		_, err := runner.objectStoreConfig(context.Background(), domain.NewSession("backup", spec, time.Now()), "backups", "", "daily", "", "", "", false, "", "")
		if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "UID does not match") {
			t.Fatalf("replacement ServiceAccount accepted: category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("rejects unbound namespace", func(t *testing.T) {
		unbound := domain.NewSession("backup", spec, time.Now())
		unbound.Spec.SessionNamespace = "other"
		runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").WithKubernetesClient(kubeClient).WithControllerClient(runtimeClient).WithControllerNamespace("controller-system")
		_, err := runner.objectStoreConfig(context.Background(), unbound, "backups", "", "daily", "", "", "", false, "", "")
		if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "no ServiceAccount binding") {
			t.Fatalf("unbound namespace accepted: category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("allows ambient controller credentials", func(t *testing.T) {
		withoutControllerSecret := &v1alpha1.ObjectStoreProfile{
			ObjectMeta: metav1.ObjectMeta{Name: "ambient-store"},
			Spec: v1alpha1.ObjectStoreProfileSpec{
				Backend: v1alpha1.ObjectStoreBackendS3, Bucket: "backups", Endpoint: "https://s3.example.test",
				ServiceAccountRefs: []v1alpha1.ObjectStoreServiceAccountReference{{Name: "s3-transfer", Namespace: "tenant", UID: "sa-uid", IdentityFingerprint: kube.ServiceAccountIdentityFingerprint(&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "s3-transfer", Namespace: "tenant", UID: types.UID("sa-uid")}})}},
			},
		}
		runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(withoutControllerSecret).Build()
		runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").WithKubernetesClient(kubeClient).WithControllerClient(runtimeClient)
		config, err := runner.objectStoreConfig(context.Background(), domain.NewSession("backup", spec, time.Now()), "backups", "", "daily", "", "", "", false, "", "")
		if err != nil {
			t.Fatalf("ambient controller credentials rejected: %v", err)
		}
		if !config.UseAmbientCredentials || config.AccessKey != "" || config.SecretKey != "" || config.CredentialsSecretUID != "" {
			t.Fatalf("ambient controller credentials leaked into config: %#v", config)
		}
	})
}

func TestRunnerObjectStoreProfileRejectsDeletionAndAuthAmbiguity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	base := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend: v1alpha1.ObjectStoreBackendS3,
			Bucket:  "backups", Endpoint: "https://s3.example.test",
			AllowedNamespaces:                       []v1alpha1.ObjectStoreNamespace{"tenant"},
			CredentialsSecret:                       &v1alpha1.ObjectStoreCredentialsReference{Name: "credentials"},
			AllowStaticCredentialsInTenantNamespace: true,
		},
	}
	kubeClient := clientfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "controller-system", UID: types.UID("secret-uid")},
		Data:       map[string][]byte{kube.BackupAccessKeyDataKey: []byte("access"), kube.BackupSecretKeyDataKey: []byte("secret")},
	})
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{SourceNamespace: "tenant", SessionNamespace: "tenant"}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "store"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant"
	session := domain.NewSession("backup", spec, time.Now())

	t.Run("deleting profile", func(t *testing.T) {
		profile := base.DeepCopy()
		profile.DeletionTimestamp = &now
		profile.Finalizers = []string{"migrate.sealos.io/test-finalizer"}
		runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
		runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").WithKubernetesClient(kubeClient).WithControllerClient(runtimeClient).WithControllerNamespace("controller-system")
		_, err := runner.objectStoreConfig(context.Background(), session, "backups", "", "daily", "", "", "", false, "", "")
		if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "being deleted") {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("static and workload identity", func(t *testing.T) {
		profile := base.DeepCopy()
		profile.Spec.ServiceAccountRefs = []v1alpha1.ObjectStoreServiceAccountReference{{
			Name: "s3-transfer", Namespace: "tenant", UID: "sa-uid", IdentityFingerprint: strings.Repeat("a", 64),
		}}
		runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
		runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").WithKubernetesClient(kubeClient).WithControllerClient(runtimeClient).WithControllerNamespace("controller-system")
		_, err := runner.objectStoreConfig(context.Background(), session, "backups", "", "daily", "", "", "", false, "", "")
		if domain.CategoryOf(err) != domain.ErrorValidation || !strings.Contains(err.Error(), "tenant-namespace projection approval cannot be combined with workload identity") {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("static profile requires credentials Secret", func(t *testing.T) {
		profile := base.DeepCopy()
		profile.Spec.CredentialsSecret = nil
		runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
		runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").
			WithKubernetesClient(kubeClient).
			WithControllerClient(runtimeClient).
			WithControllerNamespace("controller-system")
		_, err := runner.objectStoreConfig(context.Background(), session, "backups", "", "daily", "", "", "", false, "", "")
		if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "static credential profiles require credentialsSecret") {
			t.Fatalf("static profile without credentials Secret accepted: category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestRunnerObjectStoreProfileRejectsAmbiguousServiceAccountBindings(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "ambient-store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend: v1alpha1.ObjectStoreBackendS3, Bucket: "backups", Endpoint: "https://s3.example.test",
			CredentialsSecret: &v1alpha1.ObjectStoreCredentialsReference{Name: "controller-credentials"},
			ServiceAccountRefs: []v1alpha1.ObjectStoreServiceAccountReference{
				{Name: "one", Namespace: "tenant", UID: "uid-one", IdentityFingerprint: strings.Repeat("a", 64)},
				{Name: "two", Namespace: "tenant", UID: "uid-two", IdentityFingerprint: strings.Repeat("b", 64)},
			},
		},
	}
	runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
	kubeClient := clientfake.NewSimpleClientset(
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "one", Namespace: "tenant", UID: types.UID("uid-one")}},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "controller-credentials", Namespace: "controller-system"},
			Data:       map[string][]byte{kube.BackupAccessKeyDataKey: []byte("access"), kube.BackupSecretKeyDataKey: []byte("secret")},
		},
	)
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{SourceNamespace: "tenant", SessionNamespace: "tenant"}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "ambient-store"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant"
	runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").WithKubernetesClient(kubeClient).WithControllerClient(runtimeClient)
	_, err := runner.objectStoreConfig(context.Background(), domain.NewSession("backup", spec, time.Now()), "backups", "", "daily", "", "", "", false, "", "")
	if domain.CategoryOf(err) != domain.ErrorValidation || !strings.Contains(err.Error(), "at most one binding") {
		t.Fatalf("ambiguous bindings category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRunnerObjectStoreProfileRejectsInvalidScopeConfiguration(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend:  v1alpha1.ObjectStoreBackendS3,
			Bucket:   "backups",
			Endpoint: "https://s3.example.test",
			CredentialsSecret: &v1alpha1.ObjectStoreCredentialsReference{
				Name: "credentials",
			},
			AllowedNamespaces:                       []v1alpha1.ObjectStoreNamespace{"tenant"},
			AllowStaticCredentialsInTenantNamespace: true,
		},
	}
	kubeClient := clientfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "controller-system"},
		Data: map[string][]byte{
			kube.BackupAccessKeyDataKey: []byte("access"),
			kube.BackupSecretKeyDataKey: []byte("secret"),
		},
	})
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
		SourceNamespace: "tenant", SessionNamespace: "tenant",
	}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "store"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant"
	session := domain.NewSession("backup", spec, time.Now())

	for name, mutate := range map[string]func(*v1alpha1.ObjectStoreProfileSpec){
		"path traversal prefix": func(profile *v1alpha1.ObjectStoreProfileSpec) {
			profile.Prefix = "team/../other"
		},
		"invalid namespace allowlist": func(profile *v1alpha1.ObjectStoreProfileSpec) {
			profile.AllowedNamespaces = []v1alpha1.ObjectStoreNamespace{"Tenant-A"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			profile := base.DeepCopy()
			mutate(&profile.Spec)
			runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
			runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").
				WithKubernetesClient(kubeClient).
				WithControllerClient(runtimeClient).
				WithControllerNamespace("controller-system")
			_, err := runner.objectStoreConfig(
				context.Background(), session, "backups", "", "daily", "", "", "", false, "", "",
			)
			if domain.CategoryOf(err) != domain.ErrorValidation {
				t.Fatalf("category=%s error=%v, want validation", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestRunnerObjectStoreProfileEnforcesCredentialTrustBoundaries(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend: v1alpha1.ObjectStoreBackendS3, Bucket: "backups", Endpoint: "https://s3.example.test",
			AllowedNamespaces:                       []v1alpha1.ObjectStoreNamespace{"tenant"},
			CredentialsSecret:                       &v1alpha1.ObjectStoreCredentialsReference{Name: "credentials"},
			AllowStaticCredentialsInTenantNamespace: true,
		},
	}
	kubeClient := clientfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "controller-system"},
		Data:       map[string][]byte{kube.BackupAccessKeyDataKey: []byte("access"), kube.BackupSecretKeyDataKey: []byte("secret")},
	})
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{SourceNamespace: "tenant", SessionNamespace: "tenant"}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "store"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant"
	session := domain.NewSession("backup", spec, time.Now())

	tests := []struct {
		name     string
		mutate   func(*v1alpha1.ObjectStoreProfileSpec)
		category domain.ErrorCategory
		message  string
	}{
		{
			name: "missing explicit projection approval",
			mutate: func(spec *v1alpha1.ObjectStoreProfileSpec) {
				spec.AllowStaticCredentialsInTenantNamespace = false
			},
			category: domain.ErrorPrecondition,
			message:  "explicit tenant-namespace projection approval",
		},
		{
			name: "multiple static namespaces",
			mutate: func(spec *v1alpha1.ObjectStoreProfileSpec) {
				spec.AllowedNamespaces = []v1alpha1.ObjectStoreNamespace{"tenant", "other"}
			},
			category: domain.ErrorPrecondition,
			message:  "exactly one tenant namespace",
		},
		{
			name: "workload identity with static approval",
			mutate: func(spec *v1alpha1.ObjectStoreProfileSpec) {
				spec.CredentialsSecret = nil
				spec.AllowedNamespaces = nil
				spec.ServiceAccountRefs = []v1alpha1.ObjectStoreServiceAccountReference{{Name: "s3-transfer", Namespace: "tenant", UID: "sa-uid", IdentityFingerprint: strings.Repeat("a", 64)}}
			},
			category: domain.ErrorValidation,
			message:  "cannot be combined with workload identity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := base.DeepCopy()
			test.mutate(&profile.Spec)
			runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
			runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").WithKubernetesClient(kubeClient).WithControllerClient(runtimeClient).WithControllerNamespace("controller-system")
			_, err := runner.objectStoreConfig(context.Background(), session, "backups", "", "daily", "", "", "", false, "", "")
			if domain.CategoryOf(err) != test.category || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("category=%s error=%v, want %s containing %q", domain.CategoryOf(err), err, test.category, test.message)
			}
		})
	}
}

func TestRunnerObjectStoreProfileFailsClosedWithoutNamespaceScope(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	profile := &v1alpha1.ObjectStoreProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "store"},
		Spec: v1alpha1.ObjectStoreProfileSpec{
			Backend:  v1alpha1.ObjectStoreBackendS3,
			Bucket:   "backups",
			Endpoint: "https://s3.example.test",
			CredentialsSecret: &v1alpha1.ObjectStoreCredentialsReference{
				Name: "credentials",
			},
			AllowStaticCredentialsInTenantNamespace: true,
		},
	}
	runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(profile).Build()
	kubeClient := clientfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "controller-system"},
		Data: map[string][]byte{
			kube.BackupAccessKeyDataKey: []byte("access"),
			kube.BackupSecretKeyDataKey: []byte("secret"),
		},
	})
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
		SourceNamespace: "tenant", SessionNamespace: "tenant",
	}, false, domain.SessionWorkflowOptions{})
	spec.Backup.ObjectStoreProfile = "store"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	spec.Backup.SourcePVC.Namespace = "tenant"
	runner := NewRunner(&recordingWorkflowResumer{}, nil, "tenant").
		WithKubernetesClient(kubeClient).
		WithControllerClient(runtimeClient).
		WithControllerNamespace("controller-system")
	_, err := runner.objectStoreConfig(
		context.Background(), domain.NewSession("backup", spec, time.Now()), "backups", "", "daily", "", "", "", false, "", "",
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "exactly one tenant namespace") {
		t.Fatalf("empty namespace scope accepted: category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func (r *recordingWorkflowResumer) call(name string) error {
	r.called = name
	return fmt.Errorf("%s dispatched", name)
}

func (r *recordingWorkflowResumer) ResumeReserve(context.Context, *domain.Session) error {
	return r.call("reserve")
}

func (r *recordingWorkflowResumer) ResumeOfflineMigration(context.Context, *domain.Session) error {
	return r.call("migrate")
}

func (r *recordingWorkflowResumer) ResumePodMigration(context.Context, *domain.Session) error {
	return r.call("migrate-pod")
}

func (r *recordingWorkflowResumer) ResumeCopy(context.Context, *domain.Session) error {
	return r.call("copy")
}

func (r *recordingWorkflowResumer) ResumeRename(context.Context, *domain.Session) error {
	return r.call("rename")
}

func (r *recordingWorkflowResumer) ResumeMove(context.Context, *domain.Session) error {
	return r.call("move")
}

func TestRunnerRequiresServiceBeforeReconcilingCrossNamespaceMigration(t *testing.T) {
	spec := domain.NewOfflineMigrationSessionSpec(domain.SessionCommon{
		SourceNamespace:      "source",
		TemporaryNamespace:   "system",
		DestinationNamespace: "destination",
		SessionNamespace:     "system",
		Volumes: []domain.VolumeSpec{{
			SourcePVC:      domain.ObjectReference{Name: "source"},
			DestinationPVC: domain.ObjectReference{Name: "destination"},
		}},
	}, domain.SessionWorkflowOptions{})
	session := domain.NewSession("move-session", spec, time.Now())
	runner := NewRunner(nil, nil, "system")

	err := runner.reconcileSession(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorValidation ||
		err.Error() != "controller reconcile: service is required" {
		t.Fatalf("error category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRunnerDispatchesEveryControllerWorkflow(t *testing.T) {
	common := domain.SessionCommon{
		SourceNamespace:      "system",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		SessionNamespace:     "system",
		Volumes: []domain.VolumeSpec{
			{
				SourcePVC: domain.ObjectReference{
					Namespace: "system",
					Name:      "source",
					UID:       "pvc-uid",
				},
				SourcePV: domain.ObjectReference{Name: "pv-source", UID: "pv-uid"},
				DestinationPVC: domain.ObjectReference{
					Namespace: "system",
					Name:      "destination",
				},
			},
		},
	}

	tests := []struct {
		name     string
		typeName domain.SessionType
		make     func() domain.SessionSpec
		want     string
	}{
		{name: "reserve", typeName: domain.SessionTypeReserve, make: func() domain.SessionSpec {
			return domain.NewSessionSpec(
				domain.OperationReserve,
				common,
				false,
				domain.SessionWorkflowOptions{},
			)
		}, want: "reserve"},
		{name: "migration", typeName: domain.SessionTypeMigrate, make: func() domain.SessionSpec {
			return domain.NewOfflineMigrationSessionSpec(common, domain.SessionWorkflowOptions{})
		}, want: "migrate"},
		{
			name:     "pod migration",
			typeName: domain.SessionTypeMigratePod,
			make: func() domain.SessionSpec {
				return domain.NewPodMigrationSessionSpec(
					common,
					domain.WorkloadSpec{Adapter: domain.WorkloadNone},
					domain.SessionWorkflowOptions{},
					1,
					false,
				)
			},
			want: "migrate-pod",
		},
		{name: "copy", typeName: domain.SessionTypeCopy, make: func() domain.SessionSpec {
			return domain.NewSessionSpec(
				domain.OperationCopy,
				common,
				false,
				domain.SessionWorkflowOptions{},
			)
		}, want: "copy"},
		{name: "rename", typeName: domain.SessionTypeRename, make: func() domain.SessionSpec {
			c := common
			c.DestinationNamespace = c.SourceNamespace

			return domain.NewSessionSpec(
				domain.OperationRename,
				c,
				false,
				domain.SessionWorkflowOptions{},
			)
		}, want: "rename"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resumer := &recordingWorkflowResumer{}
			runner := NewRunner(resumer, nil, "system")
			session := domain.NewSession(test.name, test.make(), time.Now())

			err := runner.reconcileSession(context.Background(), session)
			if resumer.called != test.want {
				t.Fatalf("dispatch=%q, want %q (error=%v)", resumer.called, test.want, err)
			}

			if err == nil || err.Error() != test.want+" dispatched" {
				t.Fatalf("error=%v, want %q", err, test.want+" dispatched")
			}
		})
	}

	for _, test := range []struct {
		name string
		make func() domain.SessionSpec
	}{
		{name: "backup", make: func() domain.SessionSpec {
			spec := domain.NewSessionSpec(domain.OperationBackup, common, false, domain.SessionWorkflowOptions{})
			spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "system", Name: "source", UID: "pvc-uid"}
			return spec
		}},
		{name: "restore", make: func() domain.SessionSpec {
			spec := domain.NewSessionSpec(domain.OperationRestore, common, false, domain.SessionWorkflowOptions{})
			spec.Restore.DestinationPVC = domain.ObjectReference{Namespace: "system", Name: "destination"}
			return spec
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := NewRunner(&recordingWorkflowResumer{}, nil, "system")
			session := domain.NewSession(test.name, test.make(), time.Now())
			if session.Spec.Backup != nil {
				session.Spec.Backup.ObjectStoreProfile = "default"
			}
			if session.Spec.Restore != nil {
				session.Spec.Restore.ObjectStoreProfile = "default"
			}

			err := runner.reconcileSession(context.Background(), session)
			if err == nil ||
				err.Error() != "controller reconcile: "+test.name+" controller execution requires a Kubernetes client" {
				t.Fatalf("error=%v, want missing Kubernetes client dispatch error", err)
			}
		})
	}
}

func TestTerminalSessionUsesOperationSpecificCompletionPhase(t *testing.T) {
	tests := []struct {
		name     string
		typeName domain.SessionType
		phase    domain.Phase
		want     bool
	}{
		{
			name: "migration completed", typeName: domain.SessionTypeMigrate,
			phase: domain.PhaseCompleted, want: true,
		},
		{
			name: "pod migration completed", typeName: domain.SessionTypeMigratePod,
			phase: domain.PhaseCompleted, want: true,
		},
		{
			name: "reservation reserved", typeName: domain.SessionTypeReserve,
			phase: domain.PhaseReserved, want: true,
		},
		{
			name: "copy warm copied", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseWarmCopied, want: true,
		},
		{
			name: "migration reserved", typeName: domain.SessionTypeMigrate,
			phase: domain.PhaseReserved, want: false,
		},
		{
			name: "pod migration warm copied", typeName: domain.SessionTypeMigratePod,
			phase: domain.PhaseWarmCopied, want: false,
		},
		{
			name: "reservation reserving", typeName: domain.SessionTypeReserve,
			phase: domain.PhaseReserving, want: false,
		},
		{
			name: "copy warm copying", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseWarmCopying, want: false,
		},
		{
			name: "aborted copy", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseAborted, want: true,
		},
		{
			name: "failed copy", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseFailed, want: true,
		},
		{
			name: "rolled back move", typeName: domain.SessionTypeMove,
			phase: domain.PhaseRolledBack, want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &domain.Session{
				Spec:   domain.SessionSpec{Type: test.typeName},
				Status: domain.SessionStatus{Phase: test.phase},
			}
			if got := terminalSession(session); got != test.want {
				t.Fatalf("terminalSession()=%t, want %t", got, test.want)
			}
		})
	}

	if !terminalSession(nil) {
		t.Fatal("nil session must be ignored as terminal")
	}
}

func TestRunnerCheckpointFailureUsesLatestSessionState(t *testing.T) {
	for _, test := range []struct {
		name            string
		latestPhase     domain.Phase
		wantUpdate      bool
		wantLatestPhase domain.Phase
	}{
		{name: "active session is failed", latestPhase: domain.PhaseReserved, wantUpdate: true, wantLatestPhase: domain.PhaseFailed},
		{name: "completed session wins", latestPhase: domain.PhaseCompleted, wantUpdate: false, wantLatestPhase: domain.PhaseCompleted},
		{name: "aborted session wins", latestPhase: domain.PhaseAborted, wantUpdate: false, wantLatestPhase: domain.PhaseAborted},
		{name: "already failed is unchanged", latestPhase: domain.PhaseFailed, wantUpdate: false, wantLatestPhase: domain.PhaseFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			listed := newRunnerSession(test.name)
			latest := cloneRunnerSession(listed)
			latest.Status.Phase = test.latestPhase
			store := &runnerSessionStore{
				listed: listed,
				latest: latest,
				lock:   &runnerSessionLock{},
			}
			runner := NewRunner(&recordingWorkflowResumer{}, store, "system")

			err := runner.ReconcileOnce(context.Background())
			if err == nil || !strings.Contains(err.Error(), "copy dispatched") {
				t.Fatalf("reconcile error=%v", err)
			}

			if (len(store.updates) > 0) != test.wantUpdate {
				t.Fatalf("updates=%d, want update=%t", len(store.updates), test.wantUpdate)
			}

			if store.latest.Status.Phase != test.wantLatestPhase {
				t.Fatalf(
					"latest phase=%s, want %s",
					store.latest.Status.Phase,
					test.wantLatestPhase,
				)
			}

			if !store.lock.released {
				t.Fatal("session lock was not released")
			}
		})
	}
}

func TestRunnerCheckpointFailurePreservesLockAcquisitionError(t *testing.T) {
	listed := newRunnerSession("lock-error")
	lockErr := errors.New("lock unavailable")
	store := &runnerSessionStore{listed: listed, latest: listed, acquireErr: lockErr}
	runner := NewRunner(&recordingWorkflowResumer{}, store, "system")

	err := runner.ReconcileOnce(context.Background())
	if !strings.Contains(err.Error(), "copy dispatched") || !errors.Is(err, lockErr) {
		t.Fatalf("error=%v, want dispatch and lock errors", err)
	}

	if len(store.updates) != 0 {
		t.Fatalf("session was updated after lock acquisition failed: %d", len(store.updates))
	}
}
