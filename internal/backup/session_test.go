package backup

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type recordingBackupSessionStore struct {
	created *domain.Session
	updated []*domain.Session
}

func (s *recordingBackupSessionStore) Create(_ context.Context, session *domain.Session) error {
	s.created = session
	return session.Validate()
}

func (s *recordingBackupSessionStore) Get(
	context.Context,
	string,
	string,
) (*domain.Session, error) {
	return s.created, nil
}

func (s *recordingBackupSessionStore) Update(_ context.Context, session *domain.Session) error {
	s.updated = append(s.updated, session)
	return nil
}

func (s *recordingBackupSessionStore) List(context.Context, string) ([]*domain.Session, error) {
	return nil, nil
}

func (s *recordingBackupSessionStore) Delete(context.Context, *domain.Session) error { return nil }

type recordingBackupSessionLock struct {
	bound      bool
	released   bool
	err        error
	releaseErr error
}

func (l *recordingBackupSessionLock) Bind(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	l.bound = true
	return context.WithCancel(ctx)
}

func (l *recordingBackupSessionLock) Err() error { return l.err }

func (l *recordingBackupSessionLock) Release(context.Context) error {
	l.released = true
	return l.releaseErr
}

func (l *recordingBackupSessionLock) Delete(context.Context) error { return nil }

type lockingBackupSessionStore struct {
	recordingBackupSessionStore
	lock             *recordingBackupSessionLock
	updateErr        error
	leaseErrOnUpdate error
	createWhileBound bool
	updateWhileBound bool
}

func TestBuildResumeRequestReusesControllerProvidedStore(t *testing.T) {
	provided := testBackupObjectStore(t)
	request := Request{
		ID:               "backup-resume",
		Namespace:        "app",
		SessionNamespace: "app",
		Store:            provided,
		SessionStore:     &recordingBackupSessionStore{},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv-uid")},
	}

	session, err := buildBackupSession(request, pvc, pv)
	if err != nil {
		t.Fatal(err)
	}

	resumed, err := buildResumeRequest(
		context.Background(),
		fake.NewSimpleClientset(),
		request,
		session,
	)
	if err != nil {
		t.Fatal(err)
	}

	if resumed.Store != provided {
		t.Fatal("resume rebuilt or replaced the controller-provided object store")
	}
}

func TestPinBackupRepositoryCapturesAndRejectsRevisionDrift(t *testing.T) {
	store := &recordingBackupSessionStore{}
	session := domain.NewSession(
		"backup-repository",
		domain.NewSessionSpec(
			domain.OperationBackup,
			domain.SessionCommon{SourceNamespace: "app", SessionNamespace: "app"},
			false,
			domain.SessionWorkflowOptions{},
		),
		time.Now(),
	)
	req := Request{
		BackupRepository: "shared-s3",
		BackupRepositoryBinding: &domain.BackupRepositoryBindingStatus{
			Type:       domain.BackupRepositoryTypeS3,
			UID:        types.UID("repository-uid"),
			Generation: 3,
			S3: &domain.S3BackupRepositoryBindingStatus{
				CredentialsSecretUID: types.UID("secret-uid"),
			},
		},
		SessionStore: store,
	}

	if err := pinBackupRepository(context.Background(), req, session); err != nil {
		t.Fatal(err)
	}

	if session.Status.BackupRepository == nil ||
		session.Status.BackupRepository.UID != req.BackupRepositoryBinding.UID ||
		session.Status.BackupRepository.Generation != req.BackupRepositoryBinding.Generation ||
		session.Status.BackupRepository.S3.CredentialsSecretUID !=
			req.BackupRepositoryBinding.S3.CredentialsSecretUID ||
		len(store.updated) != 1 {
		t.Fatalf(
			"repository pin=%q/%d updates=%d",
			session.Status.BackupRepository.UID,
			session.Status.BackupRepository.Generation,
			len(store.updated),
		)
	}

	if err := pinBackupRepository(context.Background(), req, session); err != nil {
		t.Fatalf("same repository revision rejected: %v", err)
	}

	drifted := req
	drifted.BackupRepositoryBinding = copyRepositoryBinding(req.BackupRepositoryBinding)
	drifted.BackupRepositoryBinding.Generation++

	if err := pinBackupRepository(
		context.Background(),
		drifted,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("generation drift error=%v category=%q", err, domain.CategoryOf(err))
	}

	drifted = req
	drifted.BackupRepositoryBinding = copyRepositoryBinding(req.BackupRepositoryBinding)
	drifted.BackupRepositoryBinding.UID = "replacement-uid"

	if err := pinBackupRepository(
		context.Background(),
		drifted,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("UID drift error=%v category=%q", err, domain.CategoryOf(err))
	}

	drifted = req
	drifted.BackupRepositoryBinding = copyRepositoryBinding(req.BackupRepositoryBinding)
	drifted.BackupRepositoryBinding.S3.CredentialsSecretUID = "replacement-secret-uid"

	if err := pinBackupRepository(
		context.Background(),
		drifted,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("Secret UID drift error=%v category=%q", err, domain.CategoryOf(err))
	}

	drifted = req
	drifted.BackupRepositoryBinding = copyRepositoryBinding(req.BackupRepositoryBinding)
	drifted.BackupRepositoryBinding.Type = domain.BackupRepositoryTypePVC
	drifted.BackupRepositoryBinding.S3 = nil
	drifted.BackupRepositoryBinding.PVC = &domain.PVCBackupRepositoryBindingStatus{
		ClaimUID: "claim-uid",
	}

	if err := pinBackupRepository(
		context.Background(),
		drifted,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("backend drift error=%v category=%q", err, domain.CategoryOf(err))
	}
}

func TestPinPVCBackupRepositoryRejectsClaimReplacement(t *testing.T) {
	store := &recordingBackupSessionStore{}
	session := domain.NewSession(
		"backup-pvc-repository",
		domain.NewSessionSpec(
			domain.OperationBackup,
			domain.SessionCommon{SourceNamespace: "app", SessionNamespace: "app"},
			false,
			domain.SessionWorkflowOptions{},
		),
		time.Now(),
	)
	req := Request{
		BackupRepository: "archive",
		BackupRepositoryBinding: &domain.BackupRepositoryBindingStatus{
			Type:       domain.BackupRepositoryTypePVC,
			UID:        "repository-uid",
			Generation: 1,
			PVC: &domain.PVCBackupRepositoryBindingStatus{
				ClaimUID: "claim-uid",
			},
		},
		SessionStore: store,
	}

	if err := pinBackupRepository(context.Background(), req, session); err != nil {
		t.Fatal(err)
	}

	drifted := req
	drifted.BackupRepositoryBinding = copyRepositoryBinding(req.BackupRepositoryBinding)
	drifted.BackupRepositoryBinding.PVC.ClaimUID = "replacement-claim-uid"

	if err := pinBackupRepository(
		context.Background(),
		drifted,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("claim UID drift error=%v category=%q", err, domain.CategoryOf(err))
	}
}

func TestValidateRepositoryBindingRejectsIncompleteBackendIdentity(t *testing.T) {
	tests := []struct {
		name    string
		binding *domain.BackupRepositoryBindingStatus
	}{
		{name: "nil", binding: nil},
		{
			name: "s3 without secret UID",
			binding: &domain.BackupRepositoryBindingStatus{
				Type: domain.BackupRepositoryTypeS3,
				S3:   &domain.S3BackupRepositoryBindingStatus{},
			},
		},
		{
			name: "s3 with pvc status",
			binding: &domain.BackupRepositoryBindingStatus{
				Type: domain.BackupRepositoryTypeS3,
				S3: &domain.S3BackupRepositoryBindingStatus{
					CredentialsSecretUID: "secret-uid",
				},
				PVC: &domain.PVCBackupRepositoryBindingStatus{ClaimUID: "claim-uid"},
			},
		},
		{
			name: "pvc without claim UID",
			binding: &domain.BackupRepositoryBindingStatus{
				Type: domain.BackupRepositoryTypePVC,
				PVC:  &domain.PVCBackupRepositoryBindingStatus{},
			},
		},
		{
			name: "unsupported type",
			binding: &domain.BackupRepositoryBindingStatus{
				Type: "filesystem",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRepositoryBinding(tt.binding)
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
			}
		})
	}
}

func TestResumeRestoreKeepsFailureCheckpointInsideSessionLease(t *testing.T) {
	lock := &recordingBackupSessionLock{}
	store := &lockingBackupSessionStore{lock: lock}
	session := domain.NewSession(
		"restore-session",
		domain.NewSessionSpec(
			domain.OperationRestore,
			domain.SessionCommon{SessionNamespace: "sessions", DestinationNamespace: "default"},
			false,
			domain.SessionWorkflowOptions{},
		),
		time.Now(),
	)

	err := ResumeRestore(
		context.Background(),
		nil,
		Request{SessionStore: store},
		session,
	)
	if err == nil {
		t.Fatal("ResumeRestore() succeeded without a Kubernetes client")
	}

	if !lock.bound || !lock.released {
		t.Fatalf("session lease lifecycle bound=%t released=%t", lock.bound, lock.released)
	}

	if !store.updateWhileBound {
		t.Fatal("restore status checkpoint was written outside the session lease")
	}

	if session.Status.Phase != domain.PhaseFailed {
		t.Fatalf("phase=%s, want %s", session.Status.Phase, domain.PhaseFailed)
	}
}

func (s *lockingBackupSessionStore) Create(ctx context.Context, session *domain.Session) error {
	s.createWhileBound = s.lock != nil && s.lock.bound && !s.lock.released
	return s.recordingBackupSessionStore.Create(ctx, session)
}

func (s *lockingBackupSessionStore) Update(ctx context.Context, session *domain.Session) error {
	s.updateWhileBound = s.lock != nil && s.lock.bound && !s.lock.released
	if s.lock != nil && s.leaseErrOnUpdate != nil {
		s.lock.err = s.leaseErrOnUpdate
	}

	if s.updateErr != nil {
		return s.updateErr
	}

	return s.recordingBackupSessionStore.Update(ctx, session)
}

func (s *lockingBackupSessionStore) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return s.lock, nil
}

type recordingBackupOpenEBSManager struct {
	prepared           kube.OpenEBSLVMSharedResult
	enabled            []domain.OpenEBSLVMSharedMount
	restored           []domain.OpenEBSLVMSharedMount
	validatedRestores  []domain.OpenEBSLVMSharedMount
	enableErr          error
	validateRestoreErr error
	shared             bool
	sharedErr          error
	onRestore          func()
}

func (m *recordingBackupOpenEBSManager) Shared(
	context.Context,
	domain.ObjectReference,
	domain.ObjectReference,
	string,
) (bool, error) {
	if m.sharedErr != nil || m.shared {
		return m.shared, m.sharedErr
	}
	return !m.prepared.NeedsChange, nil
}

func (m *recordingBackupOpenEBSManager) PrepareShared(
	context.Context,
	domain.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	return m.prepared, nil
}

func (m *recordingBackupOpenEBSManager) EnsureShared(
	context.Context,
	domain.ObjectReference,
	domain.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	return kube.OpenEBSLVMSharedResult{}, nil
}

func (m *recordingBackupOpenEBSManager) EnableShared(
	_ context.Context,
	_ string,
	mount domain.OpenEBSLVMSharedMount,
) error {
	m.enabled = append(m.enabled, mount)
	return m.enableErr
}

func (m *recordingBackupOpenEBSManager) ValidateRestoreShared(
	_ context.Context,
	_ string,
	mount domain.OpenEBSLVMSharedMount,
) error {
	m.validatedRestores = append(m.validatedRestores, mount)
	return m.validateRestoreErr
}

func (m *recordingBackupOpenEBSManager) RestoreShared(
	_ context.Context,
	_ string,
	mount domain.OpenEBSLVMSharedMount,
) error {
	m.restored = append(m.restored, mount)
	if m.onRestore != nil {
		m.onRestore()
	}

	return nil
}

func testBackupObjectStore(t *testing.T) *objectstore.Store {
	t.Helper()

	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		objectstore.Credentials{AccessKey: "key", SecretKey: "secret"},
	)
	if err != nil {
		t.Fatal(err)
	}

	return store
}

func TestSubmitRestoreSeparatesBackendFromS3Provider(t *testing.T) {
	objectStore, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{
			Bucket:   "backups",
			Prefix:   "pv-migrate",
			Name:     "daily",
			Provider: "Minio",
		},
		objectstore.Credentials{AccessKey: "key", SecretKey: "secret"},
	)
	if err != nil {
		t.Fatal(err)
	}

	sessions := &recordingBackupSessionStore{}

	session, err := SubmitRestore(
		context.Background(),
		fake.NewSimpleClientset(),
		Request{
			ID:               "restore-test",
			Namespace:        "app",
			PVCName:          "data",
			SessionNamespace: "pvc-migrate-system",
			Store:            objectStore,
			SessionStore:     sessions,
		},
		Plan{PVCUID: "pvc-uid"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if session.Spec.Restore.Backend != domain.BackupBackendS3 {
		t.Fatalf("restore backend=%q", session.Spec.Restore.Backend)
	}

	if session.Spec.Restore.Provider != "Minio" {
		t.Fatalf("restore provider=%q", session.Spec.Restore.Provider)
	}
}

func TestSubmitRestoreRepositoryOmitsControllerOwnedConnection(t *testing.T) {
	objectStore, err := objectstore.NewConfigOnly(
		objectstore.Config{Bucket: "repository", Name: "daily"},
	)
	if err != nil {
		t.Fatal(err)
	}

	session, err := SubmitRestore(
		context.Background(),
		fake.NewSimpleClientset(),
		Request{
			ID:               "restore-repository",
			Namespace:        "app",
			PVCName:          "data",
			SessionNamespace: "app",
			BackupRepository: "tenant-s3",
			Store:            objectStore,
			SessionStore:     &crdRepositorySessionStore{},
		},
		Plan{PVCUID: "pvc-uid"},
	)
	if err != nil {
		t.Fatal(err)
	}

	payload := session.Spec.Restore
	if payload.BackupRepository != "tenant-s3" || payload.Name != "daily" {
		t.Fatalf("repository metadata = %#v", payload)
	}

	if payload.Bucket != "" || payload.Prefix != "" || payload.Provider != "" ||
		payload.Endpoint != "" ||
		payload.Region != "" ||
		payload.ServerSideEncryption != "" ||
		payload.SSEKMSKeyID != "" {
		t.Fatalf("controller-owned object-store fields leaked into workflow: %#v", payload)
	}
}

func TestValidateResumeAllowsRepositoryBackedBackupWithoutTenantCredentials(t *testing.T) {
	store := testBackupObjectStore(t)

	session, err := buildBackupSession(
		Request{
			ID:               "repository-resume",
			Namespace:        "app",
			SessionNamespace: "app",
			BackupRepository: "archive",
			Store:            store,
			SessionStore:     &recordingBackupSessionStore{},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "data",
				UID:       types.UID("pvc-uid"),
			},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pv-data",
				UID:  types.UID("pv-uid"),
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	session.Backend = kube.SessionBackendCRD

	if err := ValidateResume(
		context.Background(),
		nil,
		Request{SessionStore: &recordingBackupSessionStore{}},
		session,
	); err != nil {
		t.Fatalf("repository-backed dry-run attempted tenant credentials: %v", err)
	}
}

func TestValidateRestoreResumeAllowsRepositoryBackedRestoreWithoutTenantCredentials(t *testing.T) {
	objectStore, err := objectstore.NewConfigOnly(
		objectstore.Config{Bucket: "repository", Name: "daily"},
	)
	if err != nil {
		t.Fatal(err)
	}

	session, err := SubmitRestore(
		context.Background(),
		fake.NewSimpleClientset(),
		Request{
			ID:               "repository-restore-resume",
			Namespace:        "app",
			PVCName:          "data",
			SessionNamespace: "app",
			BackupRepository: "archive",
			Store:            objectStore,
			SessionStore:     &crdRepositorySessionStore{},
		},
		Plan{PVCUID: "pvc-uid"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := ValidateRestoreResume(
		context.Background(),
		nil,
		Request{SessionStore: &recordingBackupSessionStore{}},
		session,
	); err != nil {
		t.Fatalf("repository-backed restore dry-run attempted tenant credentials: %v", err)
	}
}

type crdRepositorySessionStore struct{ recordingBackupSessionStore }

func (s *crdRepositorySessionStore) Create(_ context.Context, session *domain.Session) error {
	session.Backend = kube.SessionBackendCRD
	session.BackendResource = domain.ControllerKindRestore
	session.BackendUID = types.UID("restore-uid")
	s.created = session

	return session.Validate()
}

func TestBuildBackupSessionIncludesMetadataWithoutCredentials(t *testing.T) {
	store := &recordingBackupSessionStore{}
	req := Request{
		ID:                     "backup-test",
		Namespace:              "app",
		SessionNamespace:       "pvc-migrate-system",
		Online:                 true,
		OpenEBSLVMEnableShared: true,
		ToolImage:              "registry.example/tool:test",
		Store:                  testBackupObjectStore(t),
		SessionStore:           store,
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv-uid")},
	}

	session, err := buildBackupSession(req, pvc, pv)
	if err != nil {
		t.Fatal(err)
	}

	if session.Spec.Type != domain.SessionTypeBackup || session.Spec.Backup == nil ||
		!session.Spec.Backup.Online {
		t.Fatalf("backup session payload = %#v", session.Spec)
	}

	if session.Spec.Backup.SourcePVC.UID != pvc.UID || session.Spec.Backup.SourcePV.UID != pv.UID {
		t.Fatalf("source identities = %#v", session.Spec.Backup)
	}

	if session.Spec.Backup.SourcePVC.Name != pvc.Name ||
		session.Spec.Backup.SourcePV.Name != pv.Name {
		t.Fatalf("source names = %#v", session.Spec.Backup)
	}

	if session.Spec.Backup.Path != "" || session.Status.OpenEBSLVMSharedMounts != nil {
		t.Fatalf("unexpected backup state = %#v", session)
	}

	if err := session.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBuildBackupSessionRepositoryOmitsControllerOwnedConnection(t *testing.T) {
	store := &recordingBackupSessionStore{}
	req := Request{
		ID:               "repository-backup",
		Namespace:        "app",
		SessionNamespace: "app",
		BackupRepository: "tenant-s3",
		Store:            testBackupObjectStore(t),
		SessionStore:     store,
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv-uid")},
	}

	session, err := buildBackupSession(req, pvc, pv)
	if err != nil {
		t.Fatal(err)
	}

	payload := session.Spec.Backup
	if payload.BackupRepository != "tenant-s3" || payload.Name != "daily" {
		t.Fatalf("repository metadata = %#v", payload)
	}

	if payload.Bucket != "" || payload.Prefix != "" || payload.Provider != "" ||
		payload.Endpoint != "" ||
		payload.Region != "" ||
		payload.ServerSideEncryption != "" ||
		payload.SSEKMSKeyID != "" {
		t.Fatalf("controller-owned object-store fields leaked into workflow: %#v", payload)
	}
}

func TestBuildBackupSessionRejectsInvalidIDBeforePersistence(t *testing.T) {
	store := &recordingBackupSessionStore{}
	req := Request{
		ID:               "Bad_ID",
		Namespace:        "app",
		SessionNamespace: "pvc-migrate-system",
		Store:            testBackupObjectStore(t),
		SessionStore:     store,
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv-uid")},
	}

	if _, err := buildBackupSession(
		req,
		pvc,
		pv,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if store.created != nil {
		t.Fatal("invalid session ID was persisted")
	}
}

func TestBackupCredentialsAreStoredOnlyInSecret(t *testing.T) {
	client := fake.NewClientset()
	store := &recordingBackupSessionStore{}
	req := Request{
		ID:               "backup-test",
		Namespace:        "app",
		SessionNamespace: "pvc-migrate-system",
		Store:            testBackupObjectStore(t),
		SessionStore:     store,
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv-uid")},
	}

	session, err := buildBackupSession(req, pvc, pv)
	if err != nil {
		t.Fatal(err)
	}

	if err := persistBackupCredentials(context.Background(), client, req, session); err != nil {
		t.Fatal(err)
	}

	if session.Spec.Backup.CredentialsSecret.Name == "" {
		t.Fatal("credentials Secret reference was not persisted")
	}

	credentials, err := loadBackupCredentials(context.Background(), client, session)
	if err != nil {
		t.Fatal(err)
	}

	if credentials.AccessKey != "key" || credentials.SecretKey != "secret" {
		t.Fatalf("credentials = %#v", credentials)
	}
}

func TestLoadBackupCredentialsRecoversUncheckpointedSecret(t *testing.T) {
	client := fake.NewClientset()

	session := testBackupSession(t, "backup-test")
	if _, err := kube.CreateBackupCredentialsSecret(
		context.Background(),
		client,
		session.Spec.SessionNamespace,
		session.ID,
		map[string][]byte{
			kube.BackupAccessKeyDataKey: []byte("access"),
			kube.BackupSecretKeyDataKey: []byte("secret"),
		},
	); err != nil {
		t.Fatal(err)
	}

	credentials, err := loadBackupCredentials(context.Background(), client, session)
	if err != nil {
		t.Fatal(err)
	}

	if credentials.AccessKey != "access" || credentials.SecretKey != "secret" {
		t.Fatalf("credentials=%#v", credentials)
	}

	secret, err := client.CoreV1().Secrets(session.Spec.SessionNamespace).Get(
		context.Background(),
		kube.BackupCredentialsSecretName(session.ID),
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	secret.Labels[kube.SessionKey] = "another-session"
	if _, err := client.CoreV1().Secrets(secret.Namespace).Update(
		context.Background(),
		secret,
		metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := loadBackupCredentials(
		context.Background(),
		client,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPersistBackupCredentialsKeepsSecretWhenCheckpointResultIsUnknown(t *testing.T) {
	client := fake.NewClientset()
	updateErr := errors.New("connection closed after update")
	store := &lockingBackupSessionStore{updateErr: updateErr}
	req := Request{Store: testBackupObjectStore(t), SessionStore: store}
	session := testBackupSession(t, "backup-test")

	if err := persistBackupCredentials(
		context.Background(),
		client,
		req,
		session,
	); !errors.Is(
		err,
		updateErr,
	) {
		t.Fatalf("error=%v", err)
	}

	if _, err := client.CoreV1().Secrets(session.Spec.SessionNamespace).Get(
		context.Background(),
		kube.BackupCredentialsSecretName(session.ID),
		metav1.GetOptions{},
	); err != nil {
		t.Fatalf("recoverable credentials Secret was removed: %v", err)
	}
}

func TestInitialBackupHoldsSessionLeaseWhilePersistingCredentials(t *testing.T) {
	client, req := preflightFixture(t, &preflightObjectStore{})
	updateErr := errors.New("injected session update failure")
	leaseErr := errors.New("injected session lease loss")
	releaseErr := errors.New("injected session lease release failure")
	lock := &recordingBackupSessionLock{releaseErr: releaseErr}
	store := &lockingBackupSessionStore{
		lock: lock, updateErr: updateErr, leaseErrOnUpdate: leaseErr,
	}
	req.SessionNamespace = "sessions"
	req.SessionStore = store

	err := runBackupWithSession(context.Background(), client, req, "pvc", "pv")
	if !errors.Is(err, updateErr) || !errors.Is(err, leaseErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("error=%v", err)
	}

	if !store.updateWhileBound {
		t.Fatal("credentials reference was persisted without holding the session Lease")
	}

	if !store.createWhileBound {
		t.Fatal("backup session was created without holding the session Lease")
	}

	if !lock.released {
		t.Fatal("session Lease was not released after backup setup failed")
	}
}

func TestRunBackupWithSessionReusesSubmittedSession(t *testing.T) {
	store := &recordingBackupSessionStore{}
	session := testBackupSession(t, "backup-submitted")
	session.Status.Phase = domain.PhaseCompleted
	client := fake.NewSimpleClientset()
	req := Request{
		Store:         testBackupObjectStore(t),
		SessionStore:  store,
		BackupSession: session,
	}

	if err := runBackupWithSession(
		context.Background(),
		client,
		req,
		"pvc-uid",
		"pv-uid",
	); err != nil {
		t.Fatalf("runBackupWithSession() error = %v", err)
	}

	if store.created != nil {
		t.Fatal("submitted backup session was created a second time")
	}

	if len(store.updated) != 0 {
		t.Fatalf(
			"completed submitted session was unexpectedly updated %d times",
			len(store.updated),
		)
	}

	if _, err := client.CoreV1().Secrets(session.Spec.SessionNamespace).Get(
		context.Background(),
		kube.BackupCredentialsSecretName(session.ID),
		metav1.GetOptions{},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("credentials Secret was persisted during session reuse: %v", err)
	}
}

func TestRestoreBackupSharedMountsStopsPersistingAfterLeaseLoss(t *testing.T) {
	leaseErr := errors.New("injected session lease loss")
	lock := &recordingBackupSessionLock{}
	store := &recordingBackupSessionStore{}
	manager := &recordingBackupOpenEBSManager{onRestore: func() { lock.err = leaseErr }}
	session := testBackupSession(t, "backup-test")
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{{
		SourcePV:  domain.ObjectReference{Name: "pv-data", UID: "pv-uid"},
		LVMVolume: domain.ObjectReference{Namespace: "openebs", Name: "lvm-data", UID: "lvm-uid"},
	}}
	ctx := context.WithValue(
		context.Background(),
		backupSessionLockContextKey{},
		kube.SessionLock(lock),
	)

	err := restoreBackupSharedMounts(
		ctx,
		Request{SessionStore: store, OpenEBSLVMManager: manager},
		session,
	)
	if !errors.Is(err, leaseErr) {
		t.Fatalf("error=%v", err)
	}

	if len(manager.restored) != 1 {
		t.Fatalf("restore calls=%d", len(manager.restored))
	}

	if len(store.updated) != 0 {
		t.Fatalf("session updates after Lease loss=%d", len(store.updated))
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 1 {
		t.Fatalf(
			"recovery checkpoint was cleared after Lease loss: %#v",
			session.Status.OpenEBSLVMSharedMounts,
		)
	}
}

func testBackupSession(t *testing.T, id string) *domain.Session {
	t.Helper()

	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{
			SourceNamespace:  "app",
			SessionNamespace: "sessions",
		},

		false,
		domain.SessionWorkflowOptions{},
	)

	spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: "pvc-uid"}
	spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: "pv-uid"}
	spec.Backup.Backend = "s3"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"

	return domain.NewSession(id, spec, time.Now())
}

func TestFailBackupSessionDoesNotPersistAfterLeaseLoss(t *testing.T) {
	leaseErr := errors.New("injected session lease loss")
	lock := &recordingBackupSessionLock{err: leaseErr}
	store := &recordingBackupSessionStore{}
	ctx := context.WithValue(
		context.Background(),
		backupSessionLockContextKey{},
		kube.SessionLock(lock),
	)
	session := &domain.Session{ID: "backup-test"}

	err := failBackupSession(ctx, Request{SessionStore: store}, session, errors.New("copy failed"))
	if !errors.Is(err, leaseErr) {
		t.Fatalf("error=%v", err)
	}

	if len(store.updated) != 0 || session.Status.Phase != "" {
		t.Fatalf(
			"session changed after Lease loss: phase=%s updates=%d",
			session.Status.Phase,
			len(store.updated),
		)
	}
}

func TestValidateBackupOpenEBSStateRequiresFlagForUnsharedActivePVC(t *testing.T) {
	manager := &recordingBackupOpenEBSManager{prepared: kube.OpenEBSLVMSharedResult{
		NeedsChange: true,
		LVMVolume: domain.ObjectReference{
			Namespace: "openebs",
			Name:      "lvm-data",
			UID:       types.UID("lvm-uid"),
		},
	}}
	info := &PVCInfo{
		PVC: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		},
		PV: &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv-uid")},
			Spec: corev1.PersistentVolumeSpec{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
			},
		},
		Consumers: []string{"app/writer"},
	}

	req := Request{OpenEBSLVMManager: manager}
	if err := validateBackupOpenEBSState(context.Background(), req, info); err == nil {
		t.Fatal("expected unshared active PVC rejection")
	}

	req.OpenEBSLVMEnableShared = true
	if err := validateBackupOpenEBSState(
		context.Background(),
		req,
		info,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("missing session store category=%s error=%v", domain.CategoryOf(err), err)
	}

	req.SessionStore = &recordingBackupSessionStore{}
	if err := validateBackupOpenEBSState(context.Background(), req, info); err != nil {
		t.Fatal(err)
	}
}

func TestBackupSessionOpenEBSStatePersistsAndRestores(t *testing.T) {
	store := &recordingBackupSessionStore{}
	manager := &recordingBackupOpenEBSManager{prepared: kube.OpenEBSLVMSharedResult{
		NeedsChange: true,
		LVMVolume: domain.ObjectReference{
			Namespace: "openebs",
			Name:      "lvm-data",
			UID:       types.UID("lvm-uid"),
		},
		PreviousShared:    "no",
		PreviousSharedSet: true,
	}}
	info := &PVCInfo{
		PVC: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		},
		PV: &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv-uid")},
			Spec: corev1.PersistentVolumeSpec{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
			},
		},
		Consumers: []string{"app/writer"},
	}
	req := Request{SessionStore: store, OpenEBSLVMManager: manager, OpenEBSLVMEnableShared: true}

	session := &domain.Session{ID: "backup-test"}
	if err := backupSessionOpenEBSState(context.Background(), req, session, info); err != nil {
		t.Fatal(err)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 1 || len(manager.enabled) != 1 ||
		len(store.updated) != 1 {
		t.Fatalf(
			"state was not persisted before enable: session=%#v enabled=%d updates=%d",
			session.Status.OpenEBSLVMSharedMounts,
			len(manager.enabled),
			len(store.updated),
		)
	}

	if err := restoreBackupSharedMounts(context.Background(), req, session); err != nil {
		t.Fatal(err)
	}

	if len(manager.restored) != 1 || manager.restored[0].PreviousShared != "no" {
		t.Fatalf("restore calls = %#v", manager.restored)
	}
}

func TestBackupSessionDropsSharedRecoveryWhenEnableDidNotAcquireOwnership(t *testing.T) {
	store := &recordingBackupSessionStore{}
	manager := &recordingBackupOpenEBSManager{
		prepared: kube.OpenEBSLVMSharedResult{
			NeedsChange: true,
			LVMVolume: domain.ObjectReference{
				Namespace: "openebs",
				Name:      "lvm-data",
				UID:       "lvm-uid",
			},
			PreviousShared:    "no",
			PreviousSharedSet: true,
		},
		enableErr: domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			"ownership changed",
		),
	}
	info := &PVCInfo{
		PVC: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		},
		PV: &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: "pv-uid"},
			Spec: corev1.PersistentVolumeSpec{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
			},
		},
		Consumers: []string{"app/writer"},
	}
	req := Request{SessionStore: store, OpenEBSLVMManager: manager, OpenEBSLVMEnableShared: true}
	session := &domain.Session{ID: "backup-test"}

	if err := backupSessionOpenEBSState(
		context.Background(),
		req,
		session,
		info,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 0 || len(store.updated) != 2 {
		t.Fatalf(
			"stale recovery state=%#v updates=%d",
			session.Status.OpenEBSLVMSharedMounts,
			len(store.updated),
		)
	}
}

func TestBackupSessionRetainsSharedRecoveryAfterAmbiguousEnableErrorWhenOwned(t *testing.T) {
	store := &recordingBackupSessionStore{}
	manager := &recordingBackupOpenEBSManager{
		prepared: kube.OpenEBSLVMSharedResult{
			NeedsChange: true,
			LVMVolume: domain.ObjectReference{
				Namespace: "openebs",
				Name:      "lvm-data",
				UID:       "lvm-uid",
			},
		},
		enableErr: domain.NewError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"connection lost",
		),
		shared: true,
	}
	info := &PVCInfo{
		PVC: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		},
		PV: &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: "pv-uid"},
			Spec: corev1.PersistentVolumeSpec{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
			},
		},
		Consumers: []string{"app/writer"},
	}
	req := Request{SessionStore: store, OpenEBSLVMManager: manager, OpenEBSLVMEnableShared: true}
	session := &domain.Session{ID: "backup-test"}

	if err := backupSessionOpenEBSState(
		context.Background(),
		req,
		session,
		info,
	); domain.CategoryOf(
		err,
	) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 1 {
		t.Fatalf("recovery state=%#v", session.Status.OpenEBSLVMSharedMounts)
	}
}

func TestBackupSessionResumesCheckpointBeforeSharedMountWasEnabled(t *testing.T) {
	conflictErr := domain.NewError(
		domain.ErrorConflict,
		"OpenEBS LVM shared mount",
		"temporary ownership is absent",
	)
	manager := &recordingBackupOpenEBSManager{sharedErr: conflictErr}
	mount := domain.OpenEBSLVMSharedMount{
		SourcePV: domain.ObjectReference{
			Kind: "PersistentVolume",
			Name: "pv-data",
			UID:  "pv-uid",
		},
		LVMVolume: domain.ObjectReference{
			Namespace: "openebs",
			Name:      "lvm-data",
			UID:       "lvm-uid",
		},
		PreviousShared:    "no",
		PreviousSharedSet: true,
	}
	session := &domain.Session{ID: "backup-test"}
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{mount}
	info := &PVCInfo{
		PVC: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		},
		PV: &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: "pv-uid"},
			Spec: corev1.PersistentVolumeSpec{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
			},
		},
		Consumers: []string{"app/writer"},
	}
	req := Request{BackupSession: session, OpenEBSLVMManager: manager}

	if err := validateBackupOpenEBSState(context.Background(), req, info); err != nil {
		t.Fatalf("preflight rejected recoverable checkpoint: %v", err)
	}

	if len(manager.enabled) != 0 || len(manager.validatedRestores) != 1 {
		t.Fatalf(
			"preflight enabled=%d restore validations=%d",
			len(manager.enabled),
			len(manager.validatedRestores),
		)
	}

	if err := backupSessionOpenEBSState(context.Background(), req, session, info); err != nil {
		t.Fatalf("resume failed to enable checkpointed shared mount: %v", err)
	}

	if len(manager.enabled) != 1 || manager.enabled[0] != mount {
		t.Fatalf("enabled mounts=%#v", manager.enabled)
	}
}

func TestBackupSessionRejectsCheckpointWithConflictingSharedMountOwnership(t *testing.T) {
	manager := &recordingBackupOpenEBSManager{
		sharedErr: domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			"owned by another session",
		),
		validateRestoreErr: domain.NewError(
			domain.ErrorConflict,
			"restore OpenEBS LVM shared mount",
			"ownership changed",
		),
	}
	mount := domain.OpenEBSLVMSharedMount{
		SourcePV:  domain.ObjectReference{Kind: "PersistentVolume", Name: "pv-data", UID: "pv-uid"},
		LVMVolume: domain.ObjectReference{Namespace: "openebs", Name: "lvm-data", UID: "lvm-uid"},
	}
	session := &domain.Session{ID: "backup-test"}
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{mount}
	info := &PVCInfo{
		PVC: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		},
		PV: &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: "pv-uid"},
			Spec: corev1.PersistentVolumeSpec{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
			},
		},
		Consumers: []string{"app/writer"},
	}
	req := Request{BackupSession: session, OpenEBSLVMManager: manager}

	if err := validateBackupOpenEBSState(
		context.Background(),
		req,
		info,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(manager.enabled) != 0 {
		t.Fatalf("conflicting mount was enabled: %#v", manager.enabled)
	}
}

func TestResumeCompletesMatchingPublishedBackupWithoutRunningTool(t *testing.T) {
	storeAPI := &preflightObjectStore{}
	client, req := preflightFixture(t, storeAPI)
	sessionStore := &recordingBackupSessionStore{}
	req.SessionNamespace = "sessions"
	req.SessionStore = sessionStore

	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(
		context.Background(),
		"data",
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	session, err := buildBackupSession(req, pvc, pv)
	if err != nil {
		t.Fatal(err)
	}

	if err := persistBackupCredentials(context.Background(), client, req, session); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(domain.PhaseWarmCopying, "copying", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(domain.PhaseWarmCopied, "copied", time.Now()); err != nil {
		t.Fatal(err)
	}

	manifest := objectstore.Manifest{
		Version:         2,
		CreatedAt:       time.Now().UTC(),
		Bucket:          "backups",
		Prefix:          "pv-migrate",
		Name:            "daily",
		SessionID:       session.ID,
		SourceNamespace: pvc.Namespace,
		SourcePVC:       pvc.Name,
		SourcePVCUID:    string(pvc.UID),
		SourcePV:        pv.Name,
		SourcePVUID:     string(pv.UID),
		Capacity:        "1Gi",
		VolumeMode:      "Filesystem",
		Consistency:     backupConsistency(false),
		Compression:     "none",
		InventorySHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}

	storeAPI.manifest, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}

	staleProbe := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: pvc.Namespace,
		Name:      "stale-backup-probe",
		UID:       "stale-probe-uid",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: kube.ResourceRoleToolProbe,
		},
	}}
	foreignProbe := staleProbe.DeepCopy()
	foreignProbe.Name = "foreign-backup-probe"
	foreignProbe.UID = "foreign-probe-uid"
	foreignProbe.Labels[kube.SessionKey] = "other-session"

	if _, err := client.CoreV1().
		Pods(pvc.Namespace).
		Create(context.Background(), staleProbe, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Pods(pvc.Namespace).
		Create(context.Background(), foreignProbe, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	resumeReq := Request{
		SessionStore:     sessionStore,
		SessionNamespace: "sessions",
		ObjectStoreFactory: func(_ context.Context, config objectstore.Config) (*objectstore.Store, error) {
			return objectstore.NewWithClient(storeAPI, config, objectstore.Credentials{
				AccessKey: config.AccessKey,
				SecretKey: config.SecretKey,
			})
		},
	}
	if err := ValidateResume(context.Background(), client, resumeReq, session); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Pods(pvc.Namespace).
		Get(context.Background(), staleProbe.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("dry-run deleted stale backup probe: %v", err)
	}

	if err := Resume(context.Background(), client, resumeReq, session); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Pods(pvc.Namespace).
		Get(context.Background(), staleProbe.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("stale backup probe remains: %v", err)
	}

	if _, err := client.CoreV1().
		Pods(pvc.Namespace).
		Get(context.Background(), foreignProbe.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("foreign backup probe was deleted: %v", err)
	}

	if session.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("phase=%s", session.Status.Phase)
	}

	if storeAPI.puts != 0 {
		t.Fatalf("published manifest was rewritten %d times", storeAPI.puts)
	}

	secretRef := session.Spec.Backup.CredentialsSecret
	if err := kube.DeleteBackupCredentialsSecret(
		context.Background(),
		client,
		secretRef,
		session.ID,
	); err != nil {
		t.Fatal(err)
	}

	if err := ValidateResume(context.Background(), client, Request{}, session); err != nil {
		t.Fatalf("completed session required deleted credentials: %v", err)
	}
}

func TestValidateResumeRejectsPublishedBackupFromAnotherSession(t *testing.T) {
	storeAPI := &preflightObjectStore{}
	client, req := preflightFixture(t, storeAPI)
	sessionStore := &recordingBackupSessionStore{}
	req.SessionNamespace = "sessions"
	req.SessionStore = sessionStore
	pvc, _ := client.CoreV1().
		PersistentVolumeClaims("default").
		Get(context.Background(), "data", metav1.GetOptions{})
	pv, _ := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-data", metav1.GetOptions{})

	session, err := buildBackupSession(req, pvc, pv)
	if err != nil {
		t.Fatal(err)
	}

	if err := persistBackupCredentials(context.Background(), client, req, session); err != nil {
		t.Fatal(err)
	}

	manifest := objectstore.Manifest{
		Version:         2,
		CreatedAt:       time.Now().UTC(),
		Bucket:          "backups",
		Prefix:          "pv-migrate",
		Name:            "daily",
		SessionID:       "another-session",
		SourceNamespace: "default",
		SourcePVC:       "data",
		SourcePVCUID:    "pvc",
		SourcePV:        "pv-data",
		SourcePVUID:     "pv",
		Capacity:        "1Gi",
		VolumeMode:      "Filesystem",
		Consistency:     backupConsistency(false),
		Compression:     "none",
		InventorySHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}

	storeAPI.manifest, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}

	resumeReq := Request{
		SessionStore: sessionStore,
		ObjectStoreFactory: func(_ context.Context, config objectstore.Config) (*objectstore.Store, error) {
			return objectstore.NewWithClient(
				storeAPI,
				config,
				objectstore.Credentials{AccessKey: config.AccessKey, SecretKey: config.SecretKey},
			)
		},
	}
	if err := ValidateResume(
		context.Background(),
		client,
		resumeReq,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := Resume(
		context.Background(),
		client,
		resumeReq,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("resume category=%s error=%v", domain.CategoryOf(err), err)
	}

	if session.Status.Phase != domain.PhasePlanned {
		t.Fatalf("foreign manifest changed session phase to %s", session.Status.Phase)
	}
}
