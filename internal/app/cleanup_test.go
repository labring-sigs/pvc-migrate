package app

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCleanupFinalizesActivePVAndClosesRollbackWindow(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	*session.Spec.WorkloadPtr() = domain.WorkloadSpec{
		Adapter: domain.WorkloadStandalone,
		Pod: domain.ObjectReference{
			Namespace: "app",
			Name:      "database",
			UID:       types.UID("pod-uid"),
		},
	}
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("dest-pv-uid"),
	}
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "data",
		UID:       types.UID("active-pvc-uid"),
	}
	client := fake.NewClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "database",
			UID:       types.UID("pod-uid"),
			Annotations: map[string]string{
				kube.SessionKey:    session.ID,
				"example.com/keep": "value",
			},
		}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "data",
				UID:         types.UID("active-pvc-uid"),
				Annotations: map[string]string{kube.SessionKey: session.ID},
			},
		},
		managedPV("pv-destination", "dest-pv-uid", session.ID, "active", corev1.VolumeBound),
		managedPV("pv-source", "source-pv-uid", session.ID, "rollback", corev1.VolumeReleased),
	)
	store := &memoryStore{}

	service := &Service{client: client, store: store}
	if err := service.cleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{DeleteRollback: true, Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, "pv-source", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("rollback PV still exists: %v", err)
	}

	active, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, "pv-destination", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if active.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Fatalf("active reclaim policy=%s", active.Spec.PersistentVolumeReclaimPolicy)
	}

	if active.Labels[kube.SessionKey] != "" || active.Labels[kube.ResourceRoleLabel] != "" ||
		active.Labels[kube.ManagedByLabel] != "" {
		t.Fatalf("active PV ownership labels=%v", active.Labels)
	}

	if active.Annotations[kube.OriginalPolicyAnnotation] != "" ||
		active.Annotations[kube.PairedPVAnnotation] != "" {
		t.Fatalf("active PV migration annotations=%v", active.Annotations)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pvc.Annotations[kube.SessionKey] != "" {
		t.Fatalf("active PVC remains owned by %q", pvc.Annotations[kube.SessionKey])
	}

	pod, err := client.CoreV1().Pods("app").Get(ctx, "database", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pod.Annotations[kube.SessionKey] != "" || pod.Annotations["example.com/keep"] != "value" {
		t.Fatalf("standalone Pod annotations=%v", pod.Annotations)
	}

	if store.deletes != 1 {
		t.Fatalf("session deletes=%d", store.deletes)
	}
}

func TestCleanupFinalizesBackupCredentialsSecret(t *testing.T) {
	ctx := context.Background()
	session := completedBackupCleanupSession(t)

	client := fake.NewClientset()

	secret, err := kube.CreateBackupCredentialsSecret(
		ctx,
		client,
		"sessions",
		session.ID,
		map[string][]byte{
			kube.BackupAccessKeyDataKey: []byte("access"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	session.Spec.Backup.CredentialsSecret = domain.ObjectReference{
		Namespace: secret.Namespace,
		Name:      secret.Name,
		UID:       secret.UID,
	}
	store := &memoryStore{}
	service := &Service{client: client, store: store}

	secret.Labels[kube.SessionKey] = "wrong-owner"
	if _, err := client.CoreV1().
		Secrets(secret.Namespace).
		Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := service.validateCleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{Finalize: true},
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("cleanup validation category=%s error=%v", domain.CategoryOf(err), err)
	}

	secret.Labels[kube.SessionKey] = session.ID
	if _, err := client.CoreV1().
		Secrets(secret.Namespace).
		Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := service.cleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{Finalize: true},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Secrets("sessions").
		Get(ctx, secret.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("credentials Secret still exists: %v", err)
	}

	if session.Spec.Backup.CredentialsSecret.Name != "" {
		t.Fatalf("credentials Secret reference remains: %#v", session.Spec.Backup.CredentialsSecret)
	}
}

func TestCleanupFindsCredentialsSecretMissingFromBackupCheckpoint(t *testing.T) {
	ctx := context.Background()
	session := completedBackupCleanupSession(t)
	client := fake.NewClientset()

	secret, err := kube.CreateBackupCredentialsSecret(
		ctx,
		client,
		"sessions",
		session.ID,
		map[string][]byte{
			kube.BackupAccessKeyDataKey: []byte("access"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	secret.Labels[kube.SessionKey] = "wrong-owner"
	if _, err := client.CoreV1().
		Secrets(secret.Namespace).
		Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	service := &Service{client: client, store: &memoryStore{}}
	if err := service.validateCleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{Finalize: true},
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("cleanup validation category=%s error=%v", domain.CategoryOf(err), err)
	}

	secret.Labels[kube.SessionKey] = session.ID
	if _, err := client.CoreV1().
		Secrets(secret.Namespace).
		Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := service.cleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{Finalize: true},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Secrets(secret.Namespace).
		Get(ctx, secret.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("uncheckpointed credentials Secret still exists: %v", err)
	}
}

func completedBackupCleanupSession(t *testing.T) *domain.Session {
	t.Helper()

	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{
			SourceNamespace:  "app",
			SessionNamespace: "sessions",
			CreatedBy:        "test",
		},

		false,
		domain.SessionWorkflowOptions{},
	)

	spec.Backup.SourcePVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "data",
		UID:       types.UID("pvc-uid"),
	}
	spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: types.UID("pv-uid")}
	spec.Backup.Backend = "s3"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"

	session := domain.NewSession("backup-test", spec, metav1.Now().Time)
	if err := session.Transition(
		domain.PhaseWarmCopying,
		"copying",
		metav1.Now().Time,
	); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(domain.PhaseWarmCopied, "copied", metav1.Now().Time); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(
		domain.PhaseCompleted,
		"completed",
		metav1.Now().Time,
	); err != nil {
		t.Fatal(err)
	}

	return session
}

func TestCleanupAbortedSessionReleasesSourceAndDeletesDestination(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("dest-pv-uid"),
	}
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "data",
				UID:         types.UID("source-pvc-uid"),
				Annotations: map[string]string{kube.SessionKey: session.ID},
			},
		},
		managedPV("pv-source", "source-pv-uid", session.ID, "source", corev1.VolumeBound),
		managedPV(
			"pv-destination",
			"dest-pv-uid",
			session.ID,
			"destination",
			corev1.VolumeReleased,
		),
	)
	store := &memoryStore{}

	service := &Service{client: client, store: store}
	if err := service.cleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{DeleteRollback: true, Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, "pv-destination", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("destination PV still exists: %v", err)
	}

	source, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if source.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete ||
		source.Labels[kube.SessionKey] != "" {
		t.Fatalf("finalized source PV=%#v", source)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pvc.Annotations[kube.SessionKey] != "" {
		t.Fatalf("source PVC remains owned by %q", pvc.Annotations[kube.SessionKey])
	}
}

func TestStandalonePodOwnershipRelease(t *testing.T) {
	const (
		podNamespace = "app"
		podName      = "application"
		podUID       = types.UID("application-uid")
		foreignOwner = "foreign-session"
	)
	for _, test := range []struct {
		name          string
		pod           *corev1.Pod
		wantConflict  bool
		wantOwner     string
		wantOtherAnno string
	}{
		{
			name: "matching session ownership is released",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: podNamespace,
				Name:      podName,
				UID:       podUID,
				Annotations: map[string]string{
					kube.SessionKey:        "session-123",
					"example.com/business": "preserved",
				},
			}},
			wantOtherAnno: "preserved",
		},
		{
			name: "matching Pod transferred to another session",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: podNamespace,
				Name:      podName,
				UID:       podUID,
				Annotations: map[string]string{
					kube.SessionKey: foreignOwner,
				},
			}},
			wantConflict: true,
			wantOwner:    foreignOwner,
		},
		{
			name: "replacement Pod retaining stale ownership",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: podNamespace,
				Name:      podName,
				UID:       "replacement-uid",
				Annotations: map[string]string{
					kube.SessionKey: "session-123",
				},
			}},
			wantConflict: true,
			wantOwner:    "session-123",
		},
		{
			name: "unowned replacement Pod is preserved",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: podNamespace,
				Name:      podName,
				UID:       "replacement-uid",
			}},
		},
		{
			name: "foreign replacement Pod is preserved",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: podNamespace,
				Name:      podName,
				UID:       "replacement-uid",
				Annotations: map[string]string{
					kube.SessionKey: foreignOwner,
				},
			}},
			wantOwner: foreignOwner,
		},
		{name: "deleted Pod is already released"},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := appTestSession()
			if err := session.Spec.SetWorkload(domain.WorkloadSpec{
				Adapter: domain.WorkloadStandalone,
				Pod: domain.ObjectReference{
					Namespace: podNamespace,
					Name:      podName,
					UID:       podUID,
				},
			}); err != nil {
				t.Fatal(err)
			}

			var objects []runtime.Object
			if test.pod != nil {
				objects = append(objects, test.pod)
			}

			client := fake.NewClientset(objects...)
			service := &Service{client: client, store: &memoryStore{}}

			validateErr := service.validateStandalonePodOwnershipRelease(
				context.Background(),
				session,
			)

			releaseErr := service.releaseStandalonePodOwnership(context.Background(), session)
			if test.wantConflict {
				if domain.CategoryOf(validateErr) != domain.ErrorConflict ||
					domain.CategoryOf(releaseErr) != domain.ErrorConflict {
					t.Fatalf(
						"validate category=%s error=%v; release category=%s error=%v",
						domain.CategoryOf(validateErr),
						validateErr,
						domain.CategoryOf(releaseErr),
						releaseErr,
					)
				}
			} else if validateErr != nil || releaseErr != nil {
				t.Fatalf("validate error=%v; release error=%v", validateErr, releaseErr)
			}

			if test.pod == nil {
				return
			}

			pod, err := client.CoreV1().
				Pods(podNamespace).
				Get(context.Background(), podName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if owner := pod.Annotations[kube.SessionKey]; owner != test.wantOwner {
				t.Fatalf("owner=%q want=%q annotations=%v", owner, test.wantOwner, pod.Annotations)
			}

			if other := pod.Annotations["example.com/business"]; other != test.wantOtherAnno {
				t.Fatalf("business annotation=%q want=%q", other, test.wantOtherAnno)
			}
		})
	}
}

func TestCleanupAbortedSessionSkipsReplacedSourceResources(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("dest-pv-uid"),
	}
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("recreated-pvc-uid"),
		}},
		managedPV(
			"pv-destination",
			"dest-pv-uid",
			session.ID,
			"destination",
			corev1.VolumeReleased,
		),
	)
	store := &memoryStore{}

	service := &Service{client: client, store: store}
	if err := service.cleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{DeleteRollback: true, Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(ctx, "data", metav1.GetOptions{}); err != nil {
		t.Fatalf("recreated workload PVC was changed: %v", err)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, "pv-destination", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("destination PV still exists: %v", err)
	}

	if store.deletes != 1 {
		t.Fatalf("session deletes=%d", store.deletes)
	}
}

func TestCleanupAbortedCopyBeforeReservePreservesUnownedSourcePV(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseAborted
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid"),
		}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
			Name: "pv-source", UID: types.UID("source-pv-uid"),
		}, Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Namespace: "app",
				Name:      "data",
				UID:       types.UID("source-pvc-uid"),
			},
		}},
	)
	store := &memoryStore{}

	service := &Service{client: client, store: store}
	if err := service.cleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Fatalf("source reclaim policy=%s", pv.Spec.PersistentVolumeReclaimPolicy)
	}

	if store.deletes != 1 {
		t.Fatalf("session deletes=%d", store.deletes)
	}
}

func TestValidateCleanupAbortedCopyChecksUncheckpointedSourceIdentity(t *testing.T) {
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("recreated-pvc-uid"),
		}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
			Name: "pv-source", UID: types.UID("source-pv-uid"),
		}, Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		}},
	)
	service := &Service{client: client, store: &memoryStore{}}

	err := service.validateCleanupWorkflowForTest(
		context.Background(),
		session,
		CleanupOptions{Finalize: true, DeleteSession: true},
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestValidateCleanupAbortedCopyPreservesUnownedSourcePolicyChanges(t *testing.T) {
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid"),
		}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
			Name: "pv-source", UID: types.UID("source-pv-uid"),
		}, Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		}},
	)

	service := &Service{client: client, store: &memoryStore{}}
	if err := service.validateCleanupWorkflowForTest(
		context.Background(),
		session,
		CleanupOptions{Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupAbortedCopyReleasesSourceAfterCheckpointLoss(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid"),
			Annotations: map[string]string{kube.SessionKey: session.ID},
		}},
		managedPV("pv-source", "source-pv-uid", session.ID, "source", corev1.VolumeBound),
	)

	service := &Service{client: client, store: &memoryStore{}}
	if err := service.cleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pvc.Annotations[kube.SessionKey] != "" {
		t.Fatalf("source PVC remains owned by %q", pvc.Annotations[kube.SessionKey])
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete ||
		pv.Labels[kube.SessionKey] != "" ||
		pv.Labels[kube.ResourceRoleLabel] != "" {
		t.Fatalf("source PV was not restored: %#v", pv)
	}
}

func TestCleanupAbortedMigrationBeforeReservePreservesForeignOwner(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted

	const foreign = "previous-session"

	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid"),
			Annotations: map[string]string{kube.SessionKey: foreign},
		}},
		managedPV("pv-source", "source-pv-uid", foreign, "active", corev1.VolumeBound),
	)
	store := &memoryStore{}
	service := &Service{client: client, store: store}

	options := CleanupOptions{Finalize: true, DeleteSession: true}
	if err := service.validateCleanupWorkflowForTest(ctx, session, options); err != nil {
		t.Fatal(err)
	}

	if err := service.cleanupWorkflowForTest(ctx, session, options); err != nil {
		t.Fatal(err)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pvc.Annotations[kube.SessionKey] != foreign || pv.Labels[kube.SessionKey] != foreign ||
		store.deletes != 1 {
		t.Fatalf(
			"foreign ownership changed: pvc=%v pv=%v deletes=%d",
			pvc.Annotations,
			pv.Labels,
			store.deletes,
		)
	}
}

func TestCleanupAbortedMigrationReleasesOwnPreCheckpointSource(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	session.Status.Phase = domain.PhaseAborted
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	client := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid"),
			Annotations: map[string]string{kube.SessionKey: session.ID},
		}},
		managedPV("pv-source", "source-pv-uid", session.ID, "source", corev1.VolumeBound),
	)

	service := &Service{client: client, store: &memoryStore{}}
	if err := service.cleanupWorkflowForTest(
		ctx,
		session,
		CleanupOptions{Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv-source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pvc.Annotations[kube.SessionKey] != "" || pv.Labels[kube.SessionKey] != "" ||
		pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Fatalf("session ownership remains: pvc=%v pv=%#v", pvc.Annotations, pv)
	}
}

func TestCleanupRequiresRollbackClosureBeforeSessionDeletion(t *testing.T) {
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	service := &Service{client: fake.NewClientset(), store: &memoryStore{}}

	err := service.cleanupWorkflowForTest(
		context.Background(),
		session,
		CleanupOptions{DeleteSession: true},
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCleanupSingleStageSessionsRemovesDestinationAndFinalizesSource(t *testing.T) {
	for _, tc := range []struct {
		name      string
		operation domain.Operation
		phase     domain.Phase
	}{
		{name: "reserve", operation: domain.OperationReserve, phase: domain.PhaseReserved},
		{name: "copy", operation: domain.OperationCopy, phase: domain.PhaseWarmCopied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			session := appTestSession()
			setSessionOperation(session, tc.operation)
			session.Status.Phase = tc.phase
			session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
				Name: "pv-destination",
				UID:  types.UID("destination-pv-uid"),
			}
			session.Spec.Volumes[0].DestinationPVC.UID = types.UID("destination-pvc-uid")
			session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete

			sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid"),
				Annotations: map[string]string{kube.SessionKey: session.ID},
			}}
			destinationPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: "system",
				Name:      "data-migrated",
				UID:       types.UID("destination-pvc-uid"),
				Labels: map[string]string{
					kube.ManagedByLabel:    kube.ManagedByValue,
					kube.SessionKey:        session.ID,
					kube.ResourceRoleLabel: "destination",
				},
				Annotations: map[string]string{kube.SessionKey: session.ID},
			}}
			sourcePV := managedPV(
				"pv-source",
				"source-pv-uid",
				session.ID,
				"source",
				corev1.VolumeBound,
			)
			destinationPV := managedPV(
				"pv-destination",
				"destination-pv-uid",
				session.ID,
				"destination",
				corev1.VolumeReleased,
			)
			client := fake.NewClientset(sourcePVC, destinationPVC, sourcePV, destinationPV)
			store := &memoryStore{}
			service := &Service{client: client, store: store}
			options := CleanupOptions{
				DeleteTemporary: true,
				DeleteRollback:  true,
				Finalize:        true,
				DeleteSession:   true,
			}

			if err := service.validateCleanupWorkflowForTest(ctx, session, options); err != nil {
				t.Fatalf("validate cleanup: %v", err)
			}

			if err := service.cleanupWorkflowForTest(ctx, session, options); err != nil {
				t.Fatalf("cleanup: %v", err)
			}

			if _, err := client.CoreV1().
				PersistentVolumeClaims("system").
				Get(ctx, destinationPVC.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
				err,
			) {
				t.Fatalf("destination PVC still exists: %v", err)
			}

			if _, err := client.CoreV1().
				PersistentVolumes().
				Get(ctx, destinationPV.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
				err,
			) {
				t.Fatalf("destination PV still exists: %v", err)
			}

			finalSourcePVC, err := client.CoreV1().
				PersistentVolumeClaims("app").
				Get(ctx, sourcePVC.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if finalSourcePVC.Annotations[kube.SessionKey] != "" {
				t.Fatalf(
					"source PVC remains owned by %q",
					finalSourcePVC.Annotations[kube.SessionKey],
				)
			}

			finalSourcePV, err := client.CoreV1().
				PersistentVolumes().
				Get(ctx, sourcePV.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if finalSourcePV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete ||
				finalSourcePV.Labels[kube.SessionKey] != "" {
				t.Fatalf("source PV was not finalized: %#v", finalSourcePV)
			}

			if store.deletes != 1 {
				t.Fatalf("session deletes=%d", store.deletes)
			}
		})
	}
}

func TestCleanupCompletedCopyCanPreserveOutputAndDeleteSession(t *testing.T) {
	ctx := context.Background()
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseWarmCopied
	session.Spec.Volumes[0].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("destination-pv-uid"),
	}
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("destination-pvc-uid")
	session.Spec.Volumes[0].DestinationPolicy = corev1.PersistentVolumeReclaimDelete

	sourcePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid"),
		Annotations: map[string]string{kube.SessionKey: session.ID},
	}}
	destinationPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "system", Name: "data-migrated", UID: types.UID("destination-pvc-uid"),
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        session.ID,
			kube.ResourceRoleLabel: "destination",
		},
		Annotations: map[string]string{
			kube.SessionKey:             session.ID,
			kube.SourcePVAnnotation:     "pv-source",
			kube.SourcePVCUIDAnnotation: "source-pvc-uid",
		},
	}}
	sourcePV := managedPV("pv-source", "source-pv-uid", session.ID, "source", corev1.VolumeBound)
	sourcePV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: "app",
		Name:      "data",
		UID:       sourcePVC.UID,
	}
	destinationPV := managedPV(
		"pv-destination",
		"destination-pv-uid",
		session.ID,
		"destination",
		corev1.VolumeBound,
	)
	destinationPV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: "system",
		Name:      "data-migrated",
		UID:       destinationPVC.UID,
	}
	client := fake.NewClientset(sourcePVC, destinationPVC, sourcePV, destinationPV)
	store := &memoryStore{}
	service := &Service{client: client, store: store}
	options := CleanupOptions{Finalize: true, DeleteSession: true}

	if err := service.validateCleanupWorkflowForTest(ctx, session, options); err != nil {
		t.Fatal(err)
	}

	if err := service.cleanupWorkflowForTest(ctx, session, options); err != nil {
		t.Fatal(err)
	}

	for _, ref := range []domain.ObjectReference{session.Spec.Volumes[0].SourcePVC, session.Spec.Volumes[0].DestinationPVC} {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if pvc.Annotations[kube.SessionKey] != "" ||
			pvc.Annotations[kube.SourcePVAnnotation] != "" ||
			pvc.Annotations[kube.SourcePVCUIDAnnotation] != "" ||
			pvc.Labels[kube.SessionKey] != "" {
			t.Fatalf(
				"PVC %s/%s ownership=%v annotations=%v",
				pvc.Namespace,
				pvc.Name,
				pvc.Labels,
				pvc.Annotations,
			)
		}
	}

	for _, name := range []string{"pv-source", "pv-destination"} {
		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete ||
			pv.Labels[kube.SessionKey] != "" ||
			pv.Labels[kube.ResourceRoleLabel] != "" {
			t.Fatalf(
				"PV %s policy=%s labels=%v",
				name,
				pv.Spec.PersistentVolumeReclaimPolicy,
				pv.Labels,
			)
		}
	}

	if store.deletes != 1 {
		t.Fatalf("session deletes=%d", store.deletes)
	}

	if err := service.validateCleanupWorkflowForTest(ctx, session, options); err != nil {
		t.Fatalf("idempotent validation: %v", err)
	}

	if err := service.cleanupWorkflowForTest(ctx, session, options); err != nil {
		t.Fatalf("idempotent cleanup: %v", err)
	}
}

func TestCleanupRejectsSingleStageNonTerminalPhase(t *testing.T) {
	for _, operation := range []domain.Operation{domain.OperationReserve, domain.OperationCopy} {
		session := appTestSession()
		setSessionOperation(session, operation)
		session.Status.Phase = domain.PhaseFailed
		service := &Service{client: fake.NewClientset(), store: &memoryStore{}}

		err := service.validateCleanupWorkflowForTest(
			context.Background(),
			session,
			CleanupOptions{},
		)
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("operation=%s category=%s error=%v", operation, domain.CategoryOf(err), err)
		}
	}
}

func managedPV(
	name, uid, sessionID, role string,
	phase corev1.PersistentVolumePhase,
) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             types.UID(uid),
			ResourceVersion: "1",
			Labels: map[string]string{
				kube.ManagedByLabel:    kube.ManagedByValue,
				kube.SessionKey:        sessionID,
				kube.ResourceRoleLabel: role,
			},
			Annotations: map[string]string{
				kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
				kube.PairedPVAnnotation:       "paired",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		},
		Status: corev1.PersistentVolumeStatus{Phase: phase},
	}
}
