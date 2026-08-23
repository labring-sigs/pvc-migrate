package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type staticVolumeUsageReader struct {
	result kube.VolumeUsageReadResult
	err    error
	calls  int
}

func (r *staticVolumeUsageReader) Read(
	context.Context,
	kube.VolumeUsageReadOptions,
) (kube.VolumeUsageReadResult, error) {
	r.calls++
	return r.result, r.err
}

type volumeUsageReaderFunc func(context.Context, kube.VolumeUsageReadOptions) (kube.VolumeUsageReadResult, error)

func (f volumeUsageReaderFunc) Read(
	ctx context.Context,
	options kube.VolumeUsageReadOptions,
) (kube.VolumeUsageReadResult, error) {
	return f(ctx, options)
}

func shrinkUsageSession(skip bool) *domain.Session {
	common := domain.SessionCommon{Volumes: []domain.VolumeSpec{{
		SourcePVC:      domain.ObjectReference{Namespace: "app", Name: "data"},
		SourcePV:       domain.ObjectReference{Name: "pv-data"},
		SourceCapacity: "2Gi",
		Capacity:       "1Gi",
	}}}

	return &domain.Session{
		ID: "usage-test",
		Spec: domain.NewSessionSpec(
			domain.OperationCopy,
			common,
			domain.WorkloadSpec{},
			false,
			domain.SessionWorkflowOptions{SkipSourceUsageCheck: skip},
		),
	}
}

func TestVerifyShrinkUsageRequiresTrustedBackendReader(t *testing.T) {
	err := (&Service{}).verifyShrinkUsage(context.Background(), shrinkUsageSession(false))
	if err == nil || !strings.Contains(err.Error(), "--skip-source-usage-check") {
		t.Fatalf("error=%v", err)
	}
}

func TestVerifyShrinkUsageUsesBackendResult(t *testing.T) {
	reader := &staticVolumeUsageReader{
		result: kube.VolumeUsageReadResult{UsedBytes: 512 << 20, Source: "test storage CRD"},
	}

	service := &Service{config: Config{VolumeUsageReader: reader}}
	if err := service.verifyShrinkUsage(
		context.Background(),
		shrinkUsageSession(false),
	); err != nil {
		t.Fatal(err)
	}

	if reader.calls != 1 {
		t.Fatalf("calls=%d", reader.calls)
	}
}

func TestVerifyShrinkUsageRejectsBackendOverflow(t *testing.T) {
	reader := &staticVolumeUsageReader{
		result: kube.VolumeUsageReadResult{UsedBytes: 2 << 30, Source: "test storage CRD"},
	}
	service := &Service{config: Config{VolumeUsageReader: reader}}

	err := service.verifyShrinkUsage(context.Background(), shrinkUsageSession(false))
	if err == nil || !strings.Contains(err.Error(), "above destination capacity") {
		t.Fatalf("error=%v", err)
	}
}

func TestVerifyPartialSourceShrinkReportsWholeVolumeUsageAsInconclusive(t *testing.T) {
	reader := &staticVolumeUsageReader{
		result: kube.VolumeUsageReadResult{UsedBytes: 2 << 30, Source: "test storage CRD"},
	}
	service := &Service{config: Config{VolumeUsageReader: reader}}
	session := shrinkUsageSession(false)
	session.Spec.Volumes[0].TransferScope = &domain.TransferScope{
		SourcePath:      "selected/data",
		DestinationPath: ".",
	}

	err := service.verifyShrinkUsage(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "cannot prove that selected source directory") ||
		!strings.Contains(err.Error(), "selected/data") {
		t.Fatalf("error=%v category=%s", err, domain.CategoryOf(err))
	}
}

func TestVerifyShrinkUsageExplicitSkipBypassesReader(t *testing.T) {
	reader := &staticVolumeUsageReader{err: errors.New("must not be called")}

	service := &Service{config: Config{VolumeUsageReader: reader}}
	if err := service.verifyShrinkUsage(
		context.Background(),
		shrinkUsageSession(true),
	); err != nil {
		t.Fatal(err)
	}

	if reader.calls != 0 {
		t.Fatalf("calls=%d", reader.calls)
	}
}

func TestValidateFinalSyncRechecksShrinkUsageWhenAlreadyPaused(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhasePaused
	session.Spec.Volumes[0].SourceCapacity = "2Gi"
	session.Spec.Volumes[0].Capacity = "1Gi"
	reader := &staticVolumeUsageReader{
		result: kube.VolumeUsageReadResult{UsedBytes: 2 << 30, Source: "test storage CRD"},
	}
	fixture.service.config.VolumeUsageReader = reader

	err := fixture.service.ValidateFinalSync(context.Background(), session)
	if err == nil || !strings.Contains(err.Error(), "above destination capacity") {
		t.Fatalf("error=%v", err)
	}

	if reader.calls != 1 {
		t.Fatalf("reader calls=%d", reader.calls)
	}
}

func TestPauseAndFinalSyncRechecksShrinkUsageWhenAlreadyPaused(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhasePaused
	session.Spec.Volumes[0].SourceCapacity = "2Gi"
	session.Spec.Volumes[0].Capacity = "1Gi"
	fixture.service.config.VolumeUsageReader = &staticVolumeUsageReader{
		result: kube.VolumeUsageReadResult{UsedBytes: 2 << 30, Source: "test storage CRD"},
	}

	err := fixture.service.PauseAndFinalSync(context.Background(), session)
	if err == nil || !strings.Contains(err.Error(), "above destination capacity") {
		t.Fatalf("error=%v", err)
	}

	if len(fixture.copier.requests) != 0 {
		t.Fatalf("copy started despite usage overflow: %v", fixture.copier.requests)
	}
}

func TestPauseAndFinalSyncRechecksShrinkUsageAfterPause(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Spec.Volumes[0].SourceCapacity = "2Gi"
	session.Spec.Volumes[0].Capacity = "1Gi"
	checkedWhilePaused := false
	fixture.service.config.VolumeUsageReader = volumeUsageReaderFunc(
		func(context.Context, kube.VolumeUsageReadOptions) (kube.VolumeUsageReadResult, error) {
			usedBytes := int64(512 << 20)
			if session.Status.Phase == domain.PhasePaused {
				checkedWhilePaused = true
				usedBytes = 2 << 30
			}

			return kube.VolumeUsageReadResult{UsedBytes: usedBytes, Source: "test storage CRD"}, nil
		},
	)

	err := fixture.service.PauseAndFinalSync(context.Background(), session)
	if err == nil || !strings.Contains(err.Error(), "above destination capacity") {
		t.Fatalf("error=%v", err)
	}

	if !checkedWhilePaused {
		t.Fatal("source usage was not checked after the workload paused")
	}

	if len(fixture.copier.requests) != 0 {
		t.Fatalf("copy started despite post-pause usage overflow: %v", fixture.copier.requests)
	}
}

func TestValidateActivationRejectsApplicationPVCQuotaBeforeSwitch(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	markTestSessionReserved(session)
	session.Status.Phase = domain.PhaseFinalSynced
	completed := metav1.Now()
	session.Status.Volumes[0].Sync.FinalCompletedAt = &completed
	session.Spec.Volumes[0].SourceCapacity = "1Gi"
	session.Spec.Volumes[0].Capacity = "2Gi"

	session.Spec.Volumes[0].SourcePVCSpec.Resources.Requests = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse("1Gi"),
	}
	if _, err := fixture.client.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      session.Spec.Volumes[0].SourcePVC.Name,
				UID:       session.Spec.Volumes[0].SourcePVC.UID,
			},
			Spec: session.Spec.Volumes[0].SourcePVCSpec,
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.client.CoreV1().
		ResourceQuotas("app").
		Create(context.Background(), &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "application-storage"},
			Spec: corev1.ResourceQuotaSpec{
				Hard: corev1.ResourceList{
					corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
				},
			},
			Status: corev1.ResourceQuotaStatus{
				Hard: corev1.ResourceList{
					corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
				},
				Used: corev1.ResourceList{
					corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
				},
			},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	err := fixture.service.ValidateActivation(context.Background(), session)
	if err == nil || !strings.Contains(err.Error(), "quota rejected") {
		t.Fatalf("error=%v", err)
	}
}
