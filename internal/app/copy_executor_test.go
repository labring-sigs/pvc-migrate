package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type copyCheckpointStore struct {
	object *v1alpha1.ClusterCopy
	save   func(*v1alpha1.ClusterCopy) error
}

func (s *copyCheckpointStore) Create(_ context.Context, object *v1alpha1.ClusterCopy) error {
	s.object = object.DeepCopy()

	return nil
}

func (s *copyCheckpointStore) Load(
	context.Context,
	crclient.ObjectKey,
) (*v1alpha1.ClusterCopy, error) {
	return s.object.DeepCopy(), nil
}

func (s *copyCheckpointStore) List(context.Context, string) ([]*v1alpha1.ClusterCopy, error) {
	return []*v1alpha1.ClusterCopy{s.object.DeepCopy()}, nil
}

func (s *copyCheckpointStore) Save(ctx context.Context, object *v1alpha1.ClusterCopy) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.save != nil {
		if err := s.save(object); err != nil {
			return err
		}
	}

	s.object = object.DeepCopy()

	return nil
}

func (s *copyCheckpointStore) Delete(context.Context, *v1alpha1.ClusterCopy) error {
	s.object = nil
	return nil
}

type concreteCopyEngine struct {
	requests   []copyengine.CopyRequest
	cleanups   []copyengine.CleanupRequest
	copy       func(copyengine.CopyRequest) error
	cleanupErr error
}

func (e *concreteCopyEngine) Copy(
	_ context.Context,
	request copyengine.CopyRequest,
	_ copyengine.ProgressFunc,
) error {
	e.requests = append(e.requests, request)
	if e.copy != nil {
		return e.copy(request)
	}

	return nil
}

func (e *concreteCopyEngine) Cleanup(_ context.Context, request copyengine.CleanupRequest) error {
	e.cleanups = append(e.cleanups, request)
	return e.cleanupErr
}

func copyExecutorFixture(
	t *testing.T,
) (*ClusterCopyExecutor, *v1alpha1.ClusterCopy, *copyCheckpointStore, *concreteCopyEngine) {
	t.Helper()

	_, reservation, _, reserver := reservationExecutorFixture(t)
	object := &v1alpha1.ClusterCopy{
		ObjectMeta: *reservation.ObjectMeta.DeepCopy(),
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace:      "source",
			DestinationNamespace: "destination",
			SessionNamespace:     "sessions",
			CopySpec:             v1alpha1.CopySpec{Volumes: reservation.Spec.Volumes},
		},
		Status: v1alpha1.ClusterCopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.ClusterCopyPlan{
				SourceNamespace:      "source",
				DestinationNamespace: "destination",
				SessionNamespace:     "sessions",
				Volumes:              reservation.Status.Plan.Volumes,
				ToolImage:            "example/tool:v1",
				Strategies:           []string{domain.StrategyMount},
				VerifyChecksum:       true,
			},
		},
	}
	store := &copyCheckpointStore{object: object.DeepCopy()}
	engine := &concreteCopyEngine{}
	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "destination"}},
	)
	executor := NewClusterCopyExecutor(
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"sessions",
		engine,
		CopyExecutorConfig{Transfer: VolumeCopyConfig{Retries: 1}},
	)
	executor.reserver = reserver

	return executor, object, store, engine
}

func TestCopyExecutorResumesBySourceIdentityAndPreservesPlan(t *testing.T) {
	executor, object, store, engine := copyExecutorFixture(t)
	before := object.DeepCopy()
	failure := errors.New("copy transport unavailable")
	engine.copy = func(request copyengine.CopyRequest) error {
		if request.AttemptIdentity.Source.Name == "b" {
			return failure
		}

		return nil
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseWarmCopying ||
		object.Status.Volumes[0].Sync.WarmCompletedAt == nil ||
		object.Status.Volumes[1].Sync.WarmCompletedAt != nil {
		t.Fatalf("unexpected partial checkpoint: %+v", object.Status)
	}

	object.Status.Volumes[0], object.Status.Volumes[1] = object.Status.Volumes[1], object.Status.Volumes[0]
	store.object = object.DeepCopy()
	engine.copy = nil

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseWarmCopied || len(engine.requests) != 3 ||
		engine.requests[2].AttemptIdentity.Source.Name != "b" || engine.requests[2].Attempt != 2 ||
		len(engine.cleanups) != 1 || engine.cleanups[0].Source.Name != "b" {
		t.Fatalf(
			"copy recovery replayed completed work: requests=%+v, cleanups=%+v",
			engine.requests,
			engine.cleanups,
		)
	}

	if !engine.requests[0].Policy.VerifyChecksum {
		t.Fatal("copy checksum request did not reach the engine")
	}

	if !reflect.DeepEqual(object.Spec, before.Spec) ||
		!reflect.DeepEqual(object.Status.Plan, before.Status.Plan) {
		t.Fatal("execution modified its immutable spec or plan")
	}

	if err := executor.Run(t.Context(), object); err != nil || len(engine.requests) != 3 {
		t.Fatal("completed reconciliation repeated a copy pass")
	}
}

func TestCopyExecutorAttemptCheckpointFailureDoesNotStartEngine(t *testing.T) {
	executor, object, store, engine := copyExecutorFixture(t)
	failure := errors.New("attempt checkpoint failed")
	store.save = func(current *v1alpha1.ClusterCopy) error {
		if current.Status.Phase == domain.PhaseWarmCopying &&
			current.Status.Volumes[0].Sync.Attempts != 0 {
			return failure
		}

		return nil
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if len(engine.requests) != 0 || object.Status.Volumes[0].Sync.Attempts != 0 ||
		store.object.Status.Volumes[0].Sync.Attempts != 0 {
		t.Fatal("failed attempt checkpoint reached the copy engine or changed progress")
	}

	store.save = nil

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if engine.requests[0].Attempt != 1 {
		t.Fatal("failed persistence consumed an attempt identity")
	}
}

func TestCopyExecutorAbortRetriesToolCleanup(t *testing.T) {
	executor, object, _, engine := copyExecutorFixture(t)
	engine.copy = func(copyengine.CopyRequest) error { return errors.New("interrupted transfer") }

	if err := executor.Run(t.Context(), object); err == nil {
		t.Fatal("expected interrupted transfer")
	}

	failure := errors.New("chart cleanup failed")
	engine.cleanupErr = failure

	if err := executor.Abort(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Phase != domain.PhaseAborting {
		t.Fatal("failed cleanup lost the abort checkpoint")
	}

	engine.cleanupErr = nil

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted || len(engine.requests) != 1 {
		t.Fatal("abort recovery restarted transfer")
	}
}

func TestCopyExecutorCompletionCheckpointFailureReplaysOnlyUncommittedWork(t *testing.T) {
	executor, object, store, engine := copyExecutorFixture(t)
	failure := errors.New("completion checkpoint unavailable")
	store.save = func(current *v1alpha1.ClusterCopy) error {
		if len(current.Status.Volumes) != 0 &&
			current.Status.Volumes[0].Sync.WarmCompletedAt != nil {
			return failure
		}

		return nil
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Volumes[0].Sync.WarmCompletedAt != nil || len(engine.requests) != 1 {
		t.Fatal("failed completion checkpoint advanced copy progress")
	}

	store.save = nil

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.requests) != 3 || engine.requests[1].AttemptIdentity.Source.Name != "a" ||
		engine.requests[1].Attempt != 2 ||
		engine.requests[2].AttemptIdentity.Source.Name != "b" {
		t.Fatal("retry did not recover the uncommitted copy attempt")
	}
}

func TestCopyExecutorCheckpointsConsumerPlacementWithoutChangingPlan(t *testing.T) {
	executor, object, store, engine := copyExecutorFixture(t)
	object.Spec.Online = true
	object.Status.Plan.Online = true
	store.object = object.DeepCopy()
	before := object.DeepCopy()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "source", Name: "consumer", UID: "consumer-uid"},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "a",
					},
				},
			},
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if _, err := executor.client.CoreV1().
		Pods("source").
		Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := executor.client.CoreV1().Nodes().Create(t.Context(), &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-a",
			Labels: map[string]string{corev1.LabelHostname: "node-a"},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("transfer unavailable")
	engine.copy = func(request copyengine.CopyRequest) error {
		if request.AttemptIdentity.Source.Name == "b" {
			return failure
		}
		return nil
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.SourceNode != "node-a" || store.object.Status.SourceNode != "node-a" ||
		!reflect.DeepEqual(
			object.Status.Plan,
			before.Status.Plan,
		) || !reflect.DeepEqual(object.Spec, before.Spec) {
		t.Fatal("consumer placement changed the plan or was not durably recorded")
	}

	pod.Spec.NodeName = "node-b"
	if _, err := executor.client.CoreV1().
		Pods("source").
		Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	engine.copy = nil

	if err := executor.Run(t.Context(), object); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("consumer drift was accepted: %v", err)
	}

	if len(engine.requests) != 2 || object.Status.SourceNode != "node-a" {
		t.Fatal("consumer drift started a new transfer or changed the recorded placement")
	}
}

func TestCopyExecutorRepeatPassCheckpointFailurePreservesCompletedProgress(t *testing.T) {
	executor, object, store, engine := copyExecutorFixture(t)
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	failure := errors.New("repeat checkpoint unavailable")
	store.save = func(*v1alpha1.ClusterCopy) error { return failure }

	if err := executor.RequestCopyPass(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if !reflect.DeepEqual(object, before) || !reflect.DeepEqual(store.object, before) {
		t.Fatal("failed repeat request erased completed progress")
	}

	store.save = nil

	if err := executor.RequestCopyPass(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.requests) != 2 || object.Status.Phase != domain.PhaseWarmCopying {
		t.Fatal("requesting a pass started execution or failed to checkpoint intent")
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.requests) != 4 || engine.requests[2].Attempt != 2 ||
		engine.requests[3].Attempt != 2 ||
		object.Status.Phase != domain.PhaseWarmCopied ||
		!reflect.DeepEqual(object.Status.Plan, before.Status.Plan) {
		t.Fatal("repeat pass reused attempt identities or modified the plan")
	}
}

func TestCopyExecutorRetryRejectsNewOfflineConsumer(t *testing.T) {
	executor, object, _, engine := copyExecutorFixture(t)
	executor.transfer.config.Retries = 2
	executor.transfer.sleep = func(context.Context, time.Duration) error { return nil }
	engine.copy = func(copyengine.CopyRequest) error {
		_, err := executor.client.CoreV1().Pods("source").Create(t.Context(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "source", Name: "consumer", UID: "consumer"},
			Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{
				Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "a",
					},
				},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		return errors.New("transport interrupted")
	}

	if err := executor.Run(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("error = %v", err)
	}

	if len(engine.requests) != 1 || object.Status.Volumes[0].Sync.Attempts != 1 {
		t.Fatal("offline retry ignored the new consumer")
	}
}
