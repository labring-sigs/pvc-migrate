package kube

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestWorkflowWatchPinsSubmissionUIDBeforeFirstRead(t *testing.T) {
	initial := storedRename()
	replaced := initial.DeepCopy()
	replaced.UID = "replacement"

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := fake.NewSimpleDynamicClient(scheme, replaced)
	updates := 0

	_, err := WaitForWorkflow(
		t.Context(),
		client.Resource(v1alpha1.GroupVersion.WithResource("renames")).Namespace(initial.Namespace),
		initial,
		func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
		func(*v1alpha1.Rename) (bool, error) { updates++; return true, nil },
	)
	if domain.CategoryOf(err) != domain.ErrorConflict || updates != 0 {
		t.Fatalf(
			"replacement was reported as the submitted workflow: updates=%d err=%v",
			updates,
			err,
		)
	}
}

func TestWorkflowWatchDeliversConcreteCheckpoint(t *testing.T) {
	initial := storedRename()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := fake.NewSimpleDynamicClient(scheme, initial)
	stream := watch.NewRaceFreeFake()
	started := make(chan struct{})
	client.PrependWatchReactor(
		"renames",
		func(action ktesting.Action) (bool, watch.Interface, error) {
			request, ok := action.(ktesting.WatchAction)
			if !ok {
				t.Error("expected watch action")
			} else if request.GetWatchRestrictions().Fields.String() != "metadata.name="+initial.Name {
				t.Error("watch was not scoped to the submitted name")
			}

			close(started)

			return true, stream, nil
		},
	)

	result := make(chan error, 1)
	go func() {
		object, err := WaitForWorkflow(
			t.Context(),
			client.Resource(v1alpha1.GroupVersion.WithResource("renames")).
				Namespace(initial.Namespace),
			initial,
			func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
			func(current *v1alpha1.Rename) (bool, error) {
				return current.Status.Phase == domain.PhaseCompleted, nil
			},
		)
		if err == nil && (object == nil || object.Status.Phase != domain.PhaseCompleted) {
			t.Error("lost typed completion checkpoint")
		}

		result <- err
	}()

	<-started

	completed := initial.DeepCopy()
	completed.Status.Phase = domain.PhaseCompleted

	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(completed)
	if err != nil {
		t.Fatal(err)
	}

	stream.Modify(&unstructured.Unstructured{Object: data})

	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowWatchKeepsIdentityAcrossReconnect(t *testing.T) {
	for _, ending := range []string{"closed", "expired"} {
		t.Run(ending, func(t *testing.T) {
			initial := storedRename()

			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}

			client := fake.NewSimpleDynamicClient(scheme)
			gets := 0
			client.PrependReactor(
				"get",
				"renames",
				func(ktesting.Action) (bool, runtime.Object, error) {
					gets++

					current := initial.DeepCopy()
					if gets > 1 {
						current.UID = "replaced"
					}

					data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(current)

					return true, &unstructured.Unstructured{Object: data}, err
				},
			)
			client.PrependWatchReactor(
				"renames",
				func(ktesting.Action) (bool, watch.Interface, error) {
					stream := watch.NewRaceFreeFake()
					if ending == "closed" {
						stream.Stop()
					} else {
						stream.Error(
							&metav1.Status{
								Status: metav1.StatusFailure,
								Code:   410,
								Reason: metav1.StatusReasonExpired,
							},
						)
					}

					return true, stream, nil
				},
			)

			_, err := WaitForWorkflow(
				t.Context(),
				client.Resource(v1alpha1.GroupVersion.WithResource("renames")).
					Namespace(initial.Namespace),
				initial,
				func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				func(*v1alpha1.Rename) (bool, error) { return false, nil },
			)
			if domain.CategoryOf(err) != domain.ErrorConflict || gets != 2 {
				t.Fatalf("reconnect lost original identity: gets=%d err=%v", gets, err)
			}
		})
	}
}
