package controller

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestWorkflowQueuesRouteDeletionWithoutStatusFeedback(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		before := &v1alpha1.Copy{
			ObjectMeta: metav1.ObjectMeta{Name: "copy", UID: "uid", Generation: 1},
		}
		after := before.DeepCopy()
		now := metav1.Now()
		after.DeletionTimestamp = &now
		after.Generation++
		cancellations := 0

		p := workflowQueuePredicate(deleting, func(crclient.Object) { cancellations++ })
		if got := p.Create(event.CreateEvent{Object: before}); got == deleting {
			t.Fatalf("queue deletion=%t accepted wrong initial object", deleting)
		}

		if got := p.Create(event.CreateEvent{Object: after}); got != deleting {
			t.Fatalf("queue deletion=%t missed deleting initial object", deleting)
		}

		if got := p.Update(
			event.UpdateEvent{ObjectOld: before, ObjectNew: after},
		); got != deleting {
			t.Fatalf("queue deletion=%t routed update incorrectly", deleting)
		}

		if cancellations != 1 {
			t.Fatalf("queue deletion=%t did not cancel active execution", deleting)
		}

		status := after.DeepCopy()

		status.Status.Phase = "Aborting"
		if p.Update(event.UpdateEvent{ObjectOld: after, ObjectNew: status}) {
			t.Fatal("status write queued another finalization")
		}

		if p.Delete(event.DeleteEvent{Object: after}) {
			t.Fatal("deleted object was queued")
		}
	}
}
