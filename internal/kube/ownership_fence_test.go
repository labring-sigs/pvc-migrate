package kube

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestPVCOwnershipRejectsFenceLossAfterRead(t *testing.T) {
	for _, operation := range []string{"acquire", "release", "finalize"} {
		t.Run(operation, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: "data", Name: "volume", UID: "pvc",
				Annotations: map[string]string{SessionKey: "workflow"},
			}}
			if operation == "acquire" {
				pvc.Annotations = nil
			}

			client := fake.NewClientset(pvc)
			lost := errors.New("lease lost after PVC read")
			fence := &testLeaseFence{}
			client.PrependReactor(
				"get",
				"persistentvolumeclaims",
				func(k8stesting.Action) (bool, runtime.Object, error) {
					fence.err = lost
					return false, nil, nil
				},
			)

			ctx := WithLeaseFence(context.Background(), fence)
			ref := PVCReference(pvc)

			var err error
			switch operation {
			case "acquire":
				err = AcquirePVC(ctx, client, ref, "workflow")
			case "release":
				err = ReleasePVC(ctx, client, ref, "workflow")
			case "finalize":
				err = FinalizePVC(ctx, client, ref, "workflow", v1alpha1.PVCMetadata{})
			}

			if !errors.Is(err, lost) {
				t.Fatalf("error = %v", err)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf("mutation after fence loss: %s", action.GetVerb())
				}
			}
		})
	}
}
