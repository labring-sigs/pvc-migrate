package app

import (
	"context"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func deletionTestVolume(name string) v1alpha1.VolumeSpec {
	return v1alpha1.VolumeSpec{
		SourcePVC: v1alpha1.LocalResourceReference{Name: name, UID: types.UID("uid-" + name)},
		SourcePV: v1alpha1.LocalResourceReference{
			Name: "pv-" + name,
			UID:  types.UID("uid-pv-" + name),
		},
	}
}

// The skip is factual rather than mode-gated: every cleanup pass (abort
// included, not just workflow deletion) must tolerate a source pair that is
// already going away, or aborting a copy whose source vanished wedges the
// workflow forever.
func TestDeletionSourceMissingCoversSettlingSources(t *testing.T) {
	terminating := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "data",
			Name:      "a",
			UID:       types.UID("uid-a"),
		},
	}
	now := metav1.Now()
	terminating.DeletionTimestamp = &now

	intactPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-b", UID: types.UID("uid-pv-b")},
	}

	livePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "data",
			Name:      "c",
			UID:       types.UID("uid-c"),
		},
	}

	livePV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-c", UID: types.UID("uid-pv-c")},
	}

	tests := []struct {
		name    string
		Volume  v1alpha1.VolumeSpec
		objects []any
		missing bool
	}{
		{
			name:    "terminating pvc",
			Volume:  deletionTestVolume("a"),
			objects: []any{terminating},
			missing: true,
		},
		{
			name:    "deleted pvc",
			Volume:  deletionTestVolume("a"),
			objects: []any{},
			missing: true,
		},
		{
			name:    "intact pvc with deleted pv",
			Volume:  deletionTestVolume("b"),
			objects: []any{intactPV},
			missing: true,
		},
		{
			// The relaxation forgives only vanished storage: a live pair is
			// still validated strictly in every pass.
			name:    "intact pair",
			Volume:  deletionTestVolume("c"),
			objects: []any{livePVC, livePV},
			missing: false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client := fake.NewClientset()
			for _, object := range testCase.objects {
				var err error
				switch typed := object.(type) {
				case *corev1.PersistentVolumeClaim:
					_, err = client.CoreV1().
						PersistentVolumeClaims(typed.Namespace).
						Create(t.Context(), typed, metav1.CreateOptions{})
				case *corev1.PersistentVolume:
					_, err = client.CoreV1().
						PersistentVolumes().
						Create(t.Context(), typed, metav1.CreateOptions{})
				}

				if err != nil {
					t.Fatal(err)
				}
			}

			ctx := context.WithValue(t.Context(), workflowDeletionContextKey{}, true)

			missing, err := deletionSourceMissing(ctx, client, "data", testCase.Volume)
			if err != nil {
				t.Fatal(err)
			}

			if missing != testCase.missing {
				t.Fatalf("deletion pass: missing = %v, want %v", missing, testCase.missing)
			}

			plain, err := deletionSourceMissing(t.Context(), client, "data", testCase.Volume)
			if err != nil {
				t.Fatal(err)
			}

			if plain != testCase.missing {
				t.Fatalf("abort pass: missing = %v, want %v", plain, testCase.missing)
			}
		})
	}
}
