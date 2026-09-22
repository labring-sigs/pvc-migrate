package kube

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestExecutionFingerprintTracksOnlyExecutionInputs(t *testing.T) {
	assertExecutionFingerprint(t, &v1alpha1.Migration{},
		func(o *v1alpha1.Migration) { o.Spec.SourceNode = "worker" },
		func(o *v1alpha1.Migration) { o.Spec.UnusedStoragePolicy = "Delete" },
	)
	assertExecutionFingerprint(t, &v1alpha1.ClusterMigration{},
		func(o *v1alpha1.ClusterMigration) { o.Spec.SourceNamespace = "other" },
		func(o *v1alpha1.ClusterMigration) { o.Spec.UnusedStoragePolicy = "Delete" },
	)
	assertExecutionFingerprint(t, &v1alpha1.PodMigration{},
		func(o *v1alpha1.PodMigration) { o.Spec.PrecopyPasses = 1 },
		func(o *v1alpha1.PodMigration) {
			o.Spec.UnusedStoragePolicy = "Delete"
			o.Status.Plan = &v1alpha1.PodMigrationPlan{
				Workload: v1alpha1.WorkloadSpec{
					OriginalObject: &apiextensionsv1.JSON{Raw: []byte(`{invalid snapshot`)},
				},
			}
		},
	)
	assertExecutionFingerprint(t, &v1alpha1.ClusterPodMigration{},
		func(o *v1alpha1.ClusterPodMigration) { o.Spec.TemporaryNamespace = "other" },
		func(o *v1alpha1.ClusterPodMigration) { o.Spec.UnusedStoragePolicy = "Delete" },
	)
	assertExecutionFingerprint(t, &v1alpha1.Copy{},
		func(o *v1alpha1.Copy) { o.Spec.Online = true },
		func(o *v1alpha1.Copy) { o.Spec.UnusedStoragePolicy = "Delete" },
	)
	assertExecutionFingerprint(t, &v1alpha1.ClusterCopy{},
		func(o *v1alpha1.ClusterCopy) { o.Spec.DestinationNamespace = "other" },
		func(o *v1alpha1.ClusterCopy) { o.Spec.UnusedStoragePolicy = "Delete" },
	)
	assertExecutionFingerprint(t, &v1alpha1.Reservation{},
		func(o *v1alpha1.Reservation) { o.Spec.TargetNode = "worker" },
		func(o *v1alpha1.Reservation) { o.Spec.UnusedStoragePolicy = "Delete" },
	)
	assertExecutionFingerprint(t, &v1alpha1.ClusterReservation{},
		func(o *v1alpha1.ClusterReservation) { o.Spec.SessionNamespace = "other" },
		func(o *v1alpha1.ClusterReservation) { o.Spec.UnusedStoragePolicy = "Delete" },
	)
	assertExecutionFingerprint(t, &v1alpha1.Backup{},
		func(o *v1alpha1.Backup) { o.Spec.RepositoryRef.Name = "other" }, nil,
	)
	assertExecutionFingerprint(t, &v1alpha1.Restore{},
		func(o *v1alpha1.Restore) { o.Spec.Name = "other-snapshot" }, nil,
	)
	assertExecutionFingerprint(t, &v1alpha1.Rename{},
		func(o *v1alpha1.Rename) { o.Spec.DestinationPVC.Name = "other" }, nil,
	)
	assertExecutionFingerprint(t, &v1alpha1.Move{},
		func(o *v1alpha1.Move) { o.Spec.SourcePVC.UID = "replacement" }, nil,
	)
}

func assertExecutionFingerprint[T crclient.Object](
	t *testing.T,
	object T,
	changeInput, changeNonExecution func(T),
) {
	t.Helper()
	t.Run(reflect.TypeOf(object).Elem().Name(), func(t *testing.T) {
		before := object.DeepCopyObject()

		original, err := WorkflowExecutionIntentHash(object)
		if err != nil || original == "" {
			t.Fatalf("fingerprint=%q err=%v", original, err)
		}

		if !reflect.DeepEqual(before, object) {
			t.Fatal("fingerprinting mutated input")
		}

		object.SetGeneration(3)
		object.SetResourceVersion("changed")
		object.SetFinalizers([]string{"protection"})

		if changeNonExecution != nil {
			changeNonExecution(object)
		}

		before = object.DeepCopyObject()

		unchanged, err := WorkflowExecutionIntentHash(object)
		if err != nil || unchanged != original {
			t.Fatalf("non-execution change altered fingerprint: %v", err)
		}

		if !reflect.DeepEqual(before, object) {
			t.Fatal("fingerprinting cleared caller-owned policies or status")
		}

		changeInput(object)

		changed, err := WorkflowExecutionIntentHash(object)
		if err != nil || changed == original {
			t.Fatalf("execution change was not fingerprinted: %v", err)
		}
	})
}

func TestExecutionFingerprintRejectsMissingOrUnsupportedObjects(t *testing.T) {
	for _, object := range []crclient.Object{nil, (*v1alpha1.Copy)(nil), &corev1.Pod{}} {
		if hash, err := WorkflowExecutionIntentHash(object); err == nil || hash != "" {
			t.Fatalf("accepted unsupported object %T: hash=%q err=%v", object, hash, err)
		}
	}
}

func TestExecutionFingerprintNormalizesDefaultDeleteExtraneous(t *testing.T) {
	omitted := &v1alpha1.Copy{}
	explicit := omitted.DeepCopy()
	explicit.Spec.DeleteExtraneous = new(true)

	omittedHash, err := WorkflowExecutionIntentHash(omitted)
	if err != nil {
		t.Fatal(err)
	}

	explicitHash, err := WorkflowExecutionIntentHash(explicit)
	if err != nil {
		t.Fatal(err)
	}

	if omittedHash != explicitHash {
		t.Fatalf(
			"default-equivalent transfer specs have different fingerprints: %q != %q",
			omittedHash,
			explicitHash,
		)
	}

	explicit.Spec.DeleteExtraneous = new(false)

	falseHash, err := WorkflowExecutionIntentHash(explicit)
	if err != nil {
		t.Fatal(err)
	}

	if falseHash == omittedHash {
		t.Fatal("explicit false unexpectedly matched the default true fingerprint")
	}
}
