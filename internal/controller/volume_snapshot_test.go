package controller

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientfake "k8s.io/client-go/kubernetes/fake"
)

func TestValidateDeclarativeSourceVolumesAcceptsPlannerSnapshot(t *testing.T) {
	pvc, pv, session := declarativeVolumeFixture()
	r := NewWorkflowReconciler(nil, nil).WithKubernetesClient(
		clientfake.NewSimpleClientset(pvc, pv),
	)

	if err := r.validateDeclarativeSourceVolumes(context.Background(), session); err != nil {
		t.Fatalf("valid planner snapshot rejected: %v", err)
	}
}

func TestValidateDeclarativeSourceVolumesAcceptsPVCIdentitySnapshot(t *testing.T) {
	for _, operation := range []domain.Operation{
		domain.OperationRename,
		domain.OperationMove,
	} {
		t.Run(string(operation), func(t *testing.T) {
			pvc, pv, session := declarativeVolumeFixture()
			session.Spec = domain.NewSessionSpec(
				operation,
				session.Spec.SessionCommon,
				false,
				domain.SessionWorkflowOptions{},
			)
			volume := &session.Spec.Volumes[0]
			volume.Capacity = ""
			volume.SourceCapacity = ""
			volume.StorageClass = ""
			volume.AccessModes = nil
			volume.VolumeMode = ""

			r := NewWorkflowReconciler(nil, nil).WithKubernetesClient(
				clientfake.NewSimpleClientset(pvc, pv),
			)
			if err := r.validateDeclarativeSourceVolumes(
				context.Background(),
				session,
			); err != nil {
				t.Fatalf("valid %s identity snapshot rejected: %v", operation, err)
			}
		})
	}
}

func TestValidateDeclarativeSourceVolumesUsesSourceNamespaceForClusterWorkflow(t *testing.T) {
	pvc, pv, session := declarativeVolumeFixture()
	session.BackendResource = domain.ControllerKindClusterCopy
	session.Spec.SessionNamespace = "control"
	session.Spec.SourceNamespace = "tenant"
	session.Spec.TemporaryNamespace = "tenant"
	session.Spec.DestinationNamespace = "tenant"

	if err := NewWorkflowReconciler(nil, nil).
		WithKubernetesClient(clientfake.NewSimpleClientset(pvc, pv)).
		validateDeclarativeSourceVolumes(context.Background(), session); err != nil {
		t.Fatalf("valid cluster workflow snapshot rejected: %v", err)
	}
}

func TestValidateDeclarativeSourceVolumesRejectsForgedSnapshot(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *domain.Session)
		want   string
	}{
		{
			name: "pvc uid",
			mutate: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *domain.Session) {
				pvc.UID = types.UID("replacement")
			},
			want: "PVC",
		},
		{
			name: "pv claim ref",
			mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *domain.Session) {
				pv.Spec.ClaimRef.Name = "other"
			},
			want: "claimRef",
		},
		{
			name: "pvc spec",
			mutate: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *domain.Session) {
				pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
			},
			want: "spec changed",
		},
		{
			name: "owner references",
			mutate: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume, _ *domain.Session) {
				pvc.OwnerReferences[0].Name = "different-owner"
			},
			want: "metadata changed",
		},
		{
			name: "capacity",
			mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, _ *domain.Session) {
				pv.Spec.Capacity[corev1.ResourceStorage] = resource.MustParse("2Gi")
			},
			want: "capacity changed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pvc, pv, session := declarativeVolumeFixture()
			test.mutate(pvc, pv, session)
			r := NewWorkflowReconciler(nil, nil).WithKubernetesClient(
				clientfake.NewSimpleClientset(pvc, pv),
			)

			err := r.validateDeclarativeSourceVolumes(context.Background(), session)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateDeclarativeSourceVolumesSkipsAfterExecutionStarts(t *testing.T) {
	_, _, session := declarativeVolumeFixture()

	session.Status.Phase = domain.PhaseReserving
	if err := NewWorkflowReconciler(nil, nil).
		validateDeclarativeSourceVolumes(context.Background(), session); err != nil {
		t.Fatalf("post-planned workflow should not reread source objects: %v", err)
	}
}

func TestValidateDeclarativeSourceVolumesRequiresClient(t *testing.T) {
	_, _, session := declarativeVolumeFixture()

	err := NewWorkflowReconciler(nil, nil).
		validateDeclarativeSourceVolumes(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v, want kubernetes", domain.CategoryOf(err), err)
	}
}

func declarativeVolumeFixture() (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *domain.Session) {
	ownerController := true
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data", Namespace: "tenant", UID: types.UID("pvc-uid"),
			Labels:      map[string]string{"app": "database"},
			Annotations: map[string]string{"owner": "team-a"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "db",
					UID:        types.UID("db-uid"),
					Controller: &ownerController,
				},
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  new(corev1.PersistentVolumeMode),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
			VolumeName: "pv-data",
		},
	}
	*pvc.Spec.VolumeMode = corev1.PersistentVolumeFilesystem

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv-uid")},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID,
			},
		},
	}

	spec := domain.NewSessionSpec(
		domain.OperationCopy,
		domain.SessionCommon{
			SourceNamespace: "tenant", TemporaryNamespace: "tenant",
			DestinationNamespace: "tenant", SessionNamespace: "tenant",
			Volumes: []domain.VolumeSpec{{
				SourcePVC:           kube.PVCReference(pvc),
				SourcePV:            kube.PVReference(pv),
				SourceReclaimPolicy: pv.Spec.PersistentVolumeReclaimPolicy,
				SourcePVCSpec:       *pvc.Spec.DeepCopy(),
				SourcePVCMetadata: domain.PVCMetadata{
					Labels: maps.Clone(
						pvc.Labels,
					),
					Annotations:     kube.PVCAnnotationsForRecreation(pvc.Annotations),
					OwnerReferences: slices.Clone(pvc.OwnerReferences),
				},
				SourceCapacity: "1Gi", Capacity: "1Gi", StorageClass: "fast",
				AccessModes: slices.Clone(pvc.Spec.AccessModes), VolumeMode: *pvc.Spec.VolumeMode,
				DestinationPVC: domain.ObjectReference{Namespace: "tenant", Name: "data-copy"},
			}},
		},
		false,
		domain.SessionWorkflowOptions{},
	)

	return pvc, pv, domain.NewSession("copy-1", spec, metav1.Now().Time)
}
