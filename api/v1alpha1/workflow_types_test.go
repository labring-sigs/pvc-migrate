package v1alpha1_test

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOperationSpecsExposeOnlyTheirOwnPayload(t *testing.T) {
	common := domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system",
		DestinationNamespace: "archive", SessionNamespace: "system",
	}
	spec := domain.NewPodMigrationSessionSpec(
		common,
		domain.WorkloadSpec{Adapter: domain.WorkloadStandalone},
		domain.SessionWorkflowOptions{ToolImage: "example/tool:v1"},
		2,
		false,
	)

	object := v1alpha1.PodMigration{Spec: v1alpha1.PodMigrationSpecFromDomain(spec)}

	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}

	encoded := string(data)
	for _, forbidden := range []string{`"migrate":`, `"migratePod":`, `"reserve":`, `"copy":`, `"backup":`, `"restore":`} {
		if containsJSONField(encoded, forbidden) {
			t.Fatalf("PodMigration contains union payload %s: %s", forbidden, encoded)
		}
	}

	if !containsJSONField(encoded, `"workload":`) ||
		!containsJSONField(encoded, `"precopyPasses":`) {
		t.Fatalf("PodMigration omitted operation fields: %s", encoded)
	}

	decoded := object.Spec.Domain("app")
	if decoded.Type != domain.SessionTypeMigratePod || decoded.MigratePod == nil ||
		decoded.MigratePod.PrecopyPasses != 2 {
		t.Fatalf("domain conversion=%#v", decoded)
	}
}

func TestPodMigrationOriginalObjectUsesKubernetesJSON(t *testing.T) {
	original := json.RawMessage(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"writer"}}`)
	spec := domain.NewPodMigrationSessionSpec(
		domain.SessionCommon{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "system",
		},
		domain.WorkloadSpec{Adapter: domain.WorkloadStandalone, OriginalObject: original},
		domain.SessionWorkflowOptions{},
		1,
		false,
	)

	apiSpec := v1alpha1.PodMigrationSpecFromDomain(spec)

	data, err := json.Marshal(apiSpec)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(data), `"originalObject":{"apiVersion":"v1"`) {
		t.Fatalf("originalObject must remain structured JSON, got %s", data)
	}

	var decoded v1alpha1.PodMigrationSpec
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	domainObject := decoded.Domain("app").MigratePod.Workload.OriginalObject
	if !json.Valid(domainObject) || string(domainObject) != string(original) {
		t.Fatalf("domain originalObject=%s, want %s", domainObject, original)
	}
}

func TestWorkflowConversionsDoNotShareMutablePointers(t *testing.T) {
	replicas := int32(3)
	ordinal := int32(1)
	completed := metav1.NewTime(time.Unix(100, 0))
	spec := domain.NewPodMigrationSessionSpec(
		domain.SessionCommon{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "system",
		},
		domain.WorkloadSpec{
			Adapter:          domain.WorkloadStatefulSet,
			OriginalReplicas: &replicas,
			Ordinal:          &ordinal,
			AffectedPods:     []domain.ObjectReference{{Name: "db-1"}},
		},
		domain.SessionWorkflowOptions{}, 1, false,
	)
	apiSpec := v1alpha1.PodMigrationSpecFromDomain(spec)
	*spec.WorkloadPtr().OriginalReplicas = 8

	apiSpec.Workload.AffectedPods[0].Name = "api-mutated"
	if *apiSpec.Workload.OriginalReplicas != 3 || spec.Workload().AffectedPods[0].Name != "db-1" {
		t.Fatalf("domain to API conversion shared pointers: %#v", apiSpec.Workload)
	}

	decoded := apiSpec.Domain("app")
	*decoded.MigratePod.Workload.OriginalReplicas = 9

	decoded.MigratePod.Workload.AffectedPods[0].Name = "domain-mutated"
	if *apiSpec.Workload.OriginalReplicas != 3 ||
		apiSpec.Workload.AffectedPods[0].Name != "api-mutated" {
		t.Fatalf("API to domain conversion shared pointers: %#v", apiSpec.Workload)
	}

	status := domain.SessionStatus{
		Phase:       domain.PhaseCompleted,
		CompletedAt: &completed,
		Volumes: []domain.VolumeStatus{{
			Sync:       domain.SyncState{FinalCompletedAt: &completed},
			Activation: domain.ActivationState{ActivatedAt: &completed, RolledBackAt: &completed},
		}},
	}
	apiStatus := v1alpha1.PodMigrationStatusFromDomain(status, spec)

	completed = metav1.NewTime(time.Unix(200, 0))
	if !apiStatus.CompletedAt.Time.Equal(time.Unix(100, 0)) ||
		!apiStatus.Volumes[0].Sync.FinalCompletedAt.Time.Equal(time.Unix(100, 0)) ||
		!apiStatus.Volumes[0].Activation.ActivatedAt.Time.Equal(time.Unix(100, 0)) {
		t.Fatal("domain to API status conversion shared time pointers")
	}

	decodedStatus := apiStatus.Domain("app")

	decodedStatus.Volumes[0].Sync.FinalCompletedAt = &completed
	if apiStatus.Volumes[0].Sync.FinalCompletedAt.Time.Equal(time.Unix(200, 0)) {
		t.Fatal("API to domain status conversion shared time pointers")
	}
}

func TestWorkflowAPITypesDoNotExposeSessionEnvelope(t *testing.T) {
	for _, spec := range []any{
		v1alpha1.MigrationSpec{},
		v1alpha1.PodMigrationSpec{},
		v1alpha1.ReservationSpec{},
		v1alpha1.CopySpec{},
		v1alpha1.BackupSpec{},
		v1alpha1.RestoreSpec{},
		v1alpha1.RenameSpec{},
		v1alpha1.ClusterMoveSpec{},
	} {
		data, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(data), `"type"`) {
			t.Fatalf("operation type must be represented by Kind, got %s", data)
		}
	}

	for _, test := range []struct {
		name   string
		status any
		field  string
	}{
		{name: "backup", status: v1alpha1.BackupStatus{}, field: `"volumes"`},
		{name: "restore", status: v1alpha1.RestoreStatus{}, field: `"volumes"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.status)
			if err != nil {
				t.Fatal(err)
			}

			if strings.Contains(string(data), test.field) {
				t.Fatalf("%s status exposes unrelated field %s: %s", test.name, test.field, data)
			}
		})
	}
}

func TestNamespacedWorkflowJSONDoesNotExposeNamespaceSelectors(t *testing.T) {
	local := v1alpha1.LocalResourceReference{Name: "data"}
	objects := []any{
		v1alpha1.MigrationSpec{Volumes: []v1alpha1.VolumeSpec{{
			SourcePVC: local, SourcePV: local, DestinationPVC: local,
		}}},
		v1alpha1.PodMigrationSpec{Workload: v1alpha1.WorkloadSpec{
			Adapter: v1alpha1.WorkloadStandalone,
			Pod:     &local,
		}},
		v1alpha1.BackupSpec{SourcePVC: local, SourcePV: local, Name: "daily"},
		v1alpha1.RestoreSpec{DestinationPVC: local, Name: "daily"},
		v1alpha1.RenameSpec{PVCIdentityFields: v1alpha1.PVCIdentityFields{
			SourcePVC: local, SourcePV: local, DestinationPVC: local,
		}},
		v1alpha1.MigrationStatus{Volumes: []v1alpha1.MigrationVolumeStatus{{
			DestinationPVC: &local,
			Activation:     v1alpha1.VolumeActivationStatus{ActivePVC: &local},
		}}},
		v1alpha1.PodMigrationStatus{Workload: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &local,
		}},
		v1alpha1.RenameStatus{Volumes: []v1alpha1.PVCIdentityVolumeStatus{{
			Activation: v1alpha1.PVCIdentityActivationStatus{ActivePVC: &local},
		}}},
	}

	for _, object := range objects {
		encoded, err := json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(encoded), `"namespace"`) {
			t.Fatalf("%T exposes a namespace selector: %s", object, encoded)
		}
	}
}

func TestClusterWorkflowUsesIndependentContracts(t *testing.T) {
	migrationVolumes, ok := reflect.TypeFor[v1alpha1.ClusterMigrationSpec]().
		FieldByName("Volumes")
	if !ok || migrationVolumes.Type.Elem() != reflect.TypeFor[v1alpha1.ClusterVolumeSpec]() {
		t.Fatalf("ClusterMigration volumes type=%v", migrationVolumes.Type)
	}

	statusVolumes, ok := reflect.TypeFor[v1alpha1.ClusterMigrationStatus]().
		FieldByName("Volumes")
	if !ok ||
		statusVolumes.Type.Elem() != reflect.TypeFor[v1alpha1.ClusterMigrationVolumeStatus]() {
		t.Fatalf("ClusterMigration status volumes type=%v", statusVolumes.Type)
	}

	status := v1alpha1.ClusterMoveStatus{Volumes: []v1alpha1.ClusterPVCIdentityVolumeStatus{{
		Activation: v1alpha1.ClusterPVCIdentityActivationStatus{
			ActivePVC: &v1alpha1.ObjectReference{Namespace: "destination", Name: "data"},
		},
	}}}

	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(encoded), `"namespace":"destination"`) {
		t.Fatalf("cluster status lost qualified reference: %s", encoded)
	}
}

func TestOperationSpecsHaveIndependentFieldContracts(t *testing.T) {
	tests := []struct {
		name   string
		spec   any
		fields []string
	}{
		{
			name: "migration", spec: v1alpha1.MigrationSpec{},
			fields: []string{
				"deleteExtraneous", "skipSourceUsageCheck", "sourceNode",
				"strategies", "targetNode", "toolImage", "verifyChecksum", "volumes",
			},
		},
		{
			name: "pod migration", spec: v1alpha1.PodMigrationSpec{},
			fields: []string{
				"deleteExtraneous", "openebsLvmEnableShared", "precopyPasses",
				"skipSourceUsageCheck", "sourceNode", "strategies", "targetNode", "toolImage",
				"verifyChecksum", "volumes", "workload",
			},
		},
		{
			name: "reservation", spec: v1alpha1.ReservationSpec{},
			fields: []string{
				"skipSourceUsageCheck", "targetNode", "toolImage", "volumes",
			},
		},
		{
			name: "copy", spec: v1alpha1.CopySpec{},
			fields: []string{
				"deleteExtraneous", "online", "skipSourceUsageCheck", "sourceNode",
				"strategies", "targetNode", "toolImage", "volumes",
			},
		},
		{
			name: "backup", spec: v1alpha1.BackupSpec{},
			fields: []string{
				"deleteExtraneous",
				"name",
				"online",
				"repositoryRef",
				"openebsLvmEnableShared",
				"path",
				"sourcePV",
				"sourcePVC",
				"toolImage",
			},
		},
		{
			name: "restore", spec: v1alpha1.RestoreSpec{},
			fields: []string{
				"allowMounted",
				"createPVC",
				"deleteExtraneous",
				"destinationAccessMode",
				"destinationCapacity",
				"destinationPVC",
				"destinationStorageClass",
				"name",
				"path",
				"repositoryRef",
				"targetNode",
				"toolImage",
			},
		},
		{
			name: "rename", spec: v1alpha1.RenameSpec{},
			fields: []string{
				"destinationPVC", "sourcePV", "sourcePVC", "sourceTemplate",
			},
		},
		{
			name: "cluster move", spec: v1alpha1.ClusterMoveSpec{},
			fields: []string{
				"destinationNamespace", "identity", "sessionNamespace",
				"sourceNamespace",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := operationJSONFields(reflect.TypeOf(test.spec))
			want := append([]string(nil), test.fields...)
			sort.Strings(want)

			if !reflect.DeepEqual(got, want) {
				t.Fatalf("JSON fields=%v, want %v", got, want)
			}
		})
	}
}

func TestOperationStatusesExposeOnlyConcernedCheckpoints(t *testing.T) {
	activation := v1alpha1.VolumeActivationStatus{
		ActivePVC: &v1alpha1.LocalResourceReference{Name: "active"},
	}
	identity := v1alpha1.PVCIdentityActivationStatus{
		ActivePVC: &v1alpha1.LocalResourceReference{Name: "active"},
	}
	statuses := []struct {
		name      string
		status    any
		forbidden []string
		required  []string
	}{
		{
			name: "migration",
			status: v1alpha1.MigrationStatus{Volumes: []v1alpha1.MigrationVolumeStatus{
				{
					SourcePVCName: "data",
					Sync:          v1alpha1.MigrationSyncStatus{Attempts: 1},
					Activation:    activation,
				},
			}},
			forbidden: []string{
				`"warmPassesCompleted":`, `"warmCompletedAt":`, `"openebsLvmSharedMounts":`,
			},
			required: []string{`"sync":`, `"activation":`},
		},
		{
			name: "pod migration",
			status: v1alpha1.PodMigrationStatus{
				WarmPassesCompleted: 1,
				Workload: &v1alpha1.PodMigrationWorkloadStatus{
					Pod: &v1alpha1.LocalResourceReference{Name: "writer", UID: "current-pod-uid"},
				},
				Volumes: []v1alpha1.PodMigrationVolumeStatus{
					{
						SourcePVCName: "data",
						Sync: v1alpha1.PodMigrationSyncStatus{
							WarmCompletedAt: &metav1.Time{},
						},
						Activation: activation,
					},
				},
			},
			required: []string{
				`"warmPassesCompleted":`, `"workload":`, `"sync":`, `"activation":`,
			},
		},
		{
			name: "reservation",
			status: v1alpha1.ReservationStatus{
				Volumes: []v1alpha1.ReservationVolumeStatus{
					{SourcePVCName: "data", Reserved: true},
				},
			},
			forbidden: []string{`"sync":`, `"activation":`},
			required:  []string{`"reserved":`},
		},
		{
			name: "copy",
			status: v1alpha1.CopyStatus{Volumes: []v1alpha1.CopyVolumeStatus{{
				SourcePVCName: "data", Sync: v1alpha1.CopySyncStatus{Attempts: 2}, Reserved: true,
			}}},
			forbidden: []string{`"activation":`, `"finalCompletedAt":`, `"checksumVerified":`},
			required:  []string{`"sync":`, `"reserved":`},
		},
		{
			name: "backup",
			status: v1alpha1.BackupStatus{OpenEBSLVMSharedMounts: []v1alpha1.SharedMountStatus{{
				SourcePV: v1alpha1.LocalResourceReference{Name: "pv"},
			}}},
			forbidden: []string{`"volumes":`},
			required:  []string{`"openebsLvmSharedMounts":`},
		},
		{
			name:      "restore",
			status:    v1alpha1.RestoreStatus{},
			forbidden: []string{`"volumes":`, `"openebsLvmSharedMounts":`},
		},
		{
			name: "rename",
			status: v1alpha1.RenameStatus{Volumes: []v1alpha1.PVCIdentityVolumeStatus{{
				SourcePVCName: "data", Activation: identity,
			}}},
			forbidden: []string{
				`"reserved":`, `"sync":`, `"temporaryPVCDeleted":`,
				`"sourcePVCDeleted":`, `"destinationReserved":`,
			},
			required: []string{`"activation":`},
		},
		{
			name: "cluster move",
			status: v1alpha1.ClusterMoveStatus{Volumes: []v1alpha1.ClusterPVCIdentityVolumeStatus{{
				SourcePVCName: "data",
				Activation: v1alpha1.ClusterPVCIdentityActivationStatus{
					ActivePVC: &v1alpha1.ObjectReference{Namespace: "destination", Name: "active"},
				},
			}}},
			forbidden: []string{
				`"reserved":`, `"sync":`, `"temporaryPVCDeleted":`,
				`"sourcePVCDeleted":`, `"destinationReserved":`,
			},
			required: []string{`"activation":`},
		},
	}

	for _, test := range statuses {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.status)
			if err != nil {
				t.Fatal(err)
			}

			encoded := string(data)
			for _, field := range test.forbidden {
				if strings.Contains(encoded, field) {
					t.Fatalf("status exposes unrelated field %s: %s", field, encoded)
				}
			}

			for _, field := range test.required {
				if !strings.Contains(encoded, field) {
					t.Fatalf("status omitted required field %s: %s", field, encoded)
				}
			}
		})
	}
}

func TestStatusConversionBoundsLegacyControllerState(t *testing.T) {
	status := domain.SessionStatus{
		Message:    strings.Repeat("x", domain.MaxWorkflowMessageBytes+1),
		Conditions: make([]domain.Condition, domain.MaxWorkflowConditions+3),
		History:    make([]domain.HistoryEntry, domain.MaxWorkflowHistoryEntries+7),
		Volumes: []domain.VolumeStatus{{
			Sync: domain.SyncState{
				LastError: strings.Repeat("e", domain.MaxWorkflowMessageBytes+1),
			},
		}},
	}
	for i := range status.Conditions {
		status.Conditions[i].Type = "Condition"
	}

	converted := v1alpha1.MigrationStatusFromDomain(status, []domain.VolumeSpec{{}})
	if len(converted.Message) != domain.MaxWorkflowMessageBytes ||
		len(converted.Conditions) != domain.MaxWorkflowConditions ||
		len(converted.History) != domain.MaxWorkflowHistoryEntries ||
		len(converted.Volumes[0].Sync.LastError) != domain.MaxWorkflowMessageBytes {
		t.Fatalf(
			"bounded status sizes: message=%d conditions=%d history=%d lastError=%d",
			len(converted.Message), len(converted.Conditions), len(converted.History),
			len(converted.Volumes[0].Sync.LastError),
		)
	}
}

func TestTransferDestinationIdentityIsStatusOwned(t *testing.T) {
	spec := domain.NewSessionSpec(
		domain.OperationCopy,
		domain.SessionCommon{
			SourceNamespace: "app", TemporaryNamespace: "system",
			DestinationNamespace: "app", SessionNamespace: "system",
			Volumes: []domain.VolumeSpec{{
				SourcePVC: domain.ObjectReference{Name: "data", UID: "source-pvc-uid"},
				SourcePV:  domain.ObjectReference{Name: "source-pv", UID: "source-pv-uid"},
				DestinationPVC: domain.ObjectReference{
					Namespace: "system", Name: "data-copy", UID: "destination-pvc-uid",
					ResourceVersion: "17",
				},
				DestinationPV: domain.ObjectReference{
					Name: "destination-pv", UID: "destination-pv-uid", ResourceVersion: "19",
				},
				DestinationPolicy: corev1.PersistentVolumeReclaimRetain,
			}},
		},
		true,
		domain.SessionWorkflowOptions{},
	)
	status := domain.SessionStatus{
		Volumes: []domain.VolumeStatus{{SourcePVCName: "data", Reserved: true}},
	}

	apiSpec := v1alpha1.CopySpecFromDomain(spec)

	data, err := json.Marshal(apiSpec)
	if err != nil {
		t.Fatal(err)
	}

	encoded := string(data)
	for _, forbidden := range []string{
		`"destinationPV":`, `"destinationReclaimPolicy":`,
		`"uid":"destination-pvc-uid"`, `"resourceVersion":"17"`,
	} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("controller checkpoint leaked into Copy spec: %s", encoded)
		}
	}

	apiStatus := v1alpha1.CopyStatusFromDomain(status, spec.Volumes)
	restored := apiSpec.Domain("app")
	apiStatus.ApplyToDomainSpec(&restored)

	volume := restored.Volumes[0]
	if volume.DestinationPVC.UID != "destination-pvc-uid" ||
		volume.DestinationPVC.ResourceVersion != "17" ||
		volume.DestinationPV.Name != "destination-pv" ||
		volume.DestinationPV.UID != "destination-pv-uid" ||
		volume.DestinationPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("destination checkpoint was not restored: %#v", volume)
	}
}

func TestPodMigrationCurrentPodIdentityIsStatusOwned(t *testing.T) {
	spec := domain.NewPodMigrationSessionSpec(
		domain.SessionCommon{
			SourceNamespace: "app", TemporaryNamespace: "system",
			DestinationNamespace: "app", SessionNamespace: "system",
		},
		domain.WorkloadSpec{
			Adapter:      domain.WorkloadStandalone,
			Pod:          domain.ObjectReference{Name: "writer", UID: "original-pod-uid"},
			AffectedPods: []domain.ObjectReference{{Name: "writer", UID: "original-pod-uid"}},
		},
		domain.SessionWorkflowOptions{},
		1,
		false,
	)
	apiSpec := v1alpha1.PodMigrationSpecFromDomain(spec)

	spec.MigratePod.Workload.Pod.UID = "resumed-pod-uid"
	spec.MigratePod.Workload.AffectedPods[0].UID = "resumed-pod-uid"
	apiStatus := v1alpha1.PodMigrationStatusFromDomain(domain.SessionStatus{}, spec)
	restored := apiSpec.Domain("app")
	apiStatus.ApplyToDomainSpec(&restored)

	if apiSpec.Workload.Pod.UID != "original-pod-uid" ||
		apiStatus.Workload == nil || apiStatus.Workload.Pod.UID != "resumed-pod-uid" ||
		restored.MigratePod.Workload.Pod.UID != "resumed-pod-uid" ||
		restored.MigratePod.Workload.AffectedPods[0].UID != "resumed-pod-uid" {
		t.Fatalf(
			"PodMigration workload checkpoint spec=%#v status=%#v restored=%#v",
			apiSpec.Workload,
			apiStatus.Workload,
			restored.MigratePod.Workload,
		)
	}
}

func TestPodMigrationStatusDoesNotClearImmutableAffectedPodsSnapshot(t *testing.T) {
	common := domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system",
		DestinationNamespace: "app", SessionNamespace: "system",
	}
	originalWorkload := domain.WorkloadSpec{
		Adapter: domain.WorkloadStandalone,
		Pod:     domain.ObjectReference{Name: "writer", Namespace: "app", UID: "pod-uid"},
		AffectedPods: []domain.ObjectReference{{
			Name: "writer", Namespace: "app", UID: "pod-uid",
		}},
	}
	spec := domain.NewPodMigrationSessionSpec(
		common,
		originalWorkload,
		domain.SessionWorkflowOptions{},
		1,
		false,
	)
	apiSpec := v1alpha1.PodMigrationSpecFromDomain(spec)
	status := v1alpha1.PodMigrationStatus{Workload: &v1alpha1.PodMigrationWorkloadStatus{
		Pod: &v1alpha1.LocalResourceReference{Name: "writer", UID: "resumed-uid"},
	}}
	restored := apiSpec.Domain("app")
	status.ApplyToDomainSpec(&restored)

	workload := restored.WorkloadPtr()

	if len(workload.AffectedPods) != 1 || workload.AffectedPods[0].UID != "pod-uid" {
		t.Fatalf("partial workload status cleared recovery snapshot: %#v", workload.AffectedPods)
	}

	if workload.Pod.UID != "resumed-uid" {
		t.Fatalf("current Pod identity was not overlaid: %#v", workload.Pod)
	}
}

func TestBackupStatusPreservesOpenEBSRecoveryCheckpoint(t *testing.T) {
	status := domain.SessionStatus{
		OpenEBSLVMSharedMounts: []domain.OpenEBSLVMSharedMount{{
			SourcePV:          domain.ObjectReference{Name: "pv"},
			LVMVolume:         domain.ObjectReference{Name: "lvm"},
			PreviousShared:    "false",
			PreviousSharedSet: true,
		}},
	}

	apiStatus := v1alpha1.BackupStatusFromDomain(status)

	decoded := apiStatus.Domain()
	if len(decoded.OpenEBSLVMSharedMounts) != 1 ||
		decoded.OpenEBSLVMSharedMounts[0].LVMVolume.Name != "lvm" ||
		!decoded.OpenEBSLVMSharedMounts[0].PreviousSharedSet {
		t.Fatalf("OpenEBS checkpoint was lost: %#v", decoded.OpenEBSLVMSharedMounts)
	}
}

func TestBackupRepositoryIdentityStatusRoundTrips(t *testing.T) {
	status := domain.SessionStatus{
		BackupRepository: &domain.BackupRepositoryBindingStatus{
			Type:       domain.BackupRepositoryTypeS3,
			UID:        "repository-uid",
			Generation: 7,
			S3: &domain.S3BackupRepositoryBindingStatus{
				CredentialsSecretUID: "secret-uid",
			},
		},
	}
	backupStatus := v1alpha1.BackupStatusFromDomain(status).Domain()

	restoreStatus := v1alpha1.RestoreStatusFromDomain(status).Domain()
	for name, got := range map[string]domain.SessionStatus{"backup": backupStatus, "restore": restoreStatus} {
		if got.BackupRepository == nil || got.BackupRepository.S3 == nil ||
			got.BackupRepository.Type != status.BackupRepository.Type ||
			got.BackupRepository.UID != status.BackupRepository.UID ||
			got.BackupRepository.Generation != status.BackupRepository.Generation ||
			got.BackupRepository.S3.CredentialsSecretUID !=
				status.BackupRepository.S3.CredentialsSecretUID {
			t.Fatalf("%s backup repository identity status = %#v", name, got)
		}
	}
}

func TestPVCBackupRepositoryIdentityStatusRoundTrips(t *testing.T) {
	status := domain.SessionStatus{
		BackupRepository: &domain.BackupRepositoryBindingStatus{
			Type:       domain.BackupRepositoryTypePVC,
			UID:        "repository-uid",
			Generation: 2,
			PVC:        &domain.PVCBackupRepositoryBindingStatus{ClaimUID: "claim-uid"},
		},
	}

	got := v1alpha1.BackupStatusFromDomain(status).Domain().BackupRepository
	if got == nil || got.PVC == nil || got.S3 != nil || got.PVC.ClaimUID != "claim-uid" {
		t.Fatalf("PVC repository binding status = %#v", got)
	}
}

func TestEveryOperationSpecRoundTripsToItsSessionType(t *testing.T) {
	common := domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system",
		DestinationNamespace: "archive", SessionNamespace: "system",
	}
	cases := []struct {
		name    string
		want    domain.SessionType
		convert func(domain.SessionSpec) domain.SessionSpec
	}{
		{
			name: "migration",
			want: domain.SessionTypeMigrate,
			convert: func(s domain.SessionSpec) domain.SessionSpec {
				return v1alpha1.MigrationSpecFromDomain(s).Domain("app")
			},
		},
		{
			name: "pod migration",
			want: domain.SessionTypeMigratePod,
			convert: func(s domain.SessionSpec) domain.SessionSpec {
				return v1alpha1.PodMigrationSpecFromDomain(s).Domain("app")
			},
		},
		{
			name: "reservation",
			want: domain.SessionTypeReserve,
			convert: func(s domain.SessionSpec) domain.SessionSpec {
				return v1alpha1.ReservationSpecFromDomain(s).Domain("app")
			},
		},
		{
			name: "copy",
			want: domain.SessionTypeCopy,
			convert: func(s domain.SessionSpec) domain.SessionSpec {
				return v1alpha1.CopySpecFromDomain(s).Domain("app")
			},
		},
		{
			name: "backup",
			want: domain.SessionTypeBackup,
			convert: func(s domain.SessionSpec) domain.SessionSpec {
				return v1alpha1.BackupSpecFromDomain(s).Domain("app")
			},
		},
		{
			name: "restore",
			want: domain.SessionTypeRestore,
			convert: func(s domain.SessionSpec) domain.SessionSpec {
				return v1alpha1.RestoreSpecFromDomain(s).Domain("app")
			},
		},
		{
			name: "rename",
			want: domain.SessionTypeRename,
			convert: func(s domain.SessionSpec) domain.SessionSpec {
				return v1alpha1.RenameSpecFromDomain(s).Domain("app")
			},
		},
		{
			name: "move",
			want: domain.SessionTypeMove,
			convert: func(s domain.SessionSpec) domain.SessionSpec {
				return v1alpha1.ClusterMoveSpecFromDomain(s).Domain()
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			spec := domain.NewSessionSpec(
				domain.Operation(test.want),
				common,
				false,
				domain.SessionWorkflowOptions{},
			)
			if test.want == domain.SessionTypeMigratePod {
				spec = domain.NewPodMigrationSessionSpec(
					common,
					domain.WorkloadSpec{},
					domain.SessionWorkflowOptions{},
					1,
					false,
				)
			}

			if got := test.convert(spec); got.Type != test.want {
				t.Fatalf("type=%q, want %q", got.Type, test.want)
			}
		})
	}
}

func TestObjectTransferSpecsUseWorkflowNamespace(t *testing.T) {
	backup := v1alpha1.BackupSpec{
		SourcePVC: v1alpha1.LocalResourceReference{Name: "data"},
		SourcePV:  v1alpha1.LocalResourceReference{Name: "pv", UID: "pv-uid"},
		Name:      "backup",
	}

	backupDomain := backup.Domain("source")

	if got := backupDomain.SourceNamespace; got != "source" {
		t.Fatalf("backup source namespace=%q", got)
	}

	if got := backupDomain.Backup.SourcePVC.Namespace; got != "source" {
		t.Fatalf("backup PVC namespace=%q", got)
	}

	restore := v1alpha1.RestoreSpec{
		DestinationPVC: v1alpha1.LocalResourceReference{Name: "data"},
		Name:           "backup",
	}

	restoreDomain := restore.Domain("destination")

	if got := restoreDomain.DestinationNamespace; got != "destination" {
		t.Fatalf("restore destination namespace=%q", got)
	}

	if got := restoreDomain.Restore.DestinationPVC.Namespace; got != "destination" {
		t.Fatalf("restore PVC namespace=%q", got)
	}
}

func TestObjectTransferSpecsDoNotExposeMigrationNamespaces(t *testing.T) {
	for _, spec := range []any{
		v1alpha1.BackupSpec{},
		v1alpha1.RestoreSpec{},
	} {
		data, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}

		encoded := string(data)
		for _, forbidden := range []string{
			`"sourceNamespace":`, `"temporaryNamespace":`, `"destinationNamespace":`,
			`"sessionNamespace":`, `"volumes":`, `"namespace":`,
		} {
			if containsJSONField(encoded, forbidden) {
				t.Fatalf("object-transfer spec contains %s: %s", forbidden, encoded)
			}
		}
	}
}

func TestPVCIdentitySpecsExposeDedicatedEndpoints(t *testing.T) {
	volume := domain.VolumeSpec{
		SourcePVC: domain.ObjectReference{
			Namespace: "source", Name: "data", UID: "pvc-uid",
		},
		SourcePV: domain.ObjectReference{Name: "pv-data", UID: "pv-uid"},
		DestinationPVC: domain.ObjectReference{
			Namespace: "destination", Name: "data-new",
		},
		SourcePVCSpec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-data",
		},
		SourceReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
	}

	for _, test := range []struct {
		name string
		kind string
		data any
		got  func() domain.SessionSpec
	}{
		{
			name: "rename",
			kind: "Rename",
			data: v1alpha1.RenameSpecFromDomain(domain.SessionSpec{
				SessionCommon: domain.SessionCommon{
					SourceNamespace: "source", DestinationNamespace: "source",
					SessionNamespace: "system", Volumes: []domain.VolumeSpec{volume},
				},
				Type: domain.SessionTypeRename,
			}),
			got: func() domain.SessionSpec {
				return v1alpha1.RenameSpecFromDomain(domain.SessionSpec{
					SessionCommon: domain.SessionCommon{
						SourceNamespace: "source", DestinationNamespace: "source",
						SessionNamespace: "system", Volumes: []domain.VolumeSpec{volume},
					},
					Type: domain.SessionTypeRename,
				}).Domain("source")
			},
		},
		{
			name: "move",
			kind: "Move",
			data: v1alpha1.ClusterMoveSpecFromDomain(domain.SessionSpec{
				SessionCommon: domain.SessionCommon{
					SourceNamespace: "source", DestinationNamespace: "destination",
					SessionNamespace: "system", Volumes: []domain.VolumeSpec{volume},
				},
				Type: domain.SessionTypeMove,
			}),
			got: func() domain.SessionSpec {
				return v1alpha1.ClusterMoveSpecFromDomain(domain.SessionSpec{
					SessionCommon: domain.SessionCommon{
						SourceNamespace: "source", DestinationNamespace: "destination",
						SessionNamespace: "system", Volumes: []domain.VolumeSpec{volume},
					},
					Type: domain.SessionTypeMove,
				}).Domain()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.data)
			if err != nil {
				t.Fatal(err)
			}

			if strings.Contains(string(encoded), `"volumes":`) {
				t.Fatalf("%s exposes internal volumes payload: %s", test.kind, encoded)
			}

			decoded := test.got()
			if decoded.Type != domain.SessionType(test.kind) || len(decoded.Volumes) != 1 {
				t.Fatalf("%s domain conversion=%#v", test.kind, decoded)
			}

			got := decoded.Volumes[0]
			if got.SourcePVC.Name != volume.SourcePVC.Name ||
				got.SourcePVC.UID != volume.SourcePVC.UID ||
				got.SourcePV.UID != volume.SourcePV.UID ||
				got.DestinationPVC.Name != volume.DestinationPVC.Name ||
				got.SourcePVCSpec.VolumeName != volume.SourcePVCSpec.VolumeName ||
				got.SourceReclaimPolicy != volume.SourceReclaimPolicy {
				t.Fatalf("%s volume conversion=%#v", test.kind, got)
			}
		})
	}
}

func TestPVCIdentitySpecsRoundTripSourceMetadataWithoutSharing(t *testing.T) {
	spec := domain.SessionSpec{
		SessionCommon: domain.SessionCommon{
			SourceNamespace:      "source",
			DestinationNamespace: "source",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{{
				SourcePVC:      domain.ObjectReference{Name: "data", UID: "pvc-uid"},
				SourcePV:       domain.ObjectReference{Name: "pv-data", UID: "pv-uid"},
				DestinationPVC: domain.ObjectReference{Name: "data-new"},
				SourcePVCMetadata: domain.PVCMetadata{
					Labels:      map[string]string{"app": "demo"},
					Annotations: map[string]string{"owner": "team-a"},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1", Kind: "Pod", Name: "owner", UID: "owner-uid",
					}},
				},
			}},
		},
		Type: domain.SessionTypeRename,
	}

	apiSpec := v1alpha1.RenameSpecFromDomain(spec)
	apiSpec.SourceTemplate.Metadata.Labels["app"] = "changed"
	apiSpec.SourceTemplate.Metadata.OwnerReferences[0].Name = "changed"

	if got := spec.Volumes[0].SourcePVCMetadata.Labels["app"]; got != "demo" {
		t.Fatalf("source labels were shared with API spec: %q", got)
	}

	decoded := apiSpec.Domain("source")
	metadata := decoded.Volumes[0].SourcePVCMetadata

	if metadata.Labels["app"] != "changed" ||
		metadata.Annotations["owner"] != "team-a" ||
		metadata.OwnerReferences[0].Name != "changed" {
		t.Fatalf("metadata did not round trip: %#v", metadata)
	}
}

func TestClusterMoveRoundTripsNamespaceRoles(t *testing.T) {
	spec := domain.SessionSpec{
		SessionCommon: domain.SessionCommon{
			SourceNamespace: "source", TemporaryNamespace: "destination",
			DestinationNamespace: "destination", SessionNamespace: "control",
			Volumes: []domain.VolumeSpec{{
				SourcePVC:      domain.ObjectReference{Namespace: "source", Name: "data"},
				SourcePV:       domain.ObjectReference{Name: "pv-data"},
				DestinationPVC: domain.ObjectReference{Namespace: "destination", Name: "renamed"},
			}},
		},
		Type: domain.SessionTypeMove,
	}

	apiSpec := v1alpha1.ClusterMoveSpecFromDomain(spec)

	restored := apiSpec.Domain()
	if restored.SourceNamespace != "source" ||
		restored.DestinationNamespace != "destination" ||
		restored.SessionNamespace != "control" ||
		restored.Volumes[0].SourcePVC.Namespace != "source" ||
		restored.Volumes[0].DestinationPVC.Namespace != "destination" {
		t.Fatalf("ClusterMove namespace round trip=%#v", restored)
	}
}

func TestClusterSharedMountStatusPreservesLVMVolumeNamespace(t *testing.T) {
	now := metav1.Now()
	status := domain.SessionStatus{
		Phase:     domain.PhaseWarmCopying,
		StartedAt: now,
		UpdatedAt: now,
		OpenEBSLVMSharedMounts: []domain.OpenEBSLVMSharedMount{{
			SourcePV: domain.ObjectReference{Name: "pv-data", UID: "pv-uid"},
			LVMVolume: domain.ObjectReference{
				APIVersion: "openebs.io/v1beta1",
				Kind:       "LVMVolume",
				Namespace:  "openebs",
				Name:       "lvm-data",
				UID:        "lvm-uid",
			},
			PreviousShared:    "no",
			PreviousSharedSet: true,
		}},
	}

	apiStatus := v1alpha1.ClusterPodMigrationStatusFromDomain(status, domain.SessionSpec{})
	if apiStatus.OpenEBSLVMSharedMounts[0].LVMVolume.Namespace != "openebs" {
		t.Fatalf(
			"cluster status lost LVMVolume namespace: %#v",
			apiStatus.OpenEBSLVMSharedMounts[0],
		)
	}

	decoded := apiStatus.Domain()
	if decoded.OpenEBSLVMSharedMounts[0].LVMVolume.Namespace != "openebs" {
		t.Fatalf(
			"cluster status round trip lost LVMVolume namespace: %#v",
			decoded.OpenEBSLVMSharedMounts[0],
		)
	}
}

func containsJSONField(data, field string) bool {
	return json.Valid([]byte(data)) && strings.Contains(data, field)
}

func TestBackupRepositoryUsesTypedBackendUnion(t *testing.T) {
	repository := v1alpha1.BackupRepositorySpec{
		Type: v1alpha1.BackupRepositoryTypePVC,
		PVC: &v1alpha1.PVCBackupRepositorySpec{
			ClaimRef: v1alpha1.LocalObjectReference{Name: "snapshots"},
			SubPath:  "daily",
		},
	}

	encoded, err := json.Marshal(repository)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(encoded), `"type":"pvc"`) ||
		!strings.Contains(string(encoded), `"claimRef"`) ||
		strings.Contains(string(encoded), `"bucket"`) {
		t.Fatalf("typed repository encoding=%s", encoded)
	}
}

func TestOptionalObjectReferencesAreOmittedWhenUnset(t *testing.T) {
	object := v1alpha1.PodMigration{
		Spec: v1alpha1.PodMigrationSpec{
			Workload: v1alpha1.WorkloadSpec{Adapter: v1alpha1.WorkloadStandalone},
		},
		Status: v1alpha1.PodMigrationStatus{
			Volumes: []v1alpha1.PodMigrationVolumeStatus{{
				Activation: v1alpha1.VolumeActivationStatus{},
			}},
		},
	}

	encoded, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}

	for _, forbidden := range []string{
		`"pod":{}`,
		`"controller":{}`,
		`"destinationPVC":{}`,
		`"destinationPV":{}`,
		`"activePVC":{}`,
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("unset optional reference %s was serialized: %s", forbidden, encoded)
		}
	}
}

func operationJSONFields(typ reflect.Type) []string {
	fields := make([]string, 0, typ.NumField())

	for field := range typ.Fields() {
		tag, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if field.Anonymous && tag == "" {
			fields = append(fields, operationJSONFields(field.Type)...)
			continue
		}

		if tag != "" && tag != "-" {
			fields = append(fields, tag)
		}
	}

	sort.Strings(fields)

	return fields
}
