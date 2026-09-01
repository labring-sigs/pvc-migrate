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

	decoded := object.Spec.Domain()
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

	domainObject := decoded.Domain().MigratePod.Workload.OriginalObject
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

	decoded := apiSpec.Domain()
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

	decodedStatus := apiStatus.Domain()

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
		v1alpha1.MoveSpec{},
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

func TestOperationSpecsHaveIndependentFieldContracts(t *testing.T) {
	tests := []struct {
		name   string
		spec   any
		fields []string
	}{
		{
			name: "migration", spec: v1alpha1.MigrationSpec{},
			fields: []string{
				"createdBy", "deleteExtraneous", "destinationNamespace", "sessionNamespace",
				"skipSourceUsageCheck", "sourceNamespace", "sourceNode", "strategies",
				"targetNode", "temporaryNamespace", "toolImage", "verifyChecksum", "volumes",
			},
		},
		{
			name: "pod migration", spec: v1alpha1.PodMigrationSpec{},
			fields: []string{
				"createdBy", "deleteExtraneous", "destinationNamespace", "openebsLvmEnableShared",
				"precopyPasses", "sessionNamespace", "skipSourceUsageCheck", "sourceNamespace",
				"sourceNode", "strategies", "targetNode", "temporaryNamespace", "toolImage",
				"verifyChecksum", "volumes", "workload",
			},
		},
		{
			name: "reservation", spec: v1alpha1.ReservationSpec{},
			fields: []string{
				"createdBy", "destinationNamespace", "sessionNamespace", "skipSourceUsageCheck",
				"sourceNamespace", "targetNode", "temporaryNamespace", "toolImage", "volumes",
			},
		},
		{
			name: "copy", spec: v1alpha1.CopySpec{},
			fields: []string{
				"createdBy", "deleteExtraneous", "destinationNamespace", "online",
				"sessionNamespace", "skipSourceUsageCheck", "sourceNamespace", "sourceNode",
				"strategies", "targetNode", "temporaryNamespace", "toolImage", "volumes",
			},
		},
		{
			name: "backup", spec: v1alpha1.BackupSpec{},
			fields: []string{
				"allowInsecureEndpoint", "backend", "bucket", "createdBy", "credentialsSecret",
				"deleteExtraneous", "endpoint", "name", "online", "openebsLvmEnableShared",
				"path", "prefix", "provider", "region", "serverSideEncryption", "sessionNamespace",
				"sourceNamespace", "sourcePV", "sourcePVC", "sseKmsKeyID", "toolImage",
			},
		},
		{
			name: "restore", spec: v1alpha1.RestoreSpec{},
			fields: []string{
				"allowInsecureEndpoint", "allowMounted", "backend", "bucket", "createPVC",
				"createdBy", "credentialsSecret", "deleteExtraneous", "destinationAccessMode",
				"destinationCapacity", "destinationNamespace", "destinationPVC",
				"destinationStorageClass", "endpoint", "name", "path", "prefix", "provider",
				"region", "serverSideEncryption", "sessionNamespace", "sseKmsKeyID", "targetNode",
				"toolImage",
			},
		},
		{
			name: "rename", spec: v1alpha1.RenameSpec{},
			fields: []string{
				"createdBy", "destinationPVC", "sessionNamespace", "sourceNamespace", "sourcePV",
				"sourcePVC", "sourceTemplate",
			},
		},
		{
			name: "move", spec: v1alpha1.MoveSpec{},
			fields: []string{
				"createdBy", "destinationNamespace", "destinationPVC", "sessionNamespace",
				"sourceNamespace", "sourcePV", "sourcePVC", "sourceTemplate",
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
		ActivePVC: v1alpha1.ObjectReference{Name: "active"},
	}
	identity := v1alpha1.PVCIdentityActivationStatus{
		ActivePVC: v1alpha1.ObjectReference{Name: "active"},
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
					Pod: v1alpha1.ObjectReference{Name: "writer", UID: "current-pod-uid"},
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
				SourcePV: v1alpha1.ObjectReference{Name: "pv"},
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
			name: "move",
			status: v1alpha1.MoveStatus{Volumes: []v1alpha1.PVCIdentityVolumeStatus{{
				SourcePVCName: "data", Activation: identity,
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
	restored := apiSpec.Domain()
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
	restored := apiSpec.Domain()
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
		Pod: v1alpha1.ObjectReference{Name: "writer", Namespace: "app", UID: "resumed-uid"},
	}}
	restored := apiSpec.Domain()
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
			name:    "migration",
			want:    domain.SessionTypeMigrate,
			convert: func(s domain.SessionSpec) domain.SessionSpec { return v1alpha1.MigrationSpecFromDomain(s).Domain() },
		},
		{
			name:    "pod migration",
			want:    domain.SessionTypeMigratePod,
			convert: func(s domain.SessionSpec) domain.SessionSpec { return v1alpha1.PodMigrationSpecFromDomain(s).Domain() },
		},
		{
			name:    "reservation",
			want:    domain.SessionTypeReserve,
			convert: func(s domain.SessionSpec) domain.SessionSpec { return v1alpha1.ReservationSpecFromDomain(s).Domain() },
		},
		{
			name:    "copy",
			want:    domain.SessionTypeCopy,
			convert: func(s domain.SessionSpec) domain.SessionSpec { return v1alpha1.CopySpecFromDomain(s).Domain() },
		},
		{
			name:    "backup",
			want:    domain.SessionTypeBackup,
			convert: func(s domain.SessionSpec) domain.SessionSpec { return v1alpha1.BackupSpecFromDomain(s).Domain() },
		},
		{
			name:    "restore",
			want:    domain.SessionTypeRestore,
			convert: func(s domain.SessionSpec) domain.SessionSpec { return v1alpha1.RestoreSpecFromDomain(s).Domain() },
		},
		{
			name:    "rename",
			want:    domain.SessionTypeRename,
			convert: func(s domain.SessionSpec) domain.SessionSpec { return v1alpha1.RenameSpecFromDomain(s).Domain() },
		},
		{
			name:    "move",
			want:    domain.SessionTypeMove,
			convert: func(s domain.SessionSpec) domain.SessionSpec { return v1alpha1.MoveSpecFromDomain(s).Domain() },
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

func TestObjectTransferSpecsUseTheirTopLevelNamespace(t *testing.T) {
	backup := v1alpha1.BackupSpec{
		SourceNamespace:  "source",
		SessionNamespace: "sessions",
		SourcePVC:        v1alpha1.ObjectReference{Name: "data"},
		SourcePV:         v1alpha1.ObjectReference{Name: "pv", UID: "pv-uid"},
		Backend:          "s3",
		Bucket:           "bucket",
		Name:             "backup",
	}

	backupDomain := backup.Domain()

	if got := backupDomain.SourceNamespace; got != "source" {
		t.Fatalf("backup source namespace=%q", got)
	}

	if got := backupDomain.Backup.SourcePVC.Namespace; got != "source" {
		t.Fatalf("backup PVC namespace=%q", got)
	}

	restore := v1alpha1.RestoreSpec{
		DestinationNamespace: "destination",
		SessionNamespace:     "sessions",
		DestinationPVC:       v1alpha1.ObjectReference{Name: "data"},
		Backend:              "s3",
		Bucket:               "bucket",
		Name:                 "backup",
	}

	restoreDomain := restore.Domain()

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
		for _, forbidden := range []string{`"temporaryNamespace":`, `"volumes":`} {
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
				}).Domain()
			},
		},
		{
			name: "move",
			kind: "Move",
			data: v1alpha1.MoveSpecFromDomain(domain.SessionSpec{
				SessionCommon: domain.SessionCommon{
					SourceNamespace: "source", DestinationNamespace: "destination",
					SessionNamespace: "system", Volumes: []domain.VolumeSpec{volume},
				},
				Type: domain.SessionTypeMove,
			}),
			got: func() domain.SessionSpec {
				return v1alpha1.MoveSpecFromDomain(domain.SessionSpec{
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

	decoded := apiSpec.Domain()
	metadata := decoded.Volumes[0].SourcePVCMetadata

	if metadata.Labels["app"] != "changed" ||
		metadata.Annotations["owner"] != "team-a" ||
		metadata.OwnerReferences[0].Name != "changed" {
		t.Fatalf("metadata did not round trip: %#v", metadata)
	}
}

func containsJSONField(data, field string) bool {
	return json.Valid([]byte(data)) && strings.Contains(data, field)
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
