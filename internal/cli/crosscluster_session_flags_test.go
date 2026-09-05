package cli

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func reservedCrossClusterSession(t *testing.T) *crosscluster.Session {
	t.Helper()

	session := crosscluster.NewSession("reserved", crosscluster.Spec{
		SessionNamespace:     "control",
		SourceNamespace:      "source",
		DestinationNamespace: "destination",
		SourceCluster: kube.ClusterIdentity{
			ID: "source",
		},
		DestinationCluster: kube.ClusterIdentity{ID: "destination"},
		ToolImage:          "example.com/tool:v1",
		Strategies:         []string{"local"},
		Volumes: []crosscluster.VolumeSpec{{
			Source: crosscluster.SourceVolumeSpec{
				PVC: crosscluster.ClusterResourceRef{
					ClusterID: "source",
					Namespace: "source",
					Name:      "data",
					UID:       "source-pvc",
				},
				PV: crosscluster.ClusterResourceRef{
					ClusterID: "source",
					Name:      "source-pv",
					UID:       "source-pv",
				},
				Capacity: "1Gi",
			},
			Destination: crosscluster.DestinationVolumeSpec{
				PVC: crosscluster.ClusterResourceRef{
					ClusterID: "destination",
					Namespace: "destination",
					Name:      "copy",
					UID:       "destination-pvc",
				},
				StorageClass: crosscluster.ClusterResourceRef{
					ClusterID: "destination",
					Name:      "fast",
					UID:       "class",
				},
				Capacity:    "1Gi",
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				VolumeMode:  corev1.PersistentVolumeFilesystem,
			},
			Transfer: crosscluster.TransferSpec{SourcePath: "scoped", DestinationPath: "restored"},
		}},
	}, time.Now())

	session.Status.Phase = crosscluster.PhaseReserved
	if err := session.Validate(); err != nil {
		t.Fatal(err)
	}

	return session
}

func configureCrossClusterCopyForTest(
	t *testing.T,
	session *crosscluster.Session,
	args ...string,
) error {
	t.Helper()

	flags := &crossClusterCopyFlags{}
	command := &cobra.Command{}
	flags.bind(command, &rootState{})

	if err := command.ParseFlags(args); err != nil {
		t.Fatal(err)
	}

	return configureExistingCrossClusterCopy(command, session, flags)
}

func TestCrossClusterReservationHandoffHonorsExplicitCopySettings(t *testing.T) {
	session := reservedCrossClusterSession(t)

	volumes := append([]crosscluster.VolumeSpec(nil), session.Spec.Volumes...)
	if err := configureCrossClusterCopyForTest(
		t,
		session,
		"--verify-checksum",
		"--delete-extraneous",
		"--online",
		"--strategy=nodeport",
		"--tool-image=example.com/tool:v2",
	); err != nil {
		t.Fatal(err)
	}

	if !session.Spec.VerifyChecksum || !session.Spec.DeleteExtraneous || !session.Spec.Online ||
		!reflect.DeepEqual(
			session.Spec.Strategies,
			[]string{"nodeport"},
		) || session.Spec.ToolImage != "example.com/tool:v2" {
		t.Fatalf("copy flags were ignored: %+v", session.Spec)
	}

	if !reflect.DeepEqual(session.Spec.Volumes, volumes) {
		t.Fatal("handoff changed reserved storage or paths")
	}

	before := session.Spec
	if err := configureCrossClusterCopyForTest(t, session); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(session.Spec, before) {
		t.Fatal("omitted flags reset persisted settings")
	}
}

func TestCrossClusterSessionRejectsIgnoredPlanningFlags(t *testing.T) {
	for _, arg := range []string{
		"--source-path=other", "--destination-path=other", "--source-pvc=other", "--destination-pvc=other",
		"--source-namespace=other", "--destination-namespace=other", "--destination-capacity=2Gi",
		"--destination-storage-class=other", "--target-node=other", "--allow-volume-shrink", "--skip-source-usage-check",
	} {
		t.Run(arg, func(t *testing.T) {
			session := reservedCrossClusterSession(t)
			before := session.Spec

			err := configureCrossClusterCopyForTest(t, session, arg, "--verify-checksum")
			if err == nil || !strings.Contains(err.Error(), strings.SplitN(arg, "=", 2)[0]) {
				t.Fatalf("expected explicit planning flag rejection, got %v", err)
			}

			if !reflect.DeepEqual(session.Spec, before) {
				t.Fatal("invalid handoff mutated the session")
			}
		})
	}
}

func TestCrossClusterCopyFreezesSettingsOnceTransferStarts(t *testing.T) {
	for _, phase := range []crosscluster.Phase{
		crosscluster.PhaseTransferring, crosscluster.PhaseCompleted, crosscluster.PhaseFailed,
		crosscluster.PhaseCleaning, crosscluster.PhaseCleaned,
	} {
		t.Run(string(phase), func(t *testing.T) {
			session := reservedCrossClusterSession(t)
			session.Status.Phase = phase

			session.Spec.VerifyChecksum = true
			if phase == crosscluster.PhaseFailed {
				session.Status.Volumes[0].Transfer.Attempts = 1
			}

			if err := configureCrossClusterCopyForTest(
				t,
				session,
				"--verify-checksum",
			); err != nil {
				t.Fatalf("same settings must permit retry: %v", err)
			}

			err := configureCrossClusterCopyForTest(t, session, "--verify-checksum=false")
			if err == nil || !strings.Contains(err.Error(), "cannot change") ||
				!session.Spec.VerifyChecksum {
				t.Fatalf("transfer settings were allowed to change: %v", err)
			}
		})
	}

	session := reservedCrossClusterSession(t)
	session.Status.Phase = crosscluster.PhaseFailed
	now := metav1.Now()

	session.Status.Volumes[0].Transfer.CompletedAt = &now
	if err := configureCrossClusterCopyForTest(t, session, "--verify-checksum"); err == nil {
		t.Fatal("completed volume allowed transfer settings to change")
	}
}

func TestCrossClusterHandoffRejectsInvalidTransferSettings(t *testing.T) {
	for _, arg := range []string{"--strategy=clusterip", "--strategy=", "--tool-image=invalid image"} {
		session := reservedCrossClusterSession(t)

		before := session.Spec
		if err := configureCrossClusterCopyForTest(t, session, arg); err == nil {
			t.Fatalf("invalid settings %q were accepted", arg)
		}

		if !reflect.DeepEqual(session.Spec, before) {
			t.Fatal("invalid settings changed persisted spec")
		}
	}
}
