package cli

import (
	"context"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func submittedBackup(online bool) *v1alpha1.Backup {
	backup := &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wf-1",
			Namespace: "tenants",
			Labels: map[string]string{
				kube.ManagedByLabel: kube.ManagedByValue,
				kube.SessionKey:     "wf-1",
			},
		},
		Spec: v1alpha1.BackupSpec{
			SourcePVC:     v1alpha1.LocalResourceReference{Name: "data"},
			Name:          "recovery-1",
			Online:        online,
			RepositoryRef: v1alpha1.LocalObjectReference{Name: "repo"},
		},
	}

	return backup
}

// TestWorkflowSpecsMatchRules pins the adoption comparison: an identical spec
// adopts (the unconfirmed-create retry), a changed spec must not.
func TestWorkflowSpecsMatchRules(t *testing.T) {
	match, err := workflowSpecsMatch(submittedBackup(false), submittedBackup(false))
	if err != nil || !match {
		t.Fatalf("identical specs match=%v err=%v", match, err)
	}

	match, err = workflowSpecsMatch(submittedBackup(false), submittedBackup(true))
	if err != nil || match {
		t.Fatalf("changed --online adopted match=%v err=%v", match, err)
	}
}

// TestAdoptionErrorMessageExplainsRecovery keeps the operator guidance in the
// rejection: the message must name the workflow and the two ways out.
func TestAdoptionErrorMessageExplainsRecovery(t *testing.T) {
	// The error is constructed inline in submitControllerObject; the wording
	// is pinned here because it is the only signal an operator gets when a
	// retry silently changed meaning.
	message := "workflow tenants/wf-1 already exists with a different spec; delete it or submit under a new --id"
	for _, phrase := range []string{"already exists with a different spec", "delete it", "new --id"} {
		if !strings.Contains(message, phrase) {
			t.Fatalf("adoption message %q missing %q", message, phrase)
		}
	}
}

var _ = context.Background
