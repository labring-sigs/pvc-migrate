package backup

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestRepositoryBindingRetriesFailedCheckpoint(t *testing.T) {
	progress := v1alpha1.BackupStatus{}
	requested := &v1alpha1.BackupRepositoryBindingStatus{
		Type: v1alpha1.BackupRepositoryTypeS3,
		UID:  "repository", Generation: 1,
		S3: &v1alpha1.S3BackupRepositoryBindingStatus{CredentialsSecretUID: "credentials"},
	}

	failure := errors.New("checkpoint failed")
	if err := PinRepository(context.Background(), requested, &progress.Repository,
		func(context.Context) error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("checkpoint error = %v", err)
	}

	if progress.Repository != nil {
		t.Fatal("failed checkpoint left an apparently durable repository binding")
	}

	written := false
	if err := PinRepository(context.Background(), requested, &progress.Repository,
		func(context.Context) error { written = true; return nil }); err != nil {
		t.Fatal(err)
	}

	if !written || progress.Repository == nil {
		t.Fatal("retry skipped the repository checkpoint")
	}

	requested.S3.CredentialsSecretUID = "replacement"
	if progress.Repository.S3.CredentialsSecretUID != "credentials" {
		t.Fatal("repository binding retained mutable input ownership")
	}
}

func TestRepositoryBindingRejectsIdentityDrift(t *testing.T) {
	requested := &v1alpha1.BackupRepositoryBindingStatus{
		Type: v1alpha1.BackupRepositoryTypeS3,
		UID:  "repository",
		S3: &v1alpha1.S3BackupRepositoryBindingStatus{
			CredentialsSecretUID: "credentials",
		},
		Generation: 1,
	}

	object := plannedBackupObject()
	status := &object.Status
	store := &backupCheckpointStore{object: object.DeepCopy()}

	save := func(ctx context.Context) error { return store.Save(ctx, object) }
	for range 2 {
		if err := PinRepository(
			t.Context(),
			requested,
			&status.Repository,
			save,
		); err != nil {
			t.Fatal(err)
		}
	}

	if store.writes != 1 || !reflect.DeepEqual(store.object.Status.Repository, requested) {
		t.Fatalf(
			"binding was not pinned once: writes=%d binding=%+v",
			store.writes,
			status.Repository,
		)
	}

	for _, field := range []string{"generation", "repository UID", "storage UID"} {
		t.Run(field, func(t *testing.T) {
			changed := requested.DeepCopy()
			switch field {
			case "generation":
				changed.Generation++
			case "repository UID":
				changed.UID = "replacement"
			case "storage UID":
				changed.S3.CredentialsSecretUID = "replacement"
			}

			if err := PinRepository(
				t.Context(),
				changed,
				&status.Repository,
				save,
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("identity drift was accepted: %v", err)
			}

			if store.writes != 1 || !reflect.DeepEqual(status.Repository, requested) {
				t.Fatal("identity drift changed durable binding")
			}
		})
	}
}

func TestRepositoryCheckpointsBoundStatusMessages(t *testing.T) {
	for _, failed := range []bool{false, true} {
		status := v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned}
		message := strings.Repeat("长", domain.MaxWorkflowMessageBytes)

		var saved *v1alpha1.WorkflowStatus

		persist := func(context.Context) error { saved = status.DeepCopy(); return nil }

		var err error
		if failed {
			err = checkpointRepositoryFailure(
				context.Background(),
				&status,
				persist,
				errors.New(message),
			)
		} else {
			err = checkpointRepositoryPhase(
				context.Background(),
				&status,
				persist,
				domain.PhaseWarmCopying,
				message,
			)
		}

		if err != nil {
			t.Fatal(err)
		}

		if saved == nil || saved.Message != domain.BoundWorkflowMessage(message) {
			t.Fatal("repository checkpoint exceeded the workflow message bound")
		}
	}
}

func TestRepositoryCheckpointFailureRestoresCompleteStatus(t *testing.T) {
	for _, failure := range []bool{false, true} {
		status := v1alpha1.WorkflowStatus{
			Phase:   domain.PhaseWarmCopying,
			Message: "copying",
			History: []v1alpha1.WorkflowHistoryEntry{
				{Phase: domain.PhasePlanned, Message: "planned"},
			},
			Conditions: []v1alpha1.WorkflowCondition{{Type: "Ready", Message: "pending"}},
		}
		previous := status.DeepCopy()
		writeErr := errors.New("checkpoint unavailable")
		persist := func(context.Context) error { return writeErr }

		var err error
		if failure {
			err = checkpointRepositoryFailure(
				t.Context(),
				&status,
				persist,
				errors.New("copy failed"),
			)
		} else {
			err = checkpointRepositoryPhase(
				t.Context(),
				&status,
				persist,
				domain.PhaseWarmCopied,
				"copied",
			)
		}

		if !errors.Is(err, writeErr) || !reflect.DeepEqual(status, *previous) {
			t.Fatalf("failed write changed status: status=%+v err=%v", status, err)
		}
	}
}
