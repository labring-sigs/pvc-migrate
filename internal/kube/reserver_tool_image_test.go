package kube

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestReserverToolImageUsesTrustedImage(t *testing.T) {
	session := &domain.Session{
		Spec: domain.NewSessionSpec(
			domain.OperationReserve,
			domain.SessionCommon{},
			false,
			domain.SessionWorkflowOptions{ToolImage: "registry.example/tenant:old"},
		),
	}

	reserver := NewReserver(nil).WithTrustedToolImage("registry.example/pvc-migrate:current")
	if got := reserver.toolImage(session); got != "registry.example/pvc-migrate:current" {
		t.Fatalf("trusted tool image=%q, want controller image", got)
	}
}

func TestReserverToolImageFallsBackToSessionImage(t *testing.T) {
	session := &domain.Session{
		Spec: domain.NewSessionSpec(
			domain.OperationReserve,
			domain.SessionCommon{},
			false,
			domain.SessionWorkflowOptions{ToolImage: "registry.example/pvc-migrate:session"},
		),
	}

	if got := NewReserver(nil).toolImage(session); got != "registry.example/pvc-migrate:session" {
		t.Fatalf("session tool image=%q, want session image", got)
	}
}
