package app

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestServiceToolImageUsesTrustedControllerImage(t *testing.T) {
	session := &domain.Session{
		Spec: domain.NewSessionSpec(
			domain.OperationCopy,
			domain.SessionCommon{},
			false,
			domain.SessionWorkflowOptions{ToolImage: "registry.example/tenant:old"},
		),
	}

	service := &Service{config: Config{TrustedToolImage: "registry.example/pvc-migrate:current"}}
	if got := service.toolImage(session); got != "registry.example/pvc-migrate:current" {
		t.Fatalf("trusted tool image=%q, want controller image", got)
	}
}

func TestServiceToolImageFallsBackToSessionImage(t *testing.T) {
	session := &domain.Session{
		Spec: domain.NewSessionSpec(
			domain.OperationCopy,
			domain.SessionCommon{},
			false,
			domain.SessionWorkflowOptions{ToolImage: "registry.example/pvc-migrate:session"},
		),
	}

	service := &Service{}
	if got := service.toolImage(session); got != "registry.example/pvc-migrate:session" {
		t.Fatalf("session tool image=%q, want session image", got)
	}
}
