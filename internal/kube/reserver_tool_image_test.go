package kube

import (
	"testing"
)

func TestReserverToolImageUsesTrustedImage(t *testing.T) {
	reserver := NewReserver(nil).WithTrustedToolImage("registry.example/pvc-migrate:current")
	if got := reserver.toolImage(
		"registry.example/tenant:old",
	); got != "registry.example/pvc-migrate:current" {
		t.Fatalf("trusted tool image=%q, want controller image", got)
	}
}

func TestReserverToolImageFallsBackToSessionImage(t *testing.T) {
	if got := NewReserver(
		nil,
	).toolImage("registry.example/pvc-migrate:session"); got != "registry.example/pvc-migrate:session" {
		t.Fatalf("session tool image=%q, want session image", got)
	}
}
