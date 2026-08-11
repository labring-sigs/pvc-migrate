package kube

import (
	"strings"
	"testing"
)

func TestBoundedNamePreservesShortNames(t *testing.T) {
	if got := BoundedName("pvc-migrate", "short", "online"); got != "pvc-migrate-short-online" {
		t.Fatalf("name=%q", got)
	}
}

func TestBoundedNameKeepsLongOperationsDistinct(t *testing.T) {
	first := BoundedName("pvc-migrate", strings.Repeat("a", 80), "offline")
	second := BoundedName("pvc-migrate", strings.Repeat("b", 80), "offline")
	if len(first) > maxDNSLabelLength || len(second) > maxDNSLabelLength {
		t.Fatalf("lengths=%d,%d", len(first), len(second))
	}
	if first == second {
		t.Fatalf("long names are ambiguous: %q %q", first, second)
	}
	if !strings.Contains(first, "-offline-") {
		t.Fatalf("action was lost: %q", first)
	}
}

func TestBoundedNameKeepsLongPVCNamesDistinct(t *testing.T) {
	first := toolPodName("session", strings.Repeat("a", 80))
	second := toolPodName("session", strings.Repeat("b", 80))
	if first == second || len(first) > maxDNSLabelLength || len(second) > maxDNSLabelLength {
		t.Fatalf("reservation names=%q,%q", first, second)
	}
}
