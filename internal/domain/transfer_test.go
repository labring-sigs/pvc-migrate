package domain

import (
	"strings"
	"testing"
)

func TestNormalizeTransferPath(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{input: "", want: "."},
		{input: ".", want: "."},
		{input: " data/mysql ", want: "data/mysql"},
		{input: "data//./mysql/", want: "data/mysql"},
		{input: "tenant data/current", want: "tenant data/current"},
	} {
		got, err := NormalizeTransferPath(test.input)
		if err != nil || got != test.want {
			t.Fatalf("NormalizeTransferPath(%q) = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
	for _, input := range []string{"/data", "../data", "data/../other", `data\child`, "data\nchild", strings.Repeat("a", 4097)} {
		if _, err := NormalizeTransferPath(input); err == nil {
			t.Fatalf("NormalizeTransferPath(%q) succeeded", input)
		}
	}
}

func TestTransferScopeUsesNilForFullVolumeAndValidatesCanonicalPaths(t *testing.T) {
	full, err := NewTransferScope("", ".")
	if err != nil || full != nil {
		t.Fatalf("full scope=%#v error=%v", full, err)
	}
	scope, err := NewTransferScope("data//mysql/", "restore/mysql")
	if err != nil {
		t.Fatal(err)
	}
	if scope.SourcePath != "data/mysql" || scope.DestinationPath != "restore/mysql" {
		t.Fatalf("scope=%#v", scope)
	}
	if err := ValidateTransferScope(scope); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTransferScope(&TransferScope{SourcePath: "data/", DestinationPath: "."}); err == nil {
		t.Fatal("non-canonical scope validated")
	}
	if err := ValidateTransferScope(&TransferScope{SourcePath: ".", DestinationPath: "."}); err == nil {
		t.Fatal("explicit full-volume scope validated")
	}
}
