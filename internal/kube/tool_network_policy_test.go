package kube

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestToolNetworkPolicyHelmValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "empty keeps auto", value: "", want: nil},
		{name: "auto expands to nothing", value: "auto", want: nil},
		{
			name:  "on forces every component",
			value: "on",
			want: []string{
				"rsync.networkPolicy.enabled=true",
				"sshd.networkPolicy.enabled=true",
				"rclone.networkPolicy.enabled=true",
			},
		},
		{
			name:  "off forces every component",
			value: "off",
			want: []string{
				"rsync.networkPolicy.enabled=false",
				"sshd.networkPolicy.enabled=false",
				"rclone.networkPolicy.enabled=false",
			},
		},
		{name: "invalid expands to nothing", value: "always", want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ToolNetworkPolicyHelmValues(test.value)

			if len(got) != len(test.want) {
				t.Fatalf(
					"ToolNetworkPolicyHelmValues(%q) = %v, want %v",
					test.value,
					got,
					test.want,
				)
			}

			for index := range got {
				if got[index] != test.want[index] {
					t.Fatalf(
						"ToolNetworkPolicyHelmValues(%q)[%d] = %q, want %q",
						test.value, index, got[index], test.want[index],
					)
				}
			}
		})
	}
}

func TestNormalizeToolNetworkPolicy(t *testing.T) {
	if got, err := NormalizeToolNetworkPolicy(""); err != nil || got != ToolNetworkPolicyAuto {
		t.Fatalf("empty = %q, %v; want auto, nil", got, err)
	}

	if got, err := NormalizeToolNetworkPolicy(" off "); err != nil || got != ToolNetworkPolicyOff {
		t.Fatalf("trimmed off = %q, %v; want off, nil", got, err)
	}

	if _, err := NormalizeToolNetworkPolicy("maybe"); err == nil {
		t.Fatal("invalid value accepted")
	} else if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("error category = %v, want validation", domain.CategoryOf(err))
	}
}

func TestResolveToolNetworkPolicy(t *testing.T) {
	tests := []struct {
		name       string
		spec       string
		controller string
		want       string
	}{
		{name: "spec wins", spec: "off", controller: "on", want: "off"},
		{name: "empty spec keeps controller", spec: "", controller: "off", want: "off"},
		{name: "both empty stays empty", spec: "", controller: "", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ResolveToolNetworkPolicy(test.spec, test.controller); got != test.want {
				t.Fatalf(
					"ResolveToolNetworkPolicy(%q, %q) = %q, want %q",
					test.spec, test.controller, got, test.want,
				)
			}
		})
	}
}
