package cli

import (
	"testing"
)

// TestNamespaceFlagSurface pins the namespace-flag contract: commands that
// address one tenant namespace expose only -n/--namespace, while commands
// whose spec genuinely declares namespace roles expose the role flags.
func TestNamespaceFlagSurface(t *testing.T) {
	type expectation struct {
		flag      string
		shorthand string
	}

	for _, test := range []struct {
		name    string
		path    []string
		present []expectation
		absent  []string
	}{
		{
			name:    "cr migrate create",
			path:    []string{"cr", "migrate", "create"},
			present: []expectation{{"namespace", "n"}},
			absent:  []string{"source-namespace", "destination-namespace", "temporary-namespace"},
		},
		{
			name:    "cr copy create",
			path:    []string{"cr", "copy", "create"},
			present: []expectation{{"namespace", "n"}},
			absent:  []string{"source-namespace", "destination-namespace", "temporary-namespace"},
		},
		{
			name:    "cr reserve create",
			path:    []string{"cr", "reserve", "create"},
			present: []expectation{{"namespace", "n"}},
			absent:  []string{"source-namespace", "destination-namespace", "temporary-namespace"},
		},
		{
			name:    "cr migrate-pod create",
			path:    []string{"cr", "migrate-pod", "create"},
			present: []expectation{{"namespace", "n"}},
			absent:  []string{"source-namespace", "destination-namespace", "temporary-namespace"},
		},
		{
			name:    "cr rename create",
			path:    []string{"cr", "rename", "create"},
			present: []expectation{{"namespace", "n"}},
			absent:  []string{"source-namespace", "destination-namespace"},
		},
		{
			name:    "cr backup create",
			path:    []string{"cr", "backup", "create"},
			present: []expectation{{"namespace", "n"}},
			absent:  []string{"source-namespace", "destination-namespace"},
		},
		{
			name:    "cr restore create",
			path:    []string{"cr", "restore", "create"},
			present: []expectation{{"namespace", "n"}},
			absent:  []string{"source-namespace", "destination-namespace"},
		},
		{
			name: "cr move create",
			path: []string{"cr", "move", "create"},
			present: []expectation{
				{"source-namespace", "n"},
				{"destination-namespace", ""},
			},
			absent: []string{"namespace", "temporary-namespace"},
		},
		{
			name: "cr cluster-migrate create",
			path: []string{"cr", "cluster-migrate", "create"},
			present: []expectation{
				{"source-namespace", "n"},
				{"destination-namespace", ""},
				{"temporary-namespace", ""},
			},
			absent: []string{"namespace"},
		},
		{
			name: "cr cluster-copy create",
			path: []string{"cr", "cluster-copy", "create"},
			present: []expectation{
				{"source-namespace", "n"},
				{"destination-namespace", ""},
			},
			absent: []string{"namespace", "temporary-namespace"},
		},
		{
			name: "cr cluster-reserve create",
			path: []string{"cr", "cluster-reserve", "create"},
			present: []expectation{
				{"source-namespace", "n"},
				{"destination-namespace", ""},
			},
			absent: []string{"namespace", "temporary-namespace"},
		},
		{
			name: "session move",
			path: []string{"move"},
			present: []expectation{
				{"source-namespace", "n"},
				{"destination-namespace", ""},
			},
			absent: []string{"namespace", "temporary-namespace"},
		},
		{
			name: "session migrate",
			path: []string{"migrate"},
			present: []expectation{
				{"source-namespace", "n"},
				{"destination-namespace", ""},
				{"temporary-namespace", ""},
			},
			absent: []string{"namespace"},
		},
		{
			name: "session copy",
			path: []string{"copy"},
			present: []expectation{
				{"source-namespace", "n"},
				{"destination-namespace", ""},
			},
			absent: []string{"namespace", "temporary-namespace"},
		},
		{
			name: "session migrate-pod",
			path: []string{"migrate-pod"},
			present: []expectation{
				{"namespace", "n"},
			},
			absent: []string{"source-namespace", "destination-namespace", "temporary-namespace"},
		},
		{
			name: "recovery cleanup-orphan",
			path: []string{"recovery", "cleanup-orphan"},
			present: []expectation{
				{"namespace", "n"},
			},
			absent: []string{"source-namespace", "destination-namespace"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := NewRoot(Options{Version: "test"})
			command := findSubCommandT(t, root, test.path...)

			for _, want := range test.present {
				flag := command.Flags().Lookup(want.flag)
				if flag == nil {
					t.Fatalf("--%s must be defined on %s", want.flag, test.name)
				}

				if flag.Shorthand != want.shorthand {
					t.Fatalf(
						"--%s shorthand = %q, want %q",
						want.flag,
						flag.Shorthand,
						want.shorthand,
					)
				}
			}

			for _, name := range test.absent {
				if command.Flags().Lookup(name) != nil {
					t.Fatalf("--%s must not leak onto %s", name, test.name)
				}
			}
		})
	}
}
