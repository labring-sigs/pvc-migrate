package cli

import (
	"strings"
	"testing"
	"time"
)

func TestEffectiveTimeout(t *testing.T) {
	tests := []struct {
		name    string
		command string
		changed bool
		set     time.Duration
		want    time.Duration
	}{
		{
			name: "copy defaults to the transfer bound", command: "copy",
			want: dataTransferOperationTimeout,
		},
		{
			name: "copy resume inherits the transfer bound", command: "copy resume",
			want: dataTransferOperationTimeout,
		},
		{
			name: "migrate-pod defaults to the transfer bound", command: "migrate-pod",
			want: dataTransferOperationTimeout,
		},
		{
			name: "rename keeps the metadata default", command: "rename",
			want: 30 * time.Minute,
		},
		{
			name: "move keeps the metadata default", command: "move",
			want: 30 * time.Minute,
		},
		{
			name: "reserve keeps the metadata default", command: "reserve",
			want: 30 * time.Minute,
		},
		{
			name: "explicit flag wins on data operations", command: "copy",
			changed: true, set: 5 * time.Minute, want: 5 * time.Minute,
		},
		{
			name: "explicit zero disables the bound", command: "copy",
			changed: true, set: 0, want: 0,
		},
		{
			name: "explicit flag wins on metadata operations", command: "rename",
			changed: true, set: 1 * time.Hour, want: 1 * time.Hour,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &rootState{global: globals{timeout: 30 * time.Minute}}

			root := NewRoot(Options{})

			cmd, _, err := root.Find(strings.Fields(tc.command))
			if err != nil || cmd == nil {
				t.Fatalf("find command %s: %v", tc.command, err)
			}

			r.currentCommand = cmd

			r.timeoutExplicit = tc.changed
			if tc.changed {
				r.global.timeout = tc.set
			}

			if got := r.effectiveTimeout(); got != tc.want {
				t.Fatalf("timeout = %s, want %s", got, tc.want)
			}
		})
	}
}
