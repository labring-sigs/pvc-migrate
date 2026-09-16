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

func TestValidateCopyTimeout(t *testing.T) {
	build := func(command string, timeout, copyTimeout time.Duration) *rootState {
		root := NewRoot(Options{})

		cmd, _, err := root.Find(strings.Fields(command))
		if err != nil || cmd == nil {
			t.Fatalf("find command %s: %v", command, err)
		}

		// A concrete timeout in the table models an explicitly set flag.
		return &rootState{
			global:          globals{timeout: timeout, copyTimeout: copyTimeout},
			currentCommand:  cmd,
			timeoutExplicit: true,
		}
	}

	tests := []struct {
		name        string
		command     string
		timeout     time.Duration
		copyTimeout time.Duration
		wantErr     bool
	}{
		{
			name: "copy bound inside operation bound is valid", command: "copy",
			timeout: 24 * time.Hour, copyTimeout: 2 * time.Hour,
		},
		{
			name: "copy bound at the operation bound is rejected", command: "copy",
			timeout: 2 * time.Hour, copyTimeout: 2 * time.Hour, wantErr: true,
		},
		{
			name: "copy bound beyond the operation bound is rejected", command: "copy",
			timeout: time.Hour, copyTimeout: 2 * time.Hour, wantErr: true,
		},
		{
			name: "disabled operation bound accepts any copy bound", command: "copy",
			timeout: 0, copyTimeout: 200 * time.Hour,
		},
		{
			name: "disabled copy bound is always valid", command: "copy",
			timeout: time.Hour, copyTimeout: 0,
		},
		{
			name: "metadata commands skip validation", command: "rename",
			timeout: time.Minute, copyTimeout: time.Hour,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := build(tc.command, tc.timeout, tc.copyTimeout)

			err := r.validateCopyTimeout(r.currentCommand)
			if tc.wantErr && err == nil {
				t.Fatal("expected a validation error")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
