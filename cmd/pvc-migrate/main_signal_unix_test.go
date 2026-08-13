//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

const signalHelperMode = "PVC_MIGRATE_SIGNAL_HELPER"

func TestCommandSignalContextProcess(t *testing.T) {
	for _, test := range []struct {
		name     string
		signal   os.Signal
		exitCode int
	}{
		{name: "interrupt", signal: os.Interrupt, exitCode: 130},
		{name: "terminate", signal: syscall.SIGTERM, exitCode: 143},
	} {
		t.Run(test.name, func(t *testing.T) {
			command, lines := startSignalHelper(t, "exit")
			waitForHelperLine(t, lines, "ready")
			if err := command.Process.Signal(test.signal); err != nil {
				t.Fatal(err)
			}
			waitForHelperLine(t, lines, fmt.Sprintf("canceled %d", test.exitCode))
			err := command.Wait()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != test.exitCode {
				t.Fatalf("exit error=%v code=%d", err, exitCodeOf(exitErr))
			}
		})
	}
}

func TestSecondInterruptForcesTermination(t *testing.T) {
	command, lines := startSignalHelper(t, "wait")
	waitForHelperLine(t, lines, "ready")
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	waitForHelperLine(t, lines, "canceled 130")
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	err := command.Wait()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("wait error=%v", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGINT {
		t.Fatalf("wait status=%v", exitErr.Sys())
	}
}

func TestQueuedSecondSignalForcesTermination(t *testing.T) {
	signals := make(chan os.Signal, 2)
	forced := make(chan os.Signal, 1)
	// Fill the buffered channel before starting the consumer so the second
	// signal is unambiguously queued when the first signal is handled.
	signals <- os.Interrupt
	signals <- syscall.SIGTERM
	ctx, exitCode, stop := commandSignalContextFromChannel(context.Background(), signals, func() {}, func(received os.Signal) {
		forced <- received
	})
	t.Cleanup(stop)

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("first signal did not cancel the command context")
	}
	if exitCode() != 130 {
		t.Fatalf("exit code=%d, want 130", exitCode())
	}
	select {
	case received := <-forced:
		if received != syscall.SIGTERM {
			t.Fatalf("forced signal=%v, want %v", received, syscall.SIGTERM)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued second signal did not force termination")
	}
}

func TestSignalHelperProcess(t *testing.T) {
	mode := os.Getenv(signalHelperMode)
	if mode == "" {
		return
	}
	ctx, exitCode, stop := commandSignalContext(context.Background())
	fmt.Println("ready")
	<-ctx.Done()
	fmt.Printf("canceled %d\n", exitCode())
	if mode == "exit" {
		stop()
		os.Exit(exitCode())
	}
	select {}
}

func startSignalHelper(t *testing.T, mode string) (*exec.Cmd, <-chan string) {
	t.Helper()
	// os.Args[0] is the current test binary and the remaining arguments are constants.
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSignalHelperProcess$") //nolint:gosec
	command.Env = append(os.Environ(), signalHelperMode+"="+mode)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = command.Stdout
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_, _ = command.Process.Wait()
		}
	})
	lines := make(chan string, 4)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	return command, lines
}

func waitForHelperLine(t *testing.T, lines <-chan string, expected string) {
	t.Helper()
	select {
	case line, ok := <-lines:
		if !ok || strings.TrimSpace(line) != expected {
			t.Fatalf("helper line=%q open=%t, want %q", line, ok, expected)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for helper line %q", expected)
	}
}

func exitCodeOf(err *exec.ExitError) int {
	if err == nil {
		return 0
	}
	return err.ExitCode()
}
