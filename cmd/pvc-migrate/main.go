package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/labring-sigs/pvc-migrate/internal/cli"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

var version = "dev"
var toolImageRepository = "ghcr.io/labring-sigs/pvc-migrate"

func main() {
	os.Exit(run())
}

func run() int {
	ctx, signalExitCode, stop := commandSignalContext(context.Background())
	defer stop()
	command := cli.NewRoot(cli.Options{
		Version:             version,
		ToolImageRepository: toolImageRepository,
		In:                  os.Stdin,
		Out:                 os.Stdout,
		ErrOut:              os.Stderr,
	})
	err := command.ExecuteContext(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	return commandExitCode(ctx, signalExitCode, err)
}

func commandExitCode(ctx context.Context, signalExitCode func() int, err error) int {
	if ctx.Err() != nil {
		return signalExitCode()
	}
	if err != nil {
		return domain.ExitCode(err)
	}
	return 0
}

// commandSignalContext uses the first termination signal for graceful
// cancellation, then restores the process defaults so a second signal can
// force termination when an external driver does not stop promptly.
func commandSignalContext(parent context.Context) (context.Context, func() int, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	var exitCode atomic.Int32
	exitCode.Store(130)
	go func() {
		select {
		case received := <-signals:
			if received == syscall.SIGTERM {
				exitCode.Store(143)
			}
			signal.Stop(signals)
			cancel()
		case <-ctx.Done():
			signal.Stop(signals)
		}
	}()
	stop := func() {
		signal.Stop(signals)
		cancel()
	}
	return ctx, func() int { return int(exitCode.Load()) }, stop
}
