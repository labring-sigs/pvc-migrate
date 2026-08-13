package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
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
		writeCommandError(os.Stderr, os.Args[1:], err)
	}
	return commandExitCode(ctx, signalExitCode, err)
}

func writeCommandError(w io.Writer, args []string, err error) {
	if logFormatJSON(args) {
		_ = json.NewEncoder(w).Encode(map[string]string{"level": "ERROR", "msg": "command failed", "error": err.Error()})
		return
	}
	_, _ = fmt.Fprintf(w, "error: %v\n", err)
}

func logFormatJSON(args []string) bool {
	for index := 0; index < len(args); index++ {
		if args[index] == "--log-format" && index+1 < len(args) {
			return args[index+1] == "json"
		}
		if args[index] == "--log-format=json" {
			return true
		}
	}
	return false
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
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	return commandSignalContextFromChannel(parent, signals, func() {
		signal.Stop(signals)
	}, forceCommandTermination)
}

func commandSignalContextFromChannel(
	parent context.Context,
	signals <-chan os.Signal,
	stopNotifications func(),
	forceTermination func(os.Signal),
) (context.Context, func() int, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	var exitCode atomic.Int32
	exitCode.Store(130)
	var stopOnce sync.Once
	stopSignalNotifications := func() {
		stopOnce.Do(stopNotifications)
	}
	stop := func() {
		stopSignalNotifications()
		cancel()
	}
	go func() {
		select {
		case received := <-signals:
			if received == syscall.SIGTERM {
				exitCode.Store(143)
			}
			cancel()
			stopSignalNotifications()
			select {
			case received = <-signals:
				forceTermination(received)
			default:
			}
		case <-ctx.Done():
			stopSignalNotifications()
		}
	}()
	return ctx, func() int { return int(exitCode.Load()) }, stop
}

func forceCommandTermination(received os.Signal) {
	process, err := os.FindProcess(os.Getpid())
	if err == nil {
		if err := process.Signal(received); err == nil {
			return
		}
	}
	if received == syscall.SIGTERM {
		os.Exit(143)
	}
	os.Exit(130)
}
