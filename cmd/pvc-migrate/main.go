package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	command := cli.NewRoot(cli.Options{
		Version:             version,
		ToolImageRepository: toolImageRepository,
		In:                  os.Stdin,
		Out:                 os.Stdout,
		ErrOut:              os.Stderr,
	})
	if err := command.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		if ctx.Err() != nil {
			return 130
		}
		return domain.ExitCode(err)
	}
	return 0
}
