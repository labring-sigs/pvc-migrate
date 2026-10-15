package cli

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestControllerWaitReporterPrintsTransitionsAndKeepAlive(t *testing.T) {
	var output bytes.Buffer

	reporter := newControllerWaitReporter(&output, "Copy", "demo")
	reporter.interval = 20 * time.Millisecond

	// A repeated observation of the same phase stays silent.
	reporter.observe(domain.PhaseWarmCopying, "copying reserved volumes")
	reporter.observe(domain.PhaseWarmCopying, "copying reserved volumes")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup

	wg.Go(func() {
		reporter.run(ctx)
	})

	time.Sleep(70 * time.Millisecond)
	reporter.observe(domain.PhaseWarmCopied, "copy completed for all volumes")
	time.Sleep(50 * time.Millisecond)

	cancel()
	wg.Wait()

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected transitions plus keep-alives, got: %q", output.String())
	}

	if lines[0] != "Copy demo: WarmCopying: copying reserved volumes" {
		t.Fatalf("transition line lost its phase/message separation: %q", lines[0])
	}

	keepAlives := 0
	for _, line := range lines {
		if strings.Contains(line, "still WarmCopying (") {
			keepAlives++
		}
	}

	if keepAlives == 0 {
		t.Fatalf("no keep-alive fired during the long phase: %q", output.String())
	}

	last := lines[len(lines)-1]
	if !strings.Contains(last, "WarmCopied: copy completed") &&
		!strings.Contains(last, "still WarmCopied") {
		t.Fatalf("final phase was not reported: %q", last)
	}
}

func TestControllerWaitReporterStopsWithTheWait(t *testing.T) {
	var output bytes.Buffer

	reporter := newControllerWaitReporter(&output, "Backup", "demo")
	reporter.interval = time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup

	wg.Go(func() {
		reporter.run(ctx)
	})

	reporter.observe(domain.PhaseWarmCopying, "preparing")
	reporter.stop()
	cancel()
	wg.Wait()

	output.Reset()
	reporter.observe(domain.PhaseCompleted, "done")
	reporter.notef("late note\n")

	if output.Len() != 0 {
		t.Fatalf("stopped reporter kept writing: %q", output.String())
	}
}
