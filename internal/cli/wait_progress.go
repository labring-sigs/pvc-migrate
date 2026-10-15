package cli

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

// waitKeepAliveInterval is how long a workflow phase may persist before the
// wait reporter repeats itself. Controller-driven transfers routinely spend
// minutes inside one phase; without a keep-alive the command looks hung.
const waitKeepAliveInterval = 30 * time.Second

// controllerWaitReporter streams a workflow's phase transitions to stderr and
// repeats a keep-alive line while a phase persists, so a long-running wait
// stays visibly alive. Its methods are safe across the watch callback and the
// keep-alive goroutine.
type controllerWaitReporter struct {
	target   io.Writer
	prefix   string
	interval time.Duration

	mu      sync.Mutex
	phase   v1alpha1.WorkflowPhase
	since   time.Time
	stopped bool
}

func newControllerWaitReporter(target io.Writer, kind, name string) *controllerWaitReporter {
	return &controllerWaitReporter{
		target:   target,
		prefix:   fmt.Sprintf("%s %s", kind, name),
		interval: waitKeepAliveInterval,
	}
}

// observe records a phase observation. A phase change prints the transition
// with its status message; a repeated phase stays silent so status polish
// between transitions does not spam the terminal.
func (r *controllerWaitReporter) observe(phase v1alpha1.WorkflowPhase, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || phase == "" || phase == r.phase {
		return
	}

	r.phase = phase
	r.since = time.Now()

	_, _ = fmt.Fprintf(r.target, "%s: %s: %s\n", r.prefix, phase, message)
}

// notef prints a one-off progress line under the reporter's lock so it cannot
// interleave with a transition or keep-alive line.
func (r *controllerWaitReporter) notef(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		return
	}

	_, _ = fmt.Fprintf(r.target, format, args...)
}

// run drives the keep-alive ticker until stop or cancellation.
func (r *controllerWaitReporter) run(ctx context.Context) {
	if r.interval <= 0 {
		return
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			if r.stopped || r.phase == "" {
				r.mu.Unlock()
				continue
			}

			// The ticker runs on a fixed grid while phases start mid-grid, so
			// a tick can land right after a transition; only repeat once the
			// phase itself has persisted for a full interval.
			if elapsed := time.Since(r.since); elapsed < r.interval {
				r.mu.Unlock()
				continue
			}

			_, keepAliveErr := fmt.Fprintf(
				r.target,
				"%s: still %s (%ds in this phase)\n",
				r.prefix,
				r.phase,
				int(time.Since(r.since).Seconds()),
			)
			r.mu.Unlock()

			if keepAliveErr != nil {
				return
			}
		}
	}
}

// stop ends keep-alive output; later observations stay silent.
func (r *controllerWaitReporter) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stopped = true
}
