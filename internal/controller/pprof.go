package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// pprofLoopbackHost is fixed: profiling exposes goroutine stacks and heap
// contents, so it must stay reachable only from the controller Pod itself.
// The flag surface is a bare port precisely so no binding address can be
// expressed.
const pprofLoopbackHost = "127.0.0.1"

// pprofServer serves the Go profiling endpoints on the loopback interface
// only, as a non-leader manager Runnable: every replica exposes profiling
// locally regardless of which holder owns the leader Lease.
type pprofServer struct {
	port int
}

func newPprofServer(port int) (*pprofServer, error) {
	if port < 1 || port > 65535 {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"pprof",
			fmt.Sprintf("--pprof-port %d is invalid: the port must be between 1 and 65535", port),
		)
	}

	return &pprofServer{port: port}, nil
}

func (s *pprofServer) address() string {
	return net.JoinHostPort(pprofLoopbackHost, strconv.Itoa(s.port))
}

func (s *pprofServer) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	//nolint:noctx // profiling requests carry the manager context via BaseContext.
	listener, err := net.Listen("tcp", s.address())
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pprof",
			"listen on "+s.address(),
			err,
		)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		return server.Shutdown(shutdown)
	}
}

func (*pprofServer) NeedLeaderElection() bool {
	return false
}
