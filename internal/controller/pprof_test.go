package controller

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestPprofServerRejectsInvalidPorts pins the port contract: profiling is
// disabled through the CLI default of 0 and only ports 1-65535 are accepted,
// so the flag surface alone cannot produce a bad listener.
func TestPprofServerRejectsInvalidPorts(t *testing.T) {
	for _, port := range []int{0, -1, 65536} {
		if _, err := newPprofServer(port); err == nil {
			t.Fatalf("port %d accepted, want rejection", port)
		}
	}

	server, err := newPprofServer(6060)
	if err != nil {
		t.Fatal(err)
	}

	if server.address() != "127.0.0.1:6060" {
		t.Fatalf("address = %q, want the loopback interface", server.address())
	}
}

// TestPprofServerServesOnlyLoopback exercises the runnable end to end on a
// real listener: the profiling index answers on 127.0.0.1, the loopback
// address is the only one bound, and cancellation stops the server cleanly.
func TestPprofServerServesOnlyLoopback(t *testing.T) {
	//nolint:noctx // the probe never serves requests; it only reserves a port.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address, ok := probe.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("probe address %v is not TCP", probe.Addr())
	}

	port := address.Port
	_ = probe.Close()

	server, err := newPprofServer(port)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()

	target := fmt.Sprintf("http://%s/debug/pprof/", server.address())
	client := &http.Client{Timeout: 2 * time.Second}

	var response *http.Response

	for range 50 {
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
		if requestErr != nil {
			cancel()
			t.Fatal(requestErr)
		}

		response, err = client.Do(request)
		if err == nil {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if err != nil {
		cancel()
		t.Fatalf("profiling index unreachable on loopback: %v", err)
	}

	body, _ := io.ReadAll(response.Body)
	response.Body.Close()

	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "goroutine") {
		cancel()
		t.Fatalf("index status=%d body=%q, want the profiling index", response.StatusCode, body)
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("start returned %v, want a clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after cancellation")
	}

	// The listener must be gone; a fresh dial to the loopback address can
	// only fail once the runnable has released the port.
	dialer := &net.Dialer{Timeout: 500 * time.Millisecond}
	if _, err := dialer.DialContext(context.Background(), "tcp", server.address()); err == nil {
		t.Fatal("listener still accepting after shutdown")
	}
}
