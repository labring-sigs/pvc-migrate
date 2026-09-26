package app

import (
	"math"
	"testing"
	"time"
)

// TestRetryBackoffDelaySaturates pins the retry backoff contract: exact
// exponential multiples while they fit, and saturation — never a negative
// wrap — once the multiple would overflow time.Duration. The pre-fix
// float64 conversion wrapped 2^63 negative and turned late retries into a
// tight loop.
func TestRetryBackoffDelaySaturates(t *testing.T) {
	base := time.Second

	cases := []struct {
		retryIndex int
		want       time.Duration
	}{
		{retryIndex: 0, want: time.Second},
		{retryIndex: 1, want: 2 * time.Second},
		{retryIndex: 5, want: 32 * time.Second},
		// A one-second base still fits at 2^33 nanoseconds and saturates
		// from 2^34 onward.
		{retryIndex: 33, want: base << 33},
		{retryIndex: 34, want: time.Duration(math.MaxInt64)},
		{retryIndex: 62, want: time.Duration(math.MaxInt64)},
		{retryIndex: 63, want: time.Duration(math.MaxInt64)},
		{retryIndex: 100, want: time.Duration(math.MaxInt64)},
	}

	for _, tc := range cases {
		if got := retryBackoffDelay(base, tc.retryIndex); got != tc.want {
			t.Fatalf("retryBackoffDelay(1s, %d) = %v, want %v", tc.retryIndex, got, tc.want)
		}
	}

	if got := retryBackoffDelay(time.Hour, 100); got != time.Duration(math.MaxInt64) {
		t.Fatalf("hour-scale base must saturate, got %v", got)
	}

	if got := retryBackoffDelay(0, 3); got != 0 {
		t.Fatalf("zero base must stay zero, got %v", got)
	}
}
