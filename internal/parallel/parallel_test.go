package parallel

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestForLimitRunsEachIndexWithinLimit(t *testing.T) {
	const (
		count = 41
		limit = 4
	)
	seen := make([]atomic.Int32, count)
	var active atomic.Int32
	var maximum atomic.Int32
	ForLimit(count, limit, func(index int) {
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		seen[index].Add(1)
		time.Sleep(time.Millisecond)
		active.Add(-1)
	})
	if maximum.Load() > limit {
		t.Fatalf("maximum concurrency = %d, want <= %d", maximum.Load(), limit)
	}
	for index := range seen {
		if seen[index].Load() != 1 {
			t.Fatalf("index %d ran %d times", index, seen[index].Load())
		}
	}
}

func TestForLimitHandlesInvalidInputs(t *testing.T) {
	var calls atomic.Int32
	ForLimit(0, 1, func(int) { calls.Add(1) })
	ForLimit(1, 0, func(int) { calls.Add(1) })
	ForLimit(1, 1, nil)
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}
