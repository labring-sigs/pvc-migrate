package kube

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
)

func TestSynchronizedWriterSerializesConcurrentWrites(t *testing.T) {
	var output bytes.Buffer

	writer := NewSynchronizedWriter(&output)

	const (
		workers         = 32
		writesPerWorker = 8
	)

	var group sync.WaitGroup
	for worker := range workers {
		group.Go(func() {
			for write := range writesPerWorker {
				_, _ = writer.Write([]byte(strconv.Itoa(worker) + ":" + strconv.Itoa(write) + "\n"))
			}
		})
	}

	group.Wait()

	lines := bytes.Count(output.Bytes(), []byte("\n"))
	if lines != workers*writesPerWorker {
		t.Fatalf("write count=%d, want %d", lines, workers*writesPerWorker)
	}
}

func TestSynchronizedWriterUsesDiscardForNilTarget(t *testing.T) {
	writer := NewSynchronizedWriter(nil)

	if written, err := writer.Write([]byte("ignored")); err != nil || written != len("ignored") {
		t.Fatalf("Write()=(%d, %v), want (%d, nil)", written, err, len("ignored"))
	}
}
