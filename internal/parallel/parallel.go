package parallel

import "sync"

const DefaultLimit = 16

// For runs fn once for every index with a bounded number of workers.
func For(count int, fn func(int)) {
	ForLimit(count, DefaultLimit, fn)
}

func ForLimit(count, limit int, fn func(int)) {
	if count <= 0 || fn == nil {
		return
	}
	if limit <= 0 {
		limit = 1
	}
	workers := min(count, limit)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for index := range jobs {
				fn(index)
			}
		})
	}
	for index := range count {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
}
