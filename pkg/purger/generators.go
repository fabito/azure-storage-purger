package purger

import (
	"sync"
)

// FanIn FanIn
func FanIn(done <-chan interface{}, channels ...<-chan *TableBatchResult) chan *TableBatchResult {
	var wg sync.WaitGroup
	multiplexedStream := make(chan *TableBatchResult)

	multiplex := func(c <-chan *TableBatchResult) {
		defer wg.Done()
		for i := range c {
			select {
			case <-done:
				return
			case multiplexedStream <- i:
			}
		}
	}

	// Select from all the channels
	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	// Wait for all the reads to complete
	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()

	return multiplexedStream
}
