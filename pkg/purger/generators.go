package purger

import (
	"sync"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// RepeatFn
func RepeatFn(done <-chan interface{}, fn func() interface{}) <-chan interface{} {
	valueStream := make(chan interface{})
	go func() {
		defer close(valueStream)
		for {
			select {
			case <-done:
				return
			case valueStream <- fn():
			}
		}
	}()
	return valueStream
}

// Take Take
func Take(done <-chan interface{}, valueStream <-chan interface{}, num int) <-chan interface{} {
	takeStream := make(chan interface{})
	go func() {
		defer close(takeStream)
		for i := 0; i < num; i++ {
			select {
			case <-done:
				return
			case takeStream <- <-valueStream:
			}
		}
	}()
	return takeStream
}

// FanIn FanIn
func FanIn(done <-chan interface{}, channels ...<-chan *storage.TableBatch) chan *storage.TableBatch {
	var wg sync.WaitGroup
	multiplexedStream := make(chan *storage.TableBatch)

	multiplex := func(c <-chan *storage.TableBatch) {
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

func toString(done <-chan interface{}, valueStream <-chan interface{}) <-chan string {
	stringStream := make(chan string)
	go func() {
		defer close(stringStream)
		for v := range valueStream {
			select {
			case <-done:
				return
			case stringStream <- v.(string):
			}
		}
	}()
	return stringStream
}

// func t() {
// 	done := make(chan interface{})
// 	defer close(done)

// 	start := time.Now()

// 	rand := func() interface{} { return rand.Intn(50000000) }

// 	randIntStream := toInt(done, RepeatFn(done, rand))

// 	numFinders := runtime.NumCPU()
// 	fmt.Printf("Spinning up %d prime finders.\n", numFinders)
// 	finders := make([]<-chan interface{}, numFinders)
// 	fmt.Println("Primes:")
// 	for i := 0; i < numFinders; i++ {
// 		finders[i] = primeFinder(done, randIntStream)
// 	}

// 	for prime := range Take(done, FanIn(done, finders...), 10) {
// 		fmt.Printf("\t%d\n", prime)
// 	}

// 	fmt.Printf("Search took: %v", time.Since(start))
// }
