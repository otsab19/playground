package main

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	mutex         sync.Mutex
	apiCallCount  int
	apiCallAtomic int32
)

func makeAPICall() {
	_, err := http.Get("https://httpbin.org/delay/2")
	if err != nil {
		fmt.Println("API call failed:", err)
	}
}

func workerMutex(id int, orders chan string, wg *sync.WaitGroup, count *int) {
	defer wg.Done()
	for order := range orders {
		startTime := time.Now()
		fmt.Printf("[%s] Worker %d processing %s\n", time.Now().Format("15:04:05"), id, order)
		makeAPICall()
		mutex.Lock()
		*count++
		mutex.Unlock()
		elapsed := time.Since(startTime)
		fmt.Printf("[%s] Worker %d finished %s (Took %v)\n", time.Now().Format("15:04:05"), id, order, elapsed)
	}
}

func workerAtomic(id int, orders chan string, wg *sync.WaitGroup, count *int32) {
	defer wg.Done()
	for order := range orders {
		startTime := time.Now()
		fmt.Printf("[%s] Worker %d processing %s\n", time.Now().Format("15:04:05"), id, order)
		makeAPICall()
		atomic.AddInt32(count, 1)
		elapsed := time.Since(startTime)
		fmt.Printf("[%s] Worker %d finished %s (Took %v)\n", time.Now().Format("15:04:05"), id, order, elapsed)
	}
}

func benchmark(workerCount int, jobCount int, buffered bool, count *int, atomicCount *int32, useAtomic bool) time.Duration {
	var orders chan string
	if buffered {
		orders = make(chan string, 10)
		fmt.Println("\n--- Running with BUFFERED Channel ---\n")
	} else {
		orders = make(chan string)
		fmt.Println("\n--- Running with UNBUFFERED Channel ---\n")
	}

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		if useAtomic {
			go workerAtomic(i, orders, &wg, atomicCount)
		} else {
			go workerMutex(i, orders, &wg, count)
		}
	}

	for i := 1; i <= jobCount; i++ {
		fmt.Printf("[%s] Placing API Call %d\n", time.Now().Format("15:04:05"), i)
		orders <- fmt.Sprintf("API Call %d", i)
	}

	close(orders)
	wg.Wait()

	totalTime := time.Since(startTime)
	fmt.Printf("[%s] All API calls processed in %v\n", time.Now().Format("15:04:05"), totalTime)
	return totalTime
}

func main() {
	workerCount := 10
	jobCount := 10

	// Mutex Benchmarks
	apiCallCount = 0
	fmt.Println("\n--- Running Mutex + Unbuffered ---")
	mutexUnbufferedTime := benchmark(workerCount, jobCount, false, &apiCallCount, nil, false)

	time.Sleep(3 * time.Second)

	fmt.Println("\n--- Running Mutex + Buffered ---")
	mutexBufferedTime := benchmark(workerCount, jobCount, true, &apiCallCount, nil, false)

	time.Sleep(3 * time.Second)

	// Atomic Benchmarks
	apiCallAtomic = 0
	fmt.Println("\n--- Running Atomic + Unbuffered ---")
	atomicUnbufferedTime := benchmark(workerCount, jobCount, false, nil, &apiCallAtomic, true)

	time.Sleep(3 * time.Second)

	fmt.Println("\n--- Running Atomic + Buffered ---")
	atomicBufferedTime := benchmark(workerCount, jobCount, true, nil, &apiCallAtomic, true)

	fmt.Println("\n--- Benchmark Results ---")
	fmt.Printf("Mutex + Unbuffered Channel Execution Time: %v\n", mutexUnbufferedTime)
	fmt.Printf("Mutex + Buffered Channel Execution Time: %v\n", mutexBufferedTime)
	fmt.Printf("Atomic + Unbuffered Channel Execution Time: %v\n", atomicUnbufferedTime)
	fmt.Printf("Atomic + Buffered Channel Execution Time: %v\n", atomicBufferedTime)
	fmt.Printf("Total API Calls Processed (Mutex): %d\n", apiCallCount)
	fmt.Printf("Total API Calls Processed (Atomic): %d\n", atomic.LoadInt32(&apiCallAtomic))
}
