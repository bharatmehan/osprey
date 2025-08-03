package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bharatmehan/osprey/pkg/client"
)

func main() {
	var (
		address     = flag.String("addr", "localhost:7070", "Server address")
		operation   = flag.String("op", "set", "Operation to benchmark (set|get|mixed)")
		duration    = flag.Duration("duration", 10*time.Second, "Test duration")
		clients     = flag.Int("clients", 10, "Number of concurrent clients")
		keySize     = flag.Int("key-size", 16, "Key size in bytes")
		valueSize   = flag.Int("value-size", 100, "Value size in bytes")
		keyspace    = flag.Int("keyspace", 10000, "Size of key space")
		reportTicks = flag.Duration("report", 1*time.Second, "Reporting interval")
	)
	flag.Parse()

	fmt.Printf("Osprey Benchmark Tool\n")
	fmt.Printf("=====================\n")
	fmt.Printf("Server: %s\n", *address)
	fmt.Printf("Operation: %s\n", *operation)
	fmt.Printf("Duration: %s\n", *duration)
	fmt.Printf("Clients: %d\n", *clients)
	fmt.Printf("Key size: %d bytes\n", *keySize)
	fmt.Printf("Value size: %d bytes\n", *valueSize)
	fmt.Printf("Key space: %d\n", *keyspace)
	fmt.Printf("CPUs: %d\n", runtime.NumCPU())
	fmt.Printf("\n")

	// Test connectivity
	testClient, err := client.New(*address)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	err = testClient.Ping()
	if err != nil {
		log.Fatalf("Server ping failed: %v", err)
	}
	testClient.Close()

	// Generate test data
	keys := generateKeys(*keyspace, *keySize)
	value := generateValue(*valueSize)

	// Pre-populate for GET benchmarks
	if *operation == "get" || *operation == "mixed" {
		fmt.Printf("Pre-populating %d keys...\n", *keyspace)
		populateKeys(*address, keys, value)
		fmt.Printf("Pre-population complete\n\n")
	}

	// Statistics
	var (
		totalOps   int64
		errors     int64
		lastOps    int64
		lastErrors int64
		startTime  = time.Now()
		lastReport = startTime
	)

	// Start reporting
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(*reportTicks)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				now := time.Now()
				currentOps := atomic.LoadInt64(&totalOps)
				currentErrors := atomic.LoadInt64(&errors)

				elapsed := now.Sub(lastReport).Seconds()
				opsPerSec := float64(currentOps-lastOps) / elapsed
				errorsPerSec := float64(currentErrors-lastErrors) / elapsed

				fmt.Printf("Ops: %d (%.0f/sec), Errors: %d (%.2f/sec), Total: %d\n",
					currentOps-lastOps, opsPerSec, currentErrors-lastErrors, errorsPerSec, currentOps)

				lastOps = currentOps
				lastErrors = currentErrors
				lastReport = now

			case <-reportDone:
				return
			}
		}
	}()

	// Start benchmark workers
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runWorker(clientID, *address, *operation, keys, value, stopCh, &totalOps, &errors)
		}(i)
	}

	// Run for specified duration
	time.Sleep(*duration)
	close(stopCh)

	// Wait for workers to finish
	wg.Wait()
	close(reportDone)

	// Final statistics
	finalOps := atomic.LoadInt64(&totalOps)
	finalErrors := atomic.LoadInt64(&errors)
	totalDuration := time.Since(startTime).Seconds()

	fmt.Printf("\nBenchmark Results\n")
	fmt.Printf("=================\n")
	fmt.Printf("Total operations: %d\n", finalOps)
	fmt.Printf("Total errors: %d\n", finalErrors)
	fmt.Printf("Success rate: %.2f%%\n", float64(finalOps-finalErrors)/float64(finalOps)*100)
	fmt.Printf("Duration: %.2f seconds\n", totalDuration)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(finalOps)/totalDuration)
	fmt.Printf("Average latency: %.2f μs/op\n", totalDuration*1000000/float64(finalOps))
}

func runWorker(clientID int, address string, operation string, keys [][]byte, value []byte, stopCh <-chan struct{}, totalOps, errors *int64) {
	c, err := client.New(address)
	if err != nil {
		log.Printf("Client %d: Failed to connect: %v", clientID, err)
		return
	}
	defer c.Close()

	keyIndex := 0
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		// Select operation
		var err error
		switch operation {
		case "set":
			_, err = c.Set(string(keys[keyIndex]), value)
		case "get":
			_, err = c.Get(string(keys[keyIndex]))
		case "mixed":
			if keyIndex%2 == 0 {
				_, err = c.Set(string(keys[keyIndex]), value)
			} else {
				_, err = c.Get(string(keys[keyIndex]))
			}
		default:
			log.Fatalf("Unknown operation: %s", operation)
		}

		if err != nil {
			atomic.AddInt64(errors, 1)
		}

		atomic.AddInt64(totalOps, 1)
		keyIndex = (keyIndex + 1) % len(keys)
	}
}

func populateKeys(address string, keys [][]byte, value []byte) {
	c, err := client.New(address)
	if err != nil {
		log.Fatalf("Failed to connect for population: %v", err)
	}
	defer c.Close()

	for i, key := range keys {
		_, err := c.Set(string(key), value)
		if err != nil {
			log.Printf("Failed to populate key %d: %v", i, err)
		}

		if i%1000 == 0 {
			fmt.Printf("Populated %d/%d keys\r", i, len(keys))
		}
	}
	fmt.Printf("Populated %d/%d keys\n", len(keys), len(keys))
}

func generateKeys(count, size int) [][]byte {
	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%0*d", size-4, i)
		if len(key) > size {
			key = key[:size]
		} else {
			for len(key) < size {
				key += "x"
			}
		}
		keys[i] = []byte(key)
	}
	return keys
}

func generateValue(size int) []byte {
	value := make([]byte, size)
	for i := 0; i < size; i++ {
		value[i] = byte('a' + (i % 26))
	}
	return value
}
