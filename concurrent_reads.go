package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

var filename = flag.String("file", "benchmark.dat", "name of the file to create for the benchmark")
var filesize = flag.Int("filesize", 4096, "size of the file in Mb") // Default is 4Gb
var concurrency = flag.Int("concurrency", 1, "number of goroutines trying to perform random reads")
var reads = flag.Int("reads", 1000*1000, "number of reads to perform")
var chunk = flag.Int("chunk", 4096, "size of chunk to read, in bytes")

func createFile() {
	fmt.Printf("Creating file %s\n", *filename)
	file, err := os.Create(*filename)
	if err != nil {
		panic(fmt.Sprintf("Could not create the file: %v", err))
	}
	defer file.Close()
	// Fill the file with lots of random data
	fill_iterations := (*filesize)*(1024*1024/4096) // We write 4096 bytes on each iteration
	buf := make([]byte, 4096)
	for i := 0; i < fill_iterations; i++ {
		// Fill 4096 bytes buffer with 8 byte long numbers
		for j := 0; j < 4096; j+=8 {
			n := rand.Uint64()
			binary.LittleEndian.PutUint64(buf[j:j+8], n)
		}
		l, err := file.Write(buf)
		if l != 4096 || err != nil {
			panic(fmt.Sprintf("Could not write 4k block, only wrote %d because %v", l, err))
		}
	}	
}

// Requests are integers specifying the position in the file to read from
func reader(reqCh chan uint64, errCh chan error) {
	bound := uint64((*filesize)*(1024*1024/(*chunk))) // Reads will be chunk-aligned
	buf := make([]byte, *chunk)
	file, err := os.Open(*filename)
	if err != nil {
		errCh <- err
		return
	}
	var dependency uint64
	defer file.Close()
	for req := range reqCh {
		// Mix request with the dependency (to make sure there is no hidden parallelisation)
		offset := int64((req ^ dependency) % bound)
		_, err := file.ReadAt(buf, offset)
		if err != nil {
			errCh <- err
			return
		}
		// Update the depedency for the next read
		dependency = binary.LittleEndian.Uint64(buf[:8])
	}
	// When complete, send nil to error channel
	errCh <- nil
}

func readFile() {
	reqCh := make(chan uint64, *reads)
	errCh := make(chan error, *concurrency)
	defer close(errCh)
	// Launch the readers
	for c := 0; c<(*concurrency); c++ {
		go reader(reqCh, errCh)
	}
	// Issue requests
	start := time.Now()
	for r := 0; r<(*reads); r++ {
		reqCh <- rand.Uint64()
	}
	close(reqCh)
	// Wait for errors or completions
	for c := 0; c<(*concurrency); c++ {
		err := <- errCh
		if err != nil {
			fmt.Printf("Error reported from reader: %v\n", err)
		}
	}
	fmt.Printf("Time taken: %s\n", time.Since(start))
}

func main() {
	flag.Parse()
	if _, err := os.Stat(*filename); os.IsNotExist(err) {
		createFile()
	}
	readFile()
}
