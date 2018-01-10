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
func reader(reqCh chan int64, errCh chan error) {
	buf := make([]byte, 4096)
	file, err := os.Open(*filename)
	if err != nil {
		errCh <- err
		return
	}
	defer file.Close()
	for req := range reqCh {
		_, err := file.ReadAt(buf, req)
		if err != nil {
			errCh <- err
			return
		}
	}
	// When complete, send nil to error channel
	errCh <- nil
}

func readFile() {
	reqCh := make(chan int64)
	errCh := make(chan error)
	defer close(errCh)
	// Launch the readers
	for c := 0; c<(*concurrency); c++ {
		go reader(reqCh, errCh)
	}
	// Issue requests
	bound := int64((*filesize)*(1024*1024/4096)) // Reads will be 4k aligned
	start := time.Now()
	for r := 0; r<(*reads); r++ {
		reqCh <- rand.Int63n(bound)
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
