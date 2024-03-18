package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/dolthub/swiss"
	"github.com/zeebo/xxh3"
	"golang.org/x/exp/mmap"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var executionprofile = flag.String("execprofile", "", "write tarce execution to `file`")
var input = flag.String("input", "", "path to the input file to evaluate")

func main() {

	flag.Parse()

	if *executionprofile != "" {
		f, err := os.Create("./profiles/" + *executionprofile)
		if err != nil {
			log.Fatal("could not create trace execution profile: ", err)
		}
		defer f.Close()
		trace.Start(f)
		defer trace.Stop()
	}

	if *cpuprofile != "" {
		f, err := os.Create("./profiles/" + *cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	evaluate(*input)

	if *memprofile != "" {
		f, err := os.Create("./profiles/" + *memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

const (
	maxcity   = 10000
	chunkSize = 64 * 1024 * 256
)

type mapkey uint64

type result struct {
	city          string
	min, max, avg float64
}

func evaluate(input string) {
	readerAt, err := mmap.Open(input)
	if err != nil {
		log.Fatalf("Failed to open file with mmap: %v", err)
	}
	defer readerAt.Close()
	fileSize := readerAt.Len()
	var cursor int64

	mapOfTemp := swiss.NewMap[mapkey, *cityTemperatureInfo](maxcity)
	resultStream := make(chan *swiss.Map[mapkey, *cityTemperatureInfo], runtime.NumCPU())

	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			buffer := make([]byte, chunkSize)
			toSend := swiss.NewMap[mapkey, *cityTemperatureInfo](maxcity)

			for {
				start := atomic.AddInt64(&cursor, int64(chunkSize)) - int64(chunkSize)
				// fmt.Printf("Worker %d processing segment starting at %d\n", workerID, start)
				if int(start) >= fileSize {
					resultStream <- toSend
					fmt.Println("Worker", workerID, "finished")
					return
				}

				end := min(chunkSize, int64(fileSize)-start)
				buffer = buffer[:end]
				_, err := readerAt.ReadAt(buffer, start)
				if err != nil && !errors.Is(err, io.EOF) {
					log.Printf("Failed to read segment: %v", err)
					return
				}

				if end == chunkSize {
					pos, total := nextNewLine(readerAt, start+end)
					if pos != -1 {
						end += pos + 1
						buffer = append(buffer, total...)
					}
				}

				if start != 0 {
					pos, _ := nextNewLine(readerAt, start)
					start = pos + 1
				}
				segment := buffer[start:end]
				parseSegment(toSend, segment, workerID)
			}
		}(i)
	}

	wg.Wait()
	close(resultStream)

	for t := range resultStream {
		t.Iter(func(hash mapkey, tempInfo *cityTemperatureInfo) bool {
			if val, ok := mapOfTemp.Get(hash); ok {
				val.count += tempInfo.count
				val.sum += tempInfo.sum
				if tempInfo.min < val.min {
					val.min = tempInfo.min
				}
				if tempInfo.max > val.max {
					val.max = tempInfo.max
				}
			} else {
				mapOfTemp.Put(hash, tempInfo)
			}
			return false
		})
	}

	resultArr := make([]result, mapOfTemp.Count())
	var count int
	mapOfTemp.Iter(func(_ mapkey, info *cityTemperatureInfo) bool {
		resultArr[count] = result{
			city: info.key,
			min:  round(float64(info.min) / 10.0),
			max:  round(float64(info.max) / 10.0),
			avg:  round(float64(info.sum) / 10.0 / float64(info.count)),
		}
		count++
		return false
	})

	sort.Slice(resultArr, func(i, j int) bool {
		return resultArr[i].city < resultArr[j].city
	})

	var stringsBuilder strings.Builder
	for _, i := range resultArr {
		stringsBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", i.city, i.min, i.avg, i.max))
	}
}

func nextNewLine(readerAt *mmap.ReaderAt, start int64) (int64, []byte) {
	cursor := start
	const bufferSize = 1 << 6
	fileSize := int64((*readerAt).Len())
	var totalBuffer []byte
	buffer := make([]byte, bufferSize)

	for {
		if cursor >= fileSize {
			return -1, totalBuffer
		}

		readSize := min(bufferSize, fileSize-cursor)

		buffer = buffer[:readSize]
		_, err := (*readerAt).ReadAt(buffer, cursor)
		if err != nil {
			log.Fatal("Reading file failed:", err)
		}
		totalBuffer = append(totalBuffer, buffer...)

		for i, b := range buffer {
			if b == '\n' {
				return cursor + int64(i) - start, totalBuffer
			}
		}

		cursor += readSize
	}
}

func parseSegment(toSend *swiss.Map[mapkey, *cityTemperatureInfo], segment []byte, workerID int) {
	var start int
	var hash mapkey
	var keyStart, keyEnd int

	for index, char := range segment {
		switch char {
		case ';':
			keyStart = start
			keyEnd = index
			hash = mapkey(xxh3.Hash(segment[start:index]))
			start = index + 1
		case '\n':
			if (index-start) > 1 && hash != 0 {
				temp := customStringToIntParser(segment[start:index])
				start = index + 1
				if val, ok := toSend.Get(hash); ok {
					val.count++
					val.sum += temp
					if temp < val.min {
						val.min = temp
					}

					if temp > val.max {
						val.max = temp
					}
				} else {
					toSend.Put(hash, &cityTemperatureInfo{
						key:   string(segment[keyStart:keyEnd]),
						count: 1,
						min:   temp,
						max:   temp,
						sum:   temp,
					})
				}
				hash = 0
			}
		}
	}
}

type cityTemperatureInfo struct {
	key   string
	count int64
	min   int64
	max   int64
	sum   int64
}

func round(x float64) float64 {
	rounded := math.Round(x * 10)
	if rounded == -0.0 {
		return 0.0
	}
	return rounded / 10
}

// input: string containing signed number in the range [-99.9, 99.9]
// output: signed int in the range [-999, 999]
func customStringToIntParser(input []byte) (output int64) {
	var isNegativeNumber bool
	if input[0] == '-' {
		isNegativeNumber = true
		input = input[1:]
	}

	switch len(input) {
	case 3:
		output = int64(input[0])*10 + int64(input[2]) - int64('0')*11
	case 4:
		output = int64(input[0])*100 + int64(input[1])*10 + int64(input[3]) - (int64('0') * 111)
	}

	if isNegativeNumber {
		return -output
	}
	return
}
