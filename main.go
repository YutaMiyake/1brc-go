package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/dolthub/swiss"
	"github.com/zeebo/xxh3"
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
	chunkSize = 64 * 1024 * 512
)

type mapkey uint64

type result struct {
	city          string
	min, max, avg float64
}

func evaluate(input string) string {
	mapOfTemp, err := readFileLineByLineIntoAMap(input)
	if err != nil {
		panic(err)
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
	return stringsBuilder.String()[:stringsBuilder.Len()-2]
}

type cityTemperatureInfo struct {
	key   string
	count int64
	min   int64
	max   int64
	sum   int64
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, chunkSize)
	}}

var mapPool = sync.Pool{
	New: func() interface{} {
		return swiss.NewMap[mapkey, *cityTemperatureInfo](500)
	},
}

func readFileLineByLineIntoAMap(filepath string) (*swiss.Map[mapkey, *cityTemperatureInfo], error) {
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	mapOfTemp := swiss.NewMap[mapkey, *cityTemperatureInfo](maxcity)
	resultStream := make(chan *swiss.Map[mapkey, *cityTemperatureInfo], 10)
	chunkStream := make(chan []byte, 15)

	var wg sync.WaitGroup

	// spawn workers to consume (process) file chunks read
	for i := 0; i < runtime.NumCPU()-1; i++ {
		wg.Add(1)
		go func() {
			for chunk := range chunkStream {
				processReadChunk(chunk, resultStream)
			}
			wg.Done()
		}()
	}

	// spawn a goroutine to read file in chunks and send it to the chunk channel for further processing
	go func() {
		leftover := bufPool.Get().([]byte)[0:0]
		for {
			buf := bufPool.Get().([]byte)[:chunkSize]
			readTotal, err := file.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				panic(err)
			}
			buf = buf[:readTotal]
			lastNewLineIndex := bytes.LastIndex(buf, []byte{'\n'})
			leftover2 := make([]byte, len(buf[lastNewLineIndex+1:]))
			copy(leftover2, buf[lastNewLineIndex+1:])

			buf = append(leftover, buf[:lastNewLineIndex+1]...)
			leftover = leftover2

			chunkStream <- buf

		}
		close(chunkStream)

		// wait for all chunks to be proccessed before closing the result stream
		wg.Wait()
		close(resultStream)
	}()

	// process all city temperatures derived after processing the file chunks
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
		t.Clear()
		mapPool.Put(t)
	}

	return mapOfTemp, nil
}

func processReadChunk(buf []byte, resultStream chan<- *swiss.Map[mapkey, *cityTemperatureInfo]) {
	defer bufPool.Put(buf)

	toSend := mapPool.Get().(*swiss.Map[mapkey, *cityTemperatureInfo])
	var start int
	var hash mapkey
	var keyStart, keyEnd int

	for index, char := range buf {
		switch char {
		case ';':
			keyStart = start
			keyEnd = index
			hash = mapkey(xxh3.Hash(buf[start:index]))
			start = index + 1
		case '\n':
			if (index-start) > 1 && hash != 0 {
				temp := customStringToIntParser(buf[start:index])
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
						key:   string(buf[keyStart:keyEnd]),
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
	resultStream <- toSend
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
