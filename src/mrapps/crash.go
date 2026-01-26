package main

//
// a MapReduce pseudo-application that sometimes crashes,
// and sometimes takes a long time,
// to test MapReduce's ability to recover.
//
// go build -buildmode=plugin crash.go
//

import (
	crand "crypto/rand"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"6.5840/mr"
)

// var ccount int = 4

func maybeCrash() {
	// ccount--
	// if ccount > 0 && ccount%2 == 1 {
	// 	log.Printf("ccount %v delay", ccount)
	// 	maxms := big.NewInt(11 * 1000)
	// 	ms, _ := crand.Int(crand.Reader, maxms)
	// 	time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	// 	// os.Exit(1)
	// }
	// log.Printf("ccount %v not delay", ccount)

	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 330 {
		// crash!
		// log.Println("!!!crash")
		os.Exit(1)
	} else if rr.Int64() < 660 {
		// delay for a while.
		// log.Println("!!!sleep")
		maxms := big.NewInt(10 * 1000)
		ms, _ := crand.Int(crand.Reader, maxms)
		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	} else {
		// log.Println("!!!normal")
	}
}

func Map(filename string, contents string) []mr.KeyValue {
	maybeCrash()

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{"a", filename})
	kva = append(kva, mr.KeyValue{"b", strconv.Itoa(len(filename))})
	kva = append(kva, mr.KeyValue{"c", strconv.Itoa(len(contents))})
	kva = append(kva, mr.KeyValue{"d", "xyzzy"})
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
