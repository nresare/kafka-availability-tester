package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type StatSink struct {
	input     chan time.Duration
	stats     chan Statistics
	seen      []time.Duration
	waitGroup *sync.WaitGroup
}

type Statistics struct {
	count        uint32
	average      time.Duration
	percentile90 time.Duration
	percentile99 time.Duration
}

func (s Statistics) String() string {
	return fmt.Sprintf(
		"Statistics{count: %d, avg: %s, p90: %s, p99: %s}", s.count, s.average, s.percentile90, s.percentile99,
	)
}

func (ss *StatSink) Put(datum time.Duration) {
	ss.input <- datum
}

func (ss *StatSink) MakeStats() Statistics {
	ss.stats <- Statistics{}
	return <-ss.stats
}

func (ss *StatSink) Stop() error {
	close(ss.stats)
	ss.waitGroup.Wait()
	return nil
}

func StartNewStatSink() *StatSink {
	var waitGroup sync.WaitGroup
	ss := StatSink{
		input:     make(chan time.Duration),
		stats:     make(chan Statistics),
		waitGroup: &waitGroup,
	}
	ss.waitGroup.Add(1)
	go func() {
		for {
			select {
			case i := <-ss.input:
				ss.seen = append(ss.seen, i)
			case _, ok := <-ss.stats:
				if !ok {
					ss.waitGroup.Done()
					return
				} else {
					ss.stats <- ss.doLogSummary()
				}
			}
		}
	}()
	return &ss
}

func (ss *StatSink) doLogSummary() Statistics {
	count := len(ss.seen)
	sort.Slice(ss.seen, func(i, j int) bool { return ss.seen[i] < ss.seen[j] })
	var sum time.Duration
	for _, datum := range ss.seen {
		sum += datum
	}
	avg := time.Duration(float64(sum) / float64(count))
	stats := Statistics{
		uint32(count),
		avg,
		ss.seen[int(float32(count)*0.9)],
		ss.seen[int(float32(count)*0.99)],
	}
	ss.seen = nil
	return stats
}
