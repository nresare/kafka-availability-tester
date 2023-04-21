package main

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type StatSink struct {
	input     chan time.Duration
	log       chan struct{}
	seen      []time.Duration
	waitGroup *sync.WaitGroup
}

func (ss *StatSink) Put(datum time.Duration) {
	ss.input <- datum
}

func (ss *StatSink) LogSummary() {
	ss.log <- struct{}{}
}

func (ss *StatSink) Stop() error {
	close(ss.log)
	ss.waitGroup.Wait()
	return nil
}

func StartNewStatSink() *StatSink {
	var waitGroup sync.WaitGroup
	ss := StatSink{
		input:     make(chan time.Duration),
		log:       make(chan struct{}),
		waitGroup: &waitGroup,
	}
	ss.waitGroup.Add(1)
	go func() {
		for {
			select {
			case i := <-ss.input:
				ss.seen = append(ss.seen, i)
			case _, closed := <-ss.log:
				if closed {
					ss.waitGroup.Done()
					return
				} else {
					ss.doLogSummary()
				}
			}
		}
	}()
	return &ss
}

func (ss *StatSink) doLogSummary() {
	log.Info("Have %d messages", len(ss.seen))
	ss.seen = nil
}
