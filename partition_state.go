package main

import (
	"container/list"
	"github.com/benbjohnson/clock"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type State int8

const (
	Before State = iota
	Available
	Unavailable
)

type EventSink interface {
	submitLatency(latency time.Duration)
	changeState(state State)
}

type StateWatcher struct {
	sentChannel     chan uint64
	receivedChannel chan uint64
	waitGroup       *sync.WaitGroup
}

func (s *StateWatcher) sent(seq uint64) {
	s.sentChannel <- seq
}

func (s *StateWatcher) received(seq uint64) {
	s.receivedChannel <- seq
}

func (s *StateWatcher) stop() {
	close(s.sentChannel)
	s.waitGroup.Wait()
}

func NewStateWatcher(sink EventSink) *StateWatcher {
	return newStateWatcherWithClock(sink, clock.New())
}

func newStateWatcherWithClock(sink EventSink, clock clock.Clock) *StateWatcher {
	sent := make(chan uint64)
	received := make(chan uint64)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)

	stateWatcher := &StateWatcher{sent, received, &waitGroup}
	go statusUpdaterLoop(stateWatcher, sink, clock)
	return stateWatcher
}

// we want to make sure that the Timer waits slightly longer than timeout to make sure that
// we have already reached the timeout threshold when waking up
const AdditionalTimerTime = 10 * time.Millisecond

type partitionState struct {
	state        State
	clock        clock.Clock
	timeout      time.Duration
	lastLatency  time.Duration
	lastReceived time.Time
}

// This method is intended to run in its own goroutine and is responsible for detecting changes
// to state and calling EventSink.changeState() when that happens
func statusUpdaterLoop(stateWatcher *StateWatcher, sink EventSink, clock clock.Clock) {
	state := &partitionState{
		clock:   clock,
		timeout: 10 * time.Second,
	}
	sentTimes := list.New()

	timer := clock.Timer(state.timeout + AdditionalTimerTime)
	for {
		select {
		case <-timer.C:
			updateState(state, sink)
		case seq, more := <-stateWatcher.sentChannel:
			if !more {
				log.Info("Updater loop is being asked to quit, exiting")
				timer.Stop()
				stateWatcher.waitGroup.Done()
				return
			}
			timer.Reset(state.timeout + AdditionalTimerTime)
			sentTimes.PushBack(seqSentTime{seq: seq, sent: state.clock.Now()})
		case seq := <-stateWatcher.receivedChannel:
			sentTime := removeBySeq(sentTimes, seq)
			if sentTime == nil {
				log.Warnf("Received a response without sentChannel time, probably a duplicate")
			} else {
				state.lastReceived = clock.Now()
				state.lastLatency = clock.Now().Sub(*sentTime)
				sink.submitLatency(state.lastLatency)
				updateState(state, sink)
				timer.Stop()
			}
		}
	}
}

// If a seqSentTime with sequence number matching seq, delete the item from list
// and return a pointer to it's sentChannel time. If not found, return nil
func removeBySeq(sentTimes *list.List, seq uint64) *time.Time {
	for e := sentTimes.Front(); e != nil; e = e.Next() {
		sst := e.Value.(seqSentTime)
		if sst.seq == seq {
			sentTimes.Remove(e)
			return &sst.sent
		}
	}
	return nil
}

type seqSentTime struct {
	seq  uint64
	sent time.Time
}

func updateState(state *partitionState, sink EventSink) {
	s := calculateState(state)
	if state.state != s {
		sink.changeState(s)
		state.state = s
	}
}

func calculateState(state *partitionState) State {
	if state.lastReceived.Add(state.timeout).Before(state.clock.Now()) {
		log.Debugf("The last Received message was more than %s ago, partition is unavailable", state.timeout.String())
		return Unavailable
	}
	if state.lastLatency < state.timeout {
		return Available
	}
	return Unavailable
}
