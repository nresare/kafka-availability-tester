package main

import (
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type testEventSink struct {
	latest      time.Duration
	latestMutex sync.Mutex
	states      []State
	statesMutex sync.Mutex
}

func (ls *testEventSink) submitLatency(latency time.Duration) {
	defer ls.latestMutex.Unlock()
	ls.latestMutex.Lock()
	ls.latest = latency
}

func (ls *testEventSink) getLatestLatency() time.Duration {
	defer ls.latestMutex.Unlock()
	ls.latestMutex.Lock()
	return ls.latest
}

func (ls *testEventSink) changeState(state State) {
	defer ls.statesMutex.Unlock()
	ls.statesMutex.Lock()
	ls.states = append(ls.states, state)
}

func (ls *testEventSink) assertStateEqual(t *testing.T, expected []State) {
	defer ls.statesMutex.Unlock()
	ls.statesMutex.Lock()
	assert.Equal(t, expected, ls.states)
}

func TestLatencyReportingSink(t *testing.T) {
	c, sink, sw := setup()

	sw.sent(10)
	c.Add(14 * time.Millisecond)
	sw.received(10)

	sw.stop()
	latest := sink.getLatestLatency()
	if latest != 14*time.Millisecond {
		t.Errorf("Expected latest to be 14 milliseconds, but was %v", latest)
	}

}

// verifying that we can delete messages out of sequence
func TestRemoveNextToLast(t *testing.T) {
	_, _, sw := setup()

	sw.sent(10)
	sw.sent(11)
	sw.sent(12)

	sw.received(11)

	sw.stop()
}

// let's just verify that we can handle duplicate messages
func TestDuplicateMessage(t *testing.T) {
	_, _, sw := setup()

	sw.sent(10)

	sw.received(10)
	sw.received(10)

	sw.stop()
}

func TestStateAvailable(t *testing.T) {
	c, sink, sw := setup()

	sw.sent(10)
	c.Add(1 * time.Second)
	sw.received(10)

	sw.stop()
	sink.assertStateEqual(t, []State{Available})
}

// The partition is unavailable if it was too long since we Received anything
func TestStateTooLongAgoUnavailable(t *testing.T) {
	c, sink, sw := setup()

	sw.sent(10)
	c.Add(20 * time.Second)

	sw.stop()
	sink.assertStateEqual(t, []State{Unavailable})
}

func TestStateChange(t *testing.T) {
	c, sink, sw := setup()

	sw.sent(1)
	c.Add(20 * time.Second)
	sw.sent(2)
	c.Add(1 * time.Second)
	sw.received(1)
	sw.received(2)

	sw.stop()
	sink.assertStateEqual(t, []State{Unavailable, Available})
}

func setup() (c *clock.Mock, sink *testEventSink, stateWatcher *StateWatcher) {
	sink = &testEventSink{}
	c = clock.NewMock()
	stateWatcher = newStateWatcherWithClock(sink, c)
	return
}
