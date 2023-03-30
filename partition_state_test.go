package main

import (
	"github.com/benbjohnson/clock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type testEventSink struct {
	latest time.Duration
	states []State
}

func (ls *testEventSink) submitLatency(latency time.Duration) {
	if ls.latest != 0 {
		log.Fatal("testEventSink should only be used once")
	}
	ls.latest = latency
}

func (ls *testEventSink) changeState(state State) {
	ls.states = append(ls.states, state)
}

// just verify that we are good even though PartitionState.eventSink is not set
func TestLatencyReportingNoSink(t *testing.T) {
	c, state, _ := setup()

	state.SendAttempt(10)
	c.Add(14 * time.Millisecond)
	state.Received(10)
}

func TestLatencyReportingSink(t *testing.T) {
	c, state, eventSink := setup()

	state.SendAttempt(10)
	c.Add(14 * time.Millisecond)
	state.Received(10)

	if eventSink.latest != 14*time.Millisecond {
		t.Errorf("Expected latest to be 14 milliseconds, but was %v", eventSink.latest)
	}
}

// verifying that we can delete messages out of sequence
func TestRemoveNextToLast(t *testing.T) {
	_, state, _ := setup()

	state.SendAttempt(10)
	state.SendAttempt(11)
	state.SendAttempt(12)

	state.Received(11)

	var actual []seqSentTime
	for e := state.sentTimes.Front(); e != nil; e = e.Next() {
		actual = append(actual, e.Value.(seqSentTime))
	}

	assert.True(t, reflect.DeepEqual(actual, []seqSentTime{
		{10, time.UnixMilli(0)},
		{12, time.UnixMilli(0)},
	}))
}

// let's just verify that we can handle duplicate messages
func TestDuplicateMessage(t *testing.T) {
	_, state, _ := setup()
	state.SendAttempt(10)
	state.Received(10)
	state.Received(10)
}

func TestStateAvailable(t *testing.T) {
	c, state, _ := setup()

	state.SendAttempt(10)
	c.Add(1 * time.Second)
	state.Received(10)
	c.Add(1 * time.Second)
	state.SendAttempt(11)

	assert.Equal(t, state.state, Available)
}

// The partition is unavailable if it was too long since we Received anything
func TestStateTooLongAgoUnavailable(t *testing.T) {
	c, state, _ := setup()
	state.SendAttempt(1)
	c.Add(20 * time.Second)
	assert.Equal(t, state.state, Unavailable)
}

func TestStateChange(t *testing.T) {
	c, state, eventSink := setup()
	state.SendAttempt(1)
	c.Add(20 * time.Second)
	assert.Equal(t, state.state, Unavailable)
	c.Add(1 * time.Second)
	state.SendAttempt(2)
	c.Add(10 * time.Millisecond)
	state.Received(2)
	assert.Equal(t, state.state, Available)

	assert.Equal(t, []State{Unavailable, Available}, eventSink.states)
}

func setup() (*clock.Mock, PartitionSate, *testEventSink) {
	eventSink := &testEventSink{}
	partitionState := NewPartitionState(eventSink)
	mockClock := clock.NewMock()
	partitionState.clock = mockClock
	return mockClock, partitionState, eventSink
}
