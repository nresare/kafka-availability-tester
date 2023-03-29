package main

import (
	"github.com/benbjohnson/clock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type testLatencySink struct {
	latest time.Duration
}

func (ls *testLatencySink) submit(latency time.Duration) {
	if ls.latest != 0 {
		log.Fatal("testLatencySink should only be used once")
	}
	ls.latest = latency
}

// just verify that we are good even though PartitionState.latencySink is not set
func TestLatencyReportingNoSink(t *testing.T) {
	partitionSate := NewPartitionState()
	mockClock := clock.NewMock()
	partitionSate.clock = mockClock

	partitionSate.sent(10)
	mockClock.Add(14 * time.Millisecond)
	partitionSate.received(10)
}

func TestLatencyReportingSink(t *testing.T) {
	partitionState := NewPartitionState()
	mockClock := clock.NewMock()
	testLatencySink := testLatencySink{}
	partitionState.latencySink = &testLatencySink
	partitionState.clock = mockClock

	partitionState.sent(10)
	mockClock.Add(14 * time.Millisecond)
	partitionState.received(10)

	if testLatencySink.latest != 14*time.Millisecond {
		t.Errorf("Expected latest to be 14 milliseconds, but was %v", testLatencySink.latest)
	}
}

func TestRemoveNextToLast(t *testing.T) {
	partitionState := NewPartitionState()
	partitionState.clock = clock.NewMock()

	partitionState.sent(10)
	partitionState.sent(11)
	partitionState.sent(12)

	partitionState.received(11)

	var actual []seqSentTime
	for e := partitionState.sentTimes.Front(); e != nil; e = e.Next() {
		actual = append(actual, e.Value.(seqSentTime))
	}

	assert.True(t, reflect.DeepEqual(actual, []seqSentTime{
		{10, time.UnixMilli(0)},
		{12, time.UnixMilli(0)},
	}))
	assert.Equal(t, 2, partitionState.sentTimes.Len())

}
