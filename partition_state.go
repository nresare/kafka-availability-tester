package main

import (
	"container/list"
	"github.com/benbjohnson/clock"
	log "github.com/sirupsen/logrus"
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

// PartitionSate observes SendAttempt and Received messages and their sequences
// and uses those messages to determine latency as well as availability
// of the tracked partition. For now, this is assumed to
type PartitionSate struct {
	state        State
	partition    string
	clock        clock.Clock
	sentTimes    *list.List
	eventSink    EventSink
	timeout      time.Duration
	lastLatency  time.Duration
	lastReceived time.Time
}

func NewPartitionState(sink EventSink) PartitionSate {
	return PartitionSate{
		state:     Before,
		clock:     clock.New(),
		sentTimes: list.New(),
		eventSink: sink,
		timeout:   10 * time.Second,
	}
}

func (p *PartitionSate) SendAttempt(seq uint64) {
	p.updateState()
	p.sentTimes.PushBack(seqSentTime{seq: seq, sent: p.clock.Now()})
}

func (p *PartitionSate) Received(seq uint64) {
	// look through all the messages previously SendAttempt, match on sequence, remove and optionally submitLatency latency to sink
	for e := p.sentTimes.Front(); e != nil; e = e.Next() {
		sst := e.Value.(seqSentTime)
		if sst.seq == seq {
			p.lastReceived = p.clock.Now()
			p.lastLatency = p.clock.Now().Sub(sst.sent)
			if p.eventSink != nil {
				p.eventSink.submitLatency(p.lastLatency)
			}
			p.sentTimes.Remove(e)
			break
		}
	}
	p.updateState()
}

type seqSentTime struct {
	seq  uint64
	sent time.Time
}

func (p *PartitionSate) updateState() {
	state := p.calculateState()
	if p.state != state {
		p.eventSink.changeState(state)
		p.state = state
	}
}

func (p *PartitionSate) calculateState() State {
	if p.lastReceived.Add(p.timeout).Before(p.clock.Now()) {
		log.Debugf("The last Received message was more than %s ago, partition is unavailable", p.timeout.String())
		return Unavailable
	}
	if p.lastLatency < p.timeout {
		return Available
	}
	return Unavailable
}
