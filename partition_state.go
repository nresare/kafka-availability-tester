package main

import (
	"container/list"
	"github.com/benbjohnson/clock"
	"time"
)

type State int8

const (
	Before State = iota
	Available
	Unavailable
)

type LatencySink interface {
	submit(latency time.Duration)
}

type seqSentTime struct {
	seq  uint64
	sent time.Time
}

// PartitionSate observes sent and received messages and their sequences
// and uses those messages to determine latency as well as availability
// of the tracked partition. For now, this is assumed to
type PartitionSate struct {
	state       State
	clock       clock.Clock
	sentTimes   *list.List
	latencySink LatencySink
}

func NewPartitionState() PartitionSate {
	return PartitionSate{
		state:     Before,
		clock:     clock.New(),
		sentTimes: list.New(),
	}
}

func (p *PartitionSate) sent(seq uint64) {
	p.sentTimes.PushBack(seqSentTime{seq: seq, sent: p.clock.Now()})
}

func (p *PartitionSate) received(seq uint64) {
	for e := p.sentTimes.Front(); e != nil; e = e.Next() {
		sst := e.Value.(seqSentTime)
		if sst.seq == seq {
			if p.latencySink != nil {
				p.latencySink.submit(p.clock.Now().Sub(sst.sent))
			}
			p.sentTimes.Remove(e)
			return
		}
	}
}

func (p *PartitionSate) getState() {

}
