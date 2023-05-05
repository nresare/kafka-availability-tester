package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	ss := StartNewStatSink()
	ss.Put(10 * time.Millisecond)
	ss.Put(15 * time.Millisecond)
	ss.Put(20 * time.Millisecond)
	result := ss.MakeStats()
	assert.Equal(t,
		Statistics{
			3,
			15 * time.Millisecond,
			20 * time.Millisecond,
			20 * time.Millisecond,
			20 * time.Millisecond,
		},
		result,
	)
	log.Infof(result.String())
	assert.Nil(t, ss.Stop())
}
