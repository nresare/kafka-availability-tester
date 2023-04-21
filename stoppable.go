package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
)

type Stoppable interface {
	Stop() error
}

type stoppableChannel struct {
	stop chan struct{}
}

func installSignalThatStops(toStop *[]Stoppable, sig os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, sig)
	go func() {
		<-c
		log.Infof("Caught a SIGTERM, stopping")
		for _, closable := range *toStop {
			_ = closable.Stop()
		}
	}()
}

func (sc stoppableChannel) Stop() error {
	close(sc.stop)
	return nil
}

func makeStoppable() stoppableChannel {
	return stoppableChannel{make(chan struct{})}
}

func (sc stoppableChannel) waitUntilStopped() {
	<-sc.stop
}
