package main

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Consumer struct {
	consumer           *kafka.Consumer
	exitWaitGroup      *sync.WaitGroup
	rebalanceWaitGroup *sync.WaitGroup
	quit               chan struct{}
}

func NewConsumer(conf *kafka.ConfigMap) (*Consumer, error) {
	var exitWaitGroup sync.WaitGroup
	var rebalanceWaitGroup sync.WaitGroup

	kafkaConsumer, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer:           kafkaConsumer,
		exitWaitGroup:      &exitWaitGroup,
		rebalanceWaitGroup: &rebalanceWaitGroup,
		quit:               make(chan struct{}),
	}, nil
}

func (ac *Consumer) callback(_ *kafka.Consumer, event kafka.Event) error {
	log.Debugf("called callback: %v", event)
	switch event.(type) {
	case kafka.AssignedPartitions:
		ac.rebalanceWaitGroup.Done()
	}
	return nil
}

// This method blocks until the consumer group has rebalanced
func (ac *Consumer) subscribeAndConsume(topic string) error {
	ac.rebalanceWaitGroup.Add(1)
	ac.exitWaitGroup.Add(1)
	err := ac.consumer.SubscribeTopics([]string{topic}, ac.callback)
	if err != nil {
		return err
	}
	go ac.consume()
	ac.rebalanceWaitGroup.Wait()
	log.Infof("Successfully joined the consumer group")
	return nil
}

func (ac *Consumer) consume() {
	// Process messages
	for true {
		select {
		case <-ac.quit:
			ac.exitWaitGroup.Done()
			return
		default:
			ev, err := readMessage(ac.consumer, time.Second)
			if err != nil {
				if err.(kafka.Error).Code() != kafka.ErrTimedOut {
					log.Debug("Informal error, apparently: %v", err.(kafka.Error).Code())
				}
				// Errors are informational and automatically handled by the consumer
				continue
			}
			var msg message
			err = json.Unmarshal(ev.Value, &msg)
			if err != nil {
				log.Errorf("Failed to parse message from '%s': %v", string(ev.Value), err)
			}
			log.Infof("Latency! %s", time.Now().Sub(time.UnixMilli(msg.Timestamp)).String())
		}
	}
}

func (ac *Consumer) Close() error {
	close(ac.quit)
	ac.exitWaitGroup.Wait()
	return ac.consumer.Close()
}
