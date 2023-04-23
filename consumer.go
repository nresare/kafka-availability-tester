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
	watcher            *StateWatcher
}

func NewConsumer(conf *kafka.ConfigMap, watcher *StateWatcher) (*Consumer, error) {
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
		watcher:            watcher,
	}, nil
}

func (c *Consumer) callback(_ *kafka.Consumer, event kafka.Event) error {
	log.Debugf("called callback: %v", event)
	switch event.(type) {
	case kafka.AssignedPartitions:
		c.rebalanceWaitGroup.Done()
	}
	return nil
}

// This method blocks until the consumer group has rebalanced
func (c *Consumer) subscribeAndConsume(topic string) error {
	c.rebalanceWaitGroup.Add(1)
	c.exitWaitGroup.Add(1)
	log.Infof("Subscribing to topic '%s'", topic)
	err := c.consumer.SubscribeTopics([]string{topic}, c.callback)
	if err != nil {
		return err
	}
	go c.consume()
	c.rebalanceWaitGroup.Wait()
	log.Infof("Successfully joined the consumer group")
	return nil
}

func (c *Consumer) consume() {
	// Process messages
	for true {
		select {
		case <-c.quit:
			c.exitWaitGroup.Done()
			return
		default:
			ev, err := c.consumer.ReadMessage(time.Second)
			if err != nil {
				if err.(kafka.Error).Code() != kafka.ErrTimedOut {
					log.Debugf("Informal error, apparently: %v", err.(kafka.Error).Code())
				}
				// Errors are informational and automatically handled by the consumer
				continue
			}
			var msg message
			err = json.Unmarshal(ev.Value, &msg)
			if err != nil {
				log.Errorf("Failed to parse message from '%s': %v", string(ev.Value), err)
			}
			c.watcher.received(msg.Sequence)
		}
	}
}

func (c *Consumer) Stop() error {
	close(c.quit)
	c.exitWaitGroup.Wait()
	return c.consumer.Close()
}
