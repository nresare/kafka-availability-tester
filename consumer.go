package main

import "C"
import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"math"
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
	default:
		log.Infof("Got event from events consumer: %s", event)
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
			ev, err := ReadMessage(time.Second, c.consumer)
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

// ReadMessage is a copy of the upstream version to experiment with
func ReadMessage(timeout time.Duration, c *kafka.Consumer) (*kafka.Message, error) {

	var absTimeout time.Time
	var timeoutMs int

	if timeout > 0 {
		absTimeout = time.Now().Add(timeout)
		timeoutMs = (int)(timeout.Seconds() * 1000.0)
	} else {
		timeoutMs = (int)(timeout)
	}

	for {
		ev := c.Poll(timeoutMs)

		switch e := ev.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				return e, e.TopicPartition.Error
			}
			return e, nil
		case kafka.Error:
			return nil, e
		case kafka.OAuthBearerTokenRefresh:
			log.Infof("Providing token")
			err := sendToken(c)
			if err != nil {
				return nil, err
			}
		default:
			if e != nil {
				log.Infof("Got message: %v", e)
			}
		}

		if timeout > 0 {
			// Calculate remaining time
			timeoutMs = int(math.Max(0.0, absTimeout.Sub(time.Now()).Seconds()*1000.0))
		}

		if timeoutMs == 0 && ev == nil {
			return nil, kafka.NewError(kafka.ErrTimedOut, "", false)
		}

	}

}

func sendToken(consumer *kafka.Consumer) error {
	token, err := hardcodedFetch()
	if err != nil {
		return err
	}
	return consumer.SetOAuthBearerToken(*token)
}

func (c *Consumer) Stop() error {
	close(c.quit)
	c.exitWaitGroup.Wait()
	return c.consumer.Close()
}
