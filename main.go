package main

import "C"
import (
	"encoding/json"
	"math"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

const FlushTimeoutMs = 15 * 1000

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	// conf := ReadConfig(configFile)
	conf := kafka.ConfigMap{"bootstrap.servers": "localhost:7070"}

	topic := "test"
	p, err := kafka.NewProducer(&conf)
	if err != nil {
		log.Panicf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	conf["group.id"] = "kafka-availability-tester"
	//conf["auto.offset.reset"] = "earliest"

	c, err := kafka.NewConsumer(&conf)
	if err != nil {
		log.Panicf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	consumerQuitter := make(chan struct{})
	var consumerWaitGroup sync.WaitGroup
	consumerWaitGroup.Add(1)
	go consume(c, topic, consumerQuitter, &consumerWaitGroup)

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Warnf("Failed to deliver message: %v\n", ev.TopicPartition)
				}
			default:
				log.Info("Got event from events consumer: %s", ev)
			}

		}
	}()

	sendTimestamps(30, p, topic)

	close(consumerQuitter)
	consumerWaitGroup.Wait()
	_ = c.Close()
	p.Close()
}

func consume(consumer *kafka.Consumer, topic string, quit chan struct{}, waitGroup *sync.WaitGroup) {
	err := consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Panicf("Failed to create consumer: %v", err)
		os.Exit(1)
	}

	// Process messages

	for true {
		select {
		case <-quit:
			log.Infof("Caught quit message: terminating\n")
			waitGroup.Done()
			return
		default:
			ev, err := readMessage(consumer, time.Second)
			if err != nil {
				if err.(kafka.Error).Code() != kafka.ErrTimedOut {
					log.Infof("Informal error, apparently: %v", err.(kafka.Error).Code())
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

func readMessage(consumer *kafka.Consumer, timeout time.Duration) (*kafka.Message, error) {
	var absTimeout time.Time
	var timeoutMs int

	if timeout > 0 {
		absTimeout = time.Now().Add(timeout)
		timeoutMs = (int)(timeout.Seconds() * 1000.0)
	} else {
		timeoutMs = (int)(timeout)
	}
	for {
		ev := consumer.Poll(timeoutMs)

		switch e := ev.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				return e, e.TopicPartition.Error
			}
			return e, nil
		case kafka.Error:
			return nil, e
		default:
			log.Infof("Got event '%v'", ev)
		}

		if timeout > 0 {
			// Calculate remaining time
			timeoutMs = int(math.Max(0.0, absTimeout.Sub(time.Now()).Seconds()*1000.0))
		}

		if timeoutMs == 0 && ev == nil {
			return nil, kafka.Error{}
		}
	}

}

func sendTimestamps(count int, producer *kafka.Producer, topic string) {
	ticker := time.NewTicker(500 * time.Millisecond)
	quit := make(chan struct{})
	sequence := uint32(0)
	for {
		select {
		case <-ticker.C:
			_ = sendTimestamp(producer, topic, sequence)
			sequence++
			count--
			if count < 0 {
				return
			}
		case <-quit:
			ticker.Stop()
			return
		}
	}
}

func sendTimestamp(producer *kafka.Producer, topic string, sequence uint32) error {
	message, _ := BytesFromTimestamp(time.Now().UnixMilli(), sequence)
	log.Infof("Producing message %v", string(*message))
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte{},
		Value:          *message,
	}, nil)
	_ = producer.Flush(FlushTimeoutMs)
	if err != nil {
		return err
	}

	return nil
}
