package main

import "C"
import (
	"math"
	"os"
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

	consumer, err := NewConsumer(&conf)
	if err != nil {
		log.Panicf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	err = consumer.subscribeAndConsume(topic)
	if err != nil {
		log.Panicf("Failed to subscribeAndConsume: %v", err)
	}

	sendTimestamps(30, p, topic)

	_ = consumer.close()
	p.Close()
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
		case nil:
			// this is a timeout, calculate a new timeout and loop
		default:
			log.Debugf("Got event '%v'", ev)
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
	log.Debugf("Producing message %v", string(*message))
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
