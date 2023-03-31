package main

import (
	"encoding/json"
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
	conf := kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}

	topic := "purchases"
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
	go consume(c, topic, consumerQuitter)

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Warnf("Failed to deliver message: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	sendTimestamps(30, p, topic)

	close(consumerQuitter)
	p.Flush(FlushTimeoutMs)
	p.Close()
}

func consume(consumer *kafka.Consumer, topic string, quit chan struct{}) {
	err := consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Panicf("Failed to create consumer: %v", err)
		os.Exit(1)
	}

	// Process messages
	run := true
	for run {
		select {
		case <-quit:
			log.Infof("Caught quit message: terminating\n")
			run = false
		default:
			ev, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() != kafka.ErrTimedOut {
					log.Infof("Informal error, apparently: %v", err)
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

func sendTimestamps(count int, producer *kafka.Producer, topic string) {
	ticker := time.NewTicker(100 * time.Millisecond)
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
	if err != nil {
		return err
	}

	return nil
}
