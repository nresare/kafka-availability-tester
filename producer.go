package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config-file-path>\n",
			os.Args[0])
		os.Exit(1)
	}
	configFile := os.Args[1]
	conf := ReadConfig(configFile)

	topic := "purchases"
	p, err := kafka.NewProducer(&conf)
	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	sendTimestamps(10, p, topic)

	p.Close()
}

func sendTimestamps(count int, producer *kafka.Producer, topic string) {
	ticker := time.NewTicker(1 * time.Second)
	quit := make(chan struct{})

	for {
		select {
		case <-ticker.C:
			_ = sendTimestamp(producer, topic)
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

const FlushTimeoutMs = 15 * 1000

func sendTimestamp(producer *kafka.Producer, topic string) error {
	message, _ := BytesFromTimestamp(time.Now().UnixMilli())
	log.Infof("Producing message %v", string(*message))
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte{},
		Value:          *message,
	}, nil)
	producer.Flush(FlushTimeoutMs)

	return nil
}
