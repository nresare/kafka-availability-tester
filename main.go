package main

import "C"
import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

const FlushTimeoutMs = 15 * 1000
const ConsumerGroupId = "kafka-availability-tester"
const Period = 100 * time.Millisecond

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	err := run("localhost:7070", "test", Period)
	if err != nil {
		log.Errorf("%v", err)
		os.Exit(-1)
	}
}

func run(bootstrapServers, topic string, period time.Duration) error {
	var toStop []Stoppable
	installStoppingSignalHandler(&toStop, os.Interrupt)

	log.Infof("Connecting to boostrap address '%s'", bootstrapServers)
	// conf := ReadConfig(configFile)
	conf := kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	watcher := NewStateWatcher(LoggingEventSink{})

	producer, err := NewProducer(&conf, topic, watcher)
	if err != nil {
		return fmt.Errorf("failed to create producer %w", err)
	}
	toStop = append(toStop, producer)

	conf["group.id"] = ConsumerGroupId

	consumer, err := NewConsumer(&conf, watcher)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	toStop = append(toStop, consumer)

	err = consumer.subscribeAndConsume(topic)
	if err != nil {
		log.Panicf("Failed to subscribeAndConsume: %v", err)
	}

	log.Infof("Producing a message every %s", Period)
	producer.run(Period)

	waiter := makeWaiter()
	toStop = append(toStop, waiter)
	waiter.waitUntilStopped()

	return nil
}
