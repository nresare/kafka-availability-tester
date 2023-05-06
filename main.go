package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

const FlushTimeoutMs = 15 * 1000
const ConsumerGroupId = "kafka-availability-tester"
const SendPeriod = 100 * time.Millisecond
const StatsPeriod = 10 * time.Second

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	err := run("localhost:7080", "test", SendPeriod)
	if err != nil {
		log.Errorf("%v", err)
		os.Exit(-1)
	}
}

func run(bootstrapServers, topic string, period time.Duration) error {
	var toStop []Stoppable
	installStoppingSignalHandler(&toStop, os.Interrupt)

	log.Infof("Connecting to boostrap address '%s'", bootstrapServers)
	conf := kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"security.protocol": "SASL_SSL",
		"ssl.ca.location":   "local-testing/ca-cert.pem",
		"sasl.mechanism":    "OAUTHBEARER",
	}

	statsEventSink := StartNewStatSink()
	watcher := NewStateWatcher(StatsEventSink{statsEventSink})

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

	log.Infof("Producing a message every %s", period)
	producer.run(period)

	startPeriodicStatLogger(statsEventSink)

	waiter := makeWaiter()
	toStop = append(toStop, waiter)
	waiter.waitUntilStopped()

	return nil
}

func startPeriodicStatLogger(sink *StatSink) {
	ticker := time.NewTicker(StatsPeriod)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Infof("%s", sink.MakeStats())
			}
		}
	}()
}
