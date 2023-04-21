package main

import "C"
import (
	"fmt"
	"math"
	"os"
	"os/signal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

const FlushTimeoutMs = 15 * 1000
const ConsumerGroupId = "kafka-availability-tester"

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {

	err := run("localhost:7070", "test")
	if err != nil {
		log.Errorf("%v", err)
		os.Exit(-1)
	}
}

type Closable interface {
	// Close release all resources used by this object, including goroutines.
	Close() error
}

func run(bootstrapServers, topic string) error {
	var toClose []Closable
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		log.Infof("Caught a SIGTERM, closing")
		for _, closable := range toClose {
			_ = closable.Close()
		}
	}()

	log.Infof("Connecting to boostrap address '%s' to produce and consume topic '%s'", bootstrapServers, topic)
	// conf := ReadConfig(configFile)
	conf := kafka.ConfigMap{"bootstrap.servers": bootstrapServers}

	p, err := NewProducer(&conf, topic)
	if err != nil {
		return fmt.Errorf("failed to create producer %w", err)
	}
	toClose = append(toClose, p)

	conf["group.id"] = ConsumerGroupId

	consumer, err := NewConsumer(&conf)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	toClose = append(toClose, consumer)

	err = consumer.subscribeAndConsume(topic)
	if err != nil {
		log.Panicf("Failed to subscribeAndConsume: %v", err)
	}

	p.run()

	stop := closableChannel{make(chan struct{})}
	toClose = append(toClose, stop)
	<-stop.stop
	return nil
}

type closableChannel struct {
	stop chan struct{}
}

func (cc closableChannel) Close() error {
	close(cc.stop)
	return nil
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
