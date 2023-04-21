package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Producer struct {
	producer *kafka.Producer
	topic    string
	quit     chan struct{}
	waiter   sync.WaitGroup
	watcher  *StateWatcher
}

func NewProducer(configMap *kafka.ConfigMap, topic string, watcher *StateWatcher) (*Producer, error) {
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	return &Producer{producer: producer, topic: topic, quit: make(chan struct{}), watcher: watcher}, nil
}

func (p *Producer) Stop() error {
	close(p.quit)
	p.waiter.Wait()
	p.producer.Close()
	return nil
}

func (p *Producer) Run() {
	ticker := time.NewTicker(500 * time.Millisecond)
	sequence := uint64(0)
	p.waiter.Add(1)
	go p.eventConsumerThread()
	go func() {
		for {
			select {
			case <-ticker.C:
				_ = p.sendTimestamp(sequence)
				sequence++

			case <-p.quit:
				ticker.Stop()
				p.waiter.Done()
				return
			}
		}
	}()
}

func (p *Producer) eventConsumerThread() {
	for {
		select {
		case e := <-p.producer.Events():
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Warnf("Failed to deliver message: %v\n", ev.TopicPartition)
				}
			default:
				log.Info("Got event from events consumer: %s", ev)
				return
			}
		}
	}
}

func (p *Producer) sendTimestamp(sequence uint64) error {
	log.Info("sending message")
	message, _ := BytesFromTimestamp(time.Now().UnixMilli(), sequence)
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte{},
		Value:          *message,
	}, nil)
	_ = p.producer.Flush(FlushTimeoutMs)
	p.watcher.sent(sequence)
	if err != nil {
		return err
	}

	return nil
}
