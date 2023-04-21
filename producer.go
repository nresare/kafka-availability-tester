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
}

func NewProducer(configMap *kafka.ConfigMap, topic string) (*Producer, error) {
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	return &Producer{producer: producer, topic: topic, quit: make(chan struct{})}, nil
}

func (p *Producer) Close() error {
	close(p.quit)
	p.waiter.Wait()
	p.producer.Close()
	return nil
}

func (p *Producer) run() {
	ticker := time.NewTicker(500 * time.Millisecond)
	sequence := uint32(0)
	p.waiter.Add(1)
	go p.eventConsumerThread()
	go func() {
		for {
			select {
			case <-ticker.C:
				_ = sendTimestamp(p.producer, p.topic, sequence)
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

func sendTimestamp(producer *kafka.Producer, topic string, sequence uint32) error {
	message, _ := BytesFromTimestamp(time.Now().UnixMilli(), sequence)
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
