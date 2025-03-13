package messenger

import (
	"go-kafka-tcp-service/common/config"
	"log"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessageProducer struct {
	producer *kafka.Producer
}

func NewKafkaProducer() *MessageProducer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.Configs.KafkaServerUrl,
	})

	if err != nil {
		panic(err)
	}

	log.Println("Producer init...")

	return &MessageProducer{producer}
}

func (p *MessageProducer) ProduceMessage(topic *string, message []byte) {

	for attempts := 0; attempts < 3; attempts++ {

		err := p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
			Value:          message,
		}, nil)

		if err == nil {
			e := <-p.producer.Events()
			m := e.(*kafka.Message)
			if m.TopicPartition.Error == nil {
				atomic.AddInt64(&ProducedCount, 1)
				return
			} else {
				log.Println("Delivery failed:", m.TopicPartition.Error)
				time.Sleep(1 * time.Second)
				continue
			}
		} else {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				// Producer queue is full, wait 1s for messages
				// to be delivered then try again.
				time.Sleep(time.Second)
				continue
			}
			log.Println("Produce error:", err)
			continue
		}
	}

	log.Println("Failed to deliver message after 3 attempts")
}

func (p *MessageProducer) CloseProducer() {

	for p.producer.Flush(10000) > 0 {
		log.Print("Still waiting to flush outstanding messages\n")
	}

	p.producer.Close()
}
