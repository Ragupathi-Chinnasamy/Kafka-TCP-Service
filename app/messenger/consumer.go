package messenger

import (
	"go-kafka-tcp-service/common/config"
	"log"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessageConsumer struct {
	consumer *kafka.Consumer
}

func NewKafkaConsumer() *MessageConsumer {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.Configs.KafkaServerUrl,
		"group.id":          config.Configs.KafkaGroupId,
		"auto.offset.reset": "earliest",
		// "enable.auto.offset.store": false,
	})

	if err != nil {
		panic(err)
	}

	log.Println("Consumer init...")

	return &MessageConsumer{consumer}
}

func (c *MessageConsumer) ConsumeMessage(workerId int) {

	err := c.consumer.SubscribeTopics([]string{config.Configs.KafkaConsumerTopic}, nil)

	if err != nil {
		panic("Error consuming topic:" + err.Error())
	}

	for {
		msg, err := c.consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer Worker %d error: %v\n", workerId, err)
			continue
		}

		if len(msg.Value) > 0 {

			AddConsumerJob(ConsumerJob{
				Message: msg.Value,
			})

			atomic.AddInt64(&ConsumedCount, 1)

		} else {
			log.Println("Invalid message length")
		}
	}
}

func (c *MessageConsumer) CloseConsumer() {
	c.consumer.Close()
}
