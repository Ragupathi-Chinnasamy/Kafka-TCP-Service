package main

import (
	"go-kafka-tcp-service/app/messenger"
	"go-kafka-tcp-service/app/tcp"
	"go-kafka-tcp-service/common/config"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

func main() {

	config.Load()

	var wg sync.WaitGroup

	producer := messenger.NewKafkaProducer()
	defer producer.CloseProducer()

	consumer := messenger.NewKafkaConsumer()
	defer consumer.CloseConsumer()

	messenger.DispatchProducerWorkers(&wg, producer, config.Configs.ProducerWorkersCount)
	messenger.DispatchConsumerWorkers(&wg, consumer, config.Configs.ConsumerWorkersCount)
	messenger.DispatchResponseHandlerWorkers(&wg, config.Configs.TCPResponseWorkersCount)

	wg.Add(1)

	go func() {
		defer wg.Done()
		tcp.StartTCPServer(&wg)
	}()

	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for range ticker.C {
			log.Printf("Progress: Produced=%d, Consumed=%d, Responses Sent=%d\n",
				atomic.LoadInt64(&messenger.ProducedCount),
				atomic.LoadInt64(&messenger.ConsumedCount),
				atomic.LoadInt64(&messenger.ResponseCount),
			)
		}
	}()

	wg.Wait()

	messenger.CloseProducerJobQueue()
	messenger.CloseConsumerJobQueue()
}
