package messenger

import (
	"go-kafka-tcp-service/app/connections"
	"log"
	"sync"
	"sync/atomic"
)

type ProducerJob struct {
	Topic   *string
	Message []byte
}

type ConsumerJob struct {
	Message []byte
}

const (
	jobsCount = 100000
)

var (
	producerJobQueue = make(chan ProducerJob, jobsCount)
	consumerJobQueue = make(chan ConsumerJob, jobsCount)
	ProducedCount    int64
	ConsumedCount    int64
	ResponseCount    int64
)

func DispatchProducerWorkers(wg *sync.WaitGroup, producer *MessageProducer, workersCount int) {

	wg.Add(workersCount)

	for i := 1; i <= workersCount; i++ {

		go func() {
			defer wg.Done()

			for job := range producerJobQueue {
				producer.ProduceMessage(job.Topic, job.Message)
			}

		}()
	}

}

func AddProducerJob(job ProducerJob) {
	producerJobQueue <- job
}

func CloseProducerJobQueue() {
	close(producerJobQueue)
}

func DispatchConsumerWorkers(wg *sync.WaitGroup, consumer *MessageConsumer, workersCount int) {

	wg.Add(workersCount)

	for i := 1; i <= workersCount; i++ {

		go func(workerId int) {

			defer wg.Done()

			consumer.ConsumeMessage(workerId)

		}(i)
	}

}

func AddConsumerJob(job ConsumerJob) {
	consumerJobQueue <- job
}

func DispatchResponseHandlerWorkers(wg *sync.WaitGroup, workersCount int) {

	wg.Add(workersCount)

	for i := 1; i <= workersCount; i++ {

		go func(workerId int) {

			defer wg.Done()

			for job := range consumerJobQueue {
				SendResponse(job, workerId)
			}

		}(i)
	}
}

func SendResponse(job ConsumerJob, workerId int) {

	if len(job.Message) > 8 {
		gatewayMarking := string(job.Message[:8])

		conn, exists := connections.GetConnection(gatewayMarking)
		if !exists {
			log.Printf("Response Handler Worker %d: Connection not found for ID: %s\n", workerId, gatewayMarking)
			return
		}

		_, err := conn.Write(job.Message)
		if err != nil {
			log.Printf("Response Handler Worker %d: Failed to write response to connection %s: %v\n", workerId, gatewayMarking, err)
		} else {
			atomic.AddInt64(&ResponseCount, 1)
		}

		return
	}
}

func CloseConsumerJobQueue() {
	close(consumerJobQueue)
}
