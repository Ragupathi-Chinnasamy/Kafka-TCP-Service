package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Configurations struct {
	TcpPort                 string
	KafkaServerUrl          string
	KafkaGroupId            string
	KafkaProducerTopic      string
	KafkaConsumerTopic      string
	ProducerWorkersCount    int
	ConsumerWorkersCount    int
	TCPResponseWorkersCount int
}

var Configs *Configurations

func Load() {
	_ = godotenv.Load()

	var config Configurations

	config.TcpPort = getEnvOrError("TCP_PORT")
	config.KafkaServerUrl = getEnvOrError("KAFKA_SERVER_URL")
	config.KafkaConsumerTopic = getEnvOrError("KAFKA_CONSUMER_TOPIC")
	config.KafkaProducerTopic = getEnvOrError("KAFKA_PRODUCER_TOPIC")
	config.KafkaGroupId = getEnvOrError("KAFKA_GROUP_ID")
	config.ProducerWorkersCount = getEnvAsInt("PRODUCER_WORKERS_COUNT")
	config.ConsumerWorkersCount = getEnvAsInt("COSUMER_WORKERS_COUNT")
	config.TCPResponseWorkersCount = getEnvAsInt("TCP_RESPONSE_WORKERS_COUNT")

	Configs = &config
}

func getEnvOrError(key string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	panic(fmt.Sprintf("Environment variable %s not set", key))
}

func getEnvAsInt(key string) int {
	valueStr := getEnvOrError(key)
	var value int
	_, err := fmt.Sscanf(valueStr, "%d", &value)

	if err != nil {
		log.Panicf("Error converting: %s", key)
	}

	return value
}
