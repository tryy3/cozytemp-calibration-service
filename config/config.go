package config

import (
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers       []string
	KafkaConsumerTopic string
	KafkaProducerTopic string
	KafkaConsumerGroup string
	PostgresURL        string
}

func Load() *Config {
	return &Config{
		KafkaBrokers:       getBrokers(),
		KafkaConsumerTopic: getEnv("KAFKA_CONSUMER_TOPIC", "cozytemp-temperature-calibration-realtime"),
		KafkaProducerTopic: getEnv("KAFKA_PRODUCER_TOPIC", ""),
		KafkaConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "cozytemp-calibration-realtime-service"),
		PostgresURL:        getEnv("POSTGRES_URL", "postgresql://postgres:postgres@localhost:5432/cozytemp"),
	}
}

func getBrokers() []string {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	return strings.Split(brokers, ",")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
