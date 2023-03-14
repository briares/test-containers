package config

import (
	"time"
)

type Config struct {
	LogLevel       string
	MetricsPort    int
	ConsumerConfig ConsumerConfig
	ProducerConfig ProducerConfig
}

type ConnectionConfig struct {
	AMQPUrl  string
	Username string
	Password string
}

type ConsumerConfig struct {
	ConnectionConfig
	Queue string

	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	MaxInflightMessages int
}

type ProducerConfig struct {
	ConnectionConfig
	Topic          string
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func NewConfig() (*Config, error) {
	connectionConfig := ConnectionConfig{
		AMQPUrl:  "amqp://localhost:5672",
		Username: "",
		Password: "",
	}

	initialBackoff, _ := time.ParseDuration("125ms")
	maxBackoff, _ := time.ParseDuration("30s")
	consumerConfig := ConsumerConfig{
		ConnectionConfig:    connectionConfig,
		Queue:               "siam/q/sit/iii/incoming/gps/employee/xml",
		InitialBackoff:      initialBackoff,
		MaxBackoff:          maxBackoff,
		MaxInflightMessages: 16,
	}

	producerConfig := ProducerConfig{
		ConnectionConfig: connectionConfig,
		Topic:            "topic://siam/t/sit/iii/dev/internal/fragments",
		InitialBackoff:   initialBackoff,
		MaxBackoff:       maxBackoff,
	}

	return &Config{
		LogLevel:       "INFO",
		MetricsPort:    8080,
		ConsumerConfig: consumerConfig,
		ProducerConfig: producerConfig,
	}, nil
}
