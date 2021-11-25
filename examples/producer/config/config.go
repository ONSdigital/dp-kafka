package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config is the kafka configuration for this example
type Config struct {
	Brokers                 []string      `envconfig:"KAFKA_ADDR"`
	KafkaMaxBytes           int           `envconfig:"KAFKA_MAX_BYTES"`
	KafkaVersion            string        `envconfig:"KAFKA_VERSION"`
	ProducedTopic           string        `envconfig:"KAFKA_PRODUCED_TOPIC"`
	WaitForProducerReady    bool          `envconfig:"KAFKA_WAIT_PRODUCER_READY"`
	KafkaSecProtocol        string        `envconfig:"KAFKA_SEC_PROTO"`
	KafkaSecCACerts         string        `envconfig:"KAFKA_SEC_CA_CERTS"`
	KafkaSecClientCert      string        `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	KafkaSecClientKey       string        `envconfig:"KAFKA_SEC_CLIENT_KEY" json:"-"`
	KafkaSecSkipVerify      bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Chomp                   bool          `envconfig:"CHOMP_MSG"`
}

var cfg *Config

func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg := &Config{
		Brokers:                 []string{"localhost:39092"},
		KafkaMaxBytes:           50 * 1024 * 1024,
		KafkaVersion:            "1.0.2",
		ProducedTopic:           "myTopic",
		WaitForProducerReady:    true,
		GracefulShutdownTimeout: 5 * time.Second,
		Chomp:                   false,
	}

	return cfg, envconfig.Process("", cfg)
}
