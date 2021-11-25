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
	ConsumedTopic           string        `envconfig:"KAFKA_CONSUMED_TOPIC"`
	ConsumedGroup           string        `envconfig:"KAFKA_CONSUMED_GROUP"`
	KafkaSecProtocol        string        `envconfig:"KAFKA_SEC_PROTO"`
	KafkaSecCACerts         string        `envconfig:"KAFKA_SEC_CA_CERTS"`
	KafkaSecClientCert      string        `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	KafkaSecClientKey       string        `envconfig:"KAFKA_SEC_CLIENT_KEY" json:"-"`
	KafkaSecSkipVerify      bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Snooze                  bool          `envconfig:"SNOOZE"`
	OverSleep               bool          `envconfig:"OVERSLEEP"`
	KafkaParallelMessages   int           `envconfig:"KAFKA_PARALLEL_MESSAGES"`
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
		ConsumedTopic:           "myTopic",
		ConsumedGroup:           "myGroup",
		GracefulShutdownTimeout: 5 * time.Second,
		Snooze:                  true,
		OverSleep:               false,
		KafkaParallelMessages:   3,
	}

	return cfg, envconfig.Process("", cfg)
}
