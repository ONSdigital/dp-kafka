package kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// ProducerConfig exposes the optional configurable parameters for a producer to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type ProducerConfig struct {
	// Sarama config overrides
	KafkaVersion     *string
	MaxMessageBytes  *int
	RetryMax         *int
	KeepAlive        *time.Duration
	RetryBackoff     *time.Duration
	RetryBackoffFunc *func(retries, maxRetries int) time.Duration
	SecurityConfig   *SecurityConfig

	// dp-kafka specific config
	Topic       string
	BrokerAddrs []string
}

// Get creates a default sarama config and overwrites any values provided in pConfig
func (p *ProducerConfig) Get() (*sarama.Config, error) {
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("error validating producer config: %w", err)
	}

	// Get default Sarama config and apply overrides
	cfg := sarama.NewConfig()
	if p.KafkaVersion != nil {
		var err error
		if cfg.Version, err = sarama.ParseKafkaVersion(*p.KafkaVersion); err != nil {
			return nil, fmt.Errorf("error parsing kafka version for producer config: %w", err)
		}
	}
	if p.MaxMessageBytes != nil && *p.MaxMessageBytes > 0 {
		cfg.Producer.MaxMessageBytes = *p.MaxMessageBytes
	}
	if p.KeepAlive != nil {
		cfg.Net.KeepAlive = *p.KeepAlive
	}
	if p.RetryMax != nil {
		cfg.Producer.Retry.Max = *p.RetryMax
	}
	if p.RetryBackoff != nil {
		cfg.Producer.Retry.Backoff = *p.RetryBackoff
	}
	if p.RetryBackoffFunc != nil {
		cfg.Producer.Retry.BackoffFunc = *p.RetryBackoffFunc
	}
	if err := addAnyTLS(p.SecurityConfig, cfg); err != nil {
		return nil, fmt.Errorf("error adding tls for producer config: %w", err)
	}

	return cfg, nil
}

// Validate that compulsory values are provided in config
func (p *ProducerConfig) Validate() error {
	if p.Topic == "" {
		return errors.New("topic is compulsory but was not provided in config")
	}
	if len(p.BrokerAddrs) == 0 {
		return errors.New("brokerAddrs is compulsory but was not provided in config")
	}
	return nil
}
