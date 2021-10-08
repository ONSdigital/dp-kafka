package kafkaconfig

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
)

// Producer exposes the optional configurable parameters for a producer to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type Producer struct {
	// Sarama config overrides
	KafkaVersion     *string
	MaxMessageBytes  *int
	RetryMax         *int
	KeepAlive        *time.Duration
	RetryBackoff     *time.Duration
	RetryBackoffFunc *func(retries, maxRetries int) time.Duration
	SecurityConfig   *Security

	// dp-kafka specific config
	Topic       string
	BrokerAddrs []string
}

// Get creates a default sarama config and overwrites any values provided in pConfig
func (p *Producer) Get() (cfg *sarama.Config, err error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	// Get default Sarama config and apply overrides
	cfg = sarama.NewConfig()
	if p.KafkaVersion != nil {
		if cfg.Version, err = sarama.ParseKafkaVersion(*p.KafkaVersion); err != nil {
			return nil, err
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
	if err = addAnyTLS(p.SecurityConfig, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate that compulsory values are provided in config
func (p *Producer) Validate() (err error) {
	if p.Topic == "" {
		return errors.New("topic is compulsory but was not provided in config")
	}
	if len(p.BrokerAddrs) == 0 {
		return errors.New("brokerAddrs is compulsory but was not provided in config")
	}
	return nil
}
