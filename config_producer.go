package kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

var defaultProducerMinBrokersHealthy = 2
var defaultOtelEnabled = false

// ProducerConfig exposes the optional configurable parameters for a producer to overwrite default Sarama config values.
// Any value that is not provided will use the default Sarama config value.
type ProducerConfig struct {
	// Sarama config overrides
	KafkaVersion      *string
	MaxMessageBytes   *int
	RetryMax          *int
	KeepAlive         *time.Duration
	RetryBackoff      *time.Duration
	RetryBackoffFunc  *func(retries, maxRetries int) time.Duration
	SecurityConfig    *SecurityConfig
	MinBrokersHealthy *int

	// dp-kafka specific config
	Topic          string
	BrokerAddrs    []string
	MinRetryPeriod *time.Duration
	MaxRetryPeriod *time.Duration
	OtelEnabled    *bool
}

// Get creates a default sarama config and overwrites any values provided in pConfig
func (p *ProducerConfig) Get() (*sarama.Config, error) {
	// Get default Sarama config and apply overrides
	cfg := sarama.NewConfig()
	if p.KafkaVersion != nil {
		var err error
		if cfg.Version, err = sarama.ParseKafkaVersion(*p.KafkaVersion); err != nil {
			return nil, fmt.Errorf("error parsing kafka version: %w", err)
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
		return nil, fmt.Errorf("error adding tls: %w", err)
	}

	// Override any other optional value
	if p.MinRetryPeriod == nil {
		p.MinRetryPeriod = &defaultMinRetryPeriod
	}
	if p.MaxRetryPeriod == nil {
		p.MaxRetryPeriod = &defaultMaxRetryPeriod
	}
	if p.MinBrokersHealthy == nil {
		p.MinBrokersHealthy = &defaultProducerMinBrokersHealthy
	}
	if p.OtelEnabled == nil {
		p.OtelEnabled = &defaultOtelEnabled
	}

	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
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
	if *p.MinRetryPeriod <= 0 {
		return errors.New("minRetryPeriod must be greater than zero")
	}
	if *p.MaxRetryPeriod <= 0 {
		return errors.New("maxRetryPeriod must be greater than zero")
	}
	if *p.MinRetryPeriod > *p.MaxRetryPeriod {
		return errors.New("minRetryPeriod must be smaller or equal to maxRetryPeriod")
	}
	if *p.MinBrokersHealthy <= 0 {
		return errors.New("minBrokersHealthy must be greater than zero")
	}
	if *p.MinBrokersHealthy > len(p.BrokerAddrs) {
		return errors.New("minBrokersHealthy must be smaller or equal to the total number of brokers provided in brokerAddrs")
	}
	return nil
}
