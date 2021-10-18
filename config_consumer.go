package kafka

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
)

var (
	defaultMessageConsumeTimeout = 10 * time.Second
	defaultNumWorkers            = 1
	defaultBatchSize             = 1
	defaultBatchWaitTime         = 200 * time.Millisecond
)

// ConsumerGroupConfig exposes the configurable parameters for a consumer group
// to overwrite default config values and any other defult config values set by dp-kafka.
// Any value that is not provied will use the default Sarama config value, or the default dp-kafka value.
// The only 3 compulsory values are:
// - Topic
// - GroupName
// - BrokerAddrs
type ConsumerGroupConfig struct {
	// Sarama config overrides
	KafkaVersion          *string
	KeepAlive             *time.Duration
	RetryBackoff          *time.Duration
	RetryBackoffFunc      *func(retries int) time.Duration
	Offset                *int64
	SecurityConfig        *SecurityConfig
	MessageConsumeTimeout *time.Duration

	// dp-kafka specific config overrides
	NumWorkers    *int
	BatchSize     *int
	BatchWaitTime *time.Duration
	Topic         string
	GroupName     string
	BrokerAddrs   []string
}

// Get creates a default sarama config for a consumer-group and overwrites any values provided in cgConfig.
// If any required value is not provided or any override is invalid, an error will be returned
func (c *ConsumerGroupConfig) Get() (*sarama.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	// Get default Sarama config and apply overrides
	cfg := sarama.NewConfig()
	cfg.Consumer.MaxWaitTime = 50 * time.Millisecond
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	cfg.Consumer.Group.Session.Timeout = defaultMessageConsumeTimeout
	if c.MessageConsumeTimeout != nil {
		cfg.Consumer.Group.Session.Timeout = *c.MessageConsumeTimeout
	}
	if c.KafkaVersion != nil {
		var err error
		if cfg.Version, err = sarama.ParseKafkaVersion(*c.KafkaVersion); err != nil {
			return nil, err
		}
	}
	if c.KeepAlive != nil {
		cfg.Net.KeepAlive = *c.KeepAlive
	}
	if c.RetryBackoff != nil {
		cfg.Consumer.Retry.Backoff = *c.RetryBackoff
	}
	if c.RetryBackoffFunc != nil {
		cfg.Consumer.Retry.BackoffFunc = *c.RetryBackoffFunc
	}
	if c.Offset != nil {
		if *c.Offset != sarama.OffsetNewest && *c.Offset != sarama.OffsetOldest {
			return nil, errors.New("offset value incorrect")
		}
		cfg.Consumer.Offsets.Initial = *c.Offset
	}
	if err := addAnyTLS(c.SecurityConfig, cfg); err != nil {
		return nil, err
	}

	// Override any other optional value
	if c.NumWorkers == nil {
		c.NumWorkers = &defaultNumWorkers
	}
	if c.BatchSize == nil {
		c.BatchSize = &defaultBatchSize
	}
	if c.BatchWaitTime == nil {
		c.BatchWaitTime = &defaultBatchWaitTime
	}

	return cfg, nil
}

// Validate that compulsory values are provided in config
func (c *ConsumerGroupConfig) Validate() (err error) {
	if c.Topic == "" {
		return errors.New("topic is compulsory but was not provided in config")
	}
	if c.GroupName == "" {
		return errors.New("groupName is compulsory but was not provided in config")
	}
	if len(c.BrokerAddrs) == 0 {
		return errors.New("brokerAddrs is compulsory but was not provided in config")
	}
	return nil
}
