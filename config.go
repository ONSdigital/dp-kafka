package kafka

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
)

// ProducerConfig exposes the optional configurable parameters for a producer to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type ProducerConfig struct {
	KafkaVersion     *string
	MaxMessageBytes  *int
	RetryMax         *int
	KeepAlive        *time.Duration
	RetryBackoff     *time.Duration
	RetryBackoffFunc *func(retries, maxRetries int) time.Duration
}

// ConsumerGroupConfig exposes the optional configurable parameters for a consumer group, to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type ConsumerGroupConfig struct {
	KafkaVersion     *string
	KeepAlive        *time.Duration
	RetryBackoff     *time.Duration
	RetryBackoffFunc *func(retries int) time.Duration
	Offset           *int64
}

// getProducerConfig creates a default sarama config and overwrites any values provided in pConfig
func getProducerConfig(pConfig *ProducerConfig) (config *sarama.Config, err error) {
	config = sarama.NewConfig()
	if pConfig != nil {
		if pConfig.KafkaVersion != nil {
			config.Version, err = sarama.ParseKafkaVersion(*pConfig.KafkaVersion)
			if err != nil {
				return nil, err
			}
		}
		if pConfig.MaxMessageBytes != nil && *pConfig.MaxMessageBytes > 0 {
			config.Producer.MaxMessageBytes = *pConfig.MaxMessageBytes
		}
		if pConfig.KeepAlive != nil {
			config.Net.KeepAlive = *pConfig.KeepAlive
		}
		if pConfig.RetryMax != nil {
			config.Producer.Retry.Max = *pConfig.RetryMax
		}
		if pConfig.RetryBackoff != nil {
			config.Producer.Retry.Backoff = *pConfig.RetryBackoff
		}
		if pConfig.RetryBackoffFunc != nil {
			config.Producer.Retry.BackoffFunc = *pConfig.RetryBackoffFunc
		}
	}
	return config, nil
}

// getConsumerGroupConfig creates a default sarama config and overwrites any values provided in cgConfig
func getConsumerGroupConfig(cgConfig *ConsumerGroupConfig) (config *sarama.Config, err error) {
	config = sarama.NewConfig()
	config.Consumer.MaxWaitTime = 50 * time.Millisecond
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Group.Session.Timeout = messageConsumeTimeout
	if cgConfig != nil {
		if cgConfig.KafkaVersion != nil {
			config.Version, err = sarama.ParseKafkaVersion(*cgConfig.KafkaVersion)
			if err != nil {
				return nil, err
			}
		}
		if cgConfig.KeepAlive != nil {
			config.Net.KeepAlive = *cgConfig.KeepAlive
		}
		if cgConfig.RetryBackoff != nil {
			config.Consumer.Retry.Backoff = *cgConfig.RetryBackoff
		}
		if cgConfig.RetryBackoffFunc != nil {
			config.Consumer.Retry.BackoffFunc = *cgConfig.RetryBackoffFunc
		}
		if cgConfig.Offset != nil {
			if *cgConfig.Offset != sarama.OffsetNewest && *cgConfig.Offset != sarama.OffsetOldest {
				return nil, errors.New("offset value incorrect")
			}
			config.Consumer.Offsets.Initial = *cgConfig.Offset
		}
	}
	return config, nil
}
