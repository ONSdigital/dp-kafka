package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/Shopify/sarama"
	saramatls "github.com/Shopify/sarama/tools/tls"
)

// ErrTLSCannotLoadCACerts is returned when the certs file cannot be loaded
var ErrTLSCannotLoadCACerts = errors.New("Cannot load CA Certs")

// ProducerConfig exposes the optional configurable parameters for a producer to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type ProducerConfig struct {
	KafkaVersion     *string
	MaxMessageBytes  *int
	RetryMax         *int
	KeepAlive        *time.Duration
	RetryBackoff     *time.Duration
	RetryBackoffFunc *func(retries, maxRetries int) time.Duration
	SecurityConfig   SecurityConfig
}

// ConsumerGroupConfig exposes the optional configurable parameters for a consumer group, to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type ConsumerGroupConfig struct {
	KafkaVersion     *string
	KeepAlive        *time.Duration
	RetryBackoff     *time.Duration
	RetryBackoffFunc *func(retries int) time.Duration
	Offset           *int64
	SecurityConfig   SecurityConfig
}

// SecurityConfig is common to producers and consumer configs, above
type SecurityConfig struct {
	Protocol           *string `envconfig:"KAFKA_SEC_PROTO"`
	RootCACerts        *string `envconfig:"KAFKA_SEC_CA_CERTS"`
	ClientCert         *string `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	ClientKey          *string `envconfig:"KAFKA_SEC_CLIENT_KEY"`
	InsecureSkipVerify bool    `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
}

// getProducerConfig creates a default sarama config and overwrites any values provided in pConfig
func getProducerConfig(pConfig *ProducerConfig) (config *sarama.Config, err error) {
	config = sarama.NewConfig()
	if pConfig != nil {
		if pConfig.KafkaVersion != nil {
			if config.Version, err = sarama.ParseKafkaVersion(*pConfig.KafkaVersion); err != nil {
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
		if err = addTLS(pConfig.SecurityConfig, config); err != nil {
			return nil, err
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
			if config.Version, err = sarama.ParseKafkaVersion(*cgConfig.KafkaVersion); err != nil {
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
		if err = addTLS(cgConfig.SecurityConfig, config); err != nil {
			return nil, err
		}
	}
	return config, nil
}

func addTLS(tlsConfig SecurityConfig, saramaConfig *sarama.Config) (err error) {
	if tlsConfig.Protocol == nil || *tlsConfig.Protocol != "TLS" {
		return
	}

	var saramaTLSConfig *tls.Config
	if tlsConfig.ClientCert != nil && tlsConfig.ClientKey != nil {
		saramaTLSConfig, err = saramatls.NewConfig(*tlsConfig.ClientCert, *tlsConfig.ClientKey)
	} else {
		saramaTLSConfig, err = saramatls.NewConfig("", "")
	}
	if err != nil {
		return
	}

	if tlsConfig.RootCACerts != nil && *tlsConfig.RootCACerts != "" {
		var rootCAsBytes []byte
		if rootCAsBytes, err = ioutil.ReadFile(*tlsConfig.RootCACerts); err != nil {
			return fmt.Errorf("failed read from %q: %w", *tlsConfig.RootCACerts, err)
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(rootCAsBytes) {
			return fmt.Errorf("failed load from %q: %w", *tlsConfig.RootCACerts, ErrTLSCannotLoadCACerts)
		}
		// Use specific root CA set vs the host's set
		saramaTLSConfig.RootCAs = certPool
	}

	if tlsConfig.InsecureSkipVerify {
		saramaTLSConfig.InsecureSkipVerify = true
	}

	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = saramaTLSConfig

	return
}
