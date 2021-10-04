package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	saramatls "github.com/Shopify/sarama/tools/tls"
)

const certPrefix = "-----BEGIN " // magic string for PEM/Cert/Key (when not file path)

// ErrTLSCannotLoadCACerts is returned when the certs file cannot be loaded
var ErrTLSCannotLoadCACerts = errors.New("cannot load CA Certs")

var messageConsumeTimeout = time.Second * 10

// ProducerConfig exposes the optional configurable parameters for a producer to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type ProducerConfig struct {
	KafkaVersion     *string
	MaxMessageBytes  *int
	RetryMax         *int
	KeepAlive        *time.Duration
	RetryBackoff     *time.Duration
	RetryBackoffFunc *func(retries, maxRetries int) time.Duration
	SecurityConfig   *SecurityConfig
}

// ConsumerGroupConfig exposes the optional configurable parameters for a consumer group, to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type ConsumerGroupConfig struct {
	KafkaVersion     *string
	KeepAlive        *time.Duration
	RetryBackoff     *time.Duration
	RetryBackoffFunc *func(retries int) time.Duration
	Offset           *int64
	SecurityConfig   *SecurityConfig
}

// AdminConfig exposes the optional configurable parameters for an admin client to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type AdminConfig struct {
	KafkaVersion   *string
	KeepAlive      *time.Duration
	RetryBackoff   *time.Duration
	RetryMax       *int
	SecurityConfig *SecurityConfig
}

// SecurityConfig is common to producers and consumer configs, above
type SecurityConfig struct {
	RootCACerts        string
	ClientCert         string
	ClientKey          string
	InsecureSkipVerify bool
}

// GetProducerConfig creates a default sarama config and overwrites any values provided in pConfig
func GetProducerConfig(pConfig *ProducerConfig) (config *sarama.Config, err error) {
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
		if err = addAnyTLS(pConfig.SecurityConfig, config); err != nil {
			return nil, err
		}
	}
	return config, nil
}

// GetConsumerGroupConfig creates a default sarama config and overwrites any values provided in cgConfig
func GetConsumerGroupConfig(cgConfig *ConsumerGroupConfig) (config *sarama.Config, err error) {
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
		if err = addAnyTLS(cgConfig.SecurityConfig, config); err != nil {
			return nil, err
		}
	}
	return config, nil
}

// GetAdminConfig creates a default sarama config and overwrites any values provided in pConfig
func GetAdminConfig(cfg *AdminConfig) (config *sarama.Config, err error) {
	config = sarama.NewConfig()
	if cfg != nil {
		if cfg.KafkaVersion != nil {
			if config.Version, err = sarama.ParseKafkaVersion(*cfg.KafkaVersion); err != nil {
				return nil, err
			}
		}
		if cfg.KeepAlive != nil {
			config.Net.KeepAlive = *cfg.KeepAlive
		}
		if cfg.RetryMax != nil {
			config.Admin.Retry.Max = *cfg.RetryMax
		}
		if cfg.RetryBackoff != nil {
			config.Admin.Retry.Backoff = *cfg.RetryBackoff
		}
		if err = addAnyTLS(cfg.SecurityConfig, config); err != nil {
			return nil, err
		}
	}
	return config, nil
}

func expandNewlines(s string) string {
	return strings.ReplaceAll(s, `\n`, "\n")
}

func addAnyTLS(tlsConfig *SecurityConfig, saramaConfig *sarama.Config) (err error) {
	if tlsConfig == nil {
		return
	}

	var saramaTLSConfig *tls.Config
	if strings.HasPrefix(tlsConfig.ClientCert, certPrefix) {
		// create cert from strings (not files), cf https://github.com/Shopify/sarama/blob/master/tools/tls/config.go
		var cert tls.Certificate
		if cert, err = tls.X509KeyPair(
			[]byte(expandNewlines(tlsConfig.ClientCert)),
			[]byte(expandNewlines(tlsConfig.ClientKey)),
		); err != nil {
			return
		}
		saramaTLSConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}
	} else {
		// cert in files
		if saramaTLSConfig, err = saramatls.NewConfig(tlsConfig.ClientCert, tlsConfig.ClientKey); err != nil {
			return
		}
	}

	if tlsConfig.RootCACerts != "" {
		var rootCAsBytes []byte
		if strings.HasPrefix(tlsConfig.RootCACerts, certPrefix) {
			rootCAsBytes = []byte(expandNewlines(tlsConfig.RootCACerts))
		} else {
			if rootCAsBytes, err = ioutil.ReadFile(tlsConfig.RootCACerts); err != nil {
				return fmt.Errorf("failed read from %q: %w", tlsConfig.RootCACerts, err)
			}
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(rootCAsBytes) {
			return fmt.Errorf("failed load from %q: %w", tlsConfig.RootCACerts, ErrTLSCannotLoadCACerts)
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

func GetSecurityConfig(caCerts, clientCert, clientKey string, skipVerify bool) *SecurityConfig {
	return &SecurityConfig{
		RootCACerts:        caCerts,
		ClientCert:         clientCert,
		ClientKey:          clientKey,
		InsecureSkipVerify: skipVerify,
	}
}
