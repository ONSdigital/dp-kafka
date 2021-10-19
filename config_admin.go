package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// AdminConfig exposes the optional configurable parameters for an admin client to overwrite default Sarama config values.
// Any value that is not provied will use the default Sarama config value.
type AdminConfig struct {
	KafkaVersion   *string
	KeepAlive      *time.Duration
	RetryBackoff   *time.Duration
	RetryMax       *int
	SecurityConfig *SecurityConfig
}

// Get creates a default sarama config and overwrites any values provided in pConfig
func (a *AdminConfig) Get() (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	if a.KafkaVersion != nil {
		var err error
		if cfg.Version, err = sarama.ParseKafkaVersion(*a.KafkaVersion); err != nil {
			return nil, fmt.Errorf("error parsing kafka version for admin config: %w", err)
		}
	}
	if a.KeepAlive != nil {
		cfg.Net.KeepAlive = *a.KeepAlive
	}
	if a.RetryMax != nil {
		cfg.Admin.Retry.Max = *a.RetryMax
	}
	if a.RetryBackoff != nil {
		cfg.Admin.Retry.Backoff = *a.RetryBackoff
	}
	if err := addAnyTLS(a.SecurityConfig, cfg); err != nil {
		return nil, fmt.Errorf("error adding tls for admin config: %w", err)
	}
	return cfg, nil
}
