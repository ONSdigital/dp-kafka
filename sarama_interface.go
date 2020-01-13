package kafka

import "github.com/Shopify/sarama"

//go:generate moq -out ./mock/sarama.go -pkg mock . Sarama
//go:generate moq -out ./mock/sarama_async_producer.go -pkg mock . AsyncProducer

// Sarama is an interface representing the Sarama library.
type Sarama interface {
	NewAsyncProducer(addrs []string, conf *sarama.Config) (sarama.AsyncProducer, error)
}

// AsyncProducer is a wrapper arround sarama.AsyncProducer
type AsyncProducer = sarama.AsyncProducer

// SaramaClient implements Sarama interface and wraps the real calls to Sarama library.
type SaramaClient struct{}

// NewAsyncProducer creates a new sarama.AsyncProducer using the given broker addresses and configuration.
func (s *SaramaClient) NewAsyncProducer(addrs []string, conf *sarama.Config) (AsyncProducer, error) {
	return sarama.NewAsyncProducer(addrs, conf)
}
