package kafka

import "github.com/Shopify/sarama"

//go:generate moq -out ./mock/sarama_async_producer.go -pkg mock . AsyncProducer
//go:generate moq -out ./mock/sarama_consumer_group.go -pkg mock . SaramaConsumerGroup

// AsyncProducer is a wrapper around sarama.AsyncProducer
type AsyncProducer = sarama.AsyncProducer

// SaramaConsumerGroup is a wrapper around sarama.ConsumerGroup
type SaramaConsumerGroup = sarama.ConsumerGroup

// Types for sarama initialisers
type (
	producerInitialiser      = func(addrs []string, config *sarama.Config) (sarama.AsyncProducer, error)
	consumerGroupInitialiser = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)
)

var saramaNewAsyncProducer = func(addrs []string, config *sarama.Config) (sarama.AsyncProducer, error) {
	return sarama.NewAsyncProducer(addrs, config)
}

var saramaNewConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(addrs, groupID, config)
}
