package interfaces

import "github.com/Shopify/sarama"

//go:generate moq -out ../mock/sarama_async_producer.go -pkg mock . SaramaAsyncProducer

// SaramaAsyncProducer is a wrapper around sarama.AsyncProducer
type SaramaAsyncProducer = sarama.AsyncProducer

// Types for sarama initialisers
type ProducerInitialiser = func(addrs []string, config *sarama.Config) (sarama.AsyncProducer, error)

var SaramaNewAsyncProducer = sarama.NewAsyncProducer
