//revive:disable:var-naming fixed in future version inorder to avoid breaking change
package interfaces

// TODO rename package to something more adequate
//revive:enable:var-naming

import "github.com/Shopify/sarama"

//go:generate moq -out ../mock/sarama_async_producer.go -pkg mock . SaramaAsyncProducer

// SaramaAsyncProducer is an alias for sarama.AsyncProducer
type SaramaAsyncProducer = sarama.AsyncProducer

// ProducerInitialiser is a function that returns a sarama async producer interface
type ProducerInitialiser = func(addrs []string, config *sarama.Config) (sarama.AsyncProducer, error)
