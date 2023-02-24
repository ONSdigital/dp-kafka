package interfaces

import "github.com/Shopify/sarama"

//go:generate moq -out ../mock/sarama_broker.go -pkg mock . SaramaBroker

type SaramaBroker interface {
	Addr() string
	Connected() (bool, error)
	Open(conf *sarama.Config) error
	GetMetadata(request *sarama.MetadataRequest) (*sarama.MetadataResponse, error)
	Close() error
}

// BrokerGenerator is a function that returns a sarama broker interface
type BrokerGenerator = func(addr string) SaramaBroker
