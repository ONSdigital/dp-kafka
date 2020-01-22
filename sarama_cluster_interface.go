package kafka

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

//go:generate moq -out ./mock/sarama_cluster.go -pkg mock . SaramaCluster
//go:generate moq -out ./mock/sarama_cluster_consumer.go -pkg mock . SaramaClusterConsumer

// SaramaCluster is an interface representing the bsm sarama-cluster library.
type SaramaCluster interface {
	NewConsumer(addrs []string, groupID string, topics []string, config *cluster.Config) (SaramaClusterConsumer, error)
}

// SaramaClusterConsumer is an interface representing the bsm sarama-cluster Consumer struct
type SaramaClusterConsumer interface {
	Close() (err error)
	Messages() <-chan *sarama.ConsumerMessage
	CommitOffsets() error
	Errors() <-chan error
	Notifications() <-chan *cluster.Notification
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
}

// SaramaClusterClient implements SaramaCluster interface and wraps the real calls to bsm sarama-cluster library.
type SaramaClusterClient struct{}

// NewConsumer creates a new sarama cluster consumer.
func (c *SaramaClusterClient) NewConsumer(addrs []string, groupID string, topics []string, config *cluster.Config) (SaramaClusterConsumer, error) {
	return cluster.NewConsumer(addrs, groupID, topics, config)
}
