package kafka

import (
	cluster "github.com/bsm/sarama-cluster"
)

//go:generate moq -out ./mock/sarama_cluster.go -pkg mock . SaramaCluster

// SaramaCluster is an interface representing the bsm sarama-cluster library.
type SaramaCluster interface {
	NewConsumer(addrs []string, groupID string, topics []string, config *cluster.Config) (*cluster.Consumer, error)
}

// SaramaClusterClient implements SaramaCluster interface and wraps the real calls to bsm sarama-cluster library.
type SaramaClusterClient struct{}

// NewConsumer creates a new sarama cluster consumer.
func (c *SaramaClusterClient) NewConsumer(addrs []string, groupID string, topics []string, config *cluster.Config) (*cluster.Consumer, error) {
	return cluster.NewConsumer(addrs, groupID, topics, config)
}
