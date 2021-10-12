package kafka

import "github.com/Shopify/sarama"

//go:generate moq -out ./mock/sarama_cg.go -pkg mock . SaramaConsumerGroup
//go:generate moq -out ./mock/sarama_cg_session.go -pkg mock . SaramaConsumerGroupSession
//go:generate moq -out ./mock/sarama_cg_claim.go -pkg mock . SaramaConsumerGroupClaim

// SaramaConsumerGroup is a wrapper around sarama.ConsumerGroup
type SaramaConsumerGroup = sarama.ConsumerGroup

// SaramaConsumerGroupSession is a wrapper around sarama.ConsumerGroupSession
type SaramaConsumerGroupSession = sarama.ConsumerGroupSession

// SaramaConsumerGroupClaim is a wrapper around sarama.ConsumerGroupClaim
type SaramaConsumerGroupClaim = sarama.ConsumerGroupClaim

// Types for sarama initialisers
type consumerGroupInitialiser = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)

var saramaNewConsumerGroup = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(addrs, groupID, config)
}
