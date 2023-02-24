package interfaces

import "github.com/Shopify/sarama"

//go:generate moq -out ../mock/sarama_cg.go -pkg mock . SaramaConsumerGroup
//go:generate moq -out ../mock/sarama_cg_session.go -pkg mock . SaramaConsumerGroupSession
//go:generate moq -out ../mock/sarama_cg_claim.go -pkg mock . SaramaConsumerGroupClaim

// SaramaConsumerGroup is an alias for sarama.ConsumerGroup
type SaramaConsumerGroup = sarama.ConsumerGroup

// SaramaConsumerGroupSession is an alias for sarama.ConsumerGroupSession
type SaramaConsumerGroupSession = sarama.ConsumerGroupSession

// SaramaConsumerGroupClaim is an alias for sarama.ConsumerGroupClaim
type SaramaConsumerGroupClaim = sarama.ConsumerGroupClaim

// ConsumerGroupInitialiser is a function that returns a sarama consumer group interface
type ConsumerGroupInitialiser = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)
