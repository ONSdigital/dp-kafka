package kafkatest

import (
	"context"

	kafka "github.com/ONSdigital/dp-kafka"
)

//go:generate moq -out ./mock_consumer_group.go -pkg kafkatest . ConsumerGroup
//go:generate moq -out ./mock_producer.go -pkg kafkatest . Producer

// ConsumerGroup is an interface representing a Kafka Consumer Group.
// MessageConsumer
type ConsumerGroup interface {
	Channels() *kafka.ConsumerGroupChannels
	IsInitialised() bool
	Initialise(ctx context.Context) error
	Release()
	CommitAndRelease(msg kafka.Message)
	StopListeningToConsumer(ctx context.Context) (err error)
	Close(ctx context.Context) (err error)
}

// Producer is an interface representing a Kafka Producer
type Producer interface {
	Channels() *kafka.ProducerChannels
	IsInitialised() bool
	Initialise(ctx context.Context) error
	Close(ctx context.Context) (err error)
}
