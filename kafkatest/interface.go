package kafkatest

import (
	"context"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v3/consumer"
	"github.com/ONSdigital/dp-kafka/v3/producer"
)

//go:generate moq -out ./mock/consumer_group.go -pkg mock . ConsumerGroup
//go:generate moq -out ./mock/producer.go -pkg mock . Producer

// ConsumerGroup is an interface representing a Kafka Consumer Group, as implemented in dp-kafka/consumer
type ConsumerGroup interface {
	Channels() *consumer.Channels
	State() string
	RegisterHandler(ctx context.Context, h consumer.Handler) error
	RegisterBatchHandler(ctx context.Context, batchHandler consumer.BatchHandler) error
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	IsInitialised() bool
	Initialise(ctx context.Context) error
	Start() error
	Stop()
	LogErrors(ctx context.Context)
	Close(ctx context.Context) (err error)
}

// Producer is an interface representing a Kafka Producer, as implemented in dp-kafka/producer
type Producer interface {
	Channels() *producer.Channels
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	LogErrors(ctx context.Context)
	IsInitialised() bool
	Initialise(ctx context.Context) error
	Close(ctx context.Context) (err error)
}
