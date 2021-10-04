package kafkatest

import (
	"context"

	"github.com/ONSdigital/dp-kafka/v2/consumer"
)

// MessageConsumer is an extension of the moq ConsumerGroup, with channels
// and implementation of required functions to emulate a fully functional Kafka ConsumerGroup
type MessageConsumer struct {
	*cgInternal
	*IConsumerGroupMock
}

// cgInternal is an internal struct to keep track of the state and channels,
// which also provides the mock methods.
type cgInternal struct {
	cgChannels    *consumer.ConsumerGroupChannels
	isInitialised bool
}

// NewMessageConsumer creates a testing consumer with new consumerGroupChannels.
// isInitialisedAtCreationTime determines if the consumer is initialised or not when it's created
func NewMessageConsumer(isInitialisedAtCreationTime bool) *MessageConsumer {
	cgChannels := consumer.CreateConsumerGroupChannels(1)
	return NewMessageConsumerWithChannels(cgChannels, isInitialisedAtCreationTime)
}

// NewMessageConsumerWithChannels creates a testing consumer with the provided consumerGroupChannels
// isInitialisedAtCreationTime determines if the consumer is initialised or not when it's created
func NewMessageConsumerWithChannels(cgChannels *consumer.ConsumerGroupChannels, isInitialisedAtCreationTime bool) *MessageConsumer {

	internal := &cgInternal{
		isInitialised: false,
		cgChannels:    cgChannels,
	}
	if isInitialisedAtCreationTime {
		internal.isInitialised = true
	}

	return &MessageConsumer{
		internal,
		&IConsumerGroupMock{
			ChannelsFunc:                internal.channelsFunc,
			IsInitialisedFunc:           internal.isInitialisedFunc,
			InitialiseFunc:              internal.initialiseFunc,
			StopListeningToConsumerFunc: internal.stopListeningToConsumerFunc,
			CloseFunc:                   internal.closeFunc,
		},
	}
}

func (internal *cgInternal) initialiseFunc(ctx context.Context) error {
	if internal.isInitialised {
		return nil
	}
	internal.isInitialised = true
	close(internal.cgChannels.Ready)
	return nil
}

func (internal *cgInternal) isInitialisedFunc() bool {
	return internal.isInitialised
}

func (internal *cgInternal) channelsFunc() *consumer.ConsumerGroupChannels {
	return internal.cgChannels
}

func (internal *cgInternal) stopListeningToConsumerFunc(ctx context.Context) error {
	close(internal.cgChannels.Closer)
	close(internal.cgChannels.Closed)
	return nil
}

func (internal *cgInternal) closeFunc(ctx context.Context) error {
	select {
	case <-internal.cgChannels.Closer:
	default:
		close(internal.cgChannels.Closer)
	}

	select {
	case <-internal.cgChannels.Closed:
	default:
		close(internal.cgChannels.Closed)
	}

	close(internal.cgChannels.Errors)
	close(internal.cgChannels.Upstream)
	return nil
}
