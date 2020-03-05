package kafkatest

import (
	"context"

	kafka "github.com/ONSdigital/dp-kafka"
)

// MessageConsumer is an extension of the moq ConsumerGroup, with channels
// and implementation of required functions to emulate a fully functional Kafka ConsumerGroup
type MessageConsumer struct {
	cgInternal
	ConsumerGroupMock
}

// cgInternal is an internal struct to keep track of the state and channels,
// which also provides the mock methods.
type cgInternal struct {
	cgChannels    *kafka.ConsumerGroupChannels
	isInitialised bool
}

// NewMessageConsumer creates a testing consumer with new consumerGroupChannels.
// initialiseAtCreationTime determines if the consumer is initialised or not when it's created
func NewMessageConsumer(initialiseAtCreationTime bool) *MessageConsumer {
	cgChannels := kafka.CreateConsumerGroupChannels(true)
	return NewMessageConsumerWithChannels(&cgChannels, initialiseAtCreationTime)
}

// NewMessageConsumerWithChannels creates a testing consumer with the provided consumerGroupChannels
// initialiseAtCreationTime determines if the consumer is initialised or not when it's created
func NewMessageConsumerWithChannels(cgChannels *kafka.ConsumerGroupChannels, initialiseAtCreationTime bool) *MessageConsumer {

	internal := cgInternal{
		isInitialised: false,
		cgChannels:    cgChannels,
	}
	if initialiseAtCreationTime {
		internal.isInitialised = true
	}

	return &MessageConsumer{
		internal,
		ConsumerGroupMock{
			ChannelsFunc:                internal.channelsFunc,
			IsInitialisedFunc:           internal.isInitialisedFunc,
			InitialiseFunc:              internal.initialiseFunc,
			ReleaseFunc:                 internal.releaseFunc,
			CommitAndReleaseFunc:        internal.commitAndReleaseFunc,
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
	close(internal.cgChannels.Init)
	return nil
}

func (internal *cgInternal) isInitialisedFunc() bool {
	return internal.isInitialised
}

func (internal *cgInternal) channelsFunc() *kafka.ConsumerGroupChannels {
	return internal.cgChannels
}

func (internal *cgInternal) releaseFunc() {
	internal.cgChannels.UpstreamDone <- true
}

func (internal *cgInternal) commitAndReleaseFunc(msg kafka.Message) {
	msg.Commit()
	internal.cgChannels.UpstreamDone <- true
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
