package kafkatest

import (
	"context"

	"github.com/ONSdigital/dp-kafka/v3/kafkatest/mock"
	"github.com/ONSdigital/dp-kafka/v3/producer"
)

// MessageProducer is an extension of the moq Producer, with channels
// and implementation of required functions to emulate a fully functional kafka Producer.
type MessageProducer struct {
	*pInternal
	*mock.ProducerMock
}

// pInternal is an internal struct to keep track of the state and channels,
// which also provides the mock methods.
type pInternal struct {
	pChannels     *producer.Channels
	isInitialised bool
}

// NewMessageProducer creates a testing producer with new producerChannels.
// isInitialisedAtCreationTime determines if the producer is initialised or not when it's created
func NewMessageProducer(isInitialisedAtCreationTime bool) *MessageProducer {
	pChannels := producer.CreateProducerChannels()
	return NewMessageProducerWithChannels(pChannels, isInitialisedAtCreationTime)
}

// NewMessageProducerWithChannels creates a testing producer with the provided producerChannels.
// isInitialisedAtCreationTime determines if the producer is initialised or not when it's created
func NewMessageProducerWithChannels(pChannels *producer.Channels, isInitialisedAtCreationTime bool) *MessageProducer {

	internal := &pInternal{
		isInitialised: false,
		pChannels:     pChannels,
	}
	if isInitialisedAtCreationTime {
		internal.isInitialised = true
	}

	return &MessageProducer{
		internal,
		&mock.ProducerMock{
			InitialiseFunc:    internal.initialiseFunc,
			IsInitialisedFunc: internal.isInitialisedFunc,
			ChannelsFunc:      internal.channelsFunc,
			CloseFunc:         internal.closeFunc,
		},
	}
}

func (internal *pInternal) initialiseFunc(ctx context.Context) error {
	if internal.isInitialised {
		return nil
	}
	internal.isInitialised = true
	close(internal.pChannels.Ready)
	return nil
}

func (internal *pInternal) isInitialisedFunc() bool {
	return internal.isInitialised
}

func (internal *pInternal) channelsFunc() *producer.Channels {
	return internal.pChannels
}

func (internal *pInternal) closeFunc(ctx context.Context) (err error) {
	close(internal.pChannels.Closer)
	close(internal.pChannels.Closed)
	close(internal.pChannels.Errors)
	close(internal.pChannels.Output)
	return nil
}
