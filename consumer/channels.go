package consumer

import (
	"errors"

	"github.com/ONSdigital/dp-kafka/v3/kafkaerror"
	"github.com/ONSdigital/dp-kafka/v3/message"
	"github.com/ONSdigital/log.go/v2/log"
)

// channel names
const (
	Errors       = "Errors"
	Ready        = "Ready"
	Consume      = "Consume"
	Closer       = "Closer"
	Closed       = "Closed"
	Upstream     = "Upstream"
	UpstreamDone = "UpstreamDone"
	Output       = "Output"
)

// Channels represents the channels used by ConsumerGroup.
type Channels struct {
	Upstream chan message.Message
	Errors   chan error
	Ready    chan struct{} // TOO we may want to rename this to 'Initialised', as it may not be 'ready' to consume
	Consume  chan bool
	Closer   chan struct{}
	Closed   chan struct{}
}

// Validate returns an Error with a list of missing channels if any consumer channel is nil
func (consumerChannels *Channels) Validate() error {
	missingChannels := []string{}
	if consumerChannels.Upstream == nil {
		missingChannels = append(missingChannels, Upstream)
	}
	if consumerChannels.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if consumerChannels.Ready == nil {
		missingChannels = append(missingChannels, Ready)
	}
	if consumerChannels.Consume == nil {
		missingChannels = append(missingChannels, Consume)
	}
	if consumerChannels.Closer == nil {
		missingChannels = append(missingChannels, Closer)
	}
	if consumerChannels.Closed == nil {
		missingChannels = append(missingChannels, Closed)
	}
	if len(missingChannels) > 0 {
		return kafkaerror.NewError(
			errors.New("failed to validate consumer group because some channels are missing"),
			log.Data{"missing_channels": missingChannels},
		)
	}
	return nil
}

// CreateConsumerGroupChannels initialises a ConsumerGroupChannels with new channels.
// You can provide the buffer size to determine the number of messages that will be buffered
// in the upstream channel (to receive messages)
func CreateConsumerGroupChannels(bufferSize int) *Channels {
	var chUpstream chan message.Message
	if bufferSize > 0 {
		// Upstream channel buffered
		chUpstream = make(chan message.Message, bufferSize)
	} else {
		// Upstream channel un-buffered
		chUpstream = make(chan message.Message)
	}
	return &Channels{
		Upstream: chUpstream,
		Errors:   make(chan error),
		Ready:    make(chan struct{}),
		Consume:  make(chan bool),
		Closer:   make(chan struct{}),
		Closed:   make(chan struct{}),
	}
}
