package kafka

import (
	"errors"

	"github.com/ONSdigital/log.go/v2/log"
)

// channel names
const (
	Errors       = "Errors"
	Initialised  = "Initialised"
	Consume      = "Consume"
	Closer       = "Closer"
	Closed       = "Closed"
	Upstream     = "Upstream"
	UpstreamDone = "UpstreamDone"
	Output       = "Output"
)

// ConsumerGroupChannels represents the channels used by ConsumerGroup.
type ConsumerGroupChannels struct {
	Upstream    chan Message
	Errors      chan error
	Initialised chan struct{}
	Consume     chan bool
	Closer      chan struct{}
	Closed      chan struct{}
	State       *ConsumerStateChannels
}

// ConsumerStateChannels represents the channels that are used to notify of consumer-group state changes
type ConsumerStateChannels struct {
	Initialising chan struct{}
	Stopped      chan struct{}
	Starting     chan struct{}
	Consuming    chan struct{}
	Stopping     chan struct{}
	Closing      chan struct{}
}

// ProducerChannels represents the channels used by Producer.
type ProducerChannels struct {
	Output      chan []byte
	Errors      chan error
	Initialised chan struct{}
	Closer      chan struct{}
	Closed      chan struct{}
}

// CreateConsumerGroupChannels initialises a ConsumerGroupChannels with new channels.
// You can provide the buffer size to determine the number of messages that will be buffered
// in the upstream channel (to receive messages)
// The State channels are not initialised until the state machine is created.
func CreateConsumerGroupChannels(bufferSize int) *ConsumerGroupChannels {
	var chUpstream chan Message
	if bufferSize > 0 {
		// Upstream channel buffered
		chUpstream = make(chan Message, bufferSize)
	} else {
		// Upstream channel un-buffered
		chUpstream = make(chan Message)
	}
	return &ConsumerGroupChannels{
		Upstream:    chUpstream,
		Errors:      make(chan error),
		Initialised: make(chan struct{}),
		Consume:     make(chan bool),
		Closer:      make(chan struct{}),
		Closed:      make(chan struct{}),
	}
}

// CreateProducerChannels initialises a ProducerChannels with new channels.
func CreateProducerChannels() *ProducerChannels {
	return &ProducerChannels{
		Output:      make(chan []byte),
		Errors:      make(chan error),
		Initialised: make(chan struct{}),
		Closer:      make(chan struct{}),
		Closed:      make(chan struct{}),
	}
}

// Validate returns an Error with a list of missing channels if any consumer channel is nil
func (consumerChannels *ConsumerGroupChannels) Validate() error {
	missingChannels := []string{}
	if consumerChannels.Upstream == nil {
		missingChannels = append(missingChannels, Upstream)
	}
	if consumerChannels.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if consumerChannels.Initialised == nil {
		missingChannels = append(missingChannels, Initialised)
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
		return NewError(
			errors.New("failed to validate consumer group because some channels are missing"),
			log.Data{"missing_channels": missingChannels},
		)
	}
	return nil
}

// Validate returns an error with a list of missing channels if any producer channel is nil
func (producerChannels *ProducerChannels) Validate() error {
	missingChannels := []string{}
	if producerChannels.Output == nil {
		missingChannels = append(missingChannels, Output)
	}
	if producerChannels.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if producerChannels.Initialised == nil {
		missingChannels = append(missingChannels, Initialised)
	}
	if producerChannels.Closer == nil {
		missingChannels = append(missingChannels, Closer)
	}
	if producerChannels.Closed == nil {
		missingChannels = append(missingChannels, Closed)
	}
	if len(missingChannels) > 0 {
		return NewError(
			errors.New("failed to validate producer because some channels are missing"),
			log.Data{"missing_channels": missingChannels},
		)
	}
	return nil
}
