package kafka

import (
	"context"

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

// ConsumerGroupChannels represents the channels used by ConsumerGroup.
type ConsumerGroupChannels struct {
	Upstream chan Message
	Errors   chan error
	Ready    chan struct{}
	Consume  chan bool
	Closer   chan struct{}
	Closed   chan struct{}
}

// ProducerChannels represents the channels used by Producer.
type ProducerChannels struct {
	Output chan []byte
	Errors chan error
	Ready  chan struct{}
	Closer chan struct{}
	Closed chan struct{}
}

// Validate returns ErrNoChannel if any consumer channel is nil
func (consumerChannels *ConsumerGroupChannels) Validate() error {
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
		return &ErrNoChannel{ChannelNames: missingChannels}
	}
	return nil
}

// LogErrors creates a go-routine that waits on chErrors channel and logs any error received. It exits on chCloser channel event.
// Provided context and errMsg will be used in the log Event.
func (consumerChannels *ConsumerGroupChannels) LogErrors(ctx context.Context, errMsg string) {
	go func() {
		for {
			select {
			case err := <-consumerChannels.Errors:
				log.Error(ctx, errMsg, err)
			case <-consumerChannels.Closer:
				return
			}
		}
	}()
}

// Validate returns ErrNoChannel if any producer channel is nil
func (producerChannels *ProducerChannels) Validate() error {
	missingChannels := []string{}
	if producerChannels.Output == nil {
		missingChannels = append(missingChannels, Output)
	}
	if producerChannels.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if producerChannels.Ready == nil {
		missingChannels = append(missingChannels, Ready)
	}
	if producerChannels.Closer == nil {
		missingChannels = append(missingChannels, Closer)
	}
	if producerChannels.Closed == nil {
		missingChannels = append(missingChannels, Closed)
	}
	if len(missingChannels) > 0 {
		return &ErrNoChannel{ChannelNames: missingChannels}
	}
	return nil
}

// LogErrors creates a go-routine that waits on chErrors channel and logs any error received. It exits on chCloser channel event.
// Provided context and errMsg will be used in the log Event.
func (producerChannels *ProducerChannels) LogErrors(ctx context.Context, errMsg string) {
	go func() {
		for {
			select {
			case err := <-producerChannels.Errors:
				log.Error(ctx, errMsg, err)
			case <-producerChannels.Closer:
				return
			}
		}
	}()
}

// CreateConsumerGroupChannels initialises a ConsumerGroupChannels with new channels.
// You can provide the buffer size to determine the number of messages that will be buffered
// in the upstream channel (to receive messages)
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
		Upstream: chUpstream,
		Errors:   make(chan error),
		Ready:    make(chan struct{}),
		Consume:  make(chan bool),
		Closer:   make(chan struct{}),
		Closed:   make(chan struct{}),
	}
}

// CreateProducerChannels initialises a ProducerChannels with new channels.
func CreateProducerChannels() *ProducerChannels {
	return &ProducerChannels{
		Output: make(chan []byte),
		Errors: make(chan error),
		Ready:  make(chan struct{}),
		Closer: make(chan struct{}),
		Closed: make(chan struct{}),
	}
}
