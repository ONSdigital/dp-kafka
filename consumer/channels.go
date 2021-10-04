package consumer

import (
	"context"

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

// ConsumerGroupChannels represents the channels used by ConsumerGroup.
type ConsumerGroupChannels struct {
	Upstream chan message.Message
	Errors   chan error
	Ready    chan struct{} // TOO we may want to rename this to 'Initialised', as it may not be 'ready' to consume
	Consume  chan bool
	Closer   chan struct{}
	Closed   chan struct{}
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

// CreateConsumerGroupChannels initialises a ConsumerGroupChannels with new channels.
// You can provide the buffer size to determine the number of messages that will be buffered
// in the upstream channel (to receive messages)
func CreateConsumerGroupChannels(bufferSize int) *ConsumerGroupChannels {
	var chUpstream chan message.Message
	if bufferSize > 0 {
		// Upstream channel buffered
		chUpstream = make(chan message.Message, bufferSize)
	} else {
		// Upstream channel un-buffered
		chUpstream = make(chan message.Message)
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
