package kafka

import (
	"context"

	"github.com/ONSdigital/log.go/log"
)

// channel names
const (
	Errors       = "Errors"
	Init         = "Init"
	Closer       = "Closer"
	Closed       = "Closed"
	Upstream     = "Upstream"
	UpstreamDone = "UpstreamDone"
	Output       = "Output"
)

// ConsumerGroupChannels represents the channels used by ConsumerGroup.
type ConsumerGroupChannels struct {
	Upstream     chan Message
	Errors       chan error
	Closer       chan struct{}
	Closed       chan struct{}
	UpstreamDone chan bool
}

// ProducerChannels represents the channels used by Producer.
type ProducerChannels struct {
	Output chan []byte
	Errors chan error
	Init   chan struct{}
	Closer chan struct{}
	Closed chan struct{}
}

// Validate returns ErrNoChannel if any consumer channel is nil
func (cCh *ConsumerGroupChannels) Validate() error {
	missingChannels := []string{}
	if cCh.Upstream == nil {
		missingChannels = append(missingChannels, Upstream)
	}
	if cCh.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if cCh.Closer == nil {
		missingChannels = append(missingChannels, Closer)
	}
	if cCh.Closed == nil {
		missingChannels = append(missingChannels, Closed)
	}
	if cCh.UpstreamDone == nil {
		missingChannels = append(missingChannels, UpstreamDone)
	}
	if len(missingChannels) > 0 {
		return &ErrNoChannel{ChannelNames: missingChannels}
	}
	return nil
}

// LogErrors creates a go-routine that waits on chErrors channel and logs any error received. It exits on chCloser channel event.
// Provided context and errMsg will be used in the log Event.
func (cCh *ConsumerGroupChannels) LogErrors(ctx context.Context, errMsg string) {
	go func() {
		for true {
			select {
			case err := <-cCh.Errors:
				log.Event(ctx, errMsg, log.Error(err))
			case <-cCh.Closer:
				return
			}
		}
	}()
}

// Validate returns ErrNoChannel if any producer channel is nil
func (pCh *ProducerChannels) Validate() error {
	missingChannels := []string{}
	if pCh.Output == nil {
		missingChannels = append(missingChannels, Output)
	}
	if pCh.Errors == nil {
		missingChannels = append(missingChannels, Errors)
	}
	if pCh.Init == nil {
		missingChannels = append(missingChannels, Init)
	}
	if pCh.Closer == nil {
		missingChannels = append(missingChannels, Closer)
	}
	if pCh.Closed == nil {
		missingChannels = append(missingChannels, Closed)
	}
	if len(missingChannels) > 0 {
		return &ErrNoChannel{ChannelNames: missingChannels}
	}
	return nil
}

// LogErrors creates a go-routine that waits on chErrors channel and logs any error received. It exits on chCloser channel event.
// Provided context and errMsg will be used in the log Event.
func (pCh *ProducerChannels) LogErrors(ctx context.Context, errMsg string) {
	go func() {
		for true {
			select {
			case err := <-pCh.Errors:
				log.Event(ctx, errMsg, log.Error(err))
			case <-pCh.Closer:
				return
			}
		}
	}()
}

// CreateConsumerGroupChannels initialises a ConsumerGroupChannels with new channels according to sync
func CreateConsumerGroupChannels(sync bool) ConsumerGroupChannels {
	var chUpstream chan Message
	if sync {
		// Sync -> upstream channel buffered, so we can send-and-wait for upstreamDone
		chUpstream = make(chan Message, 1)
	} else {
		// not sync -> upstream channel un-buffered
		chUpstream = make(chan Message)
	}
	return ConsumerGroupChannels{
		Upstream:     chUpstream,
		Errors:       make(chan error),
		Closer:       make(chan struct{}),
		Closed:       make(chan struct{}),
		UpstreamDone: make(chan bool, 1),
	}
}

// CreateProducerChannels initialises a ProducerChannels with new channels.
func CreateProducerChannels() ProducerChannels {
	return ProducerChannels{
		Output: make(chan []byte),
		Errors: make(chan error),
		Init:   make(chan struct{}),
		Closer: make(chan struct{}),
		Closed: make(chan struct{}),
	}
}
