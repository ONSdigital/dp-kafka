package producer

import (
	"errors"

	"github.com/ONSdigital/dp-kafka/v3/kafkaerror"
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

// Channels represents the channels used by Producer.
type Channels struct {
	Output chan []byte
	Errors chan error
	Ready  chan struct{}
	Closer chan struct{}
	Closed chan struct{}
}

// Validate returns an error with a list of missing channels if any producer channel is nil
func (producerChannels *Channels) Validate() error {
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
		return kafkaerror.NewError(
			errors.New("failed to validate producer because some channels are missing"),
			log.Data{"missing_channels": missingChannels},
		)
	}
	return nil
}

// CreateProducerChannels initialises a ProducerChannels with new channels.
func CreateProducerChannels() *Channels {
	return &Channels{
		Output: make(chan []byte),
		Errors: make(chan error),
		Ready:  make(chan struct{}),
		Closer: make(chan struct{}),
		Closed: make(chan struct{}),
	}
}
