package producer

import (
	"errors"
	"fmt"
)

// ErrShutdownTimedOut represents an error received due to the context deadline being exceeded
var ErrShutdownTimedOut = errors.New("shutdown context timed out")

// ErrInitSarama is used when Sarama client cannot be initialised
var ErrInitSarama = errors.New("failed to initialise client")

// ErrUninitialisedProducer is used when a caller tries to send a message to the output channel with an uninitialised producer.
var ErrUninitialisedProducer = errors.New("producer is not initialised")

// ErrNoChannel is an Error type generated when a kafka producer or consumer is created with a missing channel
type ErrNoChannel struct {
	ChannelNames []string
}

// Error returns the error message with a list of missing channels
func (e *ErrNoChannel) Error() string {
	return fmt.Sprintf("Missing channel(s): %v", e.ChannelNames)
}
