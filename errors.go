package kafka

import (
	"errors"
	"fmt"
)

// ErrShutdownTimedOut represents an error received due to the context deadline being exceeded
var ErrShutdownTimedOut = errors.New("Shutdown context timed out")

// ErrNoChannel is an Error type generated when a kafka producer or consumer is created with a missing channel
type ErrNoChannel struct {
	ChannelNames []string
}

func (e *ErrNoChannel) Error() string {
	return fmt.Sprintf("Missing channel(s): %v", e.ChannelNames)
}

// ErrBrokersNotReachable is an Error type for 'Broker Not reachable' with a list of unreacheable addresses
type ErrBrokersNotReachable struct {
	Addrs []string
}

func (e *ErrBrokersNotReachable) Error() string {
	return fmt.Sprintf("broker(s) not reachable at addresses: %v", e.Addrs)
}

// ErrInvalidBrokers is an Error type for 'Invalid topic info' with a list of invalid broker addresses
type ErrInvalidBrokers struct {
	Addrs []string
}

func (e *ErrInvalidBrokers) Error() string {
	return fmt.Sprintf("unexpected metadata response for broker(s). Invalid brokers: %v", e.Addrs)
}
