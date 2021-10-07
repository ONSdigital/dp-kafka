package consumer

import (
	"errors"
	"fmt"

	"github.com/ONSdigital/log.go/v2/log"
)

// Error is the handler package's error type. Is not meant to be compared as a
// a type, but information should be extracted via the interfaces
// it implements with callback functions. Is not guaranteed to remain exported
// so shouldn't be treated as such.
type Error struct {
	err     error
	logData map[string]interface{}
}

// NewError creates a new Error
func NewError(err error, logData map[string]interface{}) *Error {
	return &Error{
		err:     err,
		logData: logData,
	}
}

// Error implements the Go standard error interface
func (e *Error) Error() string {
	if e.err == nil {
		return "{nil error}"
	}
	return e.err.Error()
}

// LogData implements the DataLogger interface which allows you extract
// embedded log.Data from an error
func (e *Error) LogData() map[string]interface{} {
	if e.logData == nil {
		return log.Data{}
	}
	return e.logData
}

// Unwrap returns the wrapped error
func (e *Error) Unwrap() error {
	return e.err
}

// unwrapLogData recursively unwraps logData from an error
func unwrapLogData(err error) log.Data {
	var data []log.Data

	for err != nil && errors.Unwrap(err) != nil {
		if lderr, ok := err.(dataLogger); ok {
			if d := lderr.LogData(); d != nil {
				data = append(data, d)
			}
		}

		err = errors.Unwrap(err)
	}

	// flatten []log.Data into single log.Data with slice
	// entries for duplicate keyed entries, but not for duplicate
	// key-value pairs
	logData := log.Data{}
	for _, d := range data {
		for k, v := range d {
			if val, ok := logData[k]; ok {
				if val != v {
					if s, ok := val.([]interface{}); ok {
						s = append(s, v)
						logData[k] = s
					} else {
						logData[k] = []interface{}{val, v}
					}
				}
			} else {
				logData[k] = v
			}
		}
	}

	return logData
}

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

// ErrBrokersNotReachable is an Error type for 'Broker Not reachable' with a list of unreachable addresses
type ErrBrokersNotReachable struct {
	Addrs []string
}

// Error returns the error message with a list of unreachable addresses
func (e *ErrBrokersNotReachable) Error() string {
	return fmt.Sprintf("broker(s) not reachable at addresses: %v", e.Addrs)
}

// ErrInvalidBrokers is an Error type for 'Invalid topic info' with a list of invalid broker addresses
type ErrInvalidBrokers struct {
	Addrs []string
}

// Error returns the error message with a list of broker addresses that returned unexpected responses
func (e *ErrInvalidBrokers) Error() string {
	return fmt.Sprintf("unexpected metadata response for broker(s). Invalid brokers: %v", e.Addrs)
}
