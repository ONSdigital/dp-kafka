// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package kafkatest

import (
	"context"
	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v2"
	"sync"
)

// Ensure, that IProducerMock does implement kafka.IProducer.
// If this is not the case, regenerate this file with moq.
var _ kafka.IProducer = &IProducerMock{}

// IProducerMock is a mock implementation of kafka.IProducer.
//
// 	func TestSomethingThatUsesIProducer(t *testing.T) {
//
// 		// make and configure a mocked kafka.IProducer
// 		mockedIProducer := &IProducerMock{
// 			AddHeaderFunc: func(key string, value string)  {
// 				panic("mock out the AddHeader method")
// 			},
// 			ChannelsFunc: func() *kafka.ProducerChannels {
// 				panic("mock out the Channels method")
// 			},
// 			CheckerFunc: func(ctx context.Context, state *health.CheckState) error {
// 				panic("mock out the Checker method")
// 			},
// 			CloseFunc: func(ctx context.Context) error {
// 				panic("mock out the Close method")
// 			},
// 			InitialiseFunc: func(ctx context.Context) error {
// 				panic("mock out the Initialise method")
// 			},
// 			IsInitialisedFunc: func() bool {
// 				panic("mock out the IsInitialised method")
// 			},
// 		}
//
// 		// use mockedIProducer in code that requires kafka.IProducer
// 		// and then make assertions.
//
// 	}
type IProducerMock struct {
	// AddHeaderFunc mocks the AddHeader method.
	AddHeaderFunc func(key string, value string)

	// ChannelsFunc mocks the Channels method.
	ChannelsFunc func() *kafka.ProducerChannels

	// CheckerFunc mocks the Checker method.
	CheckerFunc func(ctx context.Context, state *health.CheckState) error

	// CloseFunc mocks the Close method.
	CloseFunc func(ctx context.Context) error

	// InitialiseFunc mocks the Initialise method.
	InitialiseFunc func(ctx context.Context) error

	// IsInitialisedFunc mocks the IsInitialised method.
	IsInitialisedFunc func() bool

	// calls tracks calls to the methods.
	calls struct {
		// AddHeader holds details about calls to the AddHeader method.
		AddHeader []struct {
			// Key is the key argument value.
			Key string
			// Value is the value argument value.
			Value string
		}
		// Channels holds details about calls to the Channels method.
		Channels []struct {
		}
		// Checker holds details about calls to the Checker method.
		Checker []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// State is the state argument value.
			State *health.CheckState
		}
		// Close holds details about calls to the Close method.
		Close []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
		// Initialise holds details about calls to the Initialise method.
		Initialise []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
		// IsInitialised holds details about calls to the IsInitialised method.
		IsInitialised []struct {
		}
	}
	lockAddHeader     sync.RWMutex
	lockChannels      sync.RWMutex
	lockChecker       sync.RWMutex
	lockClose         sync.RWMutex
	lockInitialise    sync.RWMutex
	lockIsInitialised sync.RWMutex
}

// AddHeader calls AddHeaderFunc.
func (mock *IProducerMock) AddHeader(key string, value string) {
	if mock.AddHeaderFunc == nil {
		panic("IProducerMock.AddHeaderFunc: method is nil but IProducer.AddHeader was just called")
	}
	callInfo := struct {
		Key   string
		Value string
	}{
		Key:   key,
		Value: value,
	}
	mock.lockAddHeader.Lock()
	mock.calls.AddHeader = append(mock.calls.AddHeader, callInfo)
	mock.lockAddHeader.Unlock()
	mock.AddHeaderFunc(key, value)
}

// AddHeaderCalls gets all the calls that were made to AddHeader.
// Check the length with:
//     len(mockedIProducer.AddHeaderCalls())
func (mock *IProducerMock) AddHeaderCalls() []struct {
	Key   string
	Value string
} {
	var calls []struct {
		Key   string
		Value string
	}
	mock.lockAddHeader.RLock()
	calls = mock.calls.AddHeader
	mock.lockAddHeader.RUnlock()
	return calls
}

// Channels calls ChannelsFunc.
func (mock *IProducerMock) Channels() *kafka.ProducerChannels {
	if mock.ChannelsFunc == nil {
		panic("IProducerMock.ChannelsFunc: method is nil but IProducer.Channels was just called")
	}
	callInfo := struct {
	}{}
	mock.lockChannels.Lock()
	mock.calls.Channels = append(mock.calls.Channels, callInfo)
	mock.lockChannels.Unlock()
	return mock.ChannelsFunc()
}

// ChannelsCalls gets all the calls that were made to Channels.
// Check the length with:
//     len(mockedIProducer.ChannelsCalls())
func (mock *IProducerMock) ChannelsCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockChannels.RLock()
	calls = mock.calls.Channels
	mock.lockChannels.RUnlock()
	return calls
}

// Checker calls CheckerFunc.
func (mock *IProducerMock) Checker(ctx context.Context, state *health.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("IProducerMock.CheckerFunc: method is nil but IProducer.Checker was just called")
	}
	callInfo := struct {
		Ctx   context.Context
		State *health.CheckState
	}{
		Ctx:   ctx,
		State: state,
	}
	mock.lockChecker.Lock()
	mock.calls.Checker = append(mock.calls.Checker, callInfo)
	mock.lockChecker.Unlock()
	return mock.CheckerFunc(ctx, state)
}

// CheckerCalls gets all the calls that were made to Checker.
// Check the length with:
//     len(mockedIProducer.CheckerCalls())
func (mock *IProducerMock) CheckerCalls() []struct {
	Ctx   context.Context
	State *health.CheckState
} {
	var calls []struct {
		Ctx   context.Context
		State *health.CheckState
	}
	mock.lockChecker.RLock()
	calls = mock.calls.Checker
	mock.lockChecker.RUnlock()
	return calls
}

// Close calls CloseFunc.
func (mock *IProducerMock) Close(ctx context.Context) error {
	if mock.CloseFunc == nil {
		panic("IProducerMock.CloseFunc: method is nil but IProducer.Close was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc(ctx)
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedIProducer.CloseCalls())
func (mock *IProducerMock) CloseCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
	return calls
}

// Initialise calls InitialiseFunc.
func (mock *IProducerMock) Initialise(ctx context.Context) error {
	if mock.InitialiseFunc == nil {
		panic("IProducerMock.InitialiseFunc: method is nil but IProducer.Initialise was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	mock.lockInitialise.Lock()
	mock.calls.Initialise = append(mock.calls.Initialise, callInfo)
	mock.lockInitialise.Unlock()
	return mock.InitialiseFunc(ctx)
}

// InitialiseCalls gets all the calls that were made to Initialise.
// Check the length with:
//     len(mockedIProducer.InitialiseCalls())
func (mock *IProducerMock) InitialiseCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	mock.lockInitialise.RLock()
	calls = mock.calls.Initialise
	mock.lockInitialise.RUnlock()
	return calls
}

// IsInitialised calls IsInitialisedFunc.
func (mock *IProducerMock) IsInitialised() bool {
	if mock.IsInitialisedFunc == nil {
		panic("IProducerMock.IsInitialisedFunc: method is nil but IProducer.IsInitialised was just called")
	}
	callInfo := struct {
	}{}
	mock.lockIsInitialised.Lock()
	mock.calls.IsInitialised = append(mock.calls.IsInitialised, callInfo)
	mock.lockIsInitialised.Unlock()
	return mock.IsInitialisedFunc()
}

// IsInitialisedCalls gets all the calls that were made to IsInitialised.
// Check the length with:
//     len(mockedIProducer.IsInitialisedCalls())
func (mock *IProducerMock) IsInitialisedCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockIsInitialised.RLock()
	calls = mock.calls.IsInitialised
	mock.lockIsInitialised.RUnlock()
	return calls
}
