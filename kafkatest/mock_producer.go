// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package kafkatest

import (
	"context"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/dp-kafka/v3/avro"
	"sync"
)

var (
	lockIProducerMockChannels      sync.RWMutex
	lockIProducerMockChecker       sync.RWMutex
	lockIProducerMockClose         sync.RWMutex
	lockIProducerMockInitialise    sync.RWMutex
	lockIProducerMockIsInitialised sync.RWMutex
	lockIProducerMockLogErrors     sync.RWMutex
	lockIProducerMockSend          sync.RWMutex
)

// Ensure, that IProducerMock does implement kafka.IProducer.
// If this is not the case, regenerate this file with moq.
var _ kafka.IProducer = &IProducerMock{}

// IProducerMock is a mock implementation of kafka.IProducer.
//
//     func TestSomethingThatUsesIProducer(t *testing.T) {
//
//         // make and configure a mocked kafka.IProducer
//         mockedIProducer := &IProducerMock{
//             ChannelsFunc: func() *kafka.ProducerChannels {
// 	               panic("mock out the Channels method")
//             },
//             CheckerFunc: func(ctx context.Context, state *healthcheck.CheckState) error {
// 	               panic("mock out the Checker method")
//             },
//             CloseFunc: func(ctx context.Context) error {
// 	               panic("mock out the Close method")
//             },
//             InitialiseFunc: func(ctx context.Context) error {
// 	               panic("mock out the Initialise method")
//             },
//             IsInitialisedFunc: func() bool {
// 	               panic("mock out the IsInitialised method")
//             },
//             LogErrorsFunc: func(ctx context.Context)  {
// 	               panic("mock out the LogErrors method")
//             },
//             SendFunc: func(schema *avro.Schema, event interface{}) error {
// 	               panic("mock out the Send method")
//             },
//         }
//
//         // use mockedIProducer in code that requires kafka.IProducer
//         // and then make assertions.
//
//     }
type IProducerMock struct {
	// ChannelsFunc mocks the Channels method.
	ChannelsFunc func() *kafka.ProducerChannels

	// CheckerFunc mocks the Checker method.
	CheckerFunc func(ctx context.Context, state *healthcheck.CheckState) error

	// CloseFunc mocks the Close method.
	CloseFunc func(ctx context.Context) error

	// InitialiseFunc mocks the Initialise method.
	InitialiseFunc func(ctx context.Context) error

	// IsInitialisedFunc mocks the IsInitialised method.
	IsInitialisedFunc func() bool

	// LogErrorsFunc mocks the LogErrors method.
	LogErrorsFunc func(ctx context.Context)

	// SendFunc mocks the Send method.
	SendFunc func(schema *avro.Schema, event interface{}) error

	// calls tracks calls to the methods.
	calls struct {
		// Channels holds details about calls to the Channels method.
		Channels []struct {
		}
		// Checker holds details about calls to the Checker method.
		Checker []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// State is the state argument value.
			State *healthcheck.CheckState
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
		// LogErrors holds details about calls to the LogErrors method.
		LogErrors []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
		// Send holds details about calls to the Send method.
		Send []struct {
			// Schema is the schema argument value.
			Schema *avro.Schema
			// Event is the event argument value.
			Event interface{}
		}
	}
}

// Channels calls ChannelsFunc.
func (mock *IProducerMock) Channels() *kafka.ProducerChannels {
	if mock.ChannelsFunc == nil {
		panic("IProducerMock.ChannelsFunc: method is nil but IProducer.Channels was just called")
	}
	callInfo := struct {
	}{}
	lockIProducerMockChannels.Lock()
	mock.calls.Channels = append(mock.calls.Channels, callInfo)
	lockIProducerMockChannels.Unlock()
	return mock.ChannelsFunc()
}

// ChannelsCalls gets all the calls that were made to Channels.
// Check the length with:
//     len(mockedIProducer.ChannelsCalls())
func (mock *IProducerMock) ChannelsCalls() []struct {
} {
	var calls []struct {
	}
	lockIProducerMockChannels.RLock()
	calls = mock.calls.Channels
	lockIProducerMockChannels.RUnlock()
	return calls
}

// Checker calls CheckerFunc.
func (mock *IProducerMock) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("IProducerMock.CheckerFunc: method is nil but IProducer.Checker was just called")
	}
	callInfo := struct {
		Ctx   context.Context
		State *healthcheck.CheckState
	}{
		Ctx:   ctx,
		State: state,
	}
	lockIProducerMockChecker.Lock()
	mock.calls.Checker = append(mock.calls.Checker, callInfo)
	lockIProducerMockChecker.Unlock()
	return mock.CheckerFunc(ctx, state)
}

// CheckerCalls gets all the calls that were made to Checker.
// Check the length with:
//     len(mockedIProducer.CheckerCalls())
func (mock *IProducerMock) CheckerCalls() []struct {
	Ctx   context.Context
	State *healthcheck.CheckState
} {
	var calls []struct {
		Ctx   context.Context
		State *healthcheck.CheckState
	}
	lockIProducerMockChecker.RLock()
	calls = mock.calls.Checker
	lockIProducerMockChecker.RUnlock()
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
	lockIProducerMockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	lockIProducerMockClose.Unlock()
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
	lockIProducerMockClose.RLock()
	calls = mock.calls.Close
	lockIProducerMockClose.RUnlock()
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
	lockIProducerMockInitialise.Lock()
	mock.calls.Initialise = append(mock.calls.Initialise, callInfo)
	lockIProducerMockInitialise.Unlock()
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
	lockIProducerMockInitialise.RLock()
	calls = mock.calls.Initialise
	lockIProducerMockInitialise.RUnlock()
	return calls
}

// IsInitialised calls IsInitialisedFunc.
func (mock *IProducerMock) IsInitialised() bool {
	if mock.IsInitialisedFunc == nil {
		panic("IProducerMock.IsInitialisedFunc: method is nil but IProducer.IsInitialised was just called")
	}
	callInfo := struct {
	}{}
	lockIProducerMockIsInitialised.Lock()
	mock.calls.IsInitialised = append(mock.calls.IsInitialised, callInfo)
	lockIProducerMockIsInitialised.Unlock()
	return mock.IsInitialisedFunc()
}

// IsInitialisedCalls gets all the calls that were made to IsInitialised.
// Check the length with:
//     len(mockedIProducer.IsInitialisedCalls())
func (mock *IProducerMock) IsInitialisedCalls() []struct {
} {
	var calls []struct {
	}
	lockIProducerMockIsInitialised.RLock()
	calls = mock.calls.IsInitialised
	lockIProducerMockIsInitialised.RUnlock()
	return calls
}

// LogErrors calls LogErrorsFunc.
func (mock *IProducerMock) LogErrors(ctx context.Context) {
	if mock.LogErrorsFunc == nil {
		panic("IProducerMock.LogErrorsFunc: method is nil but IProducer.LogErrors was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	lockIProducerMockLogErrors.Lock()
	mock.calls.LogErrors = append(mock.calls.LogErrors, callInfo)
	lockIProducerMockLogErrors.Unlock()
	mock.LogErrorsFunc(ctx)
}

// LogErrorsCalls gets all the calls that were made to LogErrors.
// Check the length with:
//     len(mockedIProducer.LogErrorsCalls())
func (mock *IProducerMock) LogErrorsCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	lockIProducerMockLogErrors.RLock()
	calls = mock.calls.LogErrors
	lockIProducerMockLogErrors.RUnlock()
	return calls
}

// Send calls SendFunc.
func (mock *IProducerMock) Send(schema *avro.Schema, event interface{}) error {
	if mock.SendFunc == nil {
		panic("IProducerMock.SendFunc: method is nil but IProducer.Send was just called")
	}
	callInfo := struct {
		Schema *avro.Schema
		Event  interface{}
	}{
		Schema: schema,
		Event:  event,
	}
	lockIProducerMockSend.Lock()
	mock.calls.Send = append(mock.calls.Send, callInfo)
	lockIProducerMockSend.Unlock()
	return mock.SendFunc(schema, event)
}

// SendCalls gets all the calls that were made to Send.
// Check the length with:
//     len(mockedIProducer.SendCalls())
func (mock *IProducerMock) SendCalls() []struct {
	Schema *avro.Schema
	Event  interface{}
} {
	var calls []struct {
		Schema *avro.Schema
		Event  interface{}
	}
	lockIProducerMockSend.RLock()
	calls = mock.calls.Send
	lockIProducerMockSend.RUnlock()
	return calls
}