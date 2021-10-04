// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package kafkatest

import (
	"context"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v2/consumer"
	"sync"
)

var (
	lockIConsumerGroupMockChannels                sync.RWMutex
	lockIConsumerGroupMockChecker                 sync.RWMutex
	lockIConsumerGroupMockClose                   sync.RWMutex
	lockIConsumerGroupMockInitialise              sync.RWMutex
	lockIConsumerGroupMockIsInitialised           sync.RWMutex
	lockIConsumerGroupMockStopListeningToConsumer sync.RWMutex
)

// Ensure, that IConsumerGroupMock does implement consumer.IConsumerGroup.
// If this is not the case, regenerate this file with moq.
var _ consumer.IConsumerGroup = &IConsumerGroupMock{}

// IConsumerGroupMock is a mock implementation of consumer.IConsumerGroup.
//
//     func TestSomethingThatUsesIConsumerGroup(t *testing.T) {
//
//         // make and configure a mocked consumer.IConsumerGroup
//         mockedIConsumerGroup := &IConsumerGroupMock{
//             ChannelsFunc: func() *consumer.ConsumerGroupChannels {
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
//             StopListeningToConsumerFunc: func(ctx context.Context) error {
// 	               panic("mock out the StopListeningToConsumer method")
//             },
//         }
//
//         // use mockedIConsumerGroup in code that requires consumer.IConsumerGroup
//         // and then make assertions.
//
//     }
type IConsumerGroupMock struct {
	// ChannelsFunc mocks the Channels method.
	ChannelsFunc func() *consumer.ConsumerGroupChannels

	// CheckerFunc mocks the Checker method.
	CheckerFunc func(ctx context.Context, state *healthcheck.CheckState) error

	// CloseFunc mocks the Close method.
	CloseFunc func(ctx context.Context) error

	// InitialiseFunc mocks the Initialise method.
	InitialiseFunc func(ctx context.Context) error

	// IsInitialisedFunc mocks the IsInitialised method.
	IsInitialisedFunc func() bool

	// StopListeningToConsumerFunc mocks the StopListeningToConsumer method.
	StopListeningToConsumerFunc func(ctx context.Context) error

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
		// StopListeningToConsumer holds details about calls to the StopListeningToConsumer method.
		StopListeningToConsumer []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
	}
}

// Channels calls ChannelsFunc.
func (mock *IConsumerGroupMock) Channels() *consumer.ConsumerGroupChannels {
	if mock.ChannelsFunc == nil {
		panic("IConsumerGroupMock.ChannelsFunc: method is nil but IConsumerGroup.Channels was just called")
	}
	callInfo := struct {
	}{}
	lockIConsumerGroupMockChannels.Lock()
	mock.calls.Channels = append(mock.calls.Channels, callInfo)
	lockIConsumerGroupMockChannels.Unlock()
	return mock.ChannelsFunc()
}

// ChannelsCalls gets all the calls that were made to Channels.
// Check the length with:
//     len(mockedIConsumerGroup.ChannelsCalls())
func (mock *IConsumerGroupMock) ChannelsCalls() []struct {
} {
	var calls []struct {
	}
	lockIConsumerGroupMockChannels.RLock()
	calls = mock.calls.Channels
	lockIConsumerGroupMockChannels.RUnlock()
	return calls
}

// Checker calls CheckerFunc.
func (mock *IConsumerGroupMock) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("IConsumerGroupMock.CheckerFunc: method is nil but IConsumerGroup.Checker was just called")
	}
	callInfo := struct {
		Ctx   context.Context
		State *healthcheck.CheckState
	}{
		Ctx:   ctx,
		State: state,
	}
	lockIConsumerGroupMockChecker.Lock()
	mock.calls.Checker = append(mock.calls.Checker, callInfo)
	lockIConsumerGroupMockChecker.Unlock()
	return mock.CheckerFunc(ctx, state)
}

// CheckerCalls gets all the calls that were made to Checker.
// Check the length with:
//     len(mockedIConsumerGroup.CheckerCalls())
func (mock *IConsumerGroupMock) CheckerCalls() []struct {
	Ctx   context.Context
	State *healthcheck.CheckState
} {
	var calls []struct {
		Ctx   context.Context
		State *healthcheck.CheckState
	}
	lockIConsumerGroupMockChecker.RLock()
	calls = mock.calls.Checker
	lockIConsumerGroupMockChecker.RUnlock()
	return calls
}

// Close calls CloseFunc.
func (mock *IConsumerGroupMock) Close(ctx context.Context) error {
	if mock.CloseFunc == nil {
		panic("IConsumerGroupMock.CloseFunc: method is nil but IConsumerGroup.Close was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	lockIConsumerGroupMockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	lockIConsumerGroupMockClose.Unlock()
	return mock.CloseFunc(ctx)
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedIConsumerGroup.CloseCalls())
func (mock *IConsumerGroupMock) CloseCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	lockIConsumerGroupMockClose.RLock()
	calls = mock.calls.Close
	lockIConsumerGroupMockClose.RUnlock()
	return calls
}

// Initialise calls InitialiseFunc.
func (mock *IConsumerGroupMock) Initialise(ctx context.Context) error {
	if mock.InitialiseFunc == nil {
		panic("IConsumerGroupMock.InitialiseFunc: method is nil but IConsumerGroup.Initialise was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	lockIConsumerGroupMockInitialise.Lock()
	mock.calls.Initialise = append(mock.calls.Initialise, callInfo)
	lockIConsumerGroupMockInitialise.Unlock()
	return mock.InitialiseFunc(ctx)
}

// InitialiseCalls gets all the calls that were made to Initialise.
// Check the length with:
//     len(mockedIConsumerGroup.InitialiseCalls())
func (mock *IConsumerGroupMock) InitialiseCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	lockIConsumerGroupMockInitialise.RLock()
	calls = mock.calls.Initialise
	lockIConsumerGroupMockInitialise.RUnlock()
	return calls
}

// IsInitialised calls IsInitialisedFunc.
func (mock *IConsumerGroupMock) IsInitialised() bool {
	if mock.IsInitialisedFunc == nil {
		panic("IConsumerGroupMock.IsInitialisedFunc: method is nil but IConsumerGroup.IsInitialised was just called")
	}
	callInfo := struct {
	}{}
	lockIConsumerGroupMockIsInitialised.Lock()
	mock.calls.IsInitialised = append(mock.calls.IsInitialised, callInfo)
	lockIConsumerGroupMockIsInitialised.Unlock()
	return mock.IsInitialisedFunc()
}

// IsInitialisedCalls gets all the calls that were made to IsInitialised.
// Check the length with:
//     len(mockedIConsumerGroup.IsInitialisedCalls())
func (mock *IConsumerGroupMock) IsInitialisedCalls() []struct {
} {
	var calls []struct {
	}
	lockIConsumerGroupMockIsInitialised.RLock()
	calls = mock.calls.IsInitialised
	lockIConsumerGroupMockIsInitialised.RUnlock()
	return calls
}

// StopListeningToConsumer calls StopListeningToConsumerFunc.
func (mock *IConsumerGroupMock) StopListeningToConsumer(ctx context.Context) error {
	if mock.StopListeningToConsumerFunc == nil {
		panic("IConsumerGroupMock.StopListeningToConsumerFunc: method is nil but IConsumerGroup.StopListeningToConsumer was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	lockIConsumerGroupMockStopListeningToConsumer.Lock()
	mock.calls.StopListeningToConsumer = append(mock.calls.StopListeningToConsumer, callInfo)
	lockIConsumerGroupMockStopListeningToConsumer.Unlock()
	return mock.StopListeningToConsumerFunc(ctx)
}

// StopListeningToConsumerCalls gets all the calls that were made to StopListeningToConsumer.
// Check the length with:
//     len(mockedIConsumerGroup.StopListeningToConsumerCalls())
func (mock *IConsumerGroupMock) StopListeningToConsumerCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	lockIConsumerGroupMockStopListeningToConsumer.RLock()
	calls = mock.calls.StopListeningToConsumer
	lockIConsumerGroupMockStopListeningToConsumer.RUnlock()
	return calls
}
