// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package kafkatest

import (
	"context"
	"github.com/ONSdigital/dp-kafka"
	"sync"
)

var (
	lockIConsumerGroupMockChannels                sync.RWMutex
	lockIConsumerGroupMockClose                   sync.RWMutex
	lockIConsumerGroupMockCommitAndRelease        sync.RWMutex
	lockIConsumerGroupMockInitialise              sync.RWMutex
	lockIConsumerGroupMockIsInitialised           sync.RWMutex
	lockIConsumerGroupMockRelease                 sync.RWMutex
	lockIConsumerGroupMockStopListeningToConsumer sync.RWMutex
)

// Ensure, that IConsumerGroupMock does implement kafka.IConsumerGroup.
// If this is not the case, regenerate this file with moq.
var _ kafka.IConsumerGroup = &IConsumerGroupMock{}

// IConsumerGroupMock is a mock implementation of kafka.IConsumerGroup.
//
//     func TestSomethingThatUsesIConsumerGroup(t *testing.T) {
//
//         // make and configure a mocked kafka.IConsumerGroup
//         mockedIConsumerGroup := &IConsumerGroupMock{
//             ChannelsFunc: func() *kafka.ConsumerGroupChannels {
// 	               panic("mock out the Channels method")
//             },
//             CloseFunc: func(ctx context.Context) error {
// 	               panic("mock out the Close method")
//             },
//             CommitAndReleaseFunc: func(msg kafka.Message)  {
// 	               panic("mock out the CommitAndRelease method")
//             },
//             InitialiseFunc: func(ctx context.Context) error {
// 	               panic("mock out the Initialise method")
//             },
//             IsInitialisedFunc: func() bool {
// 	               panic("mock out the IsInitialised method")
//             },
//             ReleaseFunc: func()  {
// 	               panic("mock out the Release method")
//             },
//             StopListeningToConsumerFunc: func(ctx context.Context) error {
// 	               panic("mock out the StopListeningToConsumer method")
//             },
//         }
//
//         // use mockedIConsumerGroup in code that requires kafka.IConsumerGroup
//         // and then make assertions.
//
//     }
type IConsumerGroupMock struct {
	// ChannelsFunc mocks the Channels method.
	ChannelsFunc func() *kafka.ConsumerGroupChannels

	// CloseFunc mocks the Close method.
	CloseFunc func(ctx context.Context) error

	// CommitAndReleaseFunc mocks the CommitAndRelease method.
	CommitAndReleaseFunc func(msg kafka.Message)

	// InitialiseFunc mocks the Initialise method.
	InitialiseFunc func(ctx context.Context) error

	// IsInitialisedFunc mocks the IsInitialised method.
	IsInitialisedFunc func() bool

	// ReleaseFunc mocks the Release method.
	ReleaseFunc func()

	// StopListeningToConsumerFunc mocks the StopListeningToConsumer method.
	StopListeningToConsumerFunc func(ctx context.Context) error

	// calls tracks calls to the methods.
	calls struct {
		// Channels holds details about calls to the Channels method.
		Channels []struct {
		}
		// Close holds details about calls to the Close method.
		Close []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
		// CommitAndRelease holds details about calls to the CommitAndRelease method.
		CommitAndRelease []struct {
			// Msg is the msg argument value.
			Msg kafka.Message
		}
		// Initialise holds details about calls to the Initialise method.
		Initialise []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
		// IsInitialised holds details about calls to the IsInitialised method.
		IsInitialised []struct {
		}
		// Release holds details about calls to the Release method.
		Release []struct {
		}
		// StopListeningToConsumer holds details about calls to the StopListeningToConsumer method.
		StopListeningToConsumer []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
	}
}

// Channels calls ChannelsFunc.
func (mock *IConsumerGroupMock) Channels() *kafka.ConsumerGroupChannels {
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

// CommitAndRelease calls CommitAndReleaseFunc.
func (mock *IConsumerGroupMock) CommitAndRelease(msg kafka.Message) {
	if mock.CommitAndReleaseFunc == nil {
		panic("IConsumerGroupMock.CommitAndReleaseFunc: method is nil but IConsumerGroup.CommitAndRelease was just called")
	}
	callInfo := struct {
		Msg kafka.Message
	}{
		Msg: msg,
	}
	lockIConsumerGroupMockCommitAndRelease.Lock()
	mock.calls.CommitAndRelease = append(mock.calls.CommitAndRelease, callInfo)
	lockIConsumerGroupMockCommitAndRelease.Unlock()
	mock.CommitAndReleaseFunc(msg)
}

// CommitAndReleaseCalls gets all the calls that were made to CommitAndRelease.
// Check the length with:
//     len(mockedIConsumerGroup.CommitAndReleaseCalls())
func (mock *IConsumerGroupMock) CommitAndReleaseCalls() []struct {
	Msg kafka.Message
} {
	var calls []struct {
		Msg kafka.Message
	}
	lockIConsumerGroupMockCommitAndRelease.RLock()
	calls = mock.calls.CommitAndRelease
	lockIConsumerGroupMockCommitAndRelease.RUnlock()
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

// Release calls ReleaseFunc.
func (mock *IConsumerGroupMock) Release() {
	if mock.ReleaseFunc == nil {
		panic("IConsumerGroupMock.ReleaseFunc: method is nil but IConsumerGroup.Release was just called")
	}
	callInfo := struct {
	}{}
	lockIConsumerGroupMockRelease.Lock()
	mock.calls.Release = append(mock.calls.Release, callInfo)
	lockIConsumerGroupMockRelease.Unlock()
	mock.ReleaseFunc()
}

// ReleaseCalls gets all the calls that were made to Release.
// Check the length with:
//     len(mockedIConsumerGroup.ReleaseCalls())
func (mock *IConsumerGroupMock) ReleaseCalls() []struct {
} {
	var calls []struct {
	}
	lockIConsumerGroupMockRelease.RLock()
	calls = mock.calls.Release
	lockIConsumerGroupMockRelease.RUnlock()
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
