// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package kafkatest

import (
	"context"
	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	"sync"
)

// Ensure, that IConsumerGroupMock does implement kafka.IConsumerGroup.
// If this is not the case, regenerate this file with moq.
var _ kafka.IConsumerGroup = &IConsumerGroupMock{}

// IConsumerGroupMock is a mock implementation of kafka.IConsumerGroup.
//
//	func TestSomethingThatUsesIConsumerGroup(t *testing.T) {
//
//		// make and configure a mocked kafka.IConsumerGroup
//		mockedIConsumerGroup := &IConsumerGroupMock{
//			ChannelsFunc: func() *kafka.ConsumerGroupChannels {
//				panic("mock out the Channels method")
//			},
//			CheckerFunc: func(ctx context.Context, state *health.CheckState) error {
//				panic("mock out the Checker method")
//			},
//			CloseFunc: func(ctx context.Context, optFuncs ...kafka.OptFunc) error {
//				panic("mock out the Close method")
//			},
//			InitialiseFunc: func(ctx context.Context) error {
//				panic("mock out the Initialise method")
//			},
//			IsInitialisedFunc: func() bool {
//				panic("mock out the IsInitialised method")
//			},
//			LogErrorsFunc: func(ctx context.Context)  {
//				panic("mock out the LogErrors method")
//			},
//			OnHealthUpdateFunc: func(status string)  {
//				panic("mock out the OnHealthUpdate method")
//			},
//			RegisterBatchHandlerFunc: func(ctx context.Context, batchHandler kafka.BatchHandler) error {
//				panic("mock out the RegisterBatchHandler method")
//			},
//			RegisterHandlerFunc: func(ctx context.Context, h kafka.Handler) error {
//				panic("mock out the RegisterHandler method")
//			},
//			StartFunc: func() error {
//				panic("mock out the Start method")
//			},
//			StateFunc: func() kafka.State {
//				panic("mock out the State method")
//			},
//			StateWaitFunc: func(state kafka.State)  {
//				panic("mock out the StateWait method")
//			},
//			StopFunc: func() error {
//				panic("mock out the Stop method")
//			},
//			StopAndWaitFunc: func() error {
//				panic("mock out the StopAndWait method")
//			},
//		}
//
//		// use mockedIConsumerGroup in code that requires kafka.IConsumerGroup
//		// and then make assertions.
//
//	}
type IConsumerGroupMock struct {
	// ChannelsFunc mocks the Channels method.
	ChannelsFunc func() *kafka.ConsumerGroupChannels

	// CheckerFunc mocks the Checker method.
	CheckerFunc func(ctx context.Context, state *health.CheckState) error

	// CloseFunc mocks the Close method.
	CloseFunc func(ctx context.Context, optFuncs ...kafka.OptFunc) error

	// InitialiseFunc mocks the Initialise method.
	InitialiseFunc func(ctx context.Context) error

	// IsInitialisedFunc mocks the IsInitialised method.
	IsInitialisedFunc func() bool

	// LogErrorsFunc mocks the LogErrors method.
	LogErrorsFunc func(ctx context.Context)

	// OnHealthUpdateFunc mocks the OnHealthUpdate method.
	OnHealthUpdateFunc func(status string)

	// RegisterBatchHandlerFunc mocks the RegisterBatchHandler method.
	RegisterBatchHandlerFunc func(ctx context.Context, batchHandler kafka.BatchHandler) error

	// RegisterHandlerFunc mocks the RegisterHandler method.
	RegisterHandlerFunc func(ctx context.Context, h kafka.Handler) error

	// StartFunc mocks the Start method.
	StartFunc func() error

	// StateFunc mocks the State method.
	StateFunc func() kafka.State

	// StateWaitFunc mocks the StateWait method.
	StateWaitFunc func(state kafka.State)

	// StopFunc mocks the Stop method.
	StopFunc func() error

	// StopAndWaitFunc mocks the StopAndWait method.
	StopAndWaitFunc func() error

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
			State *health.CheckState
		}
		// Close holds details about calls to the Close method.
		Close []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// OptFuncs is the optFuncs argument value.
			OptFuncs []kafka.OptFunc
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
		// OnHealthUpdate holds details about calls to the OnHealthUpdate method.
		OnHealthUpdate []struct {
			// Status is the status argument value.
			Status string
		}
		// RegisterBatchHandler holds details about calls to the RegisterBatchHandler method.
		RegisterBatchHandler []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// BatchHandler is the batchHandler argument value.
			BatchHandler kafka.BatchHandler
		}
		// RegisterHandler holds details about calls to the RegisterHandler method.
		RegisterHandler []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// H is the h argument value.
			H kafka.Handler
		}
		// Start holds details about calls to the Start method.
		Start []struct {
		}
		// State holds details about calls to the State method.
		State []struct {
		}
		// StateWait holds details about calls to the StateWait method.
		StateWait []struct {
			// State is the state argument value.
			State kafka.State
		}
		// Stop holds details about calls to the Stop method.
		Stop []struct {
		}
		// StopAndWait holds details about calls to the StopAndWait method.
		StopAndWait []struct {
		}
	}
	lockChannels             sync.RWMutex
	lockChecker              sync.RWMutex
	lockClose                sync.RWMutex
	lockInitialise           sync.RWMutex
	lockIsInitialised        sync.RWMutex
	lockLogErrors            sync.RWMutex
	lockOnHealthUpdate       sync.RWMutex
	lockRegisterBatchHandler sync.RWMutex
	lockRegisterHandler      sync.RWMutex
	lockStart                sync.RWMutex
	lockState                sync.RWMutex
	lockStateWait            sync.RWMutex
	lockStop                 sync.RWMutex
	lockStopAndWait          sync.RWMutex
}

// Channels calls ChannelsFunc.
func (mock *IConsumerGroupMock) Channels() *kafka.ConsumerGroupChannels {
	if mock.ChannelsFunc == nil {
		panic("IConsumerGroupMock.ChannelsFunc: method is nil but IConsumerGroup.Channels was just called")
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
//
//	len(mockedIConsumerGroup.ChannelsCalls())
func (mock *IConsumerGroupMock) ChannelsCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockChannels.RLock()
	calls = mock.calls.Channels
	mock.lockChannels.RUnlock()
	return calls
}

// Checker calls CheckerFunc.
func (mock *IConsumerGroupMock) Checker(ctx context.Context, state *health.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("IConsumerGroupMock.CheckerFunc: method is nil but IConsumerGroup.Checker was just called")
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
//
//	len(mockedIConsumerGroup.CheckerCalls())
func (mock *IConsumerGroupMock) CheckerCalls() []struct {
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
func (mock *IConsumerGroupMock) Close(ctx context.Context, optFuncs ...kafka.OptFunc) error {
	if mock.CloseFunc == nil {
		panic("IConsumerGroupMock.CloseFunc: method is nil but IConsumerGroup.Close was just called")
	}
	callInfo := struct {
		Ctx      context.Context
		OptFuncs []kafka.OptFunc
	}{
		Ctx:      ctx,
		OptFuncs: optFuncs,
	}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc(ctx, optFuncs...)
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//
//	len(mockedIConsumerGroup.CloseCalls())
func (mock *IConsumerGroupMock) CloseCalls() []struct {
	Ctx      context.Context
	OptFuncs []kafka.OptFunc
} {
	var calls []struct {
		Ctx      context.Context
		OptFuncs []kafka.OptFunc
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
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
	mock.lockInitialise.Lock()
	mock.calls.Initialise = append(mock.calls.Initialise, callInfo)
	mock.lockInitialise.Unlock()
	return mock.InitialiseFunc(ctx)
}

// InitialiseCalls gets all the calls that were made to Initialise.
// Check the length with:
//
//	len(mockedIConsumerGroup.InitialiseCalls())
func (mock *IConsumerGroupMock) InitialiseCalls() []struct {
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
func (mock *IConsumerGroupMock) IsInitialised() bool {
	if mock.IsInitialisedFunc == nil {
		panic("IConsumerGroupMock.IsInitialisedFunc: method is nil but IConsumerGroup.IsInitialised was just called")
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
//
//	len(mockedIConsumerGroup.IsInitialisedCalls())
func (mock *IConsumerGroupMock) IsInitialisedCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockIsInitialised.RLock()
	calls = mock.calls.IsInitialised
	mock.lockIsInitialised.RUnlock()
	return calls
}

// LogErrors calls LogErrorsFunc.
func (mock *IConsumerGroupMock) LogErrors(ctx context.Context) {
	if mock.LogErrorsFunc == nil {
		panic("IConsumerGroupMock.LogErrorsFunc: method is nil but IConsumerGroup.LogErrors was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	mock.lockLogErrors.Lock()
	mock.calls.LogErrors = append(mock.calls.LogErrors, callInfo)
	mock.lockLogErrors.Unlock()
	mock.LogErrorsFunc(ctx)
}

// LogErrorsCalls gets all the calls that were made to LogErrors.
// Check the length with:
//
//	len(mockedIConsumerGroup.LogErrorsCalls())
func (mock *IConsumerGroupMock) LogErrorsCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	mock.lockLogErrors.RLock()
	calls = mock.calls.LogErrors
	mock.lockLogErrors.RUnlock()
	return calls
}

// OnHealthUpdate calls OnHealthUpdateFunc.
func (mock *IConsumerGroupMock) OnHealthUpdate(status string) {
	if mock.OnHealthUpdateFunc == nil {
		panic("IConsumerGroupMock.OnHealthUpdateFunc: method is nil but IConsumerGroup.OnHealthUpdate was just called")
	}
	callInfo := struct {
		Status string
	}{
		Status: status,
	}
	mock.lockOnHealthUpdate.Lock()
	mock.calls.OnHealthUpdate = append(mock.calls.OnHealthUpdate, callInfo)
	mock.lockOnHealthUpdate.Unlock()
	mock.OnHealthUpdateFunc(status)
}

// OnHealthUpdateCalls gets all the calls that were made to OnHealthUpdate.
// Check the length with:
//
//	len(mockedIConsumerGroup.OnHealthUpdateCalls())
func (mock *IConsumerGroupMock) OnHealthUpdateCalls() []struct {
	Status string
} {
	var calls []struct {
		Status string
	}
	mock.lockOnHealthUpdate.RLock()
	calls = mock.calls.OnHealthUpdate
	mock.lockOnHealthUpdate.RUnlock()
	return calls
}

// RegisterBatchHandler calls RegisterBatchHandlerFunc.
func (mock *IConsumerGroupMock) RegisterBatchHandler(ctx context.Context, batchHandler kafka.BatchHandler) error {
	if mock.RegisterBatchHandlerFunc == nil {
		panic("IConsumerGroupMock.RegisterBatchHandlerFunc: method is nil but IConsumerGroup.RegisterBatchHandler was just called")
	}
	callInfo := struct {
		Ctx          context.Context
		BatchHandler kafka.BatchHandler
	}{
		Ctx:          ctx,
		BatchHandler: batchHandler,
	}
	mock.lockRegisterBatchHandler.Lock()
	mock.calls.RegisterBatchHandler = append(mock.calls.RegisterBatchHandler, callInfo)
	mock.lockRegisterBatchHandler.Unlock()
	return mock.RegisterBatchHandlerFunc(ctx, batchHandler)
}

// RegisterBatchHandlerCalls gets all the calls that were made to RegisterBatchHandler.
// Check the length with:
//
//	len(mockedIConsumerGroup.RegisterBatchHandlerCalls())
func (mock *IConsumerGroupMock) RegisterBatchHandlerCalls() []struct {
	Ctx          context.Context
	BatchHandler kafka.BatchHandler
} {
	var calls []struct {
		Ctx          context.Context
		BatchHandler kafka.BatchHandler
	}
	mock.lockRegisterBatchHandler.RLock()
	calls = mock.calls.RegisterBatchHandler
	mock.lockRegisterBatchHandler.RUnlock()
	return calls
}

// RegisterHandler calls RegisterHandlerFunc.
func (mock *IConsumerGroupMock) RegisterHandler(ctx context.Context, h kafka.Handler) error {
	if mock.RegisterHandlerFunc == nil {
		panic("IConsumerGroupMock.RegisterHandlerFunc: method is nil but IConsumerGroup.RegisterHandler was just called")
	}
	callInfo := struct {
		Ctx context.Context
		H   kafka.Handler
	}{
		Ctx: ctx,
		H:   h,
	}
	mock.lockRegisterHandler.Lock()
	mock.calls.RegisterHandler = append(mock.calls.RegisterHandler, callInfo)
	mock.lockRegisterHandler.Unlock()
	return mock.RegisterHandlerFunc(ctx, h)
}

// RegisterHandlerCalls gets all the calls that were made to RegisterHandler.
// Check the length with:
//
//	len(mockedIConsumerGroup.RegisterHandlerCalls())
func (mock *IConsumerGroupMock) RegisterHandlerCalls() []struct {
	Ctx context.Context
	H   kafka.Handler
} {
	var calls []struct {
		Ctx context.Context
		H   kafka.Handler
	}
	mock.lockRegisterHandler.RLock()
	calls = mock.calls.RegisterHandler
	mock.lockRegisterHandler.RUnlock()
	return calls
}

// Start calls StartFunc.
func (mock *IConsumerGroupMock) Start() error {
	if mock.StartFunc == nil {
		panic("IConsumerGroupMock.StartFunc: method is nil but IConsumerGroup.Start was just called")
	}
	callInfo := struct {
	}{}
	mock.lockStart.Lock()
	mock.calls.Start = append(mock.calls.Start, callInfo)
	mock.lockStart.Unlock()
	return mock.StartFunc()
}

// StartCalls gets all the calls that were made to Start.
// Check the length with:
//
//	len(mockedIConsumerGroup.StartCalls())
func (mock *IConsumerGroupMock) StartCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockStart.RLock()
	calls = mock.calls.Start
	mock.lockStart.RUnlock()
	return calls
}

// State calls StateFunc.
func (mock *IConsumerGroupMock) State() kafka.State {
	if mock.StateFunc == nil {
		panic("IConsumerGroupMock.StateFunc: method is nil but IConsumerGroup.State was just called")
	}
	callInfo := struct {
	}{}
	mock.lockState.Lock()
	mock.calls.State = append(mock.calls.State, callInfo)
	mock.lockState.Unlock()
	return mock.StateFunc()
}

// StateCalls gets all the calls that were made to State.
// Check the length with:
//
//	len(mockedIConsumerGroup.StateCalls())
func (mock *IConsumerGroupMock) StateCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockState.RLock()
	calls = mock.calls.State
	mock.lockState.RUnlock()
	return calls
}

// StateWait calls StateWaitFunc.
func (mock *IConsumerGroupMock) StateWait(state kafka.State) {
	if mock.StateWaitFunc == nil {
		panic("IConsumerGroupMock.StateWaitFunc: method is nil but IConsumerGroup.StateWait was just called")
	}
	callInfo := struct {
		State kafka.State
	}{
		State: state,
	}
	mock.lockStateWait.Lock()
	mock.calls.StateWait = append(mock.calls.StateWait, callInfo)
	mock.lockStateWait.Unlock()
	mock.StateWaitFunc(state)
}

// StateWaitCalls gets all the calls that were made to StateWait.
// Check the length with:
//
//	len(mockedIConsumerGroup.StateWaitCalls())
func (mock *IConsumerGroupMock) StateWaitCalls() []struct {
	State kafka.State
} {
	var calls []struct {
		State kafka.State
	}
	mock.lockStateWait.RLock()
	calls = mock.calls.StateWait
	mock.lockStateWait.RUnlock()
	return calls
}

// Stop calls StopFunc.
func (mock *IConsumerGroupMock) Stop() error {
	if mock.StopFunc == nil {
		panic("IConsumerGroupMock.StopFunc: method is nil but IConsumerGroup.Stop was just called")
	}
	callInfo := struct {
	}{}
	mock.lockStop.Lock()
	mock.calls.Stop = append(mock.calls.Stop, callInfo)
	mock.lockStop.Unlock()
	return mock.StopFunc()
}

// StopCalls gets all the calls that were made to Stop.
// Check the length with:
//
//	len(mockedIConsumerGroup.StopCalls())
func (mock *IConsumerGroupMock) StopCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockStop.RLock()
	calls = mock.calls.Stop
	mock.lockStop.RUnlock()
	return calls
}

// StopAndWait calls StopAndWaitFunc.
func (mock *IConsumerGroupMock) StopAndWait() error {
	if mock.StopAndWaitFunc == nil {
		panic("IConsumerGroupMock.StopAndWaitFunc: method is nil but IConsumerGroup.StopAndWait was just called")
	}
	callInfo := struct {
	}{}
	mock.lockStopAndWait.Lock()
	mock.calls.StopAndWait = append(mock.calls.StopAndWait, callInfo)
	mock.lockStopAndWait.Unlock()
	return mock.StopAndWaitFunc()
}

// StopAndWaitCalls gets all the calls that were made to StopAndWait.
// Check the length with:
//
//	len(mockedIConsumerGroup.StopAndWaitCalls())
func (mock *IConsumerGroupMock) StopAndWaitCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockStopAndWait.RLock()
	calls = mock.calls.StopAndWait
	mock.lockStopAndWait.RUnlock()
	return calls
}
