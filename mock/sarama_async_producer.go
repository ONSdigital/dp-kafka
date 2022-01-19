// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"github.com/Shopify/sarama"
	"sync"
)

// SaramaAsyncProducerMock is a mock implementation of kafka.SaramaAsyncProducer.
//
// 	func TestSomethingThatUsesSaramaAsyncProducer(t *testing.T) {
//
// 		// make and configure a mocked kafka.SaramaAsyncProducer
// 		mockedSaramaAsyncProducer := &SaramaAsyncProducerMock{
// 			AsyncCloseFunc: func()  {
// 				panic("mock out the AsyncClose method")
// 			},
// 			CloseFunc: func() error {
// 				panic("mock out the Close method")
// 			},
// 			ErrorsFunc: func() <-chan *sarama.ProducerError {
// 				panic("mock out the Errors method")
// 			},
// 			InputFunc: func() chan<- *sarama.ProducerMessage {
// 				panic("mock out the Input method")
// 			},
// 			SuccessesFunc: func() <-chan *sarama.ProducerMessage {
// 				panic("mock out the Successes method")
// 			},
// 		}
//
// 		// use mockedSaramaAsyncProducer in code that requires kafka.SaramaAsyncProducer
// 		// and then make assertions.
//
// 	}
type SaramaAsyncProducerMock struct {
	// AsyncCloseFunc mocks the AsyncClose method.
	AsyncCloseFunc func()

	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// ErrorsFunc mocks the Errors method.
	ErrorsFunc func() <-chan *sarama.ProducerError

	// InputFunc mocks the Input method.
	InputFunc func() chan<- *sarama.ProducerMessage

	// SuccessesFunc mocks the Successes method.
	SuccessesFunc func() <-chan *sarama.ProducerMessage

	// calls tracks calls to the methods.
	calls struct {
		// AsyncClose holds details about calls to the AsyncClose method.
		AsyncClose []struct {
		}
		// Close holds details about calls to the Close method.
		Close []struct {
		}
		// Errors holds details about calls to the Errors method.
		Errors []struct {
		}
		// Input holds details about calls to the Input method.
		Input []struct {
		}
		// Successes holds details about calls to the Successes method.
		Successes []struct {
		}
	}
	lockAsyncClose sync.RWMutex
	lockClose      sync.RWMutex
	lockErrors     sync.RWMutex
	lockInput      sync.RWMutex
	lockSuccesses  sync.RWMutex
}

// AsyncClose calls AsyncCloseFunc.
func (mock *SaramaAsyncProducerMock) AsyncClose() {
	if mock.AsyncCloseFunc == nil {
		panic("SaramaAsyncProducerMock.AsyncCloseFunc: method is nil but SaramaAsyncProducer.AsyncClose was just called")
	}
	callInfo := struct {
	}{}
	mock.lockAsyncClose.Lock()
	mock.calls.AsyncClose = append(mock.calls.AsyncClose, callInfo)
	mock.lockAsyncClose.Unlock()
	mock.AsyncCloseFunc()
}

// AsyncCloseCalls gets all the calls that were made to AsyncClose.
// Check the length with:
//     len(mockedSaramaAsyncProducer.AsyncCloseCalls())
func (mock *SaramaAsyncProducerMock) AsyncCloseCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockAsyncClose.RLock()
	calls = mock.calls.AsyncClose
	mock.lockAsyncClose.RUnlock()
	return calls
}

// Close calls CloseFunc.
func (mock *SaramaAsyncProducerMock) Close() error {
	if mock.CloseFunc == nil {
		panic("SaramaAsyncProducerMock.CloseFunc: method is nil but SaramaAsyncProducer.Close was just called")
	}
	callInfo := struct {
	}{}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedSaramaAsyncProducer.CloseCalls())
func (mock *SaramaAsyncProducerMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
	return calls
}

// Errors calls ErrorsFunc.
func (mock *SaramaAsyncProducerMock) Errors() <-chan *sarama.ProducerError {
	if mock.ErrorsFunc == nil {
		panic("SaramaAsyncProducerMock.ErrorsFunc: method is nil but SaramaAsyncProducer.Errors was just called")
	}
	callInfo := struct {
	}{}
	mock.lockErrors.Lock()
	mock.calls.Errors = append(mock.calls.Errors, callInfo)
	mock.lockErrors.Unlock()
	return mock.ErrorsFunc()
}

// ErrorsCalls gets all the calls that were made to Errors.
// Check the length with:
//     len(mockedSaramaAsyncProducer.ErrorsCalls())
func (mock *SaramaAsyncProducerMock) ErrorsCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockErrors.RLock()
	calls = mock.calls.Errors
	mock.lockErrors.RUnlock()
	return calls
}

// Input calls InputFunc.
func (mock *SaramaAsyncProducerMock) Input() chan<- *sarama.ProducerMessage {
	if mock.InputFunc == nil {
		panic("SaramaAsyncProducerMock.InputFunc: method is nil but SaramaAsyncProducer.Input was just called")
	}
	callInfo := struct {
	}{}
	mock.lockInput.Lock()
	mock.calls.Input = append(mock.calls.Input, callInfo)
	mock.lockInput.Unlock()
	return mock.InputFunc()
}

// InputCalls gets all the calls that were made to Input.
// Check the length with:
//     len(mockedSaramaAsyncProducer.InputCalls())
func (mock *SaramaAsyncProducerMock) InputCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockInput.RLock()
	calls = mock.calls.Input
	mock.lockInput.RUnlock()
	return calls
}

// Successes calls SuccessesFunc.
func (mock *SaramaAsyncProducerMock) Successes() <-chan *sarama.ProducerMessage {
	if mock.SuccessesFunc == nil {
		panic("SaramaAsyncProducerMock.SuccessesFunc: method is nil but SaramaAsyncProducer.Successes was just called")
	}
	callInfo := struct {
	}{}
	mock.lockSuccesses.Lock()
	mock.calls.Successes = append(mock.calls.Successes, callInfo)
	mock.lockSuccesses.Unlock()
	return mock.SuccessesFunc()
}

// SuccessesCalls gets all the calls that were made to Successes.
// Check the length with:
//     len(mockedSaramaAsyncProducer.SuccessesCalls())
func (mock *SaramaAsyncProducerMock) SuccessesCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockSuccesses.RLock()
	calls = mock.calls.Successes
	mock.lockSuccesses.RUnlock()
	return calls
}
