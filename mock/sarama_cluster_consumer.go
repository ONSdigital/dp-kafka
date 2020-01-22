// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"github.com/ONSdigital/dp-kafka"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"sync"
)

var (
	lockSaramaClusterConsumerMockClose         sync.RWMutex
	lockSaramaClusterConsumerMockCommitOffsets sync.RWMutex
	lockSaramaClusterConsumerMockErrors        sync.RWMutex
	lockSaramaClusterConsumerMockMarkOffset    sync.RWMutex
	lockSaramaClusterConsumerMockMessages      sync.RWMutex
	lockSaramaClusterConsumerMockNotifications sync.RWMutex
)

// Ensure, that SaramaClusterConsumerMock does implement kafka.SaramaClusterConsumer.
// If this is not the case, regenerate this file with moq.
var _ kafka.SaramaClusterConsumer = &SaramaClusterConsumerMock{}

// SaramaClusterConsumerMock is a mock implementation of kafka.SaramaClusterConsumer.
//
//     func TestSomethingThatUsesSaramaClusterConsumer(t *testing.T) {
//
//         // make and configure a mocked kafka.SaramaClusterConsumer
//         mockedSaramaClusterConsumer := &SaramaClusterConsumerMock{
//             CloseFunc: func() error {
// 	               panic("mock out the Close method")
//             },
//             CommitOffsetsFunc: func() error {
// 	               panic("mock out the CommitOffsets method")
//             },
//             ErrorsFunc: func() <-chan error {
// 	               panic("mock out the Errors method")
//             },
//             MarkOffsetFunc: func(msg *sarama.ConsumerMessage, metadata string)  {
// 	               panic("mock out the MarkOffset method")
//             },
//             MessagesFunc: func() <-chan *sarama.ConsumerMessage {
// 	               panic("mock out the Messages method")
//             },
//             NotificationsFunc: func() <-chan *cluster.Notification {
// 	               panic("mock out the Notifications method")
//             },
//         }
//
//         // use mockedSaramaClusterConsumer in code that requires kafka.SaramaClusterConsumer
//         // and then make assertions.
//
//     }
type SaramaClusterConsumerMock struct {
	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// CommitOffsetsFunc mocks the CommitOffsets method.
	CommitOffsetsFunc func() error

	// ErrorsFunc mocks the Errors method.
	ErrorsFunc func() <-chan error

	// MarkOffsetFunc mocks the MarkOffset method.
	MarkOffsetFunc func(msg *sarama.ConsumerMessage, metadata string)

	// MessagesFunc mocks the Messages method.
	MessagesFunc func() <-chan *sarama.ConsumerMessage

	// NotificationsFunc mocks the Notifications method.
	NotificationsFunc func() <-chan *cluster.Notification

	// calls tracks calls to the methods.
	calls struct {
		// Close holds details about calls to the Close method.
		Close []struct {
		}
		// CommitOffsets holds details about calls to the CommitOffsets method.
		CommitOffsets []struct {
		}
		// Errors holds details about calls to the Errors method.
		Errors []struct {
		}
		// MarkOffset holds details about calls to the MarkOffset method.
		MarkOffset []struct {
			// Msg is the msg argument value.
			Msg *sarama.ConsumerMessage
			// Metadata is the metadata argument value.
			Metadata string
		}
		// Messages holds details about calls to the Messages method.
		Messages []struct {
		}
		// Notifications holds details about calls to the Notifications method.
		Notifications []struct {
		}
	}
}

// Close calls CloseFunc.
func (mock *SaramaClusterConsumerMock) Close() error {
	if mock.CloseFunc == nil {
		panic("SaramaClusterConsumerMock.CloseFunc: method is nil but SaramaClusterConsumer.Close was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaClusterConsumerMockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	lockSaramaClusterConsumerMockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedSaramaClusterConsumer.CloseCalls())
func (mock *SaramaClusterConsumerMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaClusterConsumerMockClose.RLock()
	calls = mock.calls.Close
	lockSaramaClusterConsumerMockClose.RUnlock()
	return calls
}

// CommitOffsets calls CommitOffsetsFunc.
func (mock *SaramaClusterConsumerMock) CommitOffsets() error {
	if mock.CommitOffsetsFunc == nil {
		panic("SaramaClusterConsumerMock.CommitOffsetsFunc: method is nil but SaramaClusterConsumer.CommitOffsets was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaClusterConsumerMockCommitOffsets.Lock()
	mock.calls.CommitOffsets = append(mock.calls.CommitOffsets, callInfo)
	lockSaramaClusterConsumerMockCommitOffsets.Unlock()
	return mock.CommitOffsetsFunc()
}

// CommitOffsetsCalls gets all the calls that were made to CommitOffsets.
// Check the length with:
//     len(mockedSaramaClusterConsumer.CommitOffsetsCalls())
func (mock *SaramaClusterConsumerMock) CommitOffsetsCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaClusterConsumerMockCommitOffsets.RLock()
	calls = mock.calls.CommitOffsets
	lockSaramaClusterConsumerMockCommitOffsets.RUnlock()
	return calls
}

// Errors calls ErrorsFunc.
func (mock *SaramaClusterConsumerMock) Errors() <-chan error {
	if mock.ErrorsFunc == nil {
		panic("SaramaClusterConsumerMock.ErrorsFunc: method is nil but SaramaClusterConsumer.Errors was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaClusterConsumerMockErrors.Lock()
	mock.calls.Errors = append(mock.calls.Errors, callInfo)
	lockSaramaClusterConsumerMockErrors.Unlock()
	return mock.ErrorsFunc()
}

// ErrorsCalls gets all the calls that were made to Errors.
// Check the length with:
//     len(mockedSaramaClusterConsumer.ErrorsCalls())
func (mock *SaramaClusterConsumerMock) ErrorsCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaClusterConsumerMockErrors.RLock()
	calls = mock.calls.Errors
	lockSaramaClusterConsumerMockErrors.RUnlock()
	return calls
}

// MarkOffset calls MarkOffsetFunc.
func (mock *SaramaClusterConsumerMock) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	if mock.MarkOffsetFunc == nil {
		panic("SaramaClusterConsumerMock.MarkOffsetFunc: method is nil but SaramaClusterConsumer.MarkOffset was just called")
	}
	callInfo := struct {
		Msg      *sarama.ConsumerMessage
		Metadata string
	}{
		Msg:      msg,
		Metadata: metadata,
	}
	lockSaramaClusterConsumerMockMarkOffset.Lock()
	mock.calls.MarkOffset = append(mock.calls.MarkOffset, callInfo)
	lockSaramaClusterConsumerMockMarkOffset.Unlock()
	mock.MarkOffsetFunc(msg, metadata)
}

// MarkOffsetCalls gets all the calls that were made to MarkOffset.
// Check the length with:
//     len(mockedSaramaClusterConsumer.MarkOffsetCalls())
func (mock *SaramaClusterConsumerMock) MarkOffsetCalls() []struct {
	Msg      *sarama.ConsumerMessage
	Metadata string
} {
	var calls []struct {
		Msg      *sarama.ConsumerMessage
		Metadata string
	}
	lockSaramaClusterConsumerMockMarkOffset.RLock()
	calls = mock.calls.MarkOffset
	lockSaramaClusterConsumerMockMarkOffset.RUnlock()
	return calls
}

// Messages calls MessagesFunc.
func (mock *SaramaClusterConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	if mock.MessagesFunc == nil {
		panic("SaramaClusterConsumerMock.MessagesFunc: method is nil but SaramaClusterConsumer.Messages was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaClusterConsumerMockMessages.Lock()
	mock.calls.Messages = append(mock.calls.Messages, callInfo)
	lockSaramaClusterConsumerMockMessages.Unlock()
	return mock.MessagesFunc()
}

// MessagesCalls gets all the calls that were made to Messages.
// Check the length with:
//     len(mockedSaramaClusterConsumer.MessagesCalls())
func (mock *SaramaClusterConsumerMock) MessagesCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaClusterConsumerMockMessages.RLock()
	calls = mock.calls.Messages
	lockSaramaClusterConsumerMockMessages.RUnlock()
	return calls
}

// Notifications calls NotificationsFunc.
func (mock *SaramaClusterConsumerMock) Notifications() <-chan *cluster.Notification {
	if mock.NotificationsFunc == nil {
		panic("SaramaClusterConsumerMock.NotificationsFunc: method is nil but SaramaClusterConsumer.Notifications was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaClusterConsumerMockNotifications.Lock()
	mock.calls.Notifications = append(mock.calls.Notifications, callInfo)
	lockSaramaClusterConsumerMockNotifications.Unlock()
	return mock.NotificationsFunc()
}

// NotificationsCalls gets all the calls that were made to Notifications.
// Check the length with:
//     len(mockedSaramaClusterConsumer.NotificationsCalls())
func (mock *SaramaClusterConsumerMock) NotificationsCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaClusterConsumerMockNotifications.RLock()
	calls = mock.calls.Notifications
	lockSaramaClusterConsumerMockNotifications.RUnlock()
	return calls
}
