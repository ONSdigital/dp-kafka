// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"github.com/Shopify/sarama"
	"sync"
)

var (
	lockSaramaConsumerGroupClaimMockHighWaterMarkOffset sync.RWMutex
	lockSaramaConsumerGroupClaimMockInitialOffset       sync.RWMutex
	lockSaramaConsumerGroupClaimMockMessages            sync.RWMutex
	lockSaramaConsumerGroupClaimMockPartition           sync.RWMutex
	lockSaramaConsumerGroupClaimMockTopic               sync.RWMutex
)

// SaramaConsumerGroupClaimMock is a mock implementation of kafka.SaramaConsumerGroupClaim.
//
//     func TestSomethingThatUsesSaramaConsumerGroupClaim(t *testing.T) {
//
//         // make and configure a mocked kafka.SaramaConsumerGroupClaim
//         mockedSaramaConsumerGroupClaim := &SaramaConsumerGroupClaimMock{
//             HighWaterMarkOffsetFunc: func() int64 {
// 	               panic("mock out the HighWaterMarkOffset method")
//             },
//             InitialOffsetFunc: func() int64 {
// 	               panic("mock out the InitialOffset method")
//             },
//             MessagesFunc: func() <-chan *sarama.ConsumerMessage {
// 	               panic("mock out the Messages method")
//             },
//             PartitionFunc: func() int32 {
// 	               panic("mock out the Partition method")
//             },
//             TopicFunc: func() string {
// 	               panic("mock out the Topic method")
//             },
//         }
//
//         // use mockedSaramaConsumerGroupClaim in code that requires kafka.SaramaConsumerGroupClaim
//         // and then make assertions.
//
//     }
type SaramaConsumerGroupClaimMock struct {
	// HighWaterMarkOffsetFunc mocks the HighWaterMarkOffset method.
	HighWaterMarkOffsetFunc func() int64

	// InitialOffsetFunc mocks the InitialOffset method.
	InitialOffsetFunc func() int64

	// MessagesFunc mocks the Messages method.
	MessagesFunc func() <-chan *sarama.ConsumerMessage

	// PartitionFunc mocks the Partition method.
	PartitionFunc func() int32

	// TopicFunc mocks the Topic method.
	TopicFunc func() string

	// calls tracks calls to the methods.
	calls struct {
		// HighWaterMarkOffset holds details about calls to the HighWaterMarkOffset method.
		HighWaterMarkOffset []struct {
		}
		// InitialOffset holds details about calls to the InitialOffset method.
		InitialOffset []struct {
		}
		// Messages holds details about calls to the Messages method.
		Messages []struct {
		}
		// Partition holds details about calls to the Partition method.
		Partition []struct {
		}
		// Topic holds details about calls to the Topic method.
		Topic []struct {
		}
	}
}

// HighWaterMarkOffset calls HighWaterMarkOffsetFunc.
func (mock *SaramaConsumerGroupClaimMock) HighWaterMarkOffset() int64 {
	if mock.HighWaterMarkOffsetFunc == nil {
		panic("SaramaConsumerGroupClaimMock.HighWaterMarkOffsetFunc: method is nil but SaramaConsumerGroupClaim.HighWaterMarkOffset was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaConsumerGroupClaimMockHighWaterMarkOffset.Lock()
	mock.calls.HighWaterMarkOffset = append(mock.calls.HighWaterMarkOffset, callInfo)
	lockSaramaConsumerGroupClaimMockHighWaterMarkOffset.Unlock()
	return mock.HighWaterMarkOffsetFunc()
}

// HighWaterMarkOffsetCalls gets all the calls that were made to HighWaterMarkOffset.
// Check the length with:
//     len(mockedSaramaConsumerGroupClaim.HighWaterMarkOffsetCalls())
func (mock *SaramaConsumerGroupClaimMock) HighWaterMarkOffsetCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaConsumerGroupClaimMockHighWaterMarkOffset.RLock()
	calls = mock.calls.HighWaterMarkOffset
	lockSaramaConsumerGroupClaimMockHighWaterMarkOffset.RUnlock()
	return calls
}

// InitialOffset calls InitialOffsetFunc.
func (mock *SaramaConsumerGroupClaimMock) InitialOffset() int64 {
	if mock.InitialOffsetFunc == nil {
		panic("SaramaConsumerGroupClaimMock.InitialOffsetFunc: method is nil but SaramaConsumerGroupClaim.InitialOffset was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaConsumerGroupClaimMockInitialOffset.Lock()
	mock.calls.InitialOffset = append(mock.calls.InitialOffset, callInfo)
	lockSaramaConsumerGroupClaimMockInitialOffset.Unlock()
	return mock.InitialOffsetFunc()
}

// InitialOffsetCalls gets all the calls that were made to InitialOffset.
// Check the length with:
//     len(mockedSaramaConsumerGroupClaim.InitialOffsetCalls())
func (mock *SaramaConsumerGroupClaimMock) InitialOffsetCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaConsumerGroupClaimMockInitialOffset.RLock()
	calls = mock.calls.InitialOffset
	lockSaramaConsumerGroupClaimMockInitialOffset.RUnlock()
	return calls
}

// Messages calls MessagesFunc.
func (mock *SaramaConsumerGroupClaimMock) Messages() <-chan *sarama.ConsumerMessage {
	if mock.MessagesFunc == nil {
		panic("SaramaConsumerGroupClaimMock.MessagesFunc: method is nil but SaramaConsumerGroupClaim.Messages was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaConsumerGroupClaimMockMessages.Lock()
	mock.calls.Messages = append(mock.calls.Messages, callInfo)
	lockSaramaConsumerGroupClaimMockMessages.Unlock()
	return mock.MessagesFunc()
}

// MessagesCalls gets all the calls that were made to Messages.
// Check the length with:
//     len(mockedSaramaConsumerGroupClaim.MessagesCalls())
func (mock *SaramaConsumerGroupClaimMock) MessagesCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaConsumerGroupClaimMockMessages.RLock()
	calls = mock.calls.Messages
	lockSaramaConsumerGroupClaimMockMessages.RUnlock()
	return calls
}

// Partition calls PartitionFunc.
func (mock *SaramaConsumerGroupClaimMock) Partition() int32 {
	if mock.PartitionFunc == nil {
		panic("SaramaConsumerGroupClaimMock.PartitionFunc: method is nil but SaramaConsumerGroupClaim.Partition was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaConsumerGroupClaimMockPartition.Lock()
	mock.calls.Partition = append(mock.calls.Partition, callInfo)
	lockSaramaConsumerGroupClaimMockPartition.Unlock()
	return mock.PartitionFunc()
}

// PartitionCalls gets all the calls that were made to Partition.
// Check the length with:
//     len(mockedSaramaConsumerGroupClaim.PartitionCalls())
func (mock *SaramaConsumerGroupClaimMock) PartitionCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaConsumerGroupClaimMockPartition.RLock()
	calls = mock.calls.Partition
	lockSaramaConsumerGroupClaimMockPartition.RUnlock()
	return calls
}

// Topic calls TopicFunc.
func (mock *SaramaConsumerGroupClaimMock) Topic() string {
	if mock.TopicFunc == nil {
		panic("SaramaConsumerGroupClaimMock.TopicFunc: method is nil but SaramaConsumerGroupClaim.Topic was just called")
	}
	callInfo := struct {
	}{}
	lockSaramaConsumerGroupClaimMockTopic.Lock()
	mock.calls.Topic = append(mock.calls.Topic, callInfo)
	lockSaramaConsumerGroupClaimMockTopic.Unlock()
	return mock.TopicFunc()
}

// TopicCalls gets all the calls that were made to Topic.
// Check the length with:
//     len(mockedSaramaConsumerGroupClaim.TopicCalls())
func (mock *SaramaConsumerGroupClaimMock) TopicCalls() []struct {
} {
	var calls []struct {
	}
	lockSaramaConsumerGroupClaimMockTopic.RLock()
	calls = mock.calls.Topic
	lockSaramaConsumerGroupClaimMockTopic.RUnlock()
	return calls
}
