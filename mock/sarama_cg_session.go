// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"context"
	"github.com/Shopify/sarama"
	"sync"
)

// SaramaConsumerGroupSessionMock is a mock implementation of kafka.SaramaConsumerGroupSession.
//
// 	func TestSomethingThatUsesSaramaConsumerGroupSession(t *testing.T) {
//
// 		// make and configure a mocked kafka.SaramaConsumerGroupSession
// 		mockedSaramaConsumerGroupSession := &SaramaConsumerGroupSessionMock{
// 			ClaimsFunc: func() map[string][]int32 {
// 				panic("mock out the Claims method")
// 			},
// 			CommitFunc: func()  {
// 				panic("mock out the Commit method")
// 			},
// 			ContextFunc: func() context.Context {
// 				panic("mock out the Context method")
// 			},
// 			GenerationIDFunc: func() int32 {
// 				panic("mock out the GenerationID method")
// 			},
// 			MarkMessageFunc: func(msg *sarama.ConsumerMessage, metadata string)  {
// 				panic("mock out the MarkMessage method")
// 			},
// 			MarkOffsetFunc: func(topic string, partition int32, offset int64, metadata string)  {
// 				panic("mock out the MarkOffset method")
// 			},
// 			MemberIDFunc: func() string {
// 				panic("mock out the MemberID method")
// 			},
// 			ResetOffsetFunc: func(topic string, partition int32, offset int64, metadata string)  {
// 				panic("mock out the ResetOffset method")
// 			},
// 		}
//
// 		// use mockedSaramaConsumerGroupSession in code that requires kafka.SaramaConsumerGroupSession
// 		// and then make assertions.
//
// 	}
type SaramaConsumerGroupSessionMock struct {
	// ClaimsFunc mocks the Claims method.
	ClaimsFunc func() map[string][]int32

	// CommitFunc mocks the Commit method.
	CommitFunc func()

	// ContextFunc mocks the Context method.
	ContextFunc func() context.Context

	// GenerationIDFunc mocks the GenerationID method.
	GenerationIDFunc func() int32

	// MarkMessageFunc mocks the MarkMessage method.
	MarkMessageFunc func(msg *sarama.ConsumerMessage, metadata string)

	// MarkOffsetFunc mocks the MarkOffset method.
	MarkOffsetFunc func(topic string, partition int32, offset int64, metadata string)

	// MemberIDFunc mocks the MemberID method.
	MemberIDFunc func() string

	// ResetOffsetFunc mocks the ResetOffset method.
	ResetOffsetFunc func(topic string, partition int32, offset int64, metadata string)

	// calls tracks calls to the methods.
	calls struct {
		// Claims holds details about calls to the Claims method.
		Claims []struct {
		}
		// Commit holds details about calls to the Commit method.
		Commit []struct {
		}
		// Context holds details about calls to the Context method.
		Context []struct {
		}
		// GenerationID holds details about calls to the GenerationID method.
		GenerationID []struct {
		}
		// MarkMessage holds details about calls to the MarkMessage method.
		MarkMessage []struct {
			// Msg is the msg argument value.
			Msg *sarama.ConsumerMessage
			// Metadata is the metadata argument value.
			Metadata string
		}
		// MarkOffset holds details about calls to the MarkOffset method.
		MarkOffset []struct {
			// Topic is the topic argument value.
			Topic string
			// Partition is the partition argument value.
			Partition int32
			// Offset is the offset argument value.
			Offset int64
			// Metadata is the metadata argument value.
			Metadata string
		}
		// MemberID holds details about calls to the MemberID method.
		MemberID []struct {
		}
		// ResetOffset holds details about calls to the ResetOffset method.
		ResetOffset []struct {
			// Topic is the topic argument value.
			Topic string
			// Partition is the partition argument value.
			Partition int32
			// Offset is the offset argument value.
			Offset int64
			// Metadata is the metadata argument value.
			Metadata string
		}
	}
	lockClaims       sync.RWMutex
	lockCommit       sync.RWMutex
	lockContext      sync.RWMutex
	lockGenerationID sync.RWMutex
	lockMarkMessage  sync.RWMutex
	lockMarkOffset   sync.RWMutex
	lockMemberID     sync.RWMutex
	lockResetOffset  sync.RWMutex
}

// Claims calls ClaimsFunc.
func (mock *SaramaConsumerGroupSessionMock) Claims() map[string][]int32 {
	if mock.ClaimsFunc == nil {
		panic("SaramaConsumerGroupSessionMock.ClaimsFunc: method is nil but SaramaConsumerGroupSession.Claims was just called")
	}
	callInfo := struct {
	}{}
	mock.lockClaims.Lock()
	mock.calls.Claims = append(mock.calls.Claims, callInfo)
	mock.lockClaims.Unlock()
	return mock.ClaimsFunc()
}

// ClaimsCalls gets all the calls that were made to Claims.
// Check the length with:
//     len(mockedSaramaConsumerGroupSession.ClaimsCalls())
func (mock *SaramaConsumerGroupSessionMock) ClaimsCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockClaims.RLock()
	calls = mock.calls.Claims
	mock.lockClaims.RUnlock()
	return calls
}

// Commit calls CommitFunc.
func (mock *SaramaConsumerGroupSessionMock) Commit() {
	if mock.CommitFunc == nil {
		panic("SaramaConsumerGroupSessionMock.CommitFunc: method is nil but SaramaConsumerGroupSession.Commit was just called")
	}
	callInfo := struct {
	}{}
	mock.lockCommit.Lock()
	mock.calls.Commit = append(mock.calls.Commit, callInfo)
	mock.lockCommit.Unlock()
	mock.CommitFunc()
}

// CommitCalls gets all the calls that were made to Commit.
// Check the length with:
//     len(mockedSaramaConsumerGroupSession.CommitCalls())
func (mock *SaramaConsumerGroupSessionMock) CommitCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockCommit.RLock()
	calls = mock.calls.Commit
	mock.lockCommit.RUnlock()
	return calls
}

// Context calls ContextFunc.
func (mock *SaramaConsumerGroupSessionMock) Context() context.Context {
	if mock.ContextFunc == nil {
		panic("SaramaConsumerGroupSessionMock.ContextFunc: method is nil but SaramaConsumerGroupSession.Context was just called")
	}
	callInfo := struct {
	}{}
	mock.lockContext.Lock()
	mock.calls.Context = append(mock.calls.Context, callInfo)
	mock.lockContext.Unlock()
	return mock.ContextFunc()
}

// ContextCalls gets all the calls that were made to Context.
// Check the length with:
//     len(mockedSaramaConsumerGroupSession.ContextCalls())
func (mock *SaramaConsumerGroupSessionMock) ContextCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockContext.RLock()
	calls = mock.calls.Context
	mock.lockContext.RUnlock()
	return calls
}

// GenerationID calls GenerationIDFunc.
func (mock *SaramaConsumerGroupSessionMock) GenerationID() int32 {
	if mock.GenerationIDFunc == nil {
		panic("SaramaConsumerGroupSessionMock.GenerationIDFunc: method is nil but SaramaConsumerGroupSession.GenerationID was just called")
	}
	callInfo := struct {
	}{}
	mock.lockGenerationID.Lock()
	mock.calls.GenerationID = append(mock.calls.GenerationID, callInfo)
	mock.lockGenerationID.Unlock()
	return mock.GenerationIDFunc()
}

// GenerationIDCalls gets all the calls that were made to GenerationID.
// Check the length with:
//     len(mockedSaramaConsumerGroupSession.GenerationIDCalls())
func (mock *SaramaConsumerGroupSessionMock) GenerationIDCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockGenerationID.RLock()
	calls = mock.calls.GenerationID
	mock.lockGenerationID.RUnlock()
	return calls
}

// MarkMessage calls MarkMessageFunc.
func (mock *SaramaConsumerGroupSessionMock) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	if mock.MarkMessageFunc == nil {
		panic("SaramaConsumerGroupSessionMock.MarkMessageFunc: method is nil but SaramaConsumerGroupSession.MarkMessage was just called")
	}
	callInfo := struct {
		Msg      *sarama.ConsumerMessage
		Metadata string
	}{
		Msg:      msg,
		Metadata: metadata,
	}
	mock.lockMarkMessage.Lock()
	mock.calls.MarkMessage = append(mock.calls.MarkMessage, callInfo)
	mock.lockMarkMessage.Unlock()
	mock.MarkMessageFunc(msg, metadata)
}

// MarkMessageCalls gets all the calls that were made to MarkMessage.
// Check the length with:
//     len(mockedSaramaConsumerGroupSession.MarkMessageCalls())
func (mock *SaramaConsumerGroupSessionMock) MarkMessageCalls() []struct {
	Msg      *sarama.ConsumerMessage
	Metadata string
} {
	var calls []struct {
		Msg      *sarama.ConsumerMessage
		Metadata string
	}
	mock.lockMarkMessage.RLock()
	calls = mock.calls.MarkMessage
	mock.lockMarkMessage.RUnlock()
	return calls
}

// MarkOffset calls MarkOffsetFunc.
func (mock *SaramaConsumerGroupSessionMock) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	if mock.MarkOffsetFunc == nil {
		panic("SaramaConsumerGroupSessionMock.MarkOffsetFunc: method is nil but SaramaConsumerGroupSession.MarkOffset was just called")
	}
	callInfo := struct {
		Topic     string
		Partition int32
		Offset    int64
		Metadata  string
	}{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Metadata:  metadata,
	}
	mock.lockMarkOffset.Lock()
	mock.calls.MarkOffset = append(mock.calls.MarkOffset, callInfo)
	mock.lockMarkOffset.Unlock()
	mock.MarkOffsetFunc(topic, partition, offset, metadata)
}

// MarkOffsetCalls gets all the calls that were made to MarkOffset.
// Check the length with:
//     len(mockedSaramaConsumerGroupSession.MarkOffsetCalls())
func (mock *SaramaConsumerGroupSessionMock) MarkOffsetCalls() []struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
} {
	var calls []struct {
		Topic     string
		Partition int32
		Offset    int64
		Metadata  string
	}
	mock.lockMarkOffset.RLock()
	calls = mock.calls.MarkOffset
	mock.lockMarkOffset.RUnlock()
	return calls
}

// MemberID calls MemberIDFunc.
func (mock *SaramaConsumerGroupSessionMock) MemberID() string {
	if mock.MemberIDFunc == nil {
		panic("SaramaConsumerGroupSessionMock.MemberIDFunc: method is nil but SaramaConsumerGroupSession.MemberID was just called")
	}
	callInfo := struct {
	}{}
	mock.lockMemberID.Lock()
	mock.calls.MemberID = append(mock.calls.MemberID, callInfo)
	mock.lockMemberID.Unlock()
	return mock.MemberIDFunc()
}

// MemberIDCalls gets all the calls that were made to MemberID.
// Check the length with:
//     len(mockedSaramaConsumerGroupSession.MemberIDCalls())
func (mock *SaramaConsumerGroupSessionMock) MemberIDCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockMemberID.RLock()
	calls = mock.calls.MemberID
	mock.lockMemberID.RUnlock()
	return calls
}

// ResetOffset calls ResetOffsetFunc.
func (mock *SaramaConsumerGroupSessionMock) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	if mock.ResetOffsetFunc == nil {
		panic("SaramaConsumerGroupSessionMock.ResetOffsetFunc: method is nil but SaramaConsumerGroupSession.ResetOffset was just called")
	}
	callInfo := struct {
		Topic     string
		Partition int32
		Offset    int64
		Metadata  string
	}{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Metadata:  metadata,
	}
	mock.lockResetOffset.Lock()
	mock.calls.ResetOffset = append(mock.calls.ResetOffset, callInfo)
	mock.lockResetOffset.Unlock()
	mock.ResetOffsetFunc(topic, partition, offset, metadata)
}

// ResetOffsetCalls gets all the calls that were made to ResetOffset.
// Check the length with:
//     len(mockedSaramaConsumerGroupSession.ResetOffsetCalls())
func (mock *SaramaConsumerGroupSessionMock) ResetOffsetCalls() []struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
} {
	var calls []struct {
		Topic     string
		Partition int32
		Offset    int64
		Metadata  string
	}
	mock.lockResetOffset.RLock()
	calls = mock.calls.ResetOffset
	mock.lockResetOffset.RUnlock()
	return calls
}
