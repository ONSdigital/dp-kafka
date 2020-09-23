package kafkatest

import (
	"sync"

	kafka "github.com/ONSdigital/dp-kafka"
)

var _ kafka.Message = (*Message)(nil)

// mInternal is an internal struct to keep track of the message mock state
type mInternal struct {
	data             []byte
	committed        bool
	offset           int64
	upstreamDoneChan chan struct{}
	mu               sync.Mutex
}

// Message allows a mock message to return the configured data, and capture whether commit has been called.
type Message struct {
	*mInternal
	*MessageMock
}

// NewMessage returns a new mock message containing the given data.
func NewMessage(data []byte, offset int64) *Message {
	internal := &mInternal{
		data:             data,
		committed:        false,
		offset:           offset,
		upstreamDoneChan: make(chan struct{}),
		mu:               sync.Mutex{},
	}
	return &Message{
		internal,
		&MessageMock{
			GetDataFunc:      internal.getDataFunc,
			CommitFunc:       internal.commitFunc,
			OffsetFunc:       internal.offsetFunc,
			UpstreamDoneFunc: internal.upstreamDoneFunc,
		},
	}
}

// getDataFunc returns the data that was added to the struct.
func (internal *mInternal) getDataFunc() []byte {
	return internal.data
}

// commitFunc captures the fact that the method was called.
func (internal *mInternal) commitFunc() {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	internal.committed = true
}

// committedFunc returns true if commit was called on this message.
func (internal *mInternal) committedFunc() bool {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	return internal.committed
}

// offsetFunc returns the message offset
func (internal *mInternal) offsetFunc() int64 {
	return internal.offset
}

// upstreamDoneFunc returns the message upstreamDone channel
func (internal *mInternal) upstreamDoneFunc() chan struct{} {
	return internal.upstreamDoneChan
}
