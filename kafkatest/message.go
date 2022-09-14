package kafkatest

import (
	"context"
	"sync"

	kafka "github.com/ONSdigital/dp-kafka/v2"
)

var _ kafka.Message = (*Message)(nil)

type TestHeader map[string]string

// mInternal is an internal struct to keep track of the message mock state
type mInternal struct {
	data             []byte
	marked           bool
	committed        bool
	offset           int64
	upstreamDoneChan chan struct{}
	mu               sync.Mutex
	context          context.Context
	hValue           TestHeader
}

// Message allows a mock message to return the configured data, and capture whether commit has been called.
type Message struct {
	*mInternal
	*MessageMock
}

// NewMessage returns a new mock message containing the given data.
func NewMessage(data []byte, offset int64, hValue ...TestHeader) *Message {
	internal := &mInternal{
		data:             data,
		marked:           false,
		committed:        false,
		offset:           offset,
		upstreamDoneChan: make(chan struct{}),
		mu:               sync.Mutex{},
		context:          context.TODO(),
	}

	for _, header := range hValue {
		internal.hValue = header
	}
	return &Message{
		internal,
		&MessageMock{
			GetDataFunc:          internal.getDataFunc,
			MarkFunc:             internal.markFunc,
			CommitFunc:           internal.commitFunc,
			ReleaseFunc:          internal.releaseFunc,
			CommitAndReleaseFunc: internal.commitAndReleaseFunc,
			OffsetFunc:           internal.offsetFunc,
			UpstreamDoneFunc:     internal.upstreamDoneFunc,
			ContextFunc:          internal.ContextFunc,
			GetHeaderFunc:        internal.GetHeaderFunc,
		},
	}
}

// getDataFunc returns the data that was added to the struct.
func (internal *mInternal) getDataFunc() []byte {
	return internal.data
}

// offsetFunc returns the message offset.
func (internal *mInternal) offsetFunc() int64 {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	return internal.offset
}

// markFunc captures the fact that the message was marked.
func (internal *mInternal) markFunc() {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	internal.marked = true
}

// commitFunc captures the fact that the message was marked and committed.
func (internal *mInternal) commitFunc() {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	internal.marked = true
	internal.committed = true
}

// releaseFunc closes the upstreamDone channel.
func (internal *mInternal) releaseFunc() {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	close(internal.upstreamDoneChan)
}

// commitAndReleaseFunc captures the fact that the mesage was marked and release, and closes the upstreamDone channel.
func (internal *mInternal) commitAndReleaseFunc() {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	internal.marked = true
	internal.committed = true
	close(internal.upstreamDoneChan)
}

// Context returns a context with traceid.
func (internal *mInternal) ContextFunc() context.Context {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	return internal.context
}

// GetHeader takes a key for the header and returns the value if the key exist in the header.
func (internal *mInternal) GetHeaderFunc(key string) string {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	return internal.hValue[key]
}

// upstreamDoneFunc returns the message upstreamDone channel.
func (internal *mInternal) upstreamDoneFunc() chan struct{} {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	return internal.upstreamDoneChan
}

// IsMarked returns true if the message was marked as consumed.
func (internal *mInternal) IsMarked() bool {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	return internal.marked
}

// IsCommittedFunc returns true if the message offset was committed.
func (internal *mInternal) IsCommitted() bool {
	internal.mu.Lock()
	defer internal.mu.Unlock()
	return internal.committed
}
