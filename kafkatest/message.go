package kafkatest

import (
	"context"
	"sync"

	"github.com/ONSdigital/dp-kafka/v3/mock"
)

// mInternal is an internal struct to keep track of the message mock state
type mInternal struct {
	data             []byte
	marked           bool
	committed        bool
	offset           int64
	options          *Options
	upstreamDoneChan chan struct{}
	mu               sync.Mutex
}

// Message allows a mock message to return the configured data, and capture whether commit has been called.
type Message struct {
	*mInternal
	*mock.MessageMock
}

// NewMessage returns a new mock message containing the given data.
func NewMessage(data []byte, offset int64, optionFuncs ...func(*Options) error) (*Message, error) {
	internal := &mInternal{
		data:             data,
		marked:           false,
		committed:        false,
		offset:           offset,
		options:          &Options{},
		upstreamDoneChan: make(chan struct{}),
		mu:               sync.Mutex{},
	}

	// Option paremeters values:
	for _, op := range optionFuncs {
		err := op(internal.options)
		if err != nil {
			return nil, err
		}
	}

	return &Message{
		internal,
		&mock.MessageMock{
			GetDataFunc:          internal.getDataFunc,
			GetHeaderFunc:        internal.getHeaderFunc,
			MarkFunc:             internal.markFunc,
			CommitFunc:           internal.commitFunc,
			ContextFunc:          func() context.Context { return context.Background() },
			ReleaseFunc:          internal.releaseFunc,
			CommitAndReleaseFunc: internal.commitAndReleaseFunc,
			OffsetFunc:           internal.offsetFunc,
			UpstreamDoneFunc:     internal.upstreamDoneFunc,
		},
	}, nil
}

// getDataFunc returns the data that was added to the struct.
func (internal *mInternal) getDataFunc() []byte {
	return internal.data
}

// getHeaderFunc returns the value of the given  that was added to the struct.
func (internal *mInternal) getHeaderFunc(key string) string {
	return internal.options.Headers[key]
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
