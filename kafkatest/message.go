package kafkatest

import (
	"sync"

	kafka "github.com/ONSdigital/dp-kafka"
)

var _ kafka.Message = (*Message)(nil)

// Message allows a mock message to return the configured data, and capture whether commit has been called.
type Message struct {
	data           []byte
	committed      bool
	offset         int64
	mu             sync.Mutex
	getDataCalls   int
	commitCalls    int
	committedCalls int
	offsetCalls    int
}

// NewMessage returns a new mock message containing the given data.
func NewMessage(data []byte, offset int64) *Message {
	return &Message{
		data:           data,
		offset:         offset,
		getDataCalls:   0,
		commitCalls:    0,
		committedCalls: 0,
		offsetCalls:    0,
	}
}

// GetData returns the data that was added to the struct.
func (m *Message) GetData() []byte {
	m.getDataCalls++
	return m.data
}

// GetDataCalls returns the number of calls to GetData()
func (m *Message) GetDataCalls() int {
	return m.getDataCalls
}

// Commit captures the fact that the method was called.
func (m *Message) Commit() {
	m.mu.Lock()
	m.commitCalls++
	m.committed = true
	m.mu.Unlock()
}

// CommitCalls returns the number of calls to Commit()
func (m *Message) CommitCalls() int {
	return m.commitCalls
}

// Committed returns true if commit was called on this message.
func (m *Message) Committed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committedCalls++
	return m.committed
}

// CommittedCalls returns the number of calls to Committed()
func (m *Message) CommittedCalls() int {
	return m.committedCalls
}

// Offset returns the message offset
func (m *Message) Offset() int64 {
	m.offsetCalls++
	return m.offset
}

// OffsetCalls returns the number of calls to Offset()
func (m *Message) OffsetCalls() int {
	return m.offsetCalls
}
