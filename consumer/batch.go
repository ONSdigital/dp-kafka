package consumer

import (
	"context"
	"sync"

	"github.com/ONSdigital/dp-kafka/v3/message"
)

// Batch handles adding raw messages to a batch of ObservationExtracted events.
type Batch struct {
	maxSize  int
	messages []message.Message
	mutex    *sync.Mutex
}

// NewBatch returns a new batch instance of the given size.
func NewBatch(batchSize int) *Batch {
	return &Batch{
		maxSize: batchSize,
		mutex:   &sync.Mutex{},
	}
}

// Add a message to the batch.
func (batch *Batch) Add(ctx context.Context, message message.Message) {
	batch.mutex.Lock()
	defer batch.mutex.Unlock()

	batch.messages = append(batch.messages, message)
}

// Size returns the number of messages currently in the batch.
func (batch *Batch) Size() int {
	return len(batch.messages)
}

// IsFull returns true if the batch is full based on the configured maxSize.
func (batch *Batch) IsFull() bool {
	return len(batch.messages) == batch.maxSize
}

// IsEmpty returns true if the batch has no messages in it.
func (batch *Batch) IsEmpty() bool {
	return len(batch.messages) == 0
}

// Commit is called when the batch has been processed. The last message has been released already, so at this point we just need to commit
func (batch *Batch) Commit() {
	batch.mutex.Lock()
	defer batch.mutex.Unlock()

	for i, msg := range batch.messages {
		if i < len(batch.messages)-1 {
			msg.Mark() // mark all messages
			continue
		}
		msg.Commit() // commit last one (will commit all marked offsets as consumed)
	}
	batch.clear()
}

// clear will reset to batch to contain no messages (concurrency unsafe)
func (batch *Batch) clear() {
	batch.messages = batch.messages[0:0]
}
