package kafka

import (
	"context"
	"time"

	"github.com/ONSdigital/log.go/v2/log"
)

// Handler represents a handler for processing a single kafka message.
// IMPORTANT: if maxConsumers > 1 then this method needs to be thread safe.
type Handler func(ctx context.Context, workerID int, msg Message) error

// BatchHandler represents a handler for processing a batch of kafka messages.
// This method will be called by only one go-routine at a time.
type BatchHandler func(ctx context.Context, batch []Message) error

// Commiter represents an error type that defines a bool method to enable or disable a message/batch to be committed.
type Commiter interface {
	Commit() bool
}

// shallCommit determines if a message or batch needs to be committed.
// If the error is not nil and it satisfies the Commiter interface then the user determines if the message will be committed or not.
// Otherwise, it will be committed.
func shallCommit(err error) bool {
	if cerr, ok := err.(Commiter); ok {
		return cerr.Commit()
	}
	return true
}

// handleMessage is an aux func to handle one message.
// The batch will be committed unless a 'Commiter' error is returned and its Commit() func returns false
func (cg *ConsumerGroup) handleMessage(ctx context.Context, workerID int, msg Message) {
	msgCtx, cancel := context.WithCancel(ctx)
	err := cg.handler(msgCtx, workerID, msg)
	cancel() // TODO we might need to cancel the context also when Sarama session Cleanup is called
	commit := shallCommit(err)
	if err != nil {
		logData := UnwrapLogData(err) // this will unwrap any logData present in the error
		logData["commit_message"] = commit
		log.Error(ctx, "failed to handle message", err, logData)
	}
	if commit {
		msg.Commit()
	}
}

// handleBatch is an aux func to handle one batch.
// The batch will be committed unless a 'Commiter' error is returned and its Commit() func returns false
func (cg *ConsumerGroup) handleBatch(ctx context.Context, batch *Batch) {
	batchCtx, cancel := context.WithCancel(ctx)
	err := cg.batchHandler(batchCtx, batch.messages)
	cancel() // TODO we might need to cancel the context also when Sarama session Cleanup is called
	commit := shallCommit(err)
	if err != nil {
		logData := UnwrapLogData(err) // this will unwrap any logData present in the error
		logData["commit_batch"] = commit
		log.Error(ctx, "failed to handle message batch", err, logData)
	}
	if commit {
		batch.Commit() // mark all messages and commit session
	}
	batch.Clear() // always clear batch
}

// consumeMessage is a looping func that listens to the Upstream channel and handles each received message
// This function is to be called as a go routine because it implements a blocking loop
func (cg *ConsumerGroup) consumeMessage(ctx context.Context, workerID int) {
	defer cg.wgClose.Done()

	for {
		select {
		case msg, ok := <-cg.Channels().Upstream:
			if !ok {
				log.Info(ctx, "upstream channel closed - closing event handler loop", log.Data{"worker_id": workerID})
				return
			}

			cg.handleMessage(ctx, workerID, msg)

			msg.Release()
		case <-cg.channels.Closer:
			log.Info(ctx, "closer channel closed - closing event handler loop", log.Data{"worker_id": workerID})
			return
		}
	}
}

// consumeBatch is a looping func that listens to the Upstream channel, accumulates messages and handles
// batches once it is full or the waitTime has expired.
// This function is to be called as a go routine because it implements a blocking loop
func (cg *ConsumerGroup) consumeBatch(ctx context.Context) {
	defer cg.wgClose.Done()
	batch := NewBatch(cg.batchSize)

	for {
		select {
		case msg, ok := <-cg.Channels().Upstream:
			if !ok {
				log.Info(ctx, "upstream channel closed - closing event batch handler loop")
				return
			}

			batch.Add(ctx, msg)
			if batch.IsFull() {
				cg.handleBatch(ctx, batch)
			}

			msg.Release() // always release the message

		case <-time.After(cg.batchWaitTime):
			if batch.IsEmpty() {
				continue
			}

			cg.handleBatch(ctx, batch)

		case <-cg.channels.Closer:
			log.Info(ctx, "closer channel closed - closing event batch handler loop")
			return
		}
	}
}

// listen creates one go-routine for each worker, which listens to the Upstream channel
// when a new message arrives to the channel, the handler func is called.
// when the closer channel is closed, all go-routines will exit (after finishing processing any in-flight message)
func (cg *ConsumerGroup) listen(ctx context.Context) {
	if cg.handler == nil {
		return
	}

	// workers to consume messages in parallel
	cg.wgClose.Add(cg.numWorkers)
	for w := 1; w <= cg.numWorkers; w++ {
		go cg.consumeMessage(ctx, w)
	}
}

// listen creates one go-routine for each worker, which listens to the Upstream channel
// when a new message arrives to the channel, the handler func is called.
// when the closer channel is closed, all go-routines will exit (after finishing processing any in-flight message)
func (cg *ConsumerGroup) listenBatch(ctx context.Context) {
	if cg.batchHandler == nil {
		return
	}

	// only 1 worker is created for batch handling
	cg.wgClose.Add(1)
	go cg.consumeBatch(ctx)
}
