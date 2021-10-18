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

// listen creates one go-routine for each worker, which listens to the Upstream channel
// when a new message arrives to the channel, the handler func is called.
// when the closer channel is closed, all go-routines will exit (after finishing processing any in-flight message)
func (cg *ConsumerGroup) listen(ctx context.Context) {
	if cg.handler == nil {
		return
	}

	// handleMessage is an aux func to handle one message.
	// The batch will be committed unless a 'Commiter' error is returned and its Commit() func returns false
	var handleMessage = func(workerID int, msg Message) {
		commit := true // commit messages by default
		if err := cg.handler(context.Background(), workerID, msg); err != nil {
			if cerr, ok := err.(Commiter); ok {
				if !cerr.Commit() {
					commit = false // caller does not want the message to be committed
				}
			}
			logData := UnwrapLogData(err) // this will unwrap any logData present in the error
			logData["commit_message"] = commit
			log.Error(ctx, "failed to handle message", err, logData)
		}

		if commit {
			msg.Commit()
		}
	}

	// consume is a looping func that listens to the Upstream channel and handles each received message
	var consume = func(workerID int) {
		for {
			select {
			case msg, ok := <-cg.Channels().Upstream:
				if !ok {
					log.Info(ctx, "upstream channel closed - closing event consumer loop", log.Data{"worker_id": workerID})
					return
				}

				handleMessage(workerID, msg)

				msg.Release()
			case <-cg.channels.Closer:
				log.Info(ctx, "closing event consumer loop because closer channel is closed", log.Data{"worker_id": workerID})
				return
			}
		}
	}

	// workers to consume messages in parallel
	for w := 1; w <= cg.numWorkers; w++ {
		go consume(w)
	}
}

// listen creates one go-routine for each worker, which listens to the Upstream channel
// when a new message arrives to the channel, the handler func is called.
// when the closer channel is closed, all go-routines will exit (after finishing processing any in-flight message)
func (cg *ConsumerGroup) listenBatch(ctx context.Context) {
	if cg.batchHandler == nil {
		return
	}

	// handleBatch is an aux func to handle one batch.
	// The batch will be committed unless a 'Commiter' error is returned and its Commit() func returns false
	var handleBatch = func(batch *Batch) {
		commit := true // commit batch by default
		if err := cg.batchHandler(ctx, batch.messages); err != nil {
			if cerr, ok := err.(Commiter); ok {
				if !cerr.Commit() {
					commit = false // caller does not want the batch to be committed
				}
			}
			logData := UnwrapLogData(err) // this will unwrap any logData present in the error
			logData["commit_batch"] = commit
			log.Error(ctx, "failed to handle message batch", err, logData)
		}

		if commit {
			batch.Commit() // mark all messages and commit session
		}
		batch.Clear() // always clear batch
	}

	// consume is a looping func that listens to the Upstream channel, accumulates messages and handles
	// batches once it is full or the waitTime has expired.
	var consume = func() {
		batch := NewBatch(cg.batchSize)

		for {
			select {
			case msg, ok := <-cg.Channels().Upstream:
				if !ok {
					log.Info(ctx, "upstream channel closed - closing batch consumer loop")
					return
				}

				batch.Add(ctx, msg)
				if batch.IsFull() {
					log.Info(ctx, "batch is full - processing batch", log.Data{"batchsize": batch.Size()})
					handleBatch(batch)
				}

				msg.Release() // always release the message

			case <-time.After(cg.batchWaitTime):
				if batch.IsEmpty() {
					continue
				}

				log.Info(ctx, "batch wait time reached - processing batch", log.Data{"batchsize": batch.Size()})
				handleBatch(batch)

			case <-cg.channels.Closer:
				log.Info(ctx, "closing event consumer loop because closer channel is closed")
				return
			}
		}
	}

	go consume()
}
