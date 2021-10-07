package consumer

import (
	"context"

	"github.com/ONSdigital/dp-kafka/v3/message"
	"github.com/ONSdigital/log.go/v2/log"
)

// Handler represents a handler for processing a single kafka message.
// IMPORTANT: if maxConsumers > 1 then this method needs to be thread safe.
type Handler func(ctx context.Context, workerID int, msg message.Message) error

// listen creates one go-routine for each worker, which listens to the Upstream channel
// when a new message arrives to the channel, the handler func is called.
// when the closer channel is closed, all go-routines will exit (after finishing processing any in-flight message)
func (cg *ConsumerGroup) listen(ctx context.Context) {
	if cg.handler == nil {
		return
	}

	var consume = func(workerID int) {
		for {
			select {
			case msg, ok := <-cg.Channels().Upstream:
				if !ok {
					log.Info(ctx, "upstream channel closed - closing event consumer loop", log.Data{"worker_id": workerID})
					return
				}

				if err := cg.handler(context.Background(), workerID, msg); err != nil {
					log.Error(ctx, "failed to handle message", err, log.Data{
						"log_data": unwrapLogData(err), // this will unwrap any logData present in the error
					})
				}

				msg.CommitAndRelease()
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
