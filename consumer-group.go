package kafka

import (
	"context"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
	cluster "github.com/bsm/sarama-cluster"
)

var tick = time.Millisecond * 1500

// ConsumerGroup represents a Kafka consumer group instance.
type ConsumerGroup struct {
	brokers  []string
	channels *ConsumerGroupChannels
	consumer *cluster.Consumer
	topic    string
	group    string
	sync     bool
	Check    *health.Check
}

// Incoming provides a channel of incoming messages.
func (cg ConsumerGroup) Incoming() chan Message {
	return cg.channels.Upstream
}

// Errors provides a channel of incoming errors.
func (cg ConsumerGroup) Errors() chan error {
	return cg.channels.Errors
}

// Release signals that upstream has completed an incoming message
// i.e. move on to read the next message
func (cg ConsumerGroup) Release() {
	cg.channels.UpstreamDone <- true
}

// CommitAndRelease commits the consumed message and release the consumer listener to read another message
func (cg ConsumerGroup) CommitAndRelease(msg Message) {
	msg.Commit()
	cg.Release()
}

// StopListeningToConsumer stops any more messages being consumed off kafka topic
func (cg *ConsumerGroup) StopListeningToConsumer(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	close(cg.channels.Closer)

	logData := log.Data{"topic": cg.topic, "group": cg.group}
	select {
	case <-cg.channels.Closed:
		log.Event(ctx, "StopListeningToConsumer got confirmation of closed kafka consumer listener", logData)
	case <-ctx.Done():
		err = ctx.Err()
		log.Event(ctx, "StopListeningToConsumer abandoned: context done", log.Error(err), logData)
	}
	return
}

// Close safely closes the consumer and releases all resources.
// pass in a context with a timeout or deadline.
// Passing a nil context will provide no timeout but is not recommended
func (cg *ConsumerGroup) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// close(closer) - the select{} avoids panic if already closed (by StopListeningToConsumer)
	select {
	case <-cg.channels.Closer:
	default:
		close(cg.channels.Closer)
	}

	logData := log.Data{"topic": cg.topic, "group": cg.group}
	select {
	case <-cg.channels.Closed:
		close(cg.channels.Errors)
		close(cg.channels.Upstream)

		if err = cg.consumer.Close(); err != nil {
			log.Event(ctx, "Close failed of kafka consumer group", log.Error(err), logData)
		} else {
			log.Event(ctx, "Successfully closed kafka consumer group", logData)
		}
	case <-ctx.Done():
		err = ctx.Err()
		log.Event(ctx, "Close abandoned: context done", log.Error(err), logData)
	}
	return
}

// NewSyncConsumer returns a new synchronous consumer group using default configuration.
func NewSyncConsumer(ctx context.Context, brokers []string, topic string, group string, offset int64) (ConsumerGroup, error) {
	channels := CreateConsumerGroupChannels(true)
	return NewConsumerWithChannels(ctx, brokers, topic, group, offset, true, channels)
}

// NewConsumerGroup returns a new asynchronous consumer group using default configuration.
func NewConsumerGroup(ctx context.Context, brokers []string, topic string, group string, offset int64) (ConsumerGroup, error) {
	channels := CreateConsumerGroupChannels(false)
	return NewConsumerWithChannels(ctx, brokers, topic, group, offset, false, channels)
}

// NewConsumerWithChannels returns a new consumer group using default configuration and provided channels
func NewConsumerWithChannels(
	ctx context.Context, brokers []string, topic string, group string, offset int64, sync bool,
	channels ConsumerGroupChannels) (ConsumerGroup, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	return NewConsumerWithChannelsAndClusterClient(
		ctx, brokers, topic, group, offset, sync,
		channels, &SaramaClusterClient{},
	)
}

// NewConsumerWithChannelsAndClusterClient returns a new consumer group with the provided sarama cluster client
func NewConsumerWithChannelsAndClusterClient(
	ctx context.Context, brokers []string, topic string, group string, offset int64, sync bool,
	channels ConsumerGroupChannels, cli SaramaCluster) (ConsumerGroup, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Return.Errors = true
	config.Consumer.MaxWaitTime = 50 * time.Millisecond
	config.Consumer.Offsets.Initial = offset
	config.Consumer.Offsets.Retention = 0 // indefinite retention

	logData := log.Data{"topic": topic, "group": group}

	consumer, err := cli.NewConsumer(brokers, group, []string{topic}, config)
	if err != nil {
		log.Event(ctx, "newConsumer failed", log.Error(err), logData)
		return ConsumerGroup{}, err
	}

	check := &health.Check{}

	err = channels.Validate()
	if err != nil {
		return ConsumerGroup{}, err
	}

	cg := ConsumerGroup{
		brokers:  brokers,
		consumer: consumer,
		channels: &channels,
		topic:    topic,
		group:    group,
		sync:     sync,
		Check:    check,
	}

	// listener goroutine - listen to consumer.Messages() and upstream them
	// if this blocks while upstreaming a message, we can shutdown consumer via the following goroutine
	go func() {
		logData := log.Data{"topic": topic, "group": group}

		log.Event(ctx, "Started kafka consumer listener", logData)
		defer close(cg.channels.Closed)
		for looping := true; looping; {
			select {
			case <-cg.channels.Closer:
				looping = false
			case msg := <-cg.consumer.Messages():
				cg.Incoming() <- SaramaMessage{msg, cg.consumer}
				if cg.sync {
					// wait for msg-processed or close-consumer triggers
					for loopingForSync := true; looping && loopingForSync; {
						select {
						case <-cg.channels.UpstreamDone:
							loopingForSync = false
						case <-cg.channels.Closer:
							// XXX if we read closer here, this means that the release/upstreamDone blocks unless it is buffered
							looping = false
						}
					}
				}
			}
		}
		cg.consumer.CommitOffsets()
		log.Event(ctx, "Closed kafka consumer listener", logData)
	}()

	// control goroutine - allows us to close consumer even if blocked while upstreaming a message (above)
	go func() {
		logData := log.Data{"topic": topic, "group": group}

		hasBalanced := false // avoid CommitOffsets() being called before we have balanced (otherwise causes a panic)
		for looping := true; looping; {
			select {
			case <-cg.channels.Closer:
				log.Event(ctx, "Closing kafka consumer controller", logData)
				<-cg.channels.Closed
				looping = false
			case err := <-cg.consumer.Errors():
				log.Event(ctx, "kafka consumer-group error", log.Error(err))
				cg.Errors() <- err
			case <-time.After(tick):
				if hasBalanced {
					cg.consumer.CommitOffsets()
				}
			case n, more := <-cg.consumer.Notifications():
				if more {
					hasBalanced = true
					log.Event(ctx, "Rebalancing group", log.Data{"topic": cg.topic, "group": cg.group, "partitions": n.Current[cg.topic]})
				}
			}
		}
		log.Event(ctx, "Closed kafka consumer controller", logData)
	}()

	return cg, nil
}
