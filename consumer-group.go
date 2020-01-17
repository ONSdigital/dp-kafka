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
	brokers      []string
	consumer     *cluster.Consumer
	incoming     chan Message
	errors       chan error
	closer       chan struct{}
	closed       chan struct{}
	topic        string
	group        string
	sync         bool
	upstreamDone chan bool
	Check        *health.Check
}

// Incoming provides a channel of incoming messages.
func (cg ConsumerGroup) Incoming() chan Message {
	return cg.incoming
}

// Errors provides a channel of incoming errors.
func (cg ConsumerGroup) Errors() chan error {
	return cg.errors
}

// Release signals that upstream has completed an incoming message
// i.e. move on to read the next message
func (cg ConsumerGroup) Release() {
	cg.upstreamDone <- true
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

	close(cg.closer)

	logData := log.Data{"topic": cg.topic, "group": cg.group}
	select {
	case <-cg.closed:
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
	case <-cg.closer:
	default:
		close(cg.closer)
	}

	logData := log.Data{"topic": cg.topic, "group": cg.group}
	select {
	case <-cg.closed:
		close(cg.errors)
		close(cg.incoming)

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

	if ctx == nil {
		ctx = context.Background()
	}

	return NewConsumerWithChannels(
		ctx, brokers, topic, group, offset, true,
		make(chan Message, 1), // Sync -> upstream channel buffered, so we can send-and-wait for upstreamDone
		make(chan struct{}),
		make(chan struct{}),
		make(chan error),
		make(chan bool, 1),
	)
}

// NewConsumerGroup returns a new asynchronous consumer group using default configuration.
func NewConsumerGroup(ctx context.Context, brokers []string, topic string, group string, offset int64) (ConsumerGroup, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	return NewConsumerWithChannels(
		ctx, brokers, topic, group, offset, false,
		make(chan Message), // not sync -> upstream channel unbuffered
		make(chan struct{}),
		make(chan struct{}),
		make(chan error),
		make(chan bool, 1),
	)
}

// NewConsumerWithChannels returns a new consumer group using default configuration and provided channels
func NewConsumerWithChannels(
	ctx context.Context, brokers []string, topic string, group string, offset int64, sync bool,
	chUpstream chan Message, chCloser, chClosed chan struct{}, chErrors chan error, chUpstreamDone chan bool) (ConsumerGroup, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	return NewConsumerWithChannelsAndClusterClient(
		ctx, brokers, topic, group, offset, sync,
		chUpstream, chCloser, chClosed, chErrors, chUpstreamDone, &SaramaClusterClient{},
	)
}

// NewConsumerWithChannelsAndClusterClient returns a new consumer group with the provided sarama cluster client
func NewConsumerWithChannelsAndClusterClient(
	ctx context.Context, brokers []string, topic string, group string, offset int64, sync bool,
	chUpstream chan Message, chCloser, chClosed chan struct{}, chErrors chan error, chUpstreamDone chan bool, cli SaramaCluster) (ConsumerGroup, error) {

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

	// ConsumerGroup initialized with anything that cannot cause an error
	cg := ConsumerGroup{
		brokers: brokers,
		topic:   topic,
		group:   group,
		sync:    sync,
	}

	// Initial check structure
	check := &health.Check{Name: ServiceName}
	cg.Check = check

	// Validate provided channels. ErrNoChannel should be considered fatal by caller.
	missingChannels := []string{}
	if chUpstream == nil {
		missingChannels = append(missingChannels, "Upstream")
	}
	if chCloser == nil {
		missingChannels = append(missingChannels, "Closer")
	}
	if chClosed == nil {
		missingChannels = append(missingChannels, "Closed")
	}
	if chErrors == nil {
		missingChannels = append(missingChannels, "Error")
	}
	if chUpstreamDone == nil {
		missingChannels = append(missingChannels, "UpstreamDone")
	}
	if len(missingChannels) > 0 {
		return cg, &ErrNoChannel{ChannelNames: missingChannels}
	}
	cg.incoming = chUpstream
	cg.closer = chCloser
	cg.closed = chClosed
	cg.errors = chErrors
	cg.upstreamDone = chUpstreamDone

	// Create Sarama Consumer. Errors at this point are not necessarily fatal (e.g. brokers not reachable).
	consumer, err := cli.NewConsumer(brokers, group, []string{topic}, config)
	if err != nil {
		log.Event(ctx, "newConsumer failed", log.Error(err), logData)
		return cg, err
	}
	cg.consumer = consumer

	// listener goroutine - listen to consumer.Messages() and upstream them
	// if this blocks while upstreaming a message, we can shutdown consumer via the following goroutine
	go func() {
		logData := log.Data{"topic": topic, "group": group}

		log.Event(ctx, "Started kafka consumer listener", logData)
		defer close(cg.closed)
		for looping := true; looping; {
			select {
			case <-cg.closer:
				looping = false
			case msg := <-cg.consumer.Messages():
				cg.Incoming() <- SaramaMessage{msg, cg.consumer}
				if cg.sync {
					// wait for msg-processed or close-consumer triggers
					for loopingForSync := true; looping && loopingForSync; {
						select {
						case <-cg.upstreamDone:
							loopingForSync = false
						case <-cg.closer:
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
			case <-cg.closer:
				log.Event(ctx, "Closing kafka consumer controller", logData)
				<-cg.closed
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
