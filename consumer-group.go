package kafka

import (
	"context"
	"sync"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
	cluster "github.com/bsm/sarama-cluster"
)

var tick = time.Millisecond * 1500

// ConsumerGroup represents a Kafka consumer group instance.
type ConsumerGroup struct {
	brokers   []string
	channels  *ConsumerGroupChannels
	consumer  SaramaClusterConsumer
	topic     string
	group     string
	sync      bool
	Check     *health.Check
	config    *cluster.Config
	cli       SaramaCluster
	mutexInit *sync.Mutex
}

// NewConsumerGroup returns a new consumer group using default configuration and provided channels
func NewConsumerGroup(
	ctx context.Context, brokers []string, topic string, group string, offset int64, sync bool,
	channels ConsumerGroupChannels) (ConsumerGroup, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	return NewConsumerWithClusterClient(
		ctx, brokers, topic, group, offset, sync,
		channels, &SaramaClusterClient{},
	)
}

// NewConsumerWithClusterClient returns a new consumer group with the provided sarama cluster client
func NewConsumerWithClusterClient(
	ctx context.Context, brokers []string, topic string, group string, offset int64, syncConsumer bool,
	channels ConsumerGroupChannels, cli SaramaCluster) (cg ConsumerGroup, err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Return.Errors = true
	config.Consumer.MaxWaitTime = 50 * time.Millisecond
	config.Consumer.Offsets.Initial = offset
	config.Consumer.Offsets.Retention = 0 // indefinite retention

	// ConsumerGroup initialised with provided brokers, topic, group and sync
	cg = ConsumerGroup{
		brokers:   brokers,
		topic:     topic,
		group:     group,
		sync:      syncConsumer,
		config:    config,
		cli:       cli,
		mutexInit: &sync.Mutex{},
	}

	// Initial check structure
	check := &health.Check{Name: ServiceName}
	cg.Check = check

	// Validate provided channels and assign them to consumer group. ErrNoChannel should be considered fatal by caller.
	err = channels.Validate()
	if err != nil {
		return cg, err
	}
	cg.channels = &channels

	// Initialise Sarama consumer group, and return any error (which might not be considered fatal by caller)
	err = cg.InitialiseSarama(ctx)
	return cg, err
}

// IsInitialised returns true only if Sarama consumer has been correctly initialised.
func (cg *ConsumerGroup) IsInitialised() bool {
	if cg == nil {
		return false
	}
	return cg.consumer != nil
}

// InitialiseSarama creates a new Sarama Consumer and the channel redirection, only if it was not already initialised.
func (cg *ConsumerGroup) InitialiseSarama(ctx context.Context) error {

	cg.mutexInit.Lock()
	defer cg.mutexInit.Unlock()

	// Do nothing if consumer group already initialised
	if cg.IsInitialised() {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	logData := log.Data{"topic": cg.topic, "group": cg.group}

	// Create Sarama Consumer. Errors at this point are not necessarily fatal (e.g. brokers not reachable).
	consumer, err := cg.cli.NewConsumer(cg.brokers, cg.group, []string{cg.topic}, cg.config)
	if err != nil {
		return err
	}

	// On Successful initialization, create main and control loop goroutines
	cg.consumer = consumer
	log.Event(ctx, "Initialised Sarama Consumer", logData)
	cg.createMainLoop(ctx)
	cg.createControlLoop(ctx)

	return nil
}

// Release signals that upstream has completed an incoming message
// i.e. move on to read the next message
func (cg *ConsumerGroup) Release() {
	if cg == nil {
		return
	}
	cg.channels.UpstreamDone <- true
}

// CommitAndRelease commits the consumed message and release the consumer listener to read another message
func (cg *ConsumerGroup) CommitAndRelease(msg Message) {
	if cg == nil {
		return
	}
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

	// If Sarama Consumer is not available, we can close 'closed' channel straight away
	if !cg.IsInitialised() {
		close(cg.channels.Closed)
	}

	// If Sarama Consumer is available, we wait for it to close 'closed' channel, or ctx timeout.
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

	// If Sarama Consumer is not available, we can close 'closed' channel straight away, with select{} to avoid panic if already closed
	if !cg.IsInitialised() {
		select {
		case <-cg.channels.Closed:
		default:
			close(cg.channels.Closed)
		}
	}

	logData := log.Data{"topic": cg.topic, "group": cg.group}
	select {
	case <-cg.channels.Closed:
		close(cg.channels.Errors)
		close(cg.channels.Upstream)
		// Close consumer only if it was initialised.
		if cg.IsInitialised() {
			if err = cg.consumer.Close(); err != nil {
				log.Event(ctx, "Close failed of kafka consumer group", log.Error(err), logData)
			} else {
				log.Event(ctx, "Successfully closed kafka consumer group", logData)
			}
		}
	case <-ctx.Done():
		err = ctx.Err()
		log.Event(ctx, "Close abandoned: context done", log.Error(err), logData)
	}
	return
}

// createMainLoop creates a goroutine to handle initialised consumer groups.
// It listens to consumer.Messages(), comming from Sarama, and sends them to the Upstream channel.
// If the consumer group is configured as 'sync', we wait for the UpstreamDone (of Closer) channels.
// If the closer channel is closed, it ends the loop and closes Closed channel.
func (cg *ConsumerGroup) createMainLoop(ctx context.Context) {
	go func() {
		logData := log.Data{"topic": cg.topic, "group": cg.group}
		log.Event(ctx, "Started kafka consumer listener", logData)
		defer close(cg.channels.Closed)
		for looping := true; looping; {
			select {
			case <-cg.channels.Closer:
				looping = false
			case msg := <-cg.consumer.Messages():
				cg.channels.Upstream <- SaramaMessage{msg, cg.consumer}
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
}

// createControlLoop allows us to close consumer even if blocked while upstreaming a message (main loop)
// It redirects sarama errors to caller errors channel.
// It listens to Notifications channel, and checks if the consumer group has balanced.
// It periodically checks if the consumer group has balanced, and in that case, it commits offsets.
// If the closer channel is closed, it ends the loop and closes Closed channel.
func (cg *ConsumerGroup) createControlLoop(ctx context.Context) {
	go func() {
		logData := log.Data{"topic": cg.topic, "group": cg.group}

		hasBalanced := false // avoid CommitOffsets() being called before we have balanced (otherwise causes a panic)
		for looping := true; looping; {
			select {
			case <-cg.channels.Closer:
				log.Event(ctx, "Closing kafka consumer controller", logData)
				<-cg.channels.Closed
				looping = false
			case err := <-cg.consumer.Errors():
				log.Event(ctx, "kafka consumer-group error", log.Error(err))
				cg.channels.Errors <- err
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
}
