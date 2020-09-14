package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

var tick = time.Millisecond * 1500

//go:generate moq -out ./kafkatest/mock_consumer_group.go -pkg kafkatest . IConsumerGroup

// IConsumerGroup is an interface representing a Kafka Consumer Group.
type IConsumerGroup interface {
	Channels() *ConsumerGroupChannels
	IsInitialised() bool
	Initialise(ctx context.Context) error
	Release()
	CommitAndRelease(msg Message)
	StopListeningToConsumer(ctx context.Context) (err error)
	Checker(ctx context.Context, state *health.CheckState) error
	Close(ctx context.Context) (err error)
}

// ConsumerGroup is a Kafka consumer group instance.
type ConsumerGroup struct {
	brokerAddrs []string
	brokers     []*sarama.Broker
	channels    *ConsumerGroupChannels
	saramaCg    sarama.ConsumerGroup
	topic       string
	group       string
	config      *sarama.Config
	mutex       *sync.Mutex
	wgClose     *sync.WaitGroup
}

// NewConsumerGroup returns a new consumer group using default configuration and provided channels
func NewConsumerGroup(
	ctx context.Context, brokerAddrs []string, topic string, group string, offset int64, kafkaVersion string,
	channels *ConsumerGroupChannels) (*ConsumerGroup, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// Obtain Sarama Kafka Version from kafkaVersion string
	v, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		log.Event(nil, "Error parsing Kafka version: %v", log.ERROR, log.Error(err))
		return nil, err
	}

	// Create config
	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// ConsumerGroup initialised with provided brokerAddrs, topic, group and sync
	cg := &ConsumerGroup{
		brokerAddrs: brokerAddrs,
		brokers:     []*sarama.Broker{},
		topic:       topic,
		group:       group,
		config:      config,
		mutex:       &sync.Mutex{},
		wgClose:     &sync.WaitGroup{},
	}

	// Validate provided channels and assign them to consumer group. ErrNoChannel should be considered fatal by caller.
	err = channels.Validate()
	if err != nil {
		return cg, err
	}
	cg.channels = channels

	// disable metrics to prevent memory leak on broker.Open()
	metrics.UseNilMetrics = true

	// Create broker objects
	for _, addr := range brokerAddrs {
		cg.brokers = append(cg.brokers, sarama.NewBroker(addr))
	}

	// Initialise consumer group, and log any error
	err = cg.Initialise(ctx)
	if err != nil {
		log.Event(ctx, "Initialisation error (non-fatal)", log.WARN, log.Error(err))
	}
	return cg, nil
}

// Channels returns the ConsumerGroup channels for this consumer group
func (cg *ConsumerGroup) Channels() *ConsumerGroupChannels {
	if cg == nil {
		return nil
	}
	return cg.channels
}

// IsInitialised returns true only if Sarama ConsumerGroup has been correctly initialised.
func (cg *ConsumerGroup) IsInitialised() bool {
	if cg == nil {
		return false
	}
	return cg.saramaCg != nil
}

// Initialise creates a new Sarama ConsumerGroup and the channel redirection, only if it was not already initialised.
func (cg *ConsumerGroup) Initialise(ctx context.Context) error {

	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	// Do nothing if consumer group already initialised
	if cg.IsInitialised() {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	logData := log.Data{"topic": cg.topic, "group": cg.group}

	// Create Sarama Consumer. Errors at this point are not necessarily fatal (e.g. brokers not reachable).
	saramaConsumerGroup, err := sarama.NewConsumerGroup(cg.brokerAddrs, cg.group, cg.config)
	if err != nil {
		log.Event(ctx, "Error creating consumer group client", log.ERROR, log.Error(err))
		return err
	}

	// On Successful initialization, create main and control loop goroutines
	cg.saramaCg = saramaConsumerGroup
	log.Event(ctx, "Initialised Sarama Consumer", log.INFO, logData)
	cg.createConsumeLoop(ctx)
	cg.createErrorLoop(ctx)

	// Await until the consumer has been set up
	<-cg.channels.Ready
	log.Event(ctx, "Sarama consumer up and running", log.INFO, logData)

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

	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}

	// close(closer) to indicate that the closing process has started
	// the select{} avoids panic if already closed
	select {
	case <-cg.channels.Closer:
	default:
		close(cg.channels.Closer)
	}

	logData := log.Data{"topic": cg.topic, "group": cg.group}

	// Wait for the go-routines (if-any) finish their work
	didTimeout := waitWithTimeout(ctx, cg.wgClose)
	if didTimeout {
		err := ctx.Err()
		log.Event(ctx, "StopListeningToConsumer abandoned: context done", log.WARN, log.Error(err), logData)
		return err
	}

	log.Event(ctx, "StopListeningToConsumer got confirmation of closed kafka consumer listener", log.INFO, logData)
	return nil
}

// Close safely closes the consumer and releases all resources.
// pass in a context with a timeout or deadline.
// Passing a nil context will provide no timeout but is not recommended
func (cg *ConsumerGroup) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// StopListeningToConsumer will end the go-routines(if any) by closing the 'Closer' channel
	err = cg.StopListeningToConsumer(ctx)
	if err != nil {
		return err
	}

	logData := log.Data{"topic": cg.topic, "group": cg.group}

	close(cg.channels.Errors)
	close(cg.channels.Upstream)

	// Close consumer only if it was initialised.
	if cg.IsInitialised() {
		if err = cg.saramaCg.Close(); err != nil {
			log.Event(ctx, "Close failed of kafka consumer group", log.WARN, log.Error(err), logData)
			return err
		}
	}

	// Close all brokers connections (used by healthcheck)
	for _, broker := range cg.brokers {
		broker.Close()
	}

	log.Event(ctx, "Successfully closed kafka consumer group", log.INFO, logData)
	close(cg.channels.Closed)
	return nil
}

// createConsumeLoop creates a goroutine to handle initialised consumer groups.
// It calls Consume(), to consume messages from sarama and sends them to the Upstream channel.
// If the consumer group is configured as 'sync', we wait for the UpstreamDone (of Closer) channels.
// If the contxt is done, it ends the loop and closes Closed channel.
func (cg *ConsumerGroup) createConsumeLoop(ctx context.Context) {
	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		for {
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				log.Event(ctx, "context was cancelled. Stopping consumer")
				close(cg.channels.Closer)
				return
			}
			// check if closer channel is closed, signaling that the consumer should stop
			if isClosed(cg.channels.Closer) {
				log.Event(ctx, "Closer channel was closed. Stopping consumer")
				return
			}
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := cg.saramaCg.Consume(ctx, []string{cg.topic}, cg); err != nil {
				log.Event(ctx, "error consuming", log.ERROR, log.Error(err))
				time.Sleep(tick)
			}
			// TODO sync?
			// if cg.sync {
			// 				// wait for msg-processed or close-consumer triggers
			// 				for loopingForSync := true; looping && loopingForSync; {
			// 					select {
			// 					case <-cg.channels.UpstreamDone:
			// 						loopingForSync = false
			// 					case <-cg.channels.Closer:
			// 						// XXX if we read closer here, this means that the release/upstreamDone blocks unless it is buffered
			// 						looping = false
			// 					}
			// 				}
			// 			}
			cg.channels.Ready = make(chan struct{})
		}
	}()
}

// createErrorLoop allows us to close consumer even if blocked while upstreaming a message (main loop)
// It redirects sarama errors to caller errors channel.
// It listens to Notifications channel, and checks if the consumer group has balanced.
// It periodically checks if the consumer group has balanced, and in that case, it commits offsets.
// If the closer channel is closed, it ends the loop and closes Closed channel.
func (cg *ConsumerGroup) createErrorLoop(ctx context.Context) {
	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		logData := log.Data{"topic": cg.topic, "group": cg.group}
		hasBalanced := false // avoid CommitOffsets() being called before we have balanced (otherwise causes a panic)
		for looping := true; looping; {
			select {
			case <-cg.channels.Closer:
				log.Event(ctx, "Closing kafka consumer controller", log.INFO, logData)
				looping = false
			case err := <-cg.saramaCg.Errors():
				log.Event(ctx, "kafka consumer-group error", log.ERROR, log.Error(err))
				cg.channels.Errors <- err
			case <-time.After(tick):
				if hasBalanced {
					log.Event(ctx, "SHOULD COMMIT OFFSETS")
					// cg.saramaCg.Commit()
				}
				// case n, more := <-cg.cg.Notifications():
				// 	if more {
				// 		hasBalanced = true
				// 		log.Event(ctx, "Rebalancing group", log.INFO, log.Data{"topic": cg.topic, "group": cg.group, "partitions": n.Current[cg.topic]})
				// 	}
			}
		}
		log.Event(ctx, "Closed kafka consumer controller", log.INFO, logData)
	}()
}

// waitWithTimeout blocks until all go-routines tracked by a WaitGroup are done, or until the timeout defined in a context expires.
// It returns true only if the context timeout expired
func waitWithTimeout(ctx context.Context, wg *sync.WaitGroup) bool {
	chWaiting := make(chan struct{})
	go func() {
		defer close(chWaiting)
		wg.Wait()
	}()
	select {
	case <-chWaiting:
		return false
	case <-ctx.Done():
		return true
	}
}

// isClosed checks if a channel is closed
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

/// NEW SARAMA STUFF

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (cg *ConsumerGroup) Setup(session sarama.ConsumerGroupSession) error {
	log.Event(session.Context(), "Sarama consumerGroup session Setup OK. ConsumerGroup is Ready", log.INFO, log.Data{"memberID": session.MemberID()})
	close(cg.channels.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (cg *ConsumerGroup) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Event(session.Context(), "Sarama consumerGroup session Cleanup", log.Data{"memberID": session.MemberID()})
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (cg *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {

		msg := string(message.Value)

		log.Event(nil, fmt.Sprintf("Message claimed: value = %s, timestamp = %v, topic = %s", msg, message.Timestamp, message.Topic), log.INFO)
		time.Sleep(10 * time.Second)
		session.MarkMessage(message, "")
		// session.Commit()
		log.Event(nil, fmt.Sprintf("-- Message MARKED & COMMITED: %s", msg))
		// cg.channels.Upstream <- SaramaMessage{message, cg.cg}
	}

	return nil
}
