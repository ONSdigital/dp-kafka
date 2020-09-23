package kafka

import (
	"context"
	"sync"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

var messageConsumeTimeout = time.Second * 10

//go:generate moq -out ./kafkatest/mock_consumer_group.go -pkg kafkatest . IConsumerGroup

// IConsumerGroup is an interface representing a Kafka Consumer Group.
type IConsumerGroup interface {
	Channels() *ConsumerGroupChannels
	IsInitialised() bool
	Initialise(ctx context.Context) error
	StopListeningToConsumer(ctx context.Context) (err error)
	Checker(ctx context.Context, state *health.CheckState) error
	Close(ctx context.Context) (err error)
}

// ConsumerGroup is a Kafka consumer group instance.
type ConsumerGroup struct {
	brokerAddrs     []string
	brokers         []*sarama.Broker
	channels        *ConsumerGroupChannels
	saramaCg        sarama.ConsumerGroup
	saramaCgHandler *saramaCgHandler
	saramaCgInit    consumerGroupInitialiser
	topic           string
	group           string
	config          *sarama.Config
	mutex           *sync.Mutex
	wgClose         *sync.WaitGroup
}

// NewConsumerGroup creates a new consumer group with the provided parameters
func NewConsumerGroup(ctx context.Context,
	brokerAddrs []string, topic, group string, kafkaVersion string,
	channels *ConsumerGroupChannels) (*ConsumerGroup, error) {
	return newConsumerGroup(ctx, brokerAddrs, topic, group, kafkaVersion, channels, saramaNewConsumerGroup)
}

func newConsumerGroup(ctx context.Context,
	brokerAddrs []string, topic, group string, kafkaVersion string,
	channels *ConsumerGroupChannels, cgInit consumerGroupInitialiser) (*ConsumerGroup, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// Obtain Sarama Kafka Version from kafkaVersion string
	v, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		log.Event(nil, "error parsing kafka version: %v", log.ERROR, log.Error(err))
		return nil, err
	}

	// Create config
	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Session.Timeout = messageConsumeTimeout

	// ConsumerGroup initialised with provided brokerAddrs, topic, group and sync
	cg := &ConsumerGroup{
		brokerAddrs:  brokerAddrs,
		brokers:      []*sarama.Broker{},
		topic:        topic,
		group:        group,
		config:       config,
		mutex:        &sync.Mutex{},
		wgClose:      &sync.WaitGroup{},
		saramaCgInit: cgInit,
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
		cg.createLoopUninitialised(ctx)
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
	saramaConsumerGroup, err := cg.saramaCgInit(cg.brokerAddrs, cg.group, cg.config)
	if err != nil {
		return err
	}

	// On Successful initialization, create sarama consumer handler, and loop goroutines (for messages and errors)
	cg.saramaCgHandler = &saramaCgHandler{ctx, cg.channels}
	cg.saramaCg = saramaConsumerGroup
	cg.createConsumeLoop(ctx)
	cg.createErrorLoop(ctx)

	// Await until the consumer has been set up
	<-cg.channels.Ready
	log.Event(ctx, "sarama consumer successfully initialised", log.INFO, logData)

	return nil
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
			log.Event(ctx, "close failed of kafka consumer group", log.WARN, log.Error(err), logData)
			return err
		}
	}

	// Close all brokers connections (used by healthcheck)
	for _, broker := range cg.brokers {
		broker.Close()
	}

	log.Event(ctx, "successfully closed kafka consumer group", log.INFO, logData)
	close(cg.channels.Closed)
	return nil
}

// createLoopUninitialised creates a goroutine to handle uninitialised consumer groups.
// It retries to initialise it every InitRetryPeriod, until the consumer group is initialised or being closed.
func (cg *ConsumerGroup) createLoopUninitialised(ctx context.Context) {

	// Do nothing if consumer group already initialised
	if cg.IsInitialised() {
		return
	}

	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		for {
			select {
			case <-cg.channels.Ready:
				return
			case <-cg.channels.Closer:
				log.Event(ctx, "closing uninitialised kafka consumer group", log.INFO, log.Data{"topic": cg.topic})
				return
			case <-time.After(InitRetryPeriod):
				if err := cg.Initialise(ctx); err != nil {
					continue
				}
				return
			}
		}
	}()
}

// createConsumeLoop creates a goroutine to handle initialised consumer groups.
// It calls Consume(), to consume messages from sarama and sends them to the Upstream channel.
// If the consumer group is configured as 'sync', we wait for the UpstreamDone (of Closer) channels.
// If the contxt is done, it ends the loop and closes Closed channel.
func (cg *ConsumerGroup) createConsumeLoop(ctx context.Context) {
	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		logData := log.Data{"topic": cg.topic, "group": cg.group}
		log.Event(ctx, "started kafka consumer listener loop", log.INFO, logData)
		for {
			select {
			case <-cg.channels.Closer:
				log.Event(ctx, "closed kafka consumer consume loop via closer channel", log.INFO, logData)
				return
			default:
				// 'Consume' is called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				if err := cg.saramaCg.Consume(ctx, []string{cg.topic}, cg.saramaCgHandler); err != nil {
					log.Event(ctx, "error consuming", log.ERROR, log.Error(err))
					time.Sleep(ConsumeErrRetryPeriod) // on error, retry loop after 'tick' time
					continue
				}
			}
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
		for {
			select {
			// check if closer channel is closed, signaling that the consumer should stop
			case <-cg.channels.Closer:
				log.Event(ctx, "closed kafka consumer error loop via closer channel", log.INFO, logData)
				return
			// listen to kafka errors from sarama and forward them to the Errors chanel
			case err := <-cg.saramaCg.Errors():
				cg.channels.Errors <- err
			}
		}
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
