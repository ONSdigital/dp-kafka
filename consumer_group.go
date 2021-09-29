package kafka

import (
	"context"
	"sync"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

var messageConsumeTimeout = time.Second * 10

//go:generate moq -out ./kafkatest/mock_consumer_group.go -pkg kafkatest . IConsumerGroup

type ConsumerState int

const (
	Initialising ConsumerState = iota
	Stopped
	Starting
	Consuming
	Stopping
	Closing
)

func (s ConsumerState) String() string {
	return [...]string{"Initialising", "Stopped", "Starting", "Consuming", "Stopping", "Closing"}[s]
}

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
	state           ConsumerState
	config          *sarama.Config
	mutex           *sync.Mutex
	wgClose         *sync.WaitGroup
}

// NewConsumerGroup creates a new consumer group with the provided parameters
func NewConsumerGroup(ctx context.Context, brokerAddrs []string, topic, group string,
	channels *ConsumerGroupChannels, cgConfig *ConsumerGroupConfig) (*ConsumerGroup, error) {
	return newConsumerGroup(ctx, brokerAddrs, topic, group, channels, cgConfig, saramaNewConsumerGroup)
}

func newConsumerGroup(ctx context.Context, brokerAddrs []string, topic, group string,
	channels *ConsumerGroupChannels, cgConfig *ConsumerGroupConfig, cgInit consumerGroupInitialiser) (*ConsumerGroup, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// Create config
	config, err := getConsumerGroupConfig(cgConfig)
	if err != nil {
		return nil, err
	}

	// Validate provided channels and assign them to consumer group. ErrNoChannel should be considered fatal by caller.
	err = channels.Validate()
	if err != nil {
		return nil, err
	}

	// ConsumerGroup initialised with provided brokerAddrs, topic, group and sync
	cg := &ConsumerGroup{
		brokerAddrs:  brokerAddrs,
		brokers:      []*sarama.Broker{},
		channels:     channels,
		topic:        topic,
		group:        group,
		state:        Initialising,
		config:       config,
		mutex:        &sync.Mutex{},
		wgClose:      &sync.WaitGroup{},
		saramaCgInit: cgInit,
	}

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

// State returns the state of the consumer group
func (cg *ConsumerGroup) State() ConsumerState {
	return cg.state
}

// Initialise creates a new Sarama ConsumerGroup and the consumer/error loops, only if it was not already initialised.
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

	// Create Sarama Consumer. Errors at this point are not necessarily fatal (e.g. brokers not reachable).
	saramaConsumerGroup, err := cg.saramaCgInit(cg.brokerAddrs, cg.group, cg.config)
	if err != nil {
		return err
	}

	// On Successful initialization, create sarama consumer handler, and loop goroutines (for messages and errors)
	cg.saramaCgHandler = NewSaramaCgHandler(ctx, cg.channels, &cg.state)
	cg.saramaCg = saramaConsumerGroup
	cg.createConsumeLoop(ctx)
	cg.createErrorLoop(ctx)

	// Await until the consumer has been set up
	<-cg.channels.Ready

	return nil
}

// StopListeningToConsumer stops any more messages being consumed off kafka topic
// Note that this method is 'not recoverable' meaning that after you call it, you can't go back to consuming messages again.
// If you intend to stop consuming messages temporarily, please send 'false' to the Consume channel instead.
func (cg *ConsumerGroup) StopListeningToConsumer(ctx context.Context) (err error) {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}

	// close(closer) to indicate that the closing process has started (select{} avoids panic if already closed)
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
		log.Warn(ctx, "StopListeningToConsumer abandoned: context done", log.FormatErrors([]error{err}), logData)
		return err
	}

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
			log.Warn(ctx, "close failed of kafka consumer group", log.FormatErrors([]error{err}), logData)
			return err
		}
	}

	// Close all brokers connections (used by healthcheck)
	for _, broker := range cg.brokers {
		broker.Close()
	}

	// Close the Closed channel to signal that the closing operation has completed
	close(cg.channels.Closed)
	return nil
}

// createLoopUninitialised creates a goroutine to handle uninitialised consumer groups.
// It retries to initialise the consumer with an exponential backoff retrial algorithm,
// starting with a period `InitRetryPeriod`, until the consumer group is initialised or closed.
func (cg *ConsumerGroup) createLoopUninitialised(ctx context.Context) {
	if cg.IsInitialised() {
		return // do nothing if consumer group already initialised
	}

	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		initAttempt := 1
		for {
			select {
			case <-cg.channels.Ready:
				return
			case <-cg.channels.Closer:
				log.Info(ctx, "closing uninitialised kafka consumer group", log.Data{"topic": cg.topic})
				return
			case <-time.After(getRetryTime(initAttempt, InitRetryPeriod)):
				if err := cg.Initialise(ctx); err != nil {
					log.Error(ctx, "error initialising consumer group", err, log.Data{"attempt": initAttempt})
					initAttempt++
					continue
				}
				return
			case <-ctx.Done():
				log.Error(ctx, "abandoning initialisation of consumer group - context expired", ctx.Err(), log.Data{"attempt": initAttempt})
				return
			}
		}
	}()
}

// stoppedState represents the 'Stopped' stationary state.
// It sets the state to Stopped and blocks until one of the following events happen:
// - A 'true' value is received from the Consume channel: the state is set to 'Starting' and the func will return
// - The Closer channel or the Consume channel is closed: the state is set to 'Closing' and the func will return
func (cg *ConsumerGroup) stoppedState(ctx context.Context, logData log.Data) {
	cg.state = Stopped
	logData["state"] = Stopped

	// close Ready channel (if it is not already closed)
	select {
	case <-cg.channels.Ready:
	default:
		close(cg.channels.Ready)
	}

	for {
		select {
		case <-cg.channels.Closer:
			cg.state = Closing
			logData["state"] = Closing
			log.Info(ctx, "closed kafka consumer consume loop via closer channel", logData)
			return
		case consume, ok := <-cg.channels.Consume:
			if !ok {
				cg.state = Closing
				logData["state"] = Closing
				log.Info(ctx, "closed kafka consumer consume loop because the Consume channel is closed", logData)
				return
			}
			if consume {
				cg.state = Starting
				logData["state"] = Starting
				return
			}
			// if consume is false, we re-iterate the select, as the state is already stopped
		}
	}
}

// startingState represents the 'Starting' transient state and 'Consuming' stationary state.
// It sets the state to Starting and calls saramaCg.Consume in an infinite loop,
// this will make the consumer consume messages again every time that a session is destroyed and created.
// saramaCg.Consume will set the state to consuming while the session is active, and will set it back to 'Starting' when it finishes.
// before calling consume again after a session finishes, we check if one of the following events has happened:
// - A 'false' value is received from the Consume channel: the state is set to 'Starting' and the func will return
// - The Closer channel or the Consume channel is closed: the state is set to 'Closing' and the func will return
// If saramaCg.Consume fails, we retry after waiting some time (exponential backoff between retries).
// If the consumer changes its state between retries, we abort the loop as described above.
func (cg *ConsumerGroup) startingState(ctx context.Context, logData log.Data) {
	cg.state = Starting
	logData["state"] = Starting

	// close Ready channel (if it is not already closed)
	select {
	case <-cg.channels.Ready:
	default:
		close(cg.channels.Ready)
	}

	consumeAttempt := 1
	for {
		if cg.state != Starting && cg.state != Consuming {
			// state changed during cg.saramaCg.Consume
			return
		}
		select {
		case <-cg.channels.Closer:
			cg.state = Closing
			logData["state"] = Closing
			log.Info(ctx, "closed kafka consumer consume loop via closer channel", logData)
			return
		case consume, ok := <-cg.channels.Consume:
			if !ok {
				cg.state = Closing
				logData["state"] = Closing
				log.Info(ctx, "closed kafka consumer consume loop because the Consume channel is closed", logData)
				return
			}
			if !consume {
				cg.state = Stopping
				logData["state"] = Stopping
				return
			}
		default:
			// 'Consume' is called inside an infinite loop, when a server-side rebalance happens,
			// the consumer session will need to be recreated to get the new claims
			if err := cg.saramaCg.Consume(ctx, []string{cg.topic}, cg.saramaCgHandler); err != nil {
				log.Error(ctx, "error consuming", err, log.Data{"attempt": consumeAttempt})
				select {
				// check if closer channel is closed, signaling that the consumer should stop (don't retry to Consume)
				case <-cg.channels.Closer:
					cg.state = Closing
					logData["state"] = Closing
					log.Info(ctx, "closed kafka consumer consume loop via closer channel", logData)
					return
				case consume, ok := <-cg.channels.Consume:
					if !ok {
						cg.state = Closing
						logData["state"] = Closing
						log.Info(ctx, "closed kafka consumer consume loop because the Consume channel is closed", logData)
						return
					}
					if !consume {
						cg.state = Stopping
						logData["state"] = Stopping
						return
					}
					// once the retrial time has expired, we try to consume again (continue the loop)
				case <-time.After(getRetryTime(consumeAttempt, ConsumeErrRetryPeriod)):
					consumeAttempt++
				case <-ctx.Done():
				}
			} else {
				// on successful consumption, reset the attempt counter
				consumeAttempt = 1
			}
		}
	}
}

// createConsumeLoop creates a goroutine for the consumer group once the sarama consumer has been initialised.
// The consumer will initially be set at 'Stopped' sate.
func (cg *ConsumerGroup) createConsumeLoop(ctx context.Context) {
	if cg.state != Initialising {
		log.Warn(ctx, "wrong state for createConsumeLoop", log.Data{"state": cg.state.String()})
		return
	}
	cg.state = Stopped // will not consume until we are told to do so by the Consume channel
	logData := log.Data{"topic": cg.topic, "group": cg.group, "state": cg.state}

	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		log.Info(ctx, "starting kafka consumer listener loop", logData)
		for {
			switch cg.state {
			case Stopping, Stopped:
				cg.stoppedState(ctx, logData)
			case Starting, Consuming:
				cg.startingState(ctx, logData)
			case Closing:
				return
			default:
				logData["state"] = cg.state
				log.Warn(ctx, "wrong state, aborting consume loop", logData)
			}
		}
	}()
}

// createErrorLoop creates a goroutine to consume errors returned by Sarama to the Errors channel.
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
				log.Info(ctx, "closed kafka consumer error loop via closer channel", logData)
				return
			// listen to kafka errors from sarama and forward them to the Errors chanel
			case err := <-cg.saramaCg.Errors():
				cg.channels.Errors <- err
			}
		}
	}()
}
