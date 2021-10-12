package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

//go:generate moq -out ./kafkatest/mock_consumer_group.go -pkg kafkatest . IConsumerGroup

// IConsumerGroup is an interface representing a Kafka Consumer Group, as implemented in dp-kafka/consumer
type IConsumerGroup interface {
	Channels() *ConsumerGroupChannels
	State() string
	RegisterHandler(ctx context.Context, h Handler) error
	RegisterBatchHandler(ctx context.Context, batchHandler BatchHandler) error
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	IsInitialised() bool
	Initialise(ctx context.Context) error
	OnHealthUpdate(status string)
	Start() error
	Stop()
	StopAndWait()
	LogErrors(ctx context.Context)
	Close(ctx context.Context) (err error)
}

// ConsumerGroup is a Kafka consumer group instance.
type ConsumerGroup struct {
	brokerAddrs     []string
	brokers         []SaramaBroker
	channels        *ConsumerGroupChannels
	saramaCg        sarama.ConsumerGroup
	saramaCgHandler *saramaHandler
	saramaCgInit    consumerGroupInitialiser
	topic           string
	group           string
	initialState    State // target state for a consumer that is still being initialised
	state           *StateMachine
	saramaConfig    *sarama.Config
	mutex           *sync.Mutex // Mutex for consumer funcs that are not supposed to run concurrently
	wgClose         *sync.WaitGroup
	handler         Handler
	batchHandler    BatchHandler
	numWorkers      int
	batchSize       int
	batchWaitTime   time.Duration
}

// NewConsumerGroup creates a new consumer group with the provided parameters
func NewConsumerGroup(ctx context.Context, cgConfig *ConsumerGroupConfig) (*ConsumerGroup, error) {
	return newConsumerGroup(ctx, cgConfig, saramaNewConsumerGroup)
}

func newConsumerGroup(ctx context.Context, cgConfig *ConsumerGroupConfig, cgInit consumerGroupInitialiser) (*ConsumerGroup, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Create sarama config and set any other necessary values
	cfg, err := cgConfig.Get()
	if err != nil {
		return nil, err
	}

	// upstream buffer size the maximum between number of workers and batch size
	upstreamBufferSize := *cgConfig.NumWorkers
	if *cgConfig.BatchSize > *cgConfig.NumWorkers {
		upstreamBufferSize = *cgConfig.BatchSize
	}

	// ConsumerGroup created with provided brokerAddrs, topic, group and sync
	cg := &ConsumerGroup{
		brokerAddrs:   cgConfig.BrokerAddrs,
		brokers:       []SaramaBroker{},
		channels:      CreateConsumerGroupChannels(upstreamBufferSize),
		topic:         cgConfig.Topic,
		group:         cgConfig.GroupName,
		state:         NewConsumerStateMachine(Initialising),
		initialState:  Stopped,
		saramaConfig:  cfg,
		mutex:         &sync.Mutex{},
		wgClose:       &sync.WaitGroup{},
		saramaCgInit:  cgInit,
		numWorkers:    *cgConfig.NumWorkers,
		batchSize:     *cgConfig.BatchSize,
		batchWaitTime: *cgConfig.BatchWaitTime,
	}

	// disable metrics to prevent memory leak on broker.Open()
	metrics.UseNilMetrics = true

	// create broker objects
	for _, addr := range cg.brokerAddrs {
		cg.brokers = append(cg.brokers, sarama.NewBroker(addr))
	}

	// initialise consumer group
	err = cg.Initialise(ctx)
	if err != nil {
		cg.createLoopUninitialised(ctx)
	}
	return cg, nil
}

// Channels returns the ConsumerGroup channels for this consumer group
func (cg *ConsumerGroup) Channels() *ConsumerGroupChannels {
	return cg.channels
}

// State returns the state of the consumer group
func (cg *ConsumerGroup) State() string {
	if cg.state == nil {
		return ""
	}
	return cg.state.String()
}

func (cg *ConsumerGroup) RegisterHandler(ctx context.Context, h Handler) error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()
	if cg.handler != nil || cg.batchHandler != nil {
		return errors.New("failed to register handler because a handler or batch handler had already been registered, only 1 allowed")
	}
	cg.handler = h
	cg.listen(ctx)
	return nil
}

func (cg *ConsumerGroup) RegisterBatchHandler(ctx context.Context, batchHandler BatchHandler) error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()
	if cg.handler != nil || cg.batchHandler != nil {
		return errors.New("failed to register handler because a handler or batch handler had already been registered, only 1 allowed")
	}
	cg.batchHandler = batchHandler
	cg.listenBatch(ctx)
	return nil
}

// Checker checks health of Kafka consumer-group and updates the provided CheckState accordingly
func (cg *ConsumerGroup) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	if err := Healthcheck(ctx, cg.brokers, cg.topic, cg.saramaConfig); err != nil {
		state.Update(healthcheck.StatusCritical, err.Error(), 0)
		return nil
	}
	state.Update(healthcheck.StatusOK, MsgHealthyConsumerGroup, 0)
	return nil
}

// IsInitialised returns true only if Sarama ConsumerGroup has been correctly initialised.
func (cg *ConsumerGroup) IsInitialised() bool {
	return cg.saramaCg != nil
}

// Initialise creates a new Sarama ConsumerGroup and the consumer/error loops, only if it was not already initialised.
func (cg *ConsumerGroup) Initialise(ctx context.Context) error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	// Do nothing if consumer group already initialised
	if cg.IsInitialised() {
		return nil
	}

	// Create Sarama Consumer. Errors at this point are not necessarily fatal (e.g. brokers not reachable).
	saramaConsumerGroup, err := cg.saramaCgInit(cg.brokerAddrs, cg.group, cg.saramaConfig)
	if err != nil {
		return err
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// On Successful initialization, create sarama consumer handler, and loop goroutines (for messages and errors)
	cg.saramaCgHandler = newSaramaHandler(ctx, cg.channels, cg.state)
	cg.saramaCg = saramaConsumerGroup
	cg.createConsumeLoop(ctx)
	cg.createErrorLoop(ctx)

	// Await until the consumer has been set up
	<-cg.channels.Ready

	return nil
}

// OnHealthUpdate implements the healthcheck Subscriber interface so that the kafka consumer can be notified of state changes.
// - On Health status OK: start consuming
// - On Warning or Critical: stop consuming
func (cg *ConsumerGroup) OnHealthUpdate(status string) {
	switch status {
	case healthcheck.StatusOK:
		cg.Start()
	case healthcheck.StatusWarning:
		cg.Stop()
	case healthcheck.StatusCritical:
		cg.Stop()
	}
}

// Start has different effects depending on the state:
// - Initialising: the consumer will try to start consuming straight away once it's initialised
// - Starting/Consumer: no change will happen
// - Stopping/Stopped: the consumer will start start consuming
// - Closing: an error will be returned
func (cg *ConsumerGroup) Start() error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	switch cg.state.Get() {
	case Initialising:
		cg.initialState = Starting // when the consumer is initialised, it will start straight away
		return nil
	case Stopping, Stopped:
		cg.channels.Consume <- true
		return nil
	case Starting, Consuming:
		return nil // already started, nothing to do
	default: // Closing state
		return fmt.Errorf("consummer cannot be started because it is closing")
	}
}

// Stop has different effects depending on the state:
// - Initialising: the consumer will remain in the Stopped state once initialised, without consuming messages
// - Starting/Consumer: no change will happen
// - Stopping/Stopped: the consumer will start start consuming
// - Closing: an error will be returned
// This method does not wait until the consumerGroup reaches the stopped state, it only triggers the stopping action.
func (cg *ConsumerGroup) Stop() {
	cg.stop(false)
}

// StopAndWait has different effects depending on the state:
// - Initialising: the consumer will remain in the Stopped state once initialised, without consuming messages
// - Starting/Consumer: no change will happen
// - Stopping/Stopped: the consumer will start start consuming
// - Closing: an error will be returned
// This method waits until the consumerGroup reaches the stopped state if it was starting/consuming.
func (cg *ConsumerGroup) StopAndWait() {
	cg.stop(false)
}

func (cg *ConsumerGroup) stop(sync bool) {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	switch cg.state.Get() {
	case Initialising:
		cg.initialState = Stopped // when the consumer is initialised, it will do nothing - no need to wait
		return
	case Stopping, Stopped:
		return // already stopped, nothing to do
	case Starting, Consuming:
		cg.channels.Consume <- false
		if sync {
			<-cg.saramaCgHandler.chSessionConsuming // wait until the active kafka session finishes
		}
		return
	default: // Closing state
		return // the consumer is being closed, so it is already 'stopped'
	}
}

// LogErrors creates a go-routine that waits on Errors channel and logs any error received.
// It exits on Closer channel closed.
func (cg *ConsumerGroup) LogErrors(ctx context.Context) {
	go func() {
		for {
			select {
			case err := <-cg.channels.Errors:
				logData := UnwrapLogData(err)
				logData["topic"] = cg.topic
				logData["group_name"] = cg.group
				log.Error(ctx, "kafka consumer-group error", err, logData)
			case <-cg.channels.Closer:
				return
			}
		}
	}()
}

// Close safely closes the consumer and releases all resources.
// pass in a context with a timeout or deadline.
// Passing a nil context will provide no timeout but is not recommended
func (cg *ConsumerGroup) Close(ctx context.Context) (err error) {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	// if Close has already been called, we don't have to do anything
	if cg.state.Get() == Closing {
		return
	}

	// Always close the Closed channel to signal that the closing operation has completed
	defer close(cg.channels.Closed)

	cg.state.Set(Closing)
	if ctx == nil {
		ctx = context.Background()
	}
	logData := log.Data{"topic": cg.topic, "group": cg.group, "state": cg.state}

	// Close Consume and Close channels and wait for any go-routine to finish their work
	close(cg.channels.Consume)
	close(cg.channels.Closer)
	didTimeout := WaitWithTimeout(ctx, cg.wgClose)
	if didTimeout {
		err := ctx.Err()
		log.Warn(ctx, "Close abandoned: context done", log.FormatErrors([]error{err}), logData)
		return err
	}

	// Close message-passing channels
	close(cg.channels.Errors)
	close(cg.channels.Upstream)

	// Close Sarama consumer only if it was initialised.
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
			case <-time.After(GetRetryTime(initAttempt, InitRetryPeriod)):
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
	cg.state.Set(Stopped)
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
			cg.state.Set(Closing)
			logData["state"] = Closing
			log.Info(ctx, "closed kafka consumer consume loop via closer channel", logData)
			return
		case consume, ok := <-cg.channels.Consume:
			if !ok {
				cg.state.Set(Closing)
				logData["state"] = Closing
				log.Info(ctx, "closed kafka consumer consume loop because the Consume channel is closed", logData)
				return
			}
			if consume {
				cg.state.Set(Starting)
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
	cg.state.Set(Starting)
	logData["state"] = Starting

	// close Ready channel (if it is not already closed)
	select {
	case <-cg.channels.Ready:
	default:
		close(cg.channels.Ready)
	}

	consumeAttempt := 1
	for {
		if s := cg.state.Get(); s != Starting && s != Consuming {
			// state was changed during cg.saramaCg.Consume
			return
		}
		select {
		case <-cg.channels.Closer:
			cg.state.Set(Closing)
			logData["state"] = Closing
			log.Info(ctx, "closed kafka consumer consume loop via closer channel", logData)
			return
		case consume, ok := <-cg.channels.Consume:
			if !ok {
				cg.state.Set(Closing)
				logData["state"] = Closing
				log.Info(ctx, "closed kafka consumer consume loop because the Consume channel is closed", logData)
				return
			}
			if !consume {
				cg.state.Set(Stopping)
				logData["state"] = Stopping
				return
			}
		default:
			// 'Consume' is called inside an infinite loop, when a server-side rebalance happens,
			// the consumer session will need to be recreated to get the new claims
			if err := cg.saramaCg.Consume(ctx, []string{cg.topic}, cg.saramaCgHandler); err != nil {
				log.Error(ctx, "error consuming", err, log.Data{"attempt": consumeAttempt})
				if s := cg.state.Get(); s != Starting && s != Consuming {
					// state changed during cg.saramaCg.Consume
					return
				}
				select {
				// check if closer channel is closed, signaling that the consumer should stop (don't retry to Consume)
				case <-cg.channels.Closer:
					cg.state.Set(Closing)
					logData["state"] = Closing
					log.Info(ctx, "closed kafka consumer consume loop via closer channel", logData)
					return
				case consume, ok := <-cg.channels.Consume:
					if !ok {
						cg.state.Set(Closing)
						logData["state"] = Closing
						log.Info(ctx, "closed kafka consumer consume loop because the Consume channel is closed", logData)
						return
					}
					if !consume {
						cg.state.Set(Stopping)
						logData["state"] = Stopping
						return
					}
					// once the retrial time has expired, we try to consume again (continue the loop)
				case <-time.After(GetRetryTime(consumeAttempt, ConsumeErrRetryPeriod)):
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
	if cg.state.Get() != Initialising {
		log.Warn(ctx, "wrong state for createConsumeLoop", log.Data{"state": cg.state.String()})
		return
	}

	// Set initial state
	cg.state.Set(cg.initialState)
	logData := log.Data{"topic": cg.topic, "group": cg.group, "state": cg.state.String()}

	// create loop according to state
	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		log.Info(ctx, "starting kafka consumer listener loop", logData)
		for {
			switch cg.state.Get() {
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
			case err, ok := <-cg.saramaCg.Errors():
				if !ok {
					log.Info(ctx, "closed kafka consumer error because Error channel is closed", logData)
					return
				}
				cg.channels.Errors <- err
			}
		}
	}()
}
