package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v3/interfaces"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
)

//go:generate moq -out ./kafkatest/mock_consumer_group.go -pkg kafkatest . IConsumerGroup

// IConsumerGroup is an interface representing a Kafka Consumer Group, as implemented in dp-kafka/consumer
type IConsumerGroup interface {
	Channels() *ConsumerGroupChannels
	State() State
	StateWait(state State)
	RegisterHandler(ctx context.Context, h Handler) error
	RegisterBatchHandler(ctx context.Context, batchHandler BatchHandler) error
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	IsInitialised() bool
	Initialise(ctx context.Context) error
	OnHealthUpdate(status string)
	Start() error
	Stop() error
	StopAndWait() error
	LogErrors(ctx context.Context)
	Close(ctx context.Context) error
}

// ConsumerGroup is a Kafka consumer group instance.
type ConsumerGroup struct {
	brokerAddrs       []string
	brokers           []interfaces.SaramaBroker
	channels          *ConsumerGroupChannels
	saramaCg          sarama.ConsumerGroup
	saramaCgHandler   *saramaHandler
	saramaCgInit      interfaces.ConsumerGroupInitialiser
	topic             string
	group             string
	initialState      State // target state for a consumer that is still being initialised
	state             *StateMachine
	saramaConfig      *sarama.Config
	mutex             *sync.RWMutex // Mutex for consumer funcs that are not supposed to run concurrently
	wgClose           *sync.WaitGroup
	handler           Handler
	batchHandler      BatchHandler
	numWorkers        int
	batchSize         int
	batchWaitTime     time.Duration
	minRetryPeriod    time.Duration
	maxRetryPeriod    time.Duration
	minBrokersHealthy int
	ctx               context.Context // Contet provided to the ConsumerGroup at creation time
}

// NewConsumerGroup creates a new consumer group with the provided parameters
func NewConsumerGroup(ctx context.Context, cgConfig *ConsumerGroupConfig) (*ConsumerGroup, error) {
	return newConsumerGroup(ctx, cgConfig, interfaces.SaramaNewConsumerGroup)
}

func newConsumerGroup(ctx context.Context, cgConfig *ConsumerGroupConfig, cgInit interfaces.ConsumerGroupInitialiser) (*ConsumerGroup, error) {
	if ctx == nil {
		return nil, errors.New("nil context was passed to consumer-group constructor")
	}

	// Create sarama config and set any other necessary values
	cfg, err := cgConfig.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer-group config: %w", err)
	}

	// upstream buffer size the maximum between number of workers and batch size
	upstreamBufferSize := *cgConfig.NumWorkers
	if *cgConfig.BatchSize > *cgConfig.NumWorkers {
		upstreamBufferSize = *cgConfig.BatchSize
	}

	channels := CreateConsumerGroupChannels(upstreamBufferSize)
	stateMachine := NewConsumerStateMachine()
	channels.State = stateMachine.channels

	// ConsumerGroup created with provided brokerAddrs, topic, group and sync
	cg := &ConsumerGroup{
		brokerAddrs:       cgConfig.BrokerAddrs,
		brokers:           []interfaces.SaramaBroker{},
		channels:          channels,
		topic:             cgConfig.Topic,
		group:             cgConfig.GroupName,
		state:             stateMachine,
		initialState:      Stopped,
		saramaConfig:      cfg,
		mutex:             &sync.RWMutex{},
		wgClose:           &sync.WaitGroup{},
		saramaCgInit:      cgInit,
		numWorkers:        *cgConfig.NumWorkers,
		batchSize:         *cgConfig.BatchSize,
		batchWaitTime:     *cgConfig.BatchWaitTime,
		minRetryPeriod:    *cgConfig.MinRetryPeriod,
		maxRetryPeriod:    *cgConfig.MaxRetryPeriod,
		minBrokersHealthy: *cgConfig.MinBrokersHealthy,
		ctx:               ctx,
	}

	// Close consumer group on context.Done
	go func() {
		select {
		case <-ctx.Done():
			log.Info(ctx, "closing consumer group because context is done")
			if err := cg.Close(ctx); err != nil {
				log.Error(ctx, "error closing consumer group: %w", err, log.Data{"topic": cg.topic, "group": cg.group})
			}
		case <-cg.channels.Closer:
			return
		}
	}()

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
	cg.mutex.RLock()
	defer cg.mutex.RUnlock()

	return cg.channels
}

// State returns the state of the consumer group
func (cg *ConsumerGroup) State() State {
	cg.mutex.RLock()
	defer cg.mutex.RUnlock()

	if cg.state == nil {
		return Initialising
	}
	return cg.state.Get()
}

// StateWait blocks the calling thread until the provided state is reached
func (cg *ConsumerGroup) StateWait(state State) {
	cg.state.GetChan(state).Wait()
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
	if !cg.IsInitialised() {
		return state.Update(healthcheck.StatusWarning, "kafka consumer-group is not initialised", 0)
	}
	info := Healthcheck(ctx, cg.brokers, cg.topic, cg.saramaConfig)
	if err := info.UpdateStatus(state, cg.minBrokersHealthy, MsgHealthyConsumerGroup); err != nil {
		return fmt.Errorf("error updating consumer-group healthcheck status: %w", err)
	}
	return nil
}

// IsInitialised returns true only if Sarama ConsumerGroup has been correctly initialised.
func (cg *ConsumerGroup) IsInitialised() bool {
	cg.mutex.RLock()
	defer cg.mutex.RUnlock()
	return cg.isInitialised()
}

func (cg *ConsumerGroup) isInitialised() bool {
	return cg.saramaCg != nil
}

// Initialise creates a new Sarama ConsumerGroup and the consumer/error loops, only if it was not already initialised.
func (cg *ConsumerGroup) Initialise(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context was passed to consumer-group initialise")
	}

	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	// Do nothing if consumer group already initialised
	if cg.isInitialised() {
		return nil
	}

	// Create Sarama Consumer. Errors at this point are not necessarily fatal (e.g. brokers not reachable).
	saramaConsumerGroup, err := cg.saramaCgInit(cg.brokerAddrs, cg.group, cg.saramaConfig)
	if err != nil {
		return fmt.Errorf("failed to create a new sarama consumer group: %w", err)
	}

	// On Successful initialization, create sarama consumer handler, and loop goroutines (for messages and errors)
	cg.saramaCgHandler = newSaramaHandler(ctx, cg.channels, cg.state)
	cg.saramaCg = saramaConsumerGroup
	cg.createConsumeLoop(ctx)
	cg.createErrorLoop(ctx)
	log.Info(ctx, "sarama consumer group has been initialised", log.Data{"topic": cg.topic, "group_name": cg.group, "state": cg.state})

	// Await until the consumer has been set up
	<-cg.channels.Initialised

	return nil
}

// OnHealthUpdate implements the healthcheck Subscriber interface so that the kafka consumer can be notified of state changes.
// This method is intended to be used for managed start/stop's only, and should not be called manually.
// WARNING: Having more than one notifier calling this method will result in unexpected behavior.
// - On Health status OK: start consuming
// - On Warning or Critical: stop consuming
func (cg *ConsumerGroup) OnHealthUpdate(status string) {
	switch status {
	case healthcheck.StatusOK:
		if err := cg.Start(); err != nil {
			log.Error(cg.ctx, "error starting consumer-group on a healthy state report", err, log.Data{"topic": cg.topic, "group": cg.group})
		}
	case healthcheck.StatusWarning:
		if err := cg.Stop(); err != nil {
			log.Error(cg.ctx, "error stopping consumer-group on a warning state report", err, log.Data{"topic": cg.topic, "group": cg.group})
		}
	case healthcheck.StatusCritical:
		if err := cg.Stop(); err != nil {
			log.Error(cg.ctx, "error stopping consumer-group on a critical state report", err, log.Data{"topic": cg.topic, "group": cg.group})
		}
	}
}

// Start has different effects depending on the state:
// - Initialising: the consumer will try to start consuming straight away once it's initialised
// - Starting/Consumer: no change will happen
// - Stopping/Stopped: the consumer will start consuming
// - Closing: an error will be returned
func (cg *ConsumerGroup) Start() error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	switch cg.state.Get() {
	case Initialising:
		cg.initialState = Starting // when the consumer is initialised, it will start straight away
		return nil
	case Stopping, Stopped:
		if err := SafeSendBool(cg.channels.Consume, true); err != nil {
			return fmt.Errorf("failed to send 'true' to consume channel: %w", err)
		}
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
// Any error while trying to trigger the stop will be returned
func (cg *ConsumerGroup) Stop() error {
	return cg.stop(false)
}

// StopAndWait has different effects depending on the state:
// - Initialising: the consumer will remain in the Stopped state once initialised, without consuming messages
// - Starting/Consumer: no change will happen
// - Stopping/Stopped: the consumer will start start consuming
// - Closing: an error will be returned
// This method waits until the consumerGroup reaches the stopped state if it was starting/consuming.
// Any error while trying to trigger the stop will be returned and the therad will not be blocked.
func (cg *ConsumerGroup) StopAndWait() error {
	return cg.stop(true)
}

func (cg *ConsumerGroup) stop(sync bool) error {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	switch cg.state.Get() {
	case Initialising:
		cg.initialState = Stopped // when the consumer is initialised, it will do nothing - no need to wait
		return nil
	case Stopping, Stopped:
		return nil // already stopped, nothing to do
	case Starting, Consuming:
		if err := SafeSendBool(cg.channels.Consume, false); err != nil {
			return fmt.Errorf("failed to send 'false' to consume channel: %w", err)
		}
		if sync {
			cg.saramaCgHandler.waitSessionFinish() // wait until the active kafka session finishes
		}
		return nil
	default: // Closing state
		return nil // the consumer is being closed, so it is already 'stopped'
	}
}

// LogErrors creates a go-routine that waits on Errors channel and logs any error received.
// It exits on Closer channel closed.
func (cg *ConsumerGroup) LogErrors(ctx context.Context) {
	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		for {
			select {
			case err, ok := <-cg.channels.Errors:
				if !ok {
					return
				}
				logData := UnwrapLogData(err)
				logData["topic"] = cg.topic
				logData["group_name"] = cg.group
				log.Error(ctx, "received kafka consumer-group error", err, logData)
			case <-cg.channels.Closer:
				return
			}
		}
	}()
}

// Close safely closes the consumer and releases all resources.
// pass in a context with a timeout or deadline.
func (cg *ConsumerGroup) Close(ctx context.Context) (err error) {
	if ctx == nil {
		return errors.New("nil context was passed to consumer-group close")
	}

	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	// if Close has already been called, we don't have to do anything
	if cg.state.Get() == Closing {
		return nil
	}

	// Always close the Closed channel to signal that the closing operation has completed
	defer SafeClose(cg.channels.Closed)

	cg.state.Set(Closing)
	logData := log.Data{"topic": cg.topic, "group": cg.group, "state": cg.state.String()}

	// Close Consume and Close channels and wait for any go-routine to finish their work
	SafeCloseBool(cg.channels.Consume)
	SafeClose(cg.channels.Closer)
	didTimeout := WaitWithTimeout(cg.wgClose, 2*time.Second)
	if didTimeout {
		return NewError(
			fmt.Errorf("timed out while waiting for all loops to finish and remaining messages to be processed: %w", ctx.Err()),
			logData,
		)
	}

	// Close message-passing channels
	SafeCloseErr(cg.channels.Errors)
	SafeCloseMessage(cg.channels.Upstream)

	// Close Sarama consumer only if it was initialised.
	if cg.isInitialised() {
		if err = cg.saramaCg.Close(); err != nil {
			return NewError(
				fmt.Errorf("error closing sarama consumer-group: %w", err),
				logData,
			)
		}
	}

	// Close any remaining broker connection (used by healthcheck)
	brokerErrs := []string{}
	for _, broker := range cg.brokers {
		if err := broker.Close(); err != nil {
			brokerErrs = append(brokerErrs, err.Error())
		}
	}
	log.Info(ctx, "done closing any remaining broker connection", log.Data{"close_errors": brokerErrs})

	log.Info(ctx, "successfully closed kafka consumer group", logData)
	return nil
}

// createLoopUninitialised creates a goroutine to handle uninitialised consumer groups.
// It retries to initialise the consumer with an exponential backoff retrial algorithm,
// starting with 'minRetryPeriod' and restarted when 'maxRetryPeriod' is reached.
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
			case <-cg.channels.Initialised:
				return
			case <-cg.channels.Closer:
				return
			case <-time.After(GetRetryTime(initAttempt, cg.minRetryPeriod, cg.maxRetryPeriod)):
				if err := cg.Initialise(ctx); err != nil {
					log.Warn(ctx, "error initialising consumer group, will retry", log.Data{"attempt": initAttempt, "err": err.Error()})
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

	// close Initialised channel (if it is not already closed)
	SafeClose(cg.channels.Initialised)

	logData["state"] = Stopped
	log.Info(ctx, "kafka consumer group is stopped", logData)

	for {
		select {
		case <-cg.channels.Closer:
			cg.state.Set(Closing)
			return
		case consume, ok := <-cg.channels.Consume:
			if !ok {
				cg.state.Set(Closing)
				return
			}
			if consume {
				cg.state.Set(Starting)
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
// - A 'false' value is received from the Consume channel: the state is set to 'Stopping' and the func will return
// - The Closer channel or the Consume channel is closed: the state is set to 'Closing' and the func will return
// If saramaCg.Consume fails, we retry after an initial period of 'minRetryPeriod',
// following an exponential backoff between retries, until 'maxRetryPeriod' is reached, at which point the retry algorithm is restarted.
// If the consumer changes its state between retries, we abort the loop as described above.
func (cg *ConsumerGroup) startingState(ctx context.Context, logData log.Data) {
	cg.state.Set(Starting)

	// close Initialised channel (if it is not already closed)
	SafeClose(cg.channels.Initialised)

	logData["state"] = Starting
	log.Info(ctx, "kafka consumer group is starting", logData)

	consumeAttempt := 1
	for {
		if s := cg.state.Get(); s != Starting && s != Consuming {
			// state was changed during cg.saramaCg.Consume
			return
		}
		select {
		case <-cg.channels.Closer:
			cg.state.Set(Closing)
			return
		case consume, ok := <-cg.channels.Consume:
			if !ok {
				cg.state.Set(Closing)
				return
			}
			if !consume {
				cg.state.Set(Stopping)
				return
			}
		default:
			// 'Consume' is called inside an infinite loop, when a server-side rebalance happens,
			// the consumer session will need to be recreated to get the new claims
			if err := cg.saramaCg.Consume(ctx, []string{cg.topic}, cg.saramaCgHandler); err != nil {
				log.Warn(ctx, "error consuming, will retry", log.Data{"attempt": consumeAttempt, "err": err.Error()})
				if s := cg.state.Get(); s != Starting && s != Consuming {
					// state changed during cg.saramaCg.Consume
					return
				}
				select {
				// check if closer channel is closed, signaling that the consumer should stop (don't retry to Consume)
				case <-cg.channels.Closer:
					cg.state.Set(Closing)
					return
				case consume, ok := <-cg.channels.Consume:
					if !ok {
						cg.state.Set(Closing)
						return
					}
					if !consume {
						cg.state.Set(Stopping)
						return
					}
					// once the retrial time has expired, we try to consume again (continue the loop)
				case <-time.After(GetRetryTime(consumeAttempt, cg.minRetryPeriod, cg.maxRetryPeriod)):
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
		log.Warn(ctx, "wrong state to create consume loop", log.Data{"state": cg.state.String()})
		return
	}

	// Set initial state
	cg.state.Set(cg.initialState)
	logData := log.Data{"topic": cg.topic, "group": cg.group, "state": cg.state.String()}

	// create loop according to state
	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
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
// If the closer channel or the errors channel is closed, it ends the loop and closes Closed channel.
func (cg *ConsumerGroup) createErrorLoop(ctx context.Context) {
	cg.wgClose.Add(1)
	go func() {
		defer cg.wgClose.Done()
		for {
			select {
			// check if closer channel is closed, signaling that the consumer should stop
			case <-cg.channels.Closer:
				return
			// listen to kafka errors from sarama and forward them to the Errors chanel
			case err, ok := <-cg.saramaCg.Errors():
				if !ok {
					return
				}
				if errSend := SafeSendErr(cg.channels.Errors, err); err != nil {
					log.Error(ctx, "consumer-group error sending error to the error channel", errSend, log.Data{"original_error": err, "topic": cg.topic, "group": cg.group})
				}
			}
		}
	}()
}
