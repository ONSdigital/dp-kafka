package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v3/avro"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
)

//go:generate moq -out ./kafkatest/mock_producer.go -pkg kafkatest . IProducer

// IProducer is an interface representing a Kafka Producer, as implemented in dp-kafka/producer
type IProducer interface {
	Channels() *ProducerChannels
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	LogErrors(ctx context.Context)
	IsInitialised() bool
	Initialise(ctx context.Context) error
	Send(schema *avro.Schema, event interface{}) error
	Close(ctx context.Context) (err error)
}

// Producer is a producer of Kafka messages
type Producer struct {
	producer          sarama.AsyncProducer
	producerInit      producerInitialiser
	brokerAddrs       []string
	brokers           []SaramaBroker
	topic             string
	channels          *ProducerChannels
	config            *sarama.Config
	mutex             *sync.Mutex
	wgClose           *sync.WaitGroup
	minRetryPeriod    time.Duration
	maxRetryPeriod    time.Duration
	minBrokersHealthy int
}

// New returns a new producer instance using the provided config and channels.
// The rest of the config is set to defaults. If any channel parameter is nil, an error will be returned.
func NewProducer(ctx context.Context, pConfig *ProducerConfig) (producer *Producer, err error) {
	return newProducer(ctx, pConfig, saramaNewAsyncProducer)
}

func newProducer(ctx context.Context, pConfig *ProducerConfig, pInit producerInitialiser) (*Producer, error) {
	if ctx == nil {
		return nil, errors.New("nil context was passed to producer constructor")
	}

	// Create Sarama config and set any other default values
	config, err := pConfig.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get producer config: %w", err)
	}

	// Producer initialised with provided brokers and topic
	producer := &Producer{
		producerInit:      pInit,
		brokerAddrs:       pConfig.BrokerAddrs,
		channels:          CreateProducerChannels(),
		brokers:           []SaramaBroker{},
		topic:             pConfig.Topic,
		config:            config,
		mutex:             &sync.Mutex{},
		wgClose:           &sync.WaitGroup{},
		minRetryPeriod:    *pConfig.MinRetryPeriod,
		maxRetryPeriod:    *pConfig.MaxRetryPeriod,
		minBrokersHealthy: *pConfig.MinBrokersHealthy,
	}

	// Close producer on context.Done
	go func() {
		select {
		case <-ctx.Done():
			log.Info(ctx, "closing producer because context is done")
			if err := producer.Close(ctx); err != nil {
				log.Error(ctx, "error closing producer: %w", err, log.Data{"topic": producer.topic})
			}
		case <-producer.channels.Closer:
			return
		}
	}()

	// Create broker objects
	for _, addr := range pConfig.BrokerAddrs {
		producer.brokers = append(producer.brokers, sarama.NewBroker(addr))
	}

	// Initialise producer, and log any error
	err = producer.Initialise(ctx)
	if err != nil {
		producer.createLoopUninitialised(ctx)
	}
	return producer, nil
}

// Channels returns the Producer channels for this producer
func (p *Producer) Channels() *ProducerChannels {
	return p.channels
}

// Checker checks health of Kafka producer and updates the provided CheckState accordingly
func (p *Producer) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	if !p.IsInitialised() {
		return state.Update(healthcheck.StatusWarning, "kafka producer is not initialised", 0)
	}
	info := Healthcheck(ctx, p.brokers, p.topic, p.config)
	if err := info.UpdateStatus(state, p.minBrokersHealthy, MsgHealthyProducer); err != nil {
		return fmt.Errorf("error updating producer healthcheck status: %w", err)
	}
	return nil
}

// LogErrors creates a go-routine that waits on Errors channel and logs any error received.
// It exits on Closer channel closed.
func (p *Producer) LogErrors(ctx context.Context) {
	p.wgClose.Add(1)
	go func() {
		defer p.wgClose.Done()
		for {
			select {
			case err, ok := <-p.channels.Errors:
				if !ok {
					return
				}
				logData := UnwrapLogData(err)
				logData["topic"] = p.topic
				log.Error(ctx, "received kafka producer error", err, logData)
			case <-p.channels.Closer:
				return
			}
		}
	}()
}

// IsInitialised returns true only if Sarama producer has been correctly initialised.
func (p *Producer) IsInitialised() bool {
	return p.producer != nil
}

// Initialise creates a new Sarama AsyncProducer and the channel redirection, only if it was not already initialised.
func (p *Producer) Initialise(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context was passed to producer initialise")
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Do nothing if producer already initialised
	if p.IsInitialised() {
		return nil
	}

	// Initialise AsyncProducer with default config and envMax
	saramaProducer, err := p.producerInit(p.brokerAddrs, p.config)
	if err != nil {
		return fmt.Errorf("failed to create a new sarama producer: %w", err)
	}

	// On Successful initialization, close Init channel to stop uninitialised goroutine, and create initialised goroutine
	p.producer = saramaProducer
	if err := p.createLoopInitialised(ctx); err != nil {
		if errSarama := saramaProducer.Close(); errSarama != nil {
			log.Warn(ctx, fmt.Sprintf("failed to close sarama producer: %s", errSarama.Error()), log.Data{"topic": p.topic})
		}
		p.producer = nil
		return fmt.Errorf("failed to create initialised loop: %w", err)
	}
	SafeClose(p.channels.Initialised)
	log.Info(ctx, "sarama producer has been initialised", log.Data{"topic": p.topic})
	return nil
}

// Send marshals the provided event with the provided schema, and sends it to kafka
func (p *Producer) Send(schema *avro.Schema, event interface{}) error {
	bytes, err := schema.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event with avro schema: %w", err)
	}
	if err := SafeSendBytes(p.channels.Output, bytes); err != nil {
		return fmt.Errorf("failed to send marshalled message to output channel: %w", err)
	}
	return nil
}

// Close safely closes the producer and releases all resources.
// pass in a context with a timeout or deadline.
func (p *Producer) Close(ctx context.Context) (err error) {
	if ctx == nil {
		return errors.New("nil context was passed to producer close")
	}

	// closing the Closer channel will end the go-routines(if any)
	SafeClose(p.channels.Closer)

	didTimeout := WaitWithTimeout(p.wgClose)
	if didTimeout {
		return NewError(
			fmt.Errorf("timed out while waiting for all loops to finish: %w", ctx.Err()),
			log.Data{"topic": p.topic},
		)
	}

	logData := log.Data{"topic": p.topic}

	SafeCloseErr(p.channels.Errors)
	SafeCloseBytes(p.channels.Output)

	// Close producer only if it was initialised
	if p.IsInitialised() {
		if err = p.producer.Close(); err != nil {
			return NewError(
				fmt.Errorf("error closing sarama producer: %w", err),
				logData,
			)
		}
	}

	// Close all brokers connections (used by healthcheck)
	brokerErrs := []error{}
	for _, broker := range p.brokers {
		if err := broker.Close(); err != nil {
			brokerErrs = append(brokerErrs, err)
		}
	}

	SafeClose(p.channels.Closed)

	if len(brokerErrs) > 0 {
		return fmt.Errorf("error(s) closing broker connections: %v", brokerErrs)
	}

	log.Info(ctx, "successfully closed kafka producer", logData)
	return nil
}

// createLoopUninitialised creates a goroutine to handle uninitialised producers.
// It generates errors to the Errors channel when a message is intended to be sent through the Output channel.
// If the init channel is closed, the goroutine stops because the sarama client is available.
// If the closer channel is closed, the goroutine stops because the client is being closed.
// It retries to initialise the producer after waiting for a period of time following an exponential distribution between retries.
func (p *Producer) createLoopUninitialised(ctx context.Context) {

	// Do nothing if producer already initialised
	if p.IsInitialised() {
		return
	}

	p.wgClose.Add(1)
	go func() {
		defer p.wgClose.Done()
		initAttempt := 1
		for {
			select {
			case message, ok := <-p.channels.Output:
				if !ok {
					return // output chan closed
				}
				log.Info(ctx, "error sending a message", log.Data{"message": message, "topic": p.topic}, log.FormatErrors([]error{errors.New("producer is not initialised")}))
				if err := SafeSendErr(p.channels.Errors, errors.New("producer is not initialised")); err != nil {
					return // errors chan closed
				}
			case <-p.channels.Initialised:
				return
			case <-p.channels.Closer:
				return
			case <-time.After(GetRetryTime(initAttempt, p.minRetryPeriod, p.maxRetryPeriod)):
				if err := p.Initialise(ctx); err != nil {
					log.Error(ctx, "error initialising producer", err, log.Data{"attempt": initAttempt})
					initAttempt++
					continue
				}
				return
			case <-ctx.Done():
				log.Error(ctx, "abandoning uninitialised producer - context expired", ctx.Err(), log.Data{"attempt": initAttempt})
				return
			}
		}
	}()
}

// createLoopInitialised creates a goroutine to handle initialised producers.
// It redirects sarama errors to caller errors channel.
// If forwards messages from the output channel to the sarama producer input.
// If the closer channel is closed, it ends the loop and closes Closed channel.
func (p *Producer) createLoopInitialised(ctx context.Context) error {
	// If sarama producer is not available, return error.
	if !p.IsInitialised() {
		return errors.New("failed to initialise client")
	}

	// Start kafka producer with topic. Redirect errors and messages; and handle closerChannel
	p.wgClose.Add(1)
	go func() {
		defer p.wgClose.Done()
		for {
			select {
			case err, ok := <-p.producer.Errors():
				if !ok {
					return // sarama errors chan closed
				}
				if err := SafeSendErr(p.channels.Errors, err); err != nil {
					return // errors chan closed
				}

			case message, ok := <-p.channels.Output:
				if !ok {
					return // output chan closed
				}
				err := SafeSendProducerMessage(
					p.producer.Input(),
					&sarama.ProducerMessage{Topic: p.topic, Value: sarama.StringEncoder(message)},
				)
				if err != nil {
					return // sarama producer input channel closed
				}
			case <-p.channels.Closer:
				return
			}
		}
	}()
	return nil
}
