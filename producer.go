package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-kafka/v4/avro"
	"github.com/ONSdigital/dp-kafka/v4/interfaces"
	"github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
)

//go:generate moq -out ./kafkatest/mock_producer.go -pkg kafkatest . IProducer

const (
	TraceIDHeaderKey = string(request.RequestIdKey)
)

// IProducer is an interface representing a Kafka Producer, as implemented in dp-kafka/producer
type IProducer interface {
	Channels() *ProducerChannels
	Checker(ctx context.Context, state *healthcheck.CheckState) error
	LogErrors(ctx context.Context)
	IsInitialised() bool
	Initialise(ctx context.Context) error
	Send(ctx context.Context, schema *avro.Schema, event interface{}) error
	Close(ctx context.Context) (err error)
	AddHeader(key, value string)
}

// Producer is a producer of Kafka messages
type Producer struct {
	producer          sarama.AsyncProducer
	producerInit      interfaces.ProducerInitialiser
	brokerAddrs       []string
	brokers           []interfaces.SaramaBroker
	topic             string
	channels          *ProducerChannels
	config            *sarama.Config
	mutex             *sync.RWMutex
	wgClose           *sync.WaitGroup
	minRetryPeriod    time.Duration
	maxRetryPeriod    time.Duration
	minBrokersHealthy int
	headers           []sarama.RecordHeader
}

// NewProducer returns a new producer instance using the provided config and channels.
// The rest of the config is set to defaults. If any channel parameter is nil, an error will be returned.
func NewProducer(ctx context.Context, pConfig *ProducerConfig) (producer *Producer, err error) {
	return NewProducerWithGenerators(
		ctx,
		pConfig,
		sarama.NewAsyncProducer,
		SaramaNewBroker,
	)
}

func NewProducerWithGenerators(
	ctx context.Context,
	pConfig *ProducerConfig,
	pInit interfaces.ProducerInitialiser,
	brokerGenerator interfaces.BrokerGenerator,
) (*Producer, error) {
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
		brokers:           []interfaces.SaramaBroker{},
		topic:             pConfig.Topic,
		config:            config,
		mutex:             &sync.RWMutex{},
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
			if closeErr := producer.Close(ctx); closeErr != nil {
				log.Error(ctx, "error closing producer: %w", closeErr, log.Data{"topic": producer.topic})
			}
		case <-producer.channels.Closer:
			return
		}
	}()

	// Create broker objects
	for _, addr := range pConfig.BrokerAddrs {
		producer.brokers = append(producer.brokers, brokerGenerator(addr))
	}

	// Initialise producer, and log any error
	err = producer.Initialise(ctx)
	if err != nil {
		producer.createLoopUninitialised(ctx)
	}

	traceID := ctx.Value(TraceIDHeaderKey)
	if traceID == nil {
		producer.addTraceIDHeader()
	} else {
		producer.AddHeader(TraceIDHeaderKey, traceID.(string))
	}

	return producer, nil
}

// Channels returns the Producer channels for this producer
func (p *Producer) Channels() *ProducerChannels {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

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

func (p *Producer) AddHeader(key, value string) {
	if key == "" {
		return
	}
	p.headers = append(p.headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
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
				log.Info(ctx, "received kafka producer error", log.ERROR, &log.EventErrors{
					log.EventError{
						Message:    err.Error(),
						StackTrace: stackTrace(err),
						Data:       logData,
					},
				})

			case <-p.channels.Closer:
				return
			}
		}
	}()
}

// IsInitialised returns true only if Sarama producer has been correctly initialised.
func (p *Producer) IsInitialised() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return p.isInitialised()
}

func (p *Producer) isInitialised() bool {
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
	if p.isInitialised() {
		return nil
	}

	// Initialise AsyncProducer with default config and envMax
	saramaProducer, err := p.producerInit(p.brokerAddrs, p.config)
	if err != nil {
		return fmt.Errorf("failed to create a new sarama producer: %w", err)
	}

	saramaProducer = otelsarama.WrapAsyncProducer(p.config, saramaProducer, otelsarama.WithTracerProvider(otel.GetTracerProvider()), otelsarama.WithPropagators(otel.GetTextMapPropagator()))

	// On Successful initialization, close Init channel to stop uninitialised goroutine, and create initialised goroutine
	p.producer = saramaProducer
	if err := p.createLoopInitialised(); err != nil {
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
func (p *Producer) Send(ctx context.Context, schema *avro.Schema, event interface{}) error {
	bytes, err := schema.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event with avro schema: %w", err)
	}

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if err := SafeSendBytes(p.channels.Output, BytesMessage{Value: bytes, Context: ctx}); err != nil {
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

	p.mutex.Lock()
	defer p.mutex.Unlock()

	// closing the Closer channel will end the go-routines(if any)
	SafeClose(p.channels.Closer)

	logData := log.Data{"topic": p.topic}

	SafeCloseErr(p.channels.Errors)
	SafeCloseBytes(p.channels.Output)

	// Close producer only if it was initialised
	if p.isInitialised() {
		if err = p.producer.Close(); err != nil {
			return NewError(
				fmt.Errorf("error closing sarama producer: %w", err),
				logData,
			)
		}
	}

	// Close all brokers connections (used by healthcheck)
	brokerErrs := []string{}
	for _, broker := range p.brokers {
		if err := broker.Close(); err != nil {
			brokerErrs = append(brokerErrs, err.Error())
		}
	}
	log.Info(ctx, "done closing any remaining broker connection", log.Data{"close_errors": brokerErrs})

	SafeClose(p.channels.Closed)

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
			delay := time.NewTimer(GetRetryTime(initAttempt, p.minRetryPeriod, p.maxRetryPeriod))
			select {
			case output, ok := <-p.channels.Output:
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
				if !ok {
					return // output chan closed
				}
				log.Info(ctx, "error sending a message", log.Data{"message": output.Value, "topic": p.topic}, log.FormatErrors([]error{errors.New("producer is not initialised")}))
				if err := SafeSendErr(p.channels.Errors, errors.New("producer is not initialised")); err != nil {
					return // errors chan closed
				}
			case <-p.channels.Initialised:
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
				return
			case <-p.channels.Closer:
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
				return
			case <-delay.C:
				if err := p.Initialise(ctx); err != nil {
					log.Warn(ctx, "error initialising producer, will retry", log.Data{"attempt": initAttempt, "err": err.Error()})
					initAttempt++
					continue
				}
				return
			case <-ctx.Done():
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
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
func (p *Producer) createLoopInitialised() error {
	// If sarama producer is not available, return error.
	if !p.isInitialised() {
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
					message.Context,
					p.producer.Input(),
					&sarama.ProducerMessage{Topic: p.topic, Value: sarama.StringEncoder(message.Value), Headers: p.headers},
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

func (p *Producer) addTraceIDHeader() {
	p.headers = append(p.headers, sarama.RecordHeader{
		Key:   []byte(TraceIDHeaderKey),
		Value: []byte(uuid.NewString()),
	})
}
