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

//go:generate moq -out ./kafkatest/mock_producer.go -pkg kafkatest . IProducer

// IProducer is an interface representing a Kafka Producer
type IProducer interface {
	Channels() *ProducerChannels
	IsInitialised() bool
	Initialise(ctx context.Context) error
	Checker(ctx context.Context, state *health.CheckState) error
	Close(ctx context.Context) (err error)
}

// Producer is a producer of Kafka messages
type Producer struct {
	producer     sarama.AsyncProducer
	producerInit producerInitialiser
	brokerAddrs  []string
	brokers      []*sarama.Broker
	topic        string
	channels     *ProducerChannels
	config       *sarama.Config
	mutex        *sync.Mutex
	wgClose      *sync.WaitGroup
}

// NewProducer returns a new producer instance using the provided config and channels.
// The rest of the config is set to defaults. If any channel parameter is nil, an error will be returned.
func NewProducer(ctx context.Context, brokerAddrs []string, topic string,
	channels *ProducerChannels, pConfig *ProducerConfig) (producer *Producer, err error) {
	return newProducer(ctx, brokerAddrs, topic, channels, pConfig, saramaNewAsyncProducer)
}

func newProducer(ctx context.Context, brokerAddrs []string, topic string,
	channels *ProducerChannels, pConfig *ProducerConfig, pInit producerInitialiser) (*Producer, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// Create Config
	config, err := getProducerConfig(pConfig)
	if err != nil {
		return nil, err
	}

	// Validate provided channels and assign them to producer. ErrNoChannel should be considered fatal by caller.
	err = channels.Validate()
	if err != nil {
		return nil, err
	}

	// Producer initialised with provided brokers and topic
	producer := &Producer{
		producerInit: pInit,
		brokerAddrs:  brokerAddrs,
		topic:        topic,
		config:       config,
		mutex:        &sync.Mutex{},
		wgClose:      &sync.WaitGroup{},
	}

	producer.channels = channels

	// disable metrics to prevent memory leak on broker.Open()
	metrics.UseNilMetrics = true

	// Create broker objects
	for _, addr := range brokerAddrs {
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
	if p == nil {
		return nil
	}
	return p.channels
}

// IsInitialised returns true only if Sarama producer has been correctly initialised.
func (p *Producer) IsInitialised() bool {
	if p == nil {
		return false
	}
	return p.producer != nil
}

// Initialise creates a new Sarama AsyncProducer and the channel redirection, only if it was not already initialised.
func (p *Producer) Initialise(ctx context.Context) error {

	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Do nothing if producer already initialised
	if p.IsInitialised() {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// Initialise AsyncProducer with default config and envMax
	saramaProducer, err := p.producerInit(p.brokerAddrs, p.config)
	if err != nil {
		return err
	}

	// On Successful initialization, close Init channel to stop uninitialised goroutine, and create initialised goroutine
	p.producer = saramaProducer
	log.Event(ctx, "initialised sarama producer", log.INFO, log.Data{"topic": p.topic})
	p.createLoopInitialised(ctx)
	close(p.channels.Ready)
	return nil
}

// Close safely closes the producer and releases all resources.
// pass in a context with a timeout or deadline.
// Passing a nil context will provide no timeout and this is not recommended
func (p *Producer) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// closing the Closer channel will end the go-routines(if any)
	close(p.channels.Closer)

	didTimeout := waitWithTimeout(ctx, p.wgClose)
	if didTimeout {
		log.Event(ctx, "shutdown context time exceeded, skipping graceful shutdown of producer", log.WARN)
		return ErrShutdownTimedOut
	}

	logData := log.Data{"topic": p.topic}

	close(p.channels.Errors)
	close(p.channels.Output)

	// Close producer only if it was initialised
	if p.IsInitialised() {
		if err = p.producer.Close(); err != nil {
			log.Event(ctx, "close failed of kafka producer", log.ERROR, log.Error(err), logData)
			return err
		}
	}

	// Close all brokers connections (used by healthcheck)
	for _, broker := range p.brokers {
		broker.Close()
	}

	log.Event(ctx, "successfully closed kafka producer", log.INFO, logData)
	close(p.channels.Closed)
	return nil
}

// createLoopUninitialised creates a goroutine to handle uninitialised producers.
// It generates errors to the Errors channel when a message is intended to be sent through the Output channel.
// If the closer channel is closed, it closes the closed channel straight away and stops.
// If the init channel is closed, the goroutine stops because the sarama client is available.
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
			case message := <-p.channels.Output:
				log.Event(ctx, "error sending a message", log.INFO, log.Data{"message": message, "topic": p.topic}, log.Error(ErrUninitialisedProducer))
				p.channels.Errors <- ErrUninitialisedProducer
			case <-p.channels.Ready:
				return
			case <-p.channels.Closer:
				log.Event(ctx, "closing uninitialised kafka producer", log.INFO, log.Data{"topic": p.topic})
				return
			case <-time.After(getRetryTime(initAttempt, InitRetryPeriod)):
				if err := p.Initialise(ctx); err != nil {
					log.Event(ctx, "error initialising producer", log.ERROR, log.Error(err), log.Data{"attempt": initAttempt})
					initAttempt++
					continue
				}
				return
			case <-ctx.Done():
				log.Event(ctx, "abandoning uninitialised producer - context expired", log.ERROR, log.Error(ctx.Err()), log.Data{"attempt": initAttempt})
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
		return ErrInitSarama
	}

	// Start kafka producer with topic. Redirect errors and messages; and handle closerChannel
	p.wgClose.Add(1)
	go func() {
		defer p.wgClose.Done()
		for {
			select {
			case err := <-p.producer.Errors():
				p.channels.Errors <- err
			case message := <-p.channels.Output:
				p.producer.Input() <- &sarama.ProducerMessage{Topic: p.topic, Value: sarama.StringEncoder(message)}
			case <-p.channels.Closer:
				log.Event(ctx, "closing initialised kafka producer", log.INFO, log.Data{"topic": p.topic})
				return
			}
		}
	}()
	return nil
}
