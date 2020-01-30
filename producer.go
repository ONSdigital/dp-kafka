package kafka

import (
	"context"
	"sync"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
)

// Producer provides a producer of Kafka messages
type Producer struct {
	envMax    int
	producer  sarama.AsyncProducer
	brokers   []string
	topic     string
	channels  *ProducerChannels
	Check     *health.Check
	cli       Sarama
	mutexInit *sync.Mutex
}

// NewProducer returns a new producer instance using the provided config and channels.
// The rest of the config is set to defaults. If any channel parameter is nil, an error will be returned.
func NewProducer(
	ctx context.Context, brokers []string, topic string, envMax int, channels ProducerChannels) (Producer, error) {
	return NewProducerWithSaramaClient(
		ctx, brokers, topic, envMax, channels, &SaramaClient{},
	)
}

// NewProducerWithSaramaClient returns a new producer with a provided Sarama client
func NewProducerWithSaramaClient(
	ctx context.Context, brokers []string, topic string, envMax int,
	channels ProducerChannels, cli Sarama) (producer Producer, err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// Producer initialised with provided brokers and topic
	producer = Producer{
		envMax:    envMax,
		brokers:   brokers,
		topic:     topic,
		cli:       cli,
		mutexInit: &sync.Mutex{},
	}

	// Initial Check struct
	check := &health.Check{Name: ServiceName}
	producer.Check = check

	// Validate provided channels and assign them to producer. ErrNoChannel should be considered fatal by caller.
	err = channels.Validate()
	if err != nil {
		return producer, err
	}
	producer.channels = &channels

	// Initialise Sarama producer, and return any error (which might not be considered fatal by caller)
	err = producer.InitialiseSarama(ctx)
	if err != nil {
		producer.createLoopUninitialised(ctx)
	}
	return producer, err
}

// IsInitialised returns true only if Sarama producer has been correctly initialised.
func (p *Producer) IsInitialised() bool {
	if p == nil {
		return false
	}
	return p.producer != nil
}

// InitialiseSarama creates a new Sarama AsyncProducer and the channel redirection, only if it was not already initialised.
func (p *Producer) InitialiseSarama(ctx context.Context) error {

	p.mutexInit.Lock()
	defer p.mutexInit.Unlock()

	// Do nothing if producer already initialised
	if p.IsInitialised() {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// Initialise AsyncProducer with default config and envMax
	config := sarama.NewConfig()
	if p.envMax > 0 {
		config.Producer.MaxMessageBytes = p.envMax
	}
	saramaProducer, err := p.cli.NewAsyncProducer(p.brokers, config)
	if err != nil {
		return err
	}

	// On Successful initialization, close Init channel to stop uninitialised goroutine, and create initialised goroutine
	p.producer = saramaProducer
	close(p.channels.Init)
	log.Event(ctx, "Initialised Sarama Producer", log.Data{"topic": p.topic})
	err = p.createLoopInitialised(ctx)
	return err
}

// Close safely closes the producer and releases all resources.
// pass in a context with a timeout or deadline.
// Passing a nil context will provide no timeout and this is not recommended
func (p *Producer) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	close(p.channels.Closer)

	// 'Closed' channel will be closed either by sarama client goroutine or the uninitialised goroutine.
	logData := log.Data{"topic": p.topic}
	select {
	case <-p.channels.Closed:
		close(p.channels.Errors)
		close(p.channels.Output)
		log.Event(ctx, "Successfully closed kafka producer")
		if p.IsInitialised() {
			if err = p.producer.Close(); err != nil {
				log.Event(ctx, "Close failed of kafka producer", log.Error(err), logData)
			} else {
				log.Event(ctx, "Successfully closed kafka producer", logData)
			}
		}
	case <-ctx.Done():
		log.Event(ctx, "Shutdown context time exceeded, skipping graceful shutdown of producer")
		return ErrShutdownTimedOut
	}
	return
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

	go func() {
		log.Event(ctx, "Started uninitialised kafka producer", log.Data{"topic": p.topic})
		for {
			select {
			case message := <-p.channels.Output:
				log.Event(ctx, "error sending a message", log.Data{"message": message, "topic": p.topic}, log.Error(ErrUninitialisedProducer))
				p.channels.Errors <- ErrUninitialisedProducer
			case <-p.channels.Closer:
				log.Event(ctx, "Closing uninitialised kafka producer", log.Data{"topic": p.topic})
				close(p.channels.Closed)
				return
			case <-p.channels.Init:
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
	go func() {
		defer close(p.channels.Closed)
		log.Event(ctx, "Started initialised kafka producer", log.Data{"topic": p.topic})
		for {
			select {
			case err := <-p.producer.Errors():
				log.Event(ctx, "Producer", log.Data{"topic": p.topic}, log.Error(err))
				p.channels.Errors <- err
			case message := <-p.channels.Output:
				p.producer.Input() <- &sarama.ProducerMessage{Topic: p.topic, Value: sarama.StringEncoder(message)}
			case <-p.channels.Closer:
				log.Event(ctx, "Closing initialised kafka producer", log.Data{"topic": p.topic})
				return
			}
		}
	}()
	return nil
}
