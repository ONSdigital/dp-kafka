package kafka

import (
	"context"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
)

// Producer provides a producer of Kafka messages
type Producer struct {
	envMax   int
	producer sarama.AsyncProducer
	brokers  []string
	topic    string
	channels *ProducerChannels
	Check    *health.Check
	cli      Sarama
}

// Output is the channel to send outgoing messages to.
func (producer *Producer) Output() chan []byte {
	return producer.channels.Output
}

// Errors provides errors returned from Kafka.
func (producer *Producer) Errors() chan error {
	return producer.channels.Errors
}

// Close safely closes the producer and releases all resources.
// pass in a context with a timeout or deadline.
// Passing a nil context will provide no timeout but is not recommended
func (producer *Producer) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	close(producer.channels.Closer)

	// 'Closed' channel will be closed either by sarama client goroutine or the uninitialized goroutine.
	logData := log.Data{"topic": producer.topic}
	select {
	case <-producer.channels.Closed:
		close(producer.channels.Errors)
		close(producer.channels.Output)
		log.Event(ctx, "Successfully closed kafka producer")
		if producer.producer != nil {
			if err = producer.producer.Close(); err != nil {
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

// InitializeSarama creates a new Sarama AsyncProducer and the channel redirection, only if it was not already initialized.
func (producer *Producer) InitializeSarama(ctx context.Context) error {

	// Do nothing if producer already initialized
	if producer.producer != nil {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// Initialize AsyncProducer with default config and envMax
	config := sarama.NewConfig()
	if producer.envMax > 0 {
		config.Producer.MaxMessageBytes = producer.envMax
	}
	saramaProducer, err := producer.cli.NewAsyncProducer(producer.brokers, config)
	if err != nil {
		return err
	}

	// On Successful initialization, close Init channel to stop uninitialized goroutine, and create initialized goroutine
	producer.producer = saramaProducer
	close(producer.channels.Init)
	log.Event(ctx, "Initialized Sarama Producer", log.Data{"topic": producer.topic})
	err = producer.createLoopInitialized(ctx)
	return err
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

	// Producer initialized with provided brokers and topic
	producer = Producer{
		envMax:  envMax,
		brokers: brokers,
		topic:   topic,
		cli:     cli,
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

	// Initialize Sarama producer, and return any error (which might not be considered fatal by caller)
	err = producer.InitializeSarama(ctx)
	if err != nil {
		producer.createLoopUninitialized(ctx)
	}
	return producer, err
}

// createLoopUninitialized creates a goroutine to handle uninitialized producers.
// It generates errors to the Errors channel when a message is intended to be sent through the Output channel.
// If the closer channel is closed, it closes the closed channel straight away and stops.
// If the init channel is closed, the goroutine stops because the sarama client is available.
func (producer *Producer) createLoopUninitialized(ctx context.Context) {

	// Do nothing if producer already initialized
	if producer.producer != nil {
		return
	}

	go func() {
		log.Event(ctx, "Started uninitialized kafka producer", log.Data{"topic": producer.topic})
		for {
			select {
			case message := <-producer.channels.Output:
				log.Event(ctx, "error sending a message", log.Data{"message": message, "topic": producer.topic}, log.Error(ErrUninitializedProducer))
				producer.channels.Errors <- ErrUninitializedProducer
			case <-producer.channels.Closer:
				log.Event(ctx, "Closing uninitialized kafka producer", log.Data{"topic": producer.topic})
				close(producer.channels.Closed)
				return
			case <-producer.channels.Init:
				return
			}
		}
	}()
}

// createLoopInitialized creates a goroutine to handle initialized producers.
// It redirects sarama errors to caller errors channel.
// If forwards messages from the output channel to the sarama producer input.
// If the closer channel is closed, it ends the loop and closes Closed channel.
func (producer *Producer) createLoopInitialized(ctx context.Context) error {

	// If sarama producer is not available, return error.
	if producer.producer == nil {
		return ErrInitSarama
	}

	// Start kafka producer with topic. Redirect errors and messages; and handle closerChannel
	go func() {
		defer close(producer.channels.Closed)
		log.Event(ctx, "Started initialized kafka producer", log.Data{"topic": producer.topic})
		for {
			select {
			case err := <-producer.producer.Errors():
				log.Event(ctx, "Producer", log.Data{"topic": producer.topic}, log.Error(err))
				producer.channels.Errors <- err
			case message := <-producer.channels.Output:
				producer.producer.Input() <- &sarama.ProducerMessage{Topic: producer.topic, Value: sarama.StringEncoder(message)}
			case <-producer.channels.Closer:
				log.Event(ctx, "Closing initialized kafka producer", log.Data{"topic": producer.topic})
				return
			}
		}
	}()
	return nil
}
