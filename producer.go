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

	// If Sarama Producer is not available, we can close 'closed' channel straight away
	if producer.producer == nil {
		close(producer.channels.Closed)
	}

	// If Sarama Producer is available, we wait for it to close 'closed' channel, or ctx timeout.
	select {
	case <-producer.channels.Closed:
		close(producer.channels.Errors)
		close(producer.channels.Output)
		log.Event(ctx, "Successfully closed kafka producer")
		if producer.producer == nil {
			return nil
		}
		return producer.producer.Close()

	case <-ctx.Done():
		log.Event(ctx, "Shutdown context time exceeded, skipping graceful shutdown of producer")
		return ErrShutdownTimedOut
	}
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
	producer.producer = saramaProducer
	log.Event(ctx, "Initialized Sarama Producer", log.Data{"topic": producer.topic})

	// Start kafka producer with topic. Redirect errors and messages; and handle closerChannel
	go func() {
		defer close(producer.channels.Closed)
		log.Event(ctx, "Started kafka producer", log.Data{"topic": producer.topic})
		for {
			select {
			case err := <-saramaProducer.Errors():
				log.Event(ctx, "Producer", log.Data{"topic": producer.topic}, log.Error(err))
				producer.channels.Errors <- err
			case message := <-producer.channels.Output:
				saramaProducer.Input() <- &sarama.ProducerMessage{Topic: producer.topic, Value: sarama.StringEncoder(message)}
			case <-producer.channels.Closer:
				log.Event(ctx, "Closing kafka producer", log.Data{"topic": producer.topic})
				return
			}
		}
	}()

	return nil
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
	return producer, err
}
