package kafka

import (
	"context"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
)

// Producer provides a producer of Kafka messages
type Producer struct {
	producer sarama.AsyncProducer
	brokers  []string
	topic    string
	output   chan []byte
	errors   chan error
	closer   chan struct{}
	closed   chan struct{}
	Check    *health.Check
}

// Output is the channel to send outgoing messages to.
func (producer Producer) Output() chan []byte {
	return producer.output
}

// Errors provides errors returned from Kafka.
func (producer Producer) Errors() chan error {
	return producer.errors
}

// Close safely closes the consumer and releases all resources.
// pass in a context with a timeout or deadline.
// Passing a nil context will provide no timeout but is not recommended
func (producer *Producer) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	close(producer.closer)

	select {
	case <-producer.closed:
		close(producer.errors)
		close(producer.output)
		log.Event(ctx, "Successfully closed kafka producer")
		return producer.producer.Close()

	case <-ctx.Done():
		log.Event(ctx, "Shutdown context time exceeded, skipping graceful shutdown of consumer group")
		return ErrShutdownTimedOut
	}
}

// NewProducer returns a new producer instance using the provided config and channels.
// The rest of the config is set to defaults. If any channel parameter is nil, an error will be returned.
func NewProducer(
	ctx context.Context, brokers []string, topic string, envMax int,
	outputChan chan []byte, errorChan chan error, closerChan, closedChan chan struct{}) (Producer, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	return NewProducerWithSaramaClient(
		ctx, brokers, topic, envMax,
		outputChan, errorChan, closerChan, closedChan, &SaramaClient{},
	)
}

// NewProducerWithSaramaClient returns a new producer with a provided Sarama client
func NewProducerWithSaramaClient(
	ctx context.Context, brokers []string, topic string, envMax int,
	chOutput chan []byte, chError chan error, chCloser, chClosed chan struct{}, cli Sarama) (producer Producer, err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	// Producer initialized with anything that cannot cause an error
	producer = Producer{
		brokers: brokers,
		topic:   topic,
	}

	// Initial Check struct
	check := &health.Check{Name: ServiceName}
	producer.Check = check

	// Validate provided channels and assign them to producer. ErrNoChannel should be considered fatal by caller.
	missingChannels := []string{}
	if chOutput == nil {
		missingChannels = append(missingChannels, "Output")
	}
	if chError == nil {
		missingChannels = append(missingChannels, "Error")
	}
	if chCloser == nil {
		missingChannels = append(missingChannels, "Closer")
	}
	if chClosed == nil {
		missingChannels = append(missingChannels, "Closed")
	}
	if len(missingChannels) > 0 {
		return producer, &ErrNoChannel{ChannelNames: missingChannels}
	}
	producer.output = chOutput
	producer.errors = chError
	producer.closer = chCloser
	producer.closed = chClosed

	// Create Sarama AsyncProducer with MaxMessageBytes. Errors at this point are not necessarily fatal (e.g. brokers not reachable).
	config := sarama.NewConfig()
	if envMax > 0 {
		config.Producer.MaxMessageBytes = envMax
	}
	saramaProducer, err := cli.NewAsyncProducer(brokers, config)
	if err != nil {
		return producer, err
	}
	producer.producer = saramaProducer

	// Sart kafka producer with topic. Redirect errors and messages; and handle closerChannel
	go func() {
		defer close(chClosed)
		log.Event(ctx, "Started kafka producer", log.Data{"topic": topic})
		for {
			select {
			case err := <-saramaProducer.Errors():
				log.Event(ctx, "Producer", log.Data{"topic": topic}, log.Error(err))
				chError <- err
			case message := <-chOutput:
				saramaProducer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
			case <-chCloser:
				log.Event(ctx, "Closing kafka producer", log.Data{"topic": topic})
				return
			}
		}
	}()

	// Return correctly initialized producer (err might be a non-fatal error returned by srama client creation)
	return producer, nil
}
