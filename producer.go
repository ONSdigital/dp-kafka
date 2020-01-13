package kafka

import (
	"context"
	"errors"

	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
)

// ErrShutdownTimedOut represents an error received due to the context
// deadline being exceeded
var (
	ErrShutdownTimedOut = errors.New("Shutdown context timed out")
	ErrNoOputputChannel = errors.New("OutputChannel does not exist")
	ErrNoErrorChannel   = errors.New("ErrorChannel does not exist")
	ErrNoCloserChannel  = errors.New("CloserChannel does not exist")
	ErrNoClosedChannel  = errors.New("ClosedChannel does not exist")
)

// Producer provides a producer of Kafka messages
type Producer struct {
	producer sarama.AsyncProducer
	output   chan []byte
	errors   chan error
	closer   chan struct{}
	closed   chan struct{}
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
		log.Event(nil, "Successfully closed kafka producer")
		return producer.producer.Close()

	case <-ctx.Done():
		log.Event(nil, "Shutdown context time exceeded, skipping graceful shutdown of consumer group")
		return ErrShutdownTimedOut
	}
}

// NewProducer returns a new producer instance using the provided config and channels.
// The rest of the config is set to defaults. If any channel parameter is nil, an error will be returned.
func NewProducer(brokers []string, topic string, envMax int,
	outputChan chan []byte, errorChan chan error, closerChan, closedChan chan struct{}) (Producer, error) {
	return NewProducerWithSaramaClient(brokers, topic, envMax, outputChan, errorChan, closerChan, closedChan, &SaramaClient{})
}

// NewProducerWithSaramaClient returns a new producer with a provided Sarama client
func NewProducerWithSaramaClient(brokers []string, topic string, envMax int,
	outputChan chan []byte, errorChan chan error, closerChan, closedChan chan struct{}, cli Sarama) (Producer, error) {

	// Create Sarama AsyncProducer with MaxMessageBytes
	config := sarama.NewConfig()
	if envMax > 0 {
		config.Producer.MaxMessageBytes = envMax
	}
	producer, err := cli.NewAsyncProducer(brokers, config)
	if err != nil {
		return Producer{}, err
	}

	// Validate provided channels
	if outputChan == nil {
		return Producer{}, ErrNoOputputChannel
	}
	if errorChan == nil {
		return Producer{}, ErrNoErrorChannel
	}
	if closerChan == nil {
		return Producer{}, ErrNoCloserChannel
	}
	if closedChan == nil {
		return Producer{}, ErrNoClosedChannel
	}

	// Sart kafka producer with topic. Redirect errors and messages; and handle closerChannel
	go func() {
		defer close(closedChan)
		log.Event(nil, "Started kafka producer", log.Data{"topic": topic})
		for {
			select {
			case err := <-producer.Errors():
				log.Event(nil, "Producer", log.Data{"topic": topic}, log.Error(err))
				errorChan <- err
			case message := <-outputChan:
				producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
			case <-closerChan:
				log.Event(nil, "Closing kafka producer", log.Data{"topic": topic})
				return
			}
		}
	}()

	// Return producer with channels
	return Producer{producer, outputChan, errorChan, closerChan, closedChan}, nil
}
