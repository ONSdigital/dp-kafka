package kafka

import (
	"context"

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
	outputChan chan []byte, errorChan chan error, closerChan, closedChan chan struct{}, cli Sarama) (Producer, error) {

	if ctx == nil {
		ctx = context.Background()
	}

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
	missingChannels := []string{}
	if outputChan == nil {
		missingChannels = append(missingChannels, "Output")
	}
	if errorChan == nil {
		missingChannels = append(missingChannels, "Error")
	}
	if closerChan == nil {
		missingChannels = append(missingChannels, "Closer")
	}
	if closedChan == nil {
		missingChannels = append(missingChannels, "Closed")
	}
	if len(missingChannels) > 0 {
		return Producer{}, &ErrNoChannel{ChannelNames: missingChannels}
	}

	// Sart kafka producer with topic. Redirect errors and messages; and handle closerChannel
	go func() {
		defer close(closedChan)
		log.Event(ctx, "Started kafka producer", log.Data{"topic": topic})
		for {
			select {
			case err := <-producer.Errors():
				log.Event(ctx, "Producer", log.Data{"topic": topic}, log.Error(err))
				errorChan <- err
			case message := <-outputChan:
				producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
			case <-closerChan:
				log.Event(ctx, "Closing kafka producer", log.Data{"topic": topic})
				return
			}
		}
	}()

	// Return producer with channels
	return Producer{producer, brokers, topic, outputChan, errorChan, closerChan, closedChan}, nil
}
