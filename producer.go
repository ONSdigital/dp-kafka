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
	channels ProducerChannels
	Check    *health.Check
}

// Output is the channel to send outgoing messages to.
func (producer Producer) Output() chan []byte {
	return producer.channels.Output
}

// Errors provides errors returned from Kafka.
func (producer Producer) Errors() chan error {
	return producer.channels.Errors
}

// Close safely closes the consumer and releases all resources.
// pass in a context with a timeout or deadline.
// Passing a nil context will provide no timeout but is not recommended
func (producer *Producer) Close(ctx context.Context) (err error) {

	if ctx == nil {
		ctx = context.Background()
	}

	close(producer.channels.Closer)

	select {
	case <-producer.channels.Closed:
		close(producer.channels.Errors)
		close(producer.channels.Output)
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
	ctx context.Context, brokers []string, topic string, envMax int, channels ProducerChannels) (Producer, error) {
	return NewProducerWithSaramaClient(
		ctx, brokers, topic, envMax, channels, &SaramaClient{},
	)
}

// NewProducerWithSaramaClient returns a new producer with a provided Sarama client
func NewProducerWithSaramaClient(
	ctx context.Context, brokers []string, topic string, envMax int, channels ProducerChannels, cli Sarama) (Producer, error) {

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

	check := &health.Check{}

	// Validate provided channels
	err = channels.Validate()
	if err != nil {
		return Producer{}, err
	}

	// Start kafka producer with topic. Redirect errors and messages; and handle closerChannel
	go func() {
		defer close(channels.Closed)
		log.Event(ctx, "Started kafka producer", log.Data{"topic": topic})
		for {
			select {
			case err := <-producer.Errors():
				log.Event(ctx, "Producer", log.Data{"topic": topic}, log.Error(err))
				channels.Errors <- err
			case message := <-channels.Output:
				producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
			case <-channels.Closer:
				log.Event(ctx, "Closing kafka producer", log.Data{"topic": topic})
				return
			}
		}
	}()

	// Return producer with channels and check
	return Producer{producer, brokers, topic, channels, check}, nil
}
