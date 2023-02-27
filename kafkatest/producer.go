package kafkatest

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/dp-kafka/v3/avro"
	"github.com/ONSdigital/dp-kafka/v3/mock"
	"github.com/Shopify/sarama"
)

type ProducerConfig struct {
	ChannelBufferSize int  // buffer size
	InitAtCreation    bool // Determines if the consumer is initialised or not when it's created, or it will be initialised later
}

var DefaultProducerConfig = &ProducerConfig{
	ChannelBufferSize: 30,
	InitAtCreation:    true,
}

// Producer is an extension of the moq Producer
// with implementation of required functions and Sarama mocks to emulate a fully functional kafka Producer.
type Producer struct {
	cfg            *ProducerConfig              // Mock configuration
	saramaErrors   chan *sarama.ProducerError   // Sarama level error channel
	saramaMessages chan *sarama.ProducerMessage // Sarama level produced message channel
	saramaMock     sarama.AsyncProducer         // Internal sarama async producer mock
	p              *kafka.Producer              // Internal producer
	Mock           *IProducerMock               // Implements all moq functions so users can validate calls
}

// producerInitialiser returns the saramaMock, or an error if it is nil (i.e. Sarama not initialised yet)
func (p *Producer) producerInitialiser(addrs []string, config *sarama.Config) (sarama.AsyncProducer, error) {
	if p.saramaMock == nil {
		return nil, errors.New("sarama mock not initialised")
	}
	return p.saramaMock, nil
}

// NewProducer creates a testing producer for testing.
// It behaves like a real producer, without network communication
func NewProducer(ctx context.Context, pConfig *kafka.ProducerConfig, cfg *ProducerConfig) (*Producer, error) {
	if pConfig == nil {
		return nil, errors.New("kafka producer config must be provided")
	}
	if cfg == nil {
		cfg = DefaultProducerConfig
	}

	p := &Producer{
		cfg:            cfg,
		saramaErrors:   make(chan *sarama.ProducerError, cfg.ChannelBufferSize),
		saramaMessages: make(chan *sarama.ProducerMessage, cfg.ChannelBufferSize),
	}

	saramaMock := &mock.SaramaAsyncProducerMock{
		CloseFunc: func() error {
			return nil
		},
		ErrorsFunc: func() <-chan *sarama.ProducerError {
			return p.saramaErrors
		},
		InputFunc: func() chan<- *sarama.ProducerMessage {
			return p.saramaMessages
		},
	}

	// if sarama async producer is initialised at creation, we need to set it before creating the producer
	if cfg.InitAtCreation {
		p.saramaMock = saramaMock
	}

	var err error
	p.p, err = kafka.NewProducerWithGenerators(
		ctx,
		pConfig,
		p.producerInitialiser,
		SaramaBrokerGenerator(pConfig.Topic),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for testing: %w", err)
	}

	// set saramaMock again, in case it was not set at creation time
	p.saramaMock = saramaMock

	// Map all the mock funcs to the internal producer functions
	p.Mock = &IProducerMock{
		AddHeaderFunc:     p.p.AddHeader,
		ChannelsFunc:      p.p.Channels,
		CheckerFunc:       p.p.Checker,
		CloseFunc:         p.p.Close,
		InitialiseFunc:    p.p.Initialise,
		IsInitialisedFunc: p.p.IsInitialised,
		LogErrorsFunc:     p.p.LogErrors,
		SendFunc:          p.p.Send,
	}

	return p, nil
}

// WaitForMessageSent waits for a new message being sent to Kafka, with a timeout according to the provided value
// If a message is sent, it unmarshals it into the provided 'event', using the provided avro 'schema'.
func (p *Producer) WaitForMessageSent(schema *avro.Schema, event interface{}, timeout time.Duration) error {
	delay := time.NewTimer(timeout)

	select {
	case <-delay.C:
		return errors.New("timeout while waiting for kafka message being produced")
	case <-p.p.Channels().Closer:
		if !delay.Stop() {
			<-delay.C
		}
		return errors.New("closer channel closed")
	case msg, ok := <-p.saramaMessages:
		if !delay.Stop() {
			<-delay.C
		}
		if !ok {
			return errors.New("sarama messages channel closed")
		}

		b, err := msg.Value.Encode()
		if err != nil {
			return fmt.Errorf("error encoding sent message: %w", err)
		}

		if err := schema.Unmarshal(b, event); err != nil {
			return fmt.Errorf("error unmarshalling sent message: %w", err)
		}
	}
	return nil
}

// WaitNoMessageSent waits until the timeWindow elapses.
// If during the time window the closer channel is closed,
// or a message is sent to the sarama message channel, then an error is returned
func (p *Producer) WaitNoMessageSent(timeWindow time.Duration) error {
	delay := time.NewTimer(timeWindow)

	select {
	case <-delay.C:
		return nil
	case <-p.p.Channels().Closer:
		if !delay.Stop() {
			<-delay.C
		}
		return errors.New("closer channel closed")
	case _, ok := <-p.saramaMessages:
		if !delay.Stop() {
			<-delay.C
		}
		if !ok {
			return errors.New("sarama messages channel closed")
		}
		return errors.New("unexpected message was sent within the time window")
	}
}
