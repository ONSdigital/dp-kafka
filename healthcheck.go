package kafka

import (
	"context"
	"errors"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
	"github.com/Shopify/sarama"
)

// ServiceName is the name of this service: Kafka.
const ServiceName = "Kafka"

// MsgHealthyProducer Check message returned when Kafka producer is healthy.
const MsgHealthyProducer = "kafka producer is healthy"

// MsgHealthyConsumerGroup Check message returned when Kafka consumer group is healthy.
const MsgHealthyConsumerGroup = "kafka consumer group is healthy"

// Checker checks health of Kafka producer and returns it inside its Check structure.
func (p *Producer) Checker(ctx context.Context) (*health.Check, error) {
	err := healthcheck(ctx, p.brokers, p.topic)
	currentTime := time.Now().UTC()
	p.Check.LastChecked = &currentTime
	if err != nil {
		p.Check.LastFailure = &currentTime
		p.Check.Status = getStatusFromError(err)
		p.Check.Message = err.Error()
		return p.Check, err
	}
	p.Check.LastSuccess = &currentTime
	p.Check.Status = health.StatusOK
	p.Check.Message = MsgHealthyProducer
	return p.Check, nil
}

// Checker checks health of Kafka consumer-group and returns it inside its Check structure.
func (c *ConsumerGroup) Checker(ctx context.Context) (*health.Check, error) {
	err := healthcheck(ctx, c.brokers, c.topic)
	currentTime := time.Now().UTC()
	c.Check.LastChecked = &currentTime
	if err != nil {
		c.Check.LastFailure = &currentTime
		c.Check.Status = getStatusFromError(err)
		c.Check.Message = err.Error()
		return c.Check, err
	}
	c.Check.LastSuccess = &currentTime
	c.Check.Status = health.StatusOK
	c.Check.Message = MsgHealthyConsumerGroup
	return c.Check, nil
}

// getStatusFromError decides the health status (severity) according to the provided error
func getStatusFromError(err error) string {
	switch err.(type) {
	case *ErrInvalidBrokers:
		return health.StatusWarning
	default:
		return health.StatusCritical
	}
}

// healthcheck implements the common healthcheck logic for kafka producers and consumers, by contacting the provided
// brokers and asking for topic metadata. Possible errors:
// - ErrBrokersNotReachable if a broker cannot be contacted.
// - ErrInvalidBrokers if topic metadata is not returned by a broker.
func healthcheck(ctx context.Context, brokers []string, topic string) error {
	// Validate connections to brokers
	unreachBrokers := []string{}
	invalidBrokers := []string{}
	if len(brokers) == 0 {
		return errors.New("No brokers defined")
	}
	for _, addr := range brokers {
		broker := sarama.NewBroker(addr)
		// Open a connection to broker (will not fail if cannot establish)
		err := broker.Open(nil)
		if err != nil {
			unreachBrokers = append(unreachBrokers, addr)
			log.Event(ctx, "failed to open connection to broker", log.Data{"address": addr}, log.Error(err))
			continue
		}
		defer broker.Close()
		// Metadata request (will fail if connection cannot be established)
		request := sarama.MetadataRequest{Topics: []string{topic}}
		resp, err := broker.GetMetadata(&request)
		if err != nil {
			unreachBrokers = append(unreachBrokers, addr)
			log.Event(ctx, "failed to obtain metadata from broker", log.Data{"address": addr, "topic": topic}, log.Error(err))
			continue
		}
		// Validate metadata response is as expected
		if len(resp.Topics) == 0 {
			invalidBrokers = append(invalidBrokers, addr)
			log.Event(ctx, "topic metadata not found in broker", log.Data{"address": addr, "topic": topic})
			continue
		}
	}
	// If any connection is not established, the healthcheck will fail
	if len(unreachBrokers) > 0 {
		return &ErrBrokersNotReachable{Addrs: unreachBrokers}
	}
	// If any broker returned invalid metadata response, the healthcheck will fail
	if len(invalidBrokers) > 0 {
		return &ErrInvalidBrokers{Addrs: invalidBrokers}
	}
	return nil
}
