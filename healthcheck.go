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

// minTime is the oldest time for Check structure.
var minTime = time.Unix(0, 0)

// Checker checks health of Kafka producer and returns it inside a Check structure.
func (p *Producer) Checker(ctx context.Context) (*health.Check, error) {
	err := healthcheck(ctx, p.brokers, p.topic)
	if err != nil {
		p.Check, err = checker(ctx, err)
		return p.Check, nil
	}
	p.Check = getCheck(ctx, health.StatusOK, MsgHealthyProducer)
	return p.Check, nil
}

// Checker checks health of Kafka consumer-group and returns it inside a Check structure.
func (c *ConsumerGroup) Checker(ctx context.Context) (*health.Check, error) {
	err := healthcheck(ctx, c.brokers, c.topic)
	if err != nil {
		c.Check, err = checker(ctx, err)
		return c.Check, nil
	}
	c.Check = getCheck(ctx, health.StatusOK, MsgHealthyConsumerGroup)
	return c.Check, nil
}

// checker decides the severity and gets the corresponding Check struct for the provided error.
// Common for providers and consumers.
func checker(ctx context.Context, err error) (*health.Check, error) {
	switch err.(type) {
	case *ErrInvalidBrokers:
		return getCheck(ctx, health.StatusWarning, err.Error()), err
	default:
		return getCheck(ctx, health.StatusCritical, err.Error()), err
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

// getCheck creates a Check structure and populate it according the status and message.
func getCheck(ctx context.Context, status, message string) *health.Check {

	currentTime := time.Now().UTC()

	check := &health.Check{
		Name:        ServiceName,
		Status:      status,
		Message:     message,
		LastChecked: currentTime,
		LastSuccess: minTime,
		LastFailure: minTime,
	}

	if status == health.StatusOK {
		check.LastSuccess = currentTime
	} else {
		check.LastFailure = currentTime
	}

	return check
}
