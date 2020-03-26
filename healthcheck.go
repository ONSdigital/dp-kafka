package kafka

import (
	"context"
	"errors"

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

// Checker checks health of Kafka producer and updates the provided CheckState accordingly
func (p *Producer) Checker(ctx context.Context, state *health.CheckState) error {
	err := p.healthcheck(ctx)
	if err != nil {
		state.Update(getStatusFromError(err), err.Error(), 0)
		return nil
	}
	state.Update(health.StatusOK, MsgHealthyProducer, 0)
	return nil
}

// Checker checks health of Kafka consumer-group and updates the provided CheckState accordingly
func (cg *ConsumerGroup) Checker(ctx context.Context, state *health.CheckState) error {
	err := cg.healthcheck(ctx)
	if err != nil {
		state.Update(getStatusFromError(err), err.Error(), 0)
		return nil
	}
	state.Update(health.StatusOK, MsgHealthyConsumerGroup, 0)
	return nil
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

// healthcheck performs the healthcheck logic for a kafka producer.
func (p *Producer) healthcheck(ctx context.Context) error {
	err := healthcheck(ctx, p.brokers, p.topic)
	if err != nil {
		return err
	}
	// If Sarama client is not initialised, we need to initialise it
	err = p.Initialise(ctx)
	if err != nil {
		log.Event(ctx, "error initialising sarama producer", log.WARN, log.Data{"topic": p.topic}, log.Error(err))
		return ErrInitSarama
	}
	return nil
}

// healthcheck performs the healthcheck logic for a kafka consumer group.
func (cg *ConsumerGroup) healthcheck(ctx context.Context) error {
	err := healthcheck(ctx, cg.brokers, cg.topic)
	if err != nil {
		return err
	}
	// If Sarama client is not initialised, we need to initialise it
	err = cg.Initialise(ctx)
	if err != nil {
		log.Event(ctx, "error initialising sarama consumer-group", log.WARN, log.Data{"topic": cg.topic, "group": cg.group}, log.Error(err))
		return ErrInitSarama
	}
	return nil
}

// healthcheck implements the common healthcheck logic for kafka producers and consumers, by contacting the provided
// brokers and asking for topic metadata. Possible errors:
// - ErrBrokersNotReachable if a broker cannot be contacted.
// - ErrInvalidBrokers if topic metadata is not returned by a broker.
func healthcheck(ctx context.Context, brokers []*sarama.Broker, topic string) error {

	// Vars to keep track of validation state
	unreachableBrokers := []string{}
	invalidBrokers := []string{}
	if len(brokers) == 0 {
		return errors.New("No brokers defined")
	}

	// Validate all brokers
	for _, broker := range brokers {
		reachable, valid := validateBroker(ctx, broker, topic)
		if !reachable {
			unreachableBrokers = append(unreachableBrokers, broker.Addr())
			continue
		}
		if !valid {
			invalidBrokers = append(invalidBrokers, broker.Addr())
		}
	}

	// If any connection is not established, the healthcheck will fail
	if len(unreachableBrokers) > 0 {
		return &ErrBrokersNotReachable{Addrs: unreachableBrokers}
	}

	// If any broker returned invalid metadata response, the healthcheck will fail
	if len(invalidBrokers) > 0 {
		return &ErrInvalidBrokers{Addrs: invalidBrokers}
	}

	return nil
}

func validateBroker(ctx context.Context, broker *sarama.Broker, topic string) (reachable, valid bool) {

	// Open a connection to broker (will not fail if cannot establish)
	err := broker.Open(sarama.NewConfig())
	defer broker.Close()
	if err != nil {
		log.Event(ctx, "failed to open connection to broker", log.WARN, log.Data{"address": broker.Addr()}, log.Error(err))
		return false, false
	}

	// Connect to broker - this will block until the connection is fully established, or until it fails
	isConnected, err := broker.Connected()
	if err != nil {
		log.Event(ctx, "failed to connect to broker", log.WARN, log.Data{"address": broker.Addr()}, log.Error(err))
		return false, false
	}
	if !isConnected {
		log.Event(ctx, "not connected to broker", log.WARN, log.Data{"address": broker.Addr()})
		return false, false
	}

	// Metadata request (will fail if connection cannot be established)
	request := sarama.MetadataRequest{Topics: []string{topic}}
	resp, err := broker.GetMetadata(&request)
	if err != nil {
		log.Event(ctx, "failed to obtain metadata from broker", log.WARN, log.Data{"address": broker.Addr(), "topic": topic}, log.Error(err))
		return false, false
	}

	// Validate metadata response is as expected
	if len(resp.Topics) == 0 {
		log.Event(ctx, "topic metadata not found in broker", log.WARN, log.Data{"address": broker.Addr(), "topic": topic})
		return true, false
	}

	// Broker is reachable and topic is in its metadata
	return true, true
}
