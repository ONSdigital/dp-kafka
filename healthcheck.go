package kafka

import (
	"context"
	"errors"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/v2/log"
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
		return health.StatusCritical
	default:
		return health.StatusCritical
	}
}

// healthcheck performs the healthcheck logic for a kafka producer.
func (p *Producer) healthcheck(ctx context.Context) error {
	err := healthcheck(ctx, p.brokers, p.topic, p.config)
	if err != nil {
		return err
	}
	// If Sarama client is not initialised, we need to initialise it
	err = p.Initialise(ctx)
	if err != nil {
		log.Warn(ctx, "error initialising sarama producer", log.Data{"topic": p.topic}, log.FormatErrors([]error{err}))
		return ErrInitSarama
	}
	return nil
}

// healthcheck performs the healthcheck logic for a kafka consumer group.
func (cg *ConsumerGroup) healthcheck(ctx context.Context) error {
	err := healthcheck(ctx, cg.brokers, cg.topic, cg.config)
	if err != nil {
		return err
	}
	// If Sarama client is not initialised, we need to initialise it
	err = cg.Initialise(ctx)
	if err != nil {
		log.Warn(ctx, "error initialising sarama consumer-group", log.Data{"topic": cg.topic, "group": cg.group}, log.FormatErrors([]error{err}))
		return ErrInitSarama
	}
	return nil
}

// healthcheck implements the common healthcheck logic for kafka producers and consumers, by contacting the provided
// brokers and asking for topic metadata. Possible errors:
// - ErrBrokersNotReachable if a broker cannot be contacted.
// - ErrInvalidBrokers if topic metadata is not returned by a broker.
func healthcheck(ctx context.Context, brokers []*sarama.Broker, topic string, cfg *sarama.Config) error {

	// Vars to keep track of validation state
	unreachableBrokers := []string{}
	invalidBrokers := []string{}
	if len(brokers) == 0 {
		return errors.New("no brokers defined")
	}

	// Validate all brokers
	for _, broker := range brokers {
		reachable, valid := validateBroker(ctx, broker, topic, cfg)
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

func ensureBrokerOpen(ctx context.Context, broker *sarama.Broker, cfg *sarama.Config) (err error) {
	var isConnected bool
	if isConnected, err = broker.Connected(); err != nil {
		log.Warn(ctx, "broker reports connected error - ignoring", log.FormatErrors([]error{err}), log.Data{"address": broker.Addr()})
	}
	if !isConnected {
		log.Info(ctx, "broker not connected: connecting", log.Data{"address": broker.Addr()})
		err = broker.Open(cfg)
	}
	return
}

// validateBroker checks that the provider broker is reachable and the topic is in its metadata
func validateBroker(ctx context.Context, broker *sarama.Broker, topic string, cfg *sarama.Config) (reachable, valid bool) {

	var resp *sarama.MetadataResponse
	var err error
	logData := log.Data{"address": broker.Addr(), "topic": topic}

	// Metadata request (will fail if connection cannot be established)
	request := sarama.MetadataRequest{Topics: []string{topic}}

	// note: `!reachable` also a loop condition
	for retriesLeft := 1; retriesLeft >= 0 && !reachable; retriesLeft-- {
		if err = ensureBrokerOpen(ctx, broker, cfg); err != nil {
			if retriesLeft == 0 {
				// will exit loop, err will cause failure
				continue
			}
			log.Warn(ctx, "error opening broker - will retry", log.FormatErrors([]error{err}), logData)
		}

		if resp, err = broker.GetMetadata(&request); err != nil {
			if retriesLeft > 0 {
				errs := []error{err}
				// want next retry to trigger broker.Open, so Close first
				if err = broker.Close(); err != nil {
					closeErrs := append(errs, err)
					log.Warn(ctx, "failed to obtain metadata from broker - close also failed", logData, log.FormatErrors(closeErrs))
				} else {
					log.Warn(ctx, "failed to obtain metadata from broker, closed broker for retry", logData, log.FormatErrors(errs))
				}
			}
			// when retriesLeft == 0, will exit loop and err will be returned
		} else {
			// GetMetadata success, this exits retry loop
			reachable = true
		}
	}
	// catch any errors during final retry loop
	if err != nil || !reachable {
		log.Warn(ctx, "failed to obtain metadata from broker", logData, log.FormatErrors([]error{err}))
		return
	}

	for _, metadata := range resp.Topics {
		if metadata.Name == topic {
			valid = true
			return
		}
	}

	return
}
