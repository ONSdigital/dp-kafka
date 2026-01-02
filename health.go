package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/ONSdigital/dp-kafka/v5/interfaces"
	"github.com/ONSdigital/log.go/v2/log"
)

// ServiceName is the name of this service: Kafka.
const ServiceName = "Kafka"

// MsgHealthyProducer Check message returned when Kafka producer is healthy.
const MsgHealthyProducer = "kafka producer is healthy"

// MsgHealthyConsumerGroup Check message returned when Kafka consumer group is healthy.
const MsgHealthyConsumerGroup = "kafka consumer group is healthy"

// SaramaNewBroker wraps the real call to sarama.NewBroker
var SaramaNewBroker interfaces.BrokerGenerator = func(addr string) interfaces.SaramaBroker {
	return sarama.NewBroker(addr)
}

// Healthcheck validates all the provided brokers for the provided topic.
// It returns a HealthInfoMap containing all the information.
func Healthcheck(ctx context.Context, brokers []interfaces.SaramaBroker, topic string, cfg *sarama.Config) HealthInfoMap {
	brokersHealthInfo := HealthInfoMap{topic: topic}
	for _, broker := range brokers {
		brokersHealthInfo.Set(broker, validateBroker(ctx, broker, topic, cfg))
	}
	return brokersHealthInfo
}

func ensureBrokerOpen(ctx context.Context, broker interfaces.SaramaBroker, cfg *sarama.Config) (err error) {
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

// validateBroker checks that the provider broker is reachable and the topic is in its metadata.
// If a broker is not reachable, it will retry to contact it.
// It returns the information in a HealthInfo struct
func validateBroker(ctx context.Context, broker interfaces.SaramaBroker, topic string, cfg *sarama.Config) HealthInfo {
	healthInfo := HealthInfo{}

	var resp *sarama.MetadataResponse
	var err error
	logData := log.Data{"address": broker.Addr(), "topic": topic}

	// Metadata request (will fail if connection cannot be established)
	request := sarama.MetadataRequest{Topics: []string{topic}}

	// note: `!reachable` also a loop condition
	for retriesLeft := 1; retriesLeft >= 0 && !healthInfo.Reachable; retriesLeft-- {
		if err = ensureBrokerOpen(ctx, broker, cfg); err != nil {
			if retriesLeft == 0 {
				// will exit loop, err will cause failure
				continue
			}
			log.Warn(ctx, "error opening broker - will retry", log.FormatErrors([]error{err}), logData)
		}

		if resp, err = broker.GetMetadata(&request); err != nil {
			if retriesLeft > 0 {
				closeErrs := []error{err}
				// want next retry to trigger broker.Open, so Close first
				if err = broker.Close(); err != nil {
					closeErrs = append(closeErrs, err)
					log.Warn(ctx, "failed to obtain metadata from broker - close also failed", logData, log.FormatErrors(closeErrs))
				} else {
					log.Warn(ctx, "failed to obtain metadata from broker, closed broker for retry", logData, log.FormatErrors(closeErrs))
				}
			}
			// when retriesLeft == 0, will exit loop and err will be returned
		} else {
			// GetMetadata success, this exits retry loop
			healthInfo.Reachable = true
		}
	}
	// catch any errors during final retry loop
	if err != nil || !healthInfo.Reachable {
		log.Warn(ctx, "failed to obtain metadata from broker", logData, log.FormatErrors([]error{err}))
		return healthInfo
	}

	for _, metadata := range resp.Topics {
		if metadata.Name == topic && metadata.Err == sarama.ErrNoError {
			healthInfo.HasTopic = true
			return healthInfo
		}
	}

	return healthInfo
}
