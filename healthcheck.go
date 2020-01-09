package kafka

import (
	"context"
	"errors"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
)

// ServiceName is the name of this service: Kafka
const ServiceName = "Kafka"

// MsgHealthyProducer Check message returned when Kafka producer is healthy
const MsgHealthyProducer = "kafka producer is healthy"

// MsgHealthyConsumerGroup Check message returned when Kafka consumer group is healthy
const MsgHealthyConsumerGroup = "kafka consumer group is healthy"

// List of errors
var (
	ErrNotImplemented = errors.New("healthcheck not implemented")
)

// minTime is the oldest time for Check structure.
var minTime = time.Unix(0, 0)

// Checker checks health of Kafka producer and returns it inside a Check structure. This method decides the severity of any possible error.
func (p *Producer) Checker(ctx context.Context) (*health.Check, error) {
	statusCode, err := healthcheck(ctx)
	// TODO perform any producer-specific health check?
	if err != nil {
		return getCheck(ctx, statusCode, health.StatusCritical, err.Error()), err
	}
	return getCheck(ctx, statusCode, health.StatusOK, MsgHealthyProducer), nil
}

// Checker checks health of Kafka consumer-group and returns it inside a Check structure. This method decides the severity of any possible error.
func (p *ConsumerGroup) Checker(ctx context.Context) (*health.Check, error) {
	statusCode, err := healthcheck(ctx)
	// TODO perform any consumer-specific health check?
	if err != nil {
		return getCheck(ctx, statusCode, health.StatusCritical, err.Error()), err
	}
	return getCheck(ctx, statusCode, health.StatusOK, MsgHealthyConsumerGroup), nil
}

// healthcheck implements the common healthcheck logic for kafka producers and consumers (contact broker(s)?)
func healthcheck(ctx context.Context) (code int, err error) {
	// TODO implement
	return 500, ErrNotImplemented
}

// getCheck creates a Check structure and populate it according the code, status and message
func getCheck(ctx context.Context, code int, status, message string) *health.Check {

	currentTime := time.Now().UTC()

	check := &health.Check{
		Name:        ServiceName,
		Status:      status,
		StatusCode:  code,
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
