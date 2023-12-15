package service

import (
	"context"
	"fmt"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/ONSdigital/dp-kafka/v4/examples/consumer-batch/config"
	"github.com/ONSdigital/dp-kafka/v4/examples/consumer-batch/handler"
	"github.com/ONSdigital/log.go/v2/log"
)

// constant values for this example
const (
	ticker                    = 3 * time.Second
	startStop                 = 10 * time.Second
	iterationsFromStopToStart = 2 // will result in 20 seconds in 'Stopping' / 'Stopped' state
	iterationsFromStartToStop = 4 // will result in 40 seconds in 'Starting' / 'Started' state
)

type Service struct {
	cfg      *config.Config
	consumer *kafka.ConsumerGroup
}

// Init: Create ConsumerGroup with config, and register the handler
func (svc *Service) Init(ctx context.Context, cfg *config.Config) (err error) {
	kafka.SetMaxMessageSize(int32(cfg.KafkaMaxBytes)) // TODO should this be part of config package?
	svc.cfg = cfg

	// Create handler
	h := &handler.Handler{
		Cfg: cfg,
	}

	cgConfig := &kafka.ConsumerGroupConfig{
		BrokerAddrs:   cfg.Brokers,
		Topic:         cfg.ConsumedTopic,
		GroupName:     cfg.ConsumedGroup,
		KafkaVersion:  &cfg.KafkaVersion,
		BatchSize:     &cfg.BatchSize,
		BatchWaitTime: &cfg.BatchWaitTime,
	}
	if cfg.KafkaSecProtocol == "TLS" {
		cgConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}
	// Create kafka consumer, passing relevant config
	svc.consumer, err = kafka.NewConsumerGroup(ctx, cgConfig)
	if err != nil {
		return fmt.Errorf("error creating kafka consumer: %w", err)
	}
	if err := svc.consumer.RegisterBatchHandler(ctx, h.Handle); err != nil {
		return fmt.Errorf("error registering batch handler: %w", err)
	}

	return nil
}

// Start: Create ConsumerGroup with config, and register the handler
func (svc *Service) Start(ctx context.Context) (err error) {
	log.Info(ctx, "[KAFKA-TEST] Starting ConsumerGroup (messages sent to stdout)", log.Data{"config": svc.cfg})

	svc.consumer.LogErrors(ctx)

	// start consuming now (in a real app this should be triggered by a healthy state)
	if err := svc.consumer.Start(); err != nil {
		return fmt.Errorf("consumer failed to start: %w", err)
	}

	// ticker to show the consumer state periodically
	createTickerLoop(ctx, svc.consumer)

	// create loop to start-stop the consumer periodically according to pre-defined constants
	if svc.cfg.KafkaStartStop {
		createStartStopLoop(ctx, svc.consumer)
	}

	return nil
}

func (svc *Service) Close(ctx context.Context) error {
	log.Info(ctx, "[KAFKA-TEST] Commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": svc.cfg.GracefulShutdownTimeout})
	ctx, cancel := context.WithTimeout(ctx, svc.cfg.GracefulShutdownTimeout)
	var shutdownErr error

	go func() {
		defer cancel()

		log.Info(ctx, "[KAFKA-TEST] Stopping kafka consumerGroup")
		if err := svc.consumer.StopAndWait(); err != nil {
			shutdownErr = fmt.Errorf("[KAFKA-TEST] failed to stop and wait consumerGroup in service shutdown: %w", err)
		} else {
			log.Info(ctx, "[KAFKA-TEST] Successfully stopped consumerGroup")
		}

		log.Info(ctx, "[KAFKA-TEST] Closing kafka consumerGroup")
		if err := svc.consumer.Close(ctx); err != nil {
			shutdownErr = fmt.Errorf("[KAFKA-TEST] failed to close consumerGroup in service shutdown: %w", err)
		} else {
			log.Info(ctx, "[KAFKA-TEST] Successfully closed consumerGroup")
		}
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		log.Warn(ctx, "[KAFKA-TEST] graceful shutdown timed out", log.FormatErrors([]error{ctx.Err()}))
		return ctx.Err()
	}

	if shutdownErr != nil {
		log.Error(ctx, "[KAFKA-TEST] failed to shutdown gracefully ", shutdownErr)
		return shutdownErr
	}

	log.Info(ctx, "[KAFKA-TEST] graceful shutdown was successful")
	return nil
}

// createTickerLoop will log the consumer state every tick
func createTickerLoop(ctx context.Context, cg *kafka.ConsumerGroup) {
	go func() {
		for {
			delay := time.NewTimer(ticker)
			select {
			case <-delay.C:
				log.Info(ctx, "[KAFKA-TEST] tick ", log.Data{
					"state": cg.State().String(),
				})
			case <-cg.Channels().Closed:
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
				log.Info(ctx, "[KAFKA-TEST] tick - CLOSED - ", log.Data{
					"state": cg.State().String(),
				})
			}
		}
	}()
}

// createStartStopLoop creates a loop that periodically sends true or false to the Consume channel
// - A 'Starting'/'Consuming' consumer is sent true for iterationsFromStartToStop times, then false
// - A 'Stopping'/'Stopped' consumer is sent false for iterationsFromStopToStart times, then true
func createStartStopLoop(ctx context.Context, cg *kafka.ConsumerGroup) {
	cnt := 0
	// consume start-stop loop
	go func() {
		for {
			delay := time.NewTimer(startStop)
			select {
			case <-delay.C:
				logData := log.Data{
					"state": cg.State().String(),
				}
				cnt++
				switch cg.State() {
				case kafka.Starting, kafka.Consuming:
					if cnt >= iterationsFromStartToStop {
						log.Info(ctx, "[KAFKA-TEST] ++ STOP consuming", logData)
						cnt = 0
						if err := cg.Stop(); err != nil {
							log.Warn(ctx, "consumer-group failed to stop", log.Data{"err": err})
						}
					} else {
						log.Info(ctx, "[KAFKA-TEST] START consuming", logData)
						if err := cg.Start(); err != nil {
							log.Warn(ctx, "consumer-group failed to start", log.Data{"err": err})
						}
					}
				case kafka.Stopping, kafka.Stopped:
					if cnt >= iterationsFromStopToStart {
						log.Info(ctx, "[KAFKA-TEST] ++ START consuming", logData)
						cnt = 0
						if err := cg.Start(); err != nil {
							log.Warn(ctx, "consumer-group failed to start", log.Data{"err": err})
						}
					} else {
						log.Info(ctx, "[KAFKA-TEST] STOP consuming", logData)
						if err := cg.Stop(); err != nil {
							log.Warn(ctx, "consumer-group failed to stop", log.Data{"err": err})
						}
					}
				default:
				}
			case <-cg.Channels().Closed:
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
				log.Info(ctx, "[KAFKA-TEST] consume loop - CLOSED - ", log.Data{
					"state": cg.State().String(),
				})
			}
		}
	}()
}
