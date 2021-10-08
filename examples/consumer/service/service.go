package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ONSdigital/dp-kafka/v3/consumer"
	"github.com/ONSdigital/dp-kafka/v3/examples/consumer/config"
	"github.com/ONSdigital/dp-kafka/v3/examples/consumer/handler"
	"github.com/ONSdigital/dp-kafka/v3/global"
	"github.com/ONSdigital/dp-kafka/v3/kafkaconfig"
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
	consumer *consumer.ConsumerGroup
}

// Init: Create ConsumerGroup with config, and register the handler
func (svc *Service) Init(ctx context.Context, cfg *config.Config) (err error) {
	global.SetMaxMessageSize(int32(cfg.KafkaMaxBytes)) // TODO should this be part of config package?
	svc.cfg = cfg

	// Create handler
	handler := &handler.Handler{
		Cfg: cfg,
	}

	// Create kafka consumer, passing relevant config
	svc.consumer, err = consumer.New(ctx, &kafkaconfig.ConsumerGroup{
		BrokerAddrs:  cfg.Brokers,
		Topic:        cfg.ConsumedTopic,
		GroupName:    cfg.ConsumedGroup,
		KafkaVersion: &cfg.KafkaVersion,
		NumWorkers:   &cfg.KafkaParallelMessages,
	})
	if err != nil {
		return fmt.Errorf("error creating kafka consumer: %w", err)
	}
	svc.consumer.RegisterHandler(ctx, handler.Handle)

	return nil
}

// Start: Create ConsumerGroup with config, and register the handler
func (svc *Service) Start(ctx context.Context) (err error) {
	log.Info(ctx, "[KAFKA-TEST] Starting ConsumerGroup (messages sent to stdout)", log.Data{"config": svc.cfg})

	svc.consumer.LogErrors(ctx)

	// start consuming now (in a real app this should be triggered by a healthy state)
	svc.consumer.Start()

	// ticker to show the consumer state periodically
	createTickerLoop(ctx, svc.consumer)

	// create loop to start-stop the consumer periodically according to pre-defined constants
	createStartStopLoop(ctx, svc.consumer)

	return nil
}

func (svc *Service) Close(ctx context.Context) error {
	log.Info(ctx, "[KAFKA-TEST] Commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": svc.cfg.GracefulShutdownTimeout})
	ctx, cancel := context.WithTimeout(context.Background(), svc.cfg.GracefulShutdownTimeout)
	hasShutdownError := false

	go func() {
		defer cancel()
		log.Info(ctx, "[KAFKA-TEST] Closing kafka consumerGroup")
		if err := svc.consumer.Close(ctx); err != nil {
			hasShutdownError = true
		}
		log.Info(ctx, "[KAFKA-TEST] Closed kafka consumerGroup")
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		log.Warn(ctx, "[KAFKA-TEST] graceful shutdown timed out", log.FormatErrors([]error{ctx.Err()}))
		return ctx.Err()
	}

	if hasShutdownError {
		err := errors.New("failed to shutdown gracefully")
		log.Error(ctx, "[KAFKA-TEST] failed to shutdown gracefully ", err)
		return err
	}

	log.Info(ctx, "[KAFKA-TEST] graceful shutdown was successful")
	return nil
}

// createTickerLoop will log the consumer state every tick
func createTickerLoop(ctx context.Context, cg *consumer.ConsumerGroup) {
	go func() {
		for {
			select {
			case <-time.After(ticker):
				log.Info(ctx, "[KAFKA-TEST] tick ", log.Data{
					"state": cg.State(),
				})
			case <-cg.Channels().Closed:
				log.Info(ctx, "[KAFKA-TEST] tick - CLOSED - ", log.Data{
					"state": cg.State(),
				})
			}
		}
	}()
}

// createStartStopLoop creates a loop that periodically sends true or false to the Consume channel
// - A 'Starting'/'Consuming' consumer is sent true for iterationsFromStartToStop times, then false
// - A 'Stopping'/'Stopped' consumer is sent false for iterationsFromStopToStart times, then true
func createStartStopLoop(ctx context.Context, cg *consumer.ConsumerGroup) {
	cnt := 0
	// consume start-stop loop
	go func() {
		for {
			select {
			case <-time.After(startStop):
				logData := log.Data{
					"state": cg.State(),
				}
				cnt++
				switch cg.State() {
				case consumer.Starting.String(), consumer.Consuming.String():
					if cnt >= iterationsFromStartToStop {
						log.Info(ctx, "[KAFKA-TEST] ++ STOP consuming", logData)
						cnt = 0
						cg.Stop()
					} else {
						log.Info(ctx, "[KAFKA-TEST] START consuming", logData)
						cg.Start()
					}
				case consumer.Stopping.String(), consumer.Stopped.String():
					if cnt >= iterationsFromStopToStart {
						log.Info(ctx, "[KAFKA-TEST] ++ START consuming", logData)
						cnt = 0
						cg.Start()
					} else {
						log.Info(ctx, "[KAFKA-TEST] STOP consuming", logData)
						cg.Stop()
					}
				default:
				}
			case <-cg.Channels().Closed:
				log.Info(ctx, "[KAFKA-TEST] consume loop - CLOSED - ", log.Data{
					"state": cg.State(),
				})
			}
		}
	}()
}
