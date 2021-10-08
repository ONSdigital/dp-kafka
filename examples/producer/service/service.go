package service

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ONSdigital/dp-kafka/v3/examples/producer/config"
	"github.com/ONSdigital/dp-kafka/v3/global"
	"github.com/ONSdigital/dp-kafka/v3/kafkaconfig"
	"github.com/ONSdigital/dp-kafka/v3/producer"
	"github.com/ONSdigital/log.go/v2/log"
)

// constant values for this example
const (
	ticker = 3 * time.Second
)

type Service struct {
	cfg      *config.Config
	producer *producer.Producer
}

func getProducerConfig(cfg *config.Config) *kafkaconfig.Producer {
	pCfg := &kafkaconfig.Producer{
		MaxMessageBytes: &cfg.KafkaMaxBytes,
		KafkaVersion:    &cfg.KafkaVersion,
		BrokerAddrs:     cfg.Brokers,
		Topic:           cfg.ProducedTopic,
	}
	if cfg.KafkaSecProtocol == "TLS" {
		pCfg.SecurityConfig = kafkaconfig.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}
	return pCfg
}

// Init: Create ConsumerGroup with config, and register the handler
func (svc *Service) Init(ctx context.Context, cfg *config.Config) (err error) {
	global.SetMaxMessageSize(int32(cfg.KafkaMaxBytes)) // TODO should this be part of config package?
	svc.cfg = cfg

	// Create Producer with channels and config
	svc.producer, err = producer.New(ctx, getProducerConfig(cfg))
	if err != nil {
		return fmt.Errorf("error creating kafka consumer: %w", err)
	}

	return nil
}

// Start: Create ConsumerGroup with config, and register the handler
func (svc *Service) Start(ctx context.Context, cancel context.CancelFunc) (err error) {
	log.Info(ctx, "[KAFKA-TEST] Starting Producer (stdin sent to producer)", log.Data{"config": svc.cfg})

	svc.producer.LogErrors(ctx)

	// Create loop-control channel and context
	eventLoopContext, eventLoopCancel := context.WithCancel(ctx)
	eventLoopDone := make(chan bool)

	stdinChannel := make(chan string)

	// stdin reader loop
	go func(ch chan string) {
		reader := bufio.NewReader(os.Stdin)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			if svc.cfg.Chomp {
				line = line[:len(line)-1]
			}
			ch <- line
		}
		eventLoopCancel()
		<-eventLoopDone
		close(ch)
	}(stdinChannel)

	// eventLoop
	go func() {
		defer cancel()
		defer close(eventLoopDone)
		for {
			select {

			case <-time.After(ticker):
				log.Info(ctx, "[KAFKA-TEST] tick")

			case <-eventLoopContext.Done():
				log.Info(ctx, "[KAFKA-TEST] Event loop context done", log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return

			case stdinLine := <-stdinChannel:
				// Used for this example to write messages to kafka consumer topic (should not be needed in applications)
				svc.producer.Channels().Output <- []byte(stdinLine)
				log.Info(ctx, "[KAFKA-TEST] Message output", log.Data{"messageSent": stdinLine, "messageChars": []byte(stdinLine)})
			}
		}
	}()

	return nil
}

func (svc *Service) Close(ctx context.Context) error {
	log.Info(ctx, "[KAFKA-TEST] Commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": svc.cfg.GracefulShutdownTimeout})
	ctx, cancel := context.WithTimeout(context.Background(), svc.cfg.GracefulShutdownTimeout)
	hasShutdownError := false

	go func() {
		defer cancel()
		log.Info(ctx, "[KAFKA-TEST] Closing kafka producer")
		if err := svc.producer.Close(ctx); err != nil {
			hasShutdownError = true
		}
		log.Info(ctx, "[KAFKA-TEST] Closed kafka producer")
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
