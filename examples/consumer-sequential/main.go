package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/kelseyhightower/envconfig"
)

const serviceName = "kafka-example-consumer"

// Config is the kafka configuration for this example
type Config struct {
	Brokers                 []string      `envconfig:"KAFKA_ADDR"`
	KafkaMaxBytes           int           `envconfig:"KAFKA_MAX_BYTES"`
	KafkaVersion            string        `envconfig:"KAFKA_VERSION"`
	ConsumedTopic           string        `envconfig:"KAFKA_CONSUMED_TOPIC"`
	ConsumedGroup           string        `envconfig:"KAFKA_CONSUMED_GROUP"`
	WaitForConsumerReady    bool          `envconfig:"KAFKA_WAIT_CONSUMER_READY"`
	KafkaSecProtocol        string        `envconfig:"KAFKA_SEC_PROTO"`
	KafkaSecCACerts         string        `envconfig:"KAFKA_SEC_CA_CERTS"`
	KafkaSecClientCert      string        `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	KafkaSecClientKey       string        `envconfig:"KAFKA_SEC_CLIENT_KEY" json:"-"`
	KafkaSecSkipVerify      bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Snooze                  bool          `envconfig:"SNOOZE"`
	OverSleep               bool          `envconfig:"OVERSLEEP"`
}

// period of time between tickers
const ticker = 3 * time.Second

func main() {
	log.Namespace = serviceName
	ctx, cancel := context.WithCancel(context.Background())

	if err := run(ctx, cancel); err != nil {
		log.Fatal(ctx, "fatal runtime error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cancel context.CancelFunc) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)
	go func(cancel context.CancelFunc) {
		// blocks until an os interrupt or a fatal error occurs
		sig := <-signals
		log.Info(ctx, "os signal received", log.Data{"signal": sig})
		cancel()
	}(cancel)

	// Read Config
	cfg := &Config{
		Brokers:                 []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		KafkaMaxBytes:           50 * 1024 * 1024,
		KafkaVersion:            "1.0.2",
		ConsumedTopic:           "myTopic",
		ConsumedGroup:           log.Namespace,
		WaitForConsumerReady:    true,
		GracefulShutdownTimeout: 5 * time.Second,
		Snooze:                  true,
		OverSleep:               false,
	}
	if err := envconfig.Process("", cfg); err != nil {
		return err
	}

	// run kafka Consumer Group
	consumerGroup, err := runConsumerGroup(ctx, cfg)
	if err != nil {
		return err
	}

	<-ctx.Done()
	return closeConsumerGroup(ctx, cancel, consumerGroup, cfg.GracefulShutdownTimeout)
}

func runConsumerGroup(ctx context.Context, cfg *Config) (*kafka.ConsumerGroup, error) {
	log.Info(ctx, "[KAFKA-TEST] Starting ConsumerGroup (messages sent to stdout)", log.Data{"config": cfg})
	kafka.SetMaxMessageSize(int32(cfg.KafkaMaxBytes))

	// Create ConsumerGroup with channels and config
	cgChannels := kafka.CreateConsumerGroupChannels(1)
	cgConfig := &kafka.ConsumerGroupConfig{
		KafkaVersion: &cfg.KafkaVersion,
	}
	if cfg.KafkaSecProtocol == "TLS" {
		cgConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}
	cg, err := kafka.NewConsumerGroup(ctx, cfg.Brokers, cfg.ConsumedTopic, cfg.ConsumedGroup, cgChannels, cgConfig)
	if err != nil {
		return nil, err
	}

	// go-routine to log errors from error channel
	cgChannels.LogErrors(ctx, "[KAFKA-TEST] ConsumerGroup error")

	// Consumer not initialised at creation time. We need to retry to initialise it.
	if !cg.IsInitialised() {
		if cfg.WaitForConsumerReady {
			log.Warn(ctx, "[KAFKA-TEST] Consumer could not be initialised at creation time. Waiting until we can initialise it.")
			waitForInitialised(ctx, cg.Channels())
		} else {
			log.Warn(ctx, "[KAFKA-TEST] Consumer could not be initialised at creation time. Will be initialised later.")
			go waitForInitialised(ctx, cg.Channels())
		}
	}

	// eventLoop
	consumeCount := 0
	go func() {
		for {
			delay := time.NewTimer(ticker)
			select {

			case <-delay.C:
				log.Info(ctx, "[KAFKA-TEST] tick")

			case consumedMessage, ok := <-cgChannels.Upstream:
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
				if !ok {
					break
				}
				// consumer will be nil if the broker could not be contacted, that's why we use the channel directly instead of consumer.Incoming()
				consumeCount++
				logData := log.Data{"consumeCount": consumeCount, "messageOffset": consumedMessage.Offset()}
				log.Info(ctx, "[KAFKA-TEST] Received message", logData)

				consumedData := consumedMessage.GetData()
				logData["messageString"] = string(consumedData)
				logData["messageRaw"] = consumedData
				logData["messageLen"] = len(consumedData)

				// Allows us to dictate the process for shutting down and how fast we consume messages in this example app, (should not be used in applications)
				sleepIfRequired(ctx, cfg, logData)

				consumedMessage.CommitAndRelease()
				log.Info(ctx, "[KAFKA-TEST] committed and released message", log.Data{"messageOffset": consumedMessage.Offset()})

			case <-ctx.Done():
				// Ensure timer is stopped and its resources are freed
				if !delay.Stop() {
					// if the timer has been stopped then read from the channel
					<-delay.C
				}
				return
			}
		}
	}()
	return cg, nil
}

func closeConsumerGroup(ctx context.Context, cancel context.CancelFunc, cg *kafka.ConsumerGroup, gracefulShutdownTimeout time.Duration) (err error) {
	log.Info(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": gracefulShutdownTimeout})
	ctxShutdown, shutdownCancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)

	// track shutdown gracefully closes up
	var shutdownError error

	// background graceful shutdown
	go func() {
		defer shutdownCancel()
		log.Info(ctx, "[KAFKA-TEST] Closing kafka consumerGroup")
		if err := cg.Close(ctxShutdown); err != nil {
			shutdownError = err
		}
		log.Info(ctx, "[KAFKA-TEST] Closed kafka consumerGroup")
	}()

	// wait for timeout or success (via cancel)
	select {
	case <-ctx.Done():
		err = ctx.Err()
		log.Warn(ctx, "[KAFKA-TEST] graceful shutdown abandoned during close", log.FormatErrors([]error{err}))
		return
	case <-ctxShutdown.Done():
		err = ctxShutdown.Err()
		if err == context.DeadlineExceeded {
			log.Warn(ctx, "[KAFKA-TEST] graceful shutdown timed out", log.FormatErrors([]error{err}))
			return
		} else if err != nil {
			log.Error(ctx, "[KAFKA-TEST] graceful shutdown failed during close", err)
			return
		}

		if shutdownError != nil {
			err = fmt.Errorf("failed to shutdown gracefully: %w", shutdownError)
			log.Error(ctx, "failed to shutdown gracefully ", err)
			return
		}
	}

	log.Info(ctx, "graceful shutdown was successful")
	return
}

// sleepIfRequired sleeps if config requires to do so, in order to simulate a delay.
// Snooze will cause a delay of 500ms, and OverSleep will cause a delay of the timeout plus 500 ms.
// This function is for testing purposes only and should not be used in applications.
func sleepIfRequired(ctx context.Context, cfg *Config, logData log.Data) {
	var sleep time.Duration
	if cfg.Snooze || cfg.OverSleep {
		// Snooze slows consumption for testing
		sleep = 500 * time.Millisecond
		if cfg.OverSleep {
			// OverSleep tests taking more than shutdown timeout to process a message
			sleep += cfg.GracefulShutdownTimeout + time.Second*2
		}
		logData["sleep"] = sleep
	}

	log.Info(ctx, "[KAFKA-TEST] Message consumed", logData)
	if sleep > time.Duration(0) {
		time.Sleep(sleep)
		log.Info(ctx, "[KAFKA-TEST] done sleeping")
	}
}

// waitForInitialised blocks until the consumer is initialised or closed
func waitForInitialised(ctx context.Context, cgChannels *kafka.ConsumerGroupChannels) {
	select {
	case <-cgChannels.Ready:
		log.Warn(ctx, "[KAFKA-TEST] Consumer is now initialised.")
	case <-cgChannels.Closer:
		log.Warn(ctx, "[KAFKA-TEST] Consumer is being closed.")
	case <-ctx.Done():
		log.Warn(ctx, "[KAFKA-TEST] Consumer context done - not waiting for initialisation")
	}
}
