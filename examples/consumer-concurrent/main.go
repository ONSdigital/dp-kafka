package main

import (
	"context"
	"errors"
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
	KafkaParallelMessages   int           `envconfig:"KAFKA_PARALLEL_MESSAGES"`
	ConsumedTopic           string        `envconfig:"KAFKA_CONSUMED_TOPIC"`
	ConsumedGroup           string        `envconfig:"KAFKA_CONSUMED_GROUP"`
	WaitForConsumerReady    bool          `envconfig:"KAFKA_WAIT_CONSUMER_READY"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Snooze                  bool          `envconfig:"SNOOZE"`
	OverSleep               bool          `envconfig:"OVERSLEEP"`
}

// period of time between tickers
const ticker = 3 * time.Second

var consumeCount = 0

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Fatal(ctx, "fatal runtime error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)

	// Read Config
	cfg := &Config{
		Brokers:                 []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		KafkaMaxBytes:           50 * 1024 * 1024,
		KafkaVersion:            "1.0.2",
		KafkaParallelMessages:   3,
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

	// blocks until an os interrupt or a fatal error occurs
	sig := <-signals
	log.Info(ctx, "os signal received", log.Data{"signal": sig})
	return closeConsumerGroup(ctx, consumerGroup, cfg.GracefulShutdownTimeout)
}

func runConsumerGroup(ctx context.Context, cfg *Config) (*kafka.ConsumerGroup, error) {
	log.Info(ctx, "[KAFKA-TEST] Starting ConsumerGroup (messages sent to stdout)", log.Data{"config": cfg})
	kafka.SetMaxMessageSize(int32(cfg.KafkaMaxBytes))

	// Create ConsumerGroup with channels and config
	cgChannels := kafka.CreateConsumerGroupChannels(cfg.KafkaParallelMessages)
	cgConfig := &kafka.ConsumerGroupConfig{KafkaVersion: &cfg.KafkaVersion}
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

	// eventLoop to consumer messages from Upstream channel by sending them the workers
	go func() {
		for {
			<-time.After(ticker)
			log.Info(ctx, "[KAFKA-TEST] tick")
		}
	}()

	// workers to consume messages in parallel
	for w := 1; w <= cfg.KafkaParallelMessages; w++ {
		go consume(ctx, cfg, w, cgChannels.Upstream)
	}

	return cg, nil
}

// consume waits for messages to arrive to the upstream channel and consumes them, in an infinite loop
func consume(ctx context.Context, cfg *Config, id int, upstream chan kafka.Message) {
	log.Info(ctx, "worker started consuming", log.Data{"worker_id": id})
	for {
		consumedMessage, ok := <-upstream
		if !ok {
			break
		}
		consumeCount++
		logData := log.Data{"consumeCount": consumeCount, "messageOffset": consumedMessage.Offset(), "worker_id": id}
		log.Info(ctx, "[KAFKA-TEST] Received message", logData)

		consumedData := consumedMessage.GetData()
		logData["messageString"] = string(consumedData)
		logData["messageRaw"] = consumedData
		logData["messageLen"] = len(consumedData)

		// Allows us to dictate the process for shutting down and how fast we consume messages in this example app, (should not be used in applications)
		sleepIfRequired(ctx, cfg, logData)

		consumedMessage.CommitAndRelease()
		log.Info(ctx, "[KAFKA-TEST] committed and released message", logData)
	}
}

func closeConsumerGroup(ctx context.Context, cg *kafka.ConsumerGroup, gracefulShutdownTimeout time.Duration) error {
	log.Info(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": gracefulShutdownTimeout})
	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)

	// track shutown gracefully closes up
	var hasShutdownError bool

	// background graceful shutdown
	go func() {
		defer cancel()
		log.Info(ctx, "[KAFKA-TEST] Closing kafka consumerGroup")
		if err := cg.Close(ctx); err != nil {
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
		log.Error(ctx, "failed to shutdown gracefully ", err)
		return err
	}

	log.Info(ctx, "graceful shutdown was successful")
	return nil
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
	}
}
