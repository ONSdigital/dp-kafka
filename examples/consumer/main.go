package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/kelseyhightower/envconfig"
)

const serviceName = "kafka-example-consumer"

// Config is the kafka configuration for this example
type Config struct {
	Brokers               []string `envconfig:"KAFKA_ADDR"`
	KafkaMaxBytes         int      `envconfig:"KAFKA_MAX_BYTES"`
	KafkaVersion          string   `envconfig:"KAFKA_VERSION"`
	KafkaParallelMessages int      `envconfig:"KAFKA_PARALLEL_MESSAGES"`
	ConsumedTopic         string   `envconfig:"KAFKA_CONSUMED_TOPIC"`
	ConsumedGroup         string   `envconfig:"KAFKA_CONSUMED_GROUP"`
	// WaitForConsumerReady    bool          `envconfig:"KAFKA_WAIT_CONSUMER_READY"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Snooze                  bool          `envconfig:"SNOOZE"`
	OverSleep               bool          `envconfig:"OVERSLEEP"`
}

// period of time between tickers
const ticker = 3 * time.Second

// start-stop period and iterations in each state
const (
	startStop                 = 10 * time.Second
	iterationsFromStopToStart = 2 // will result in 20 seconds in 'Stopping' / 'Stopped' state
	iterationsFromStartToStop = 4 // will result in 40 seconds in 'Starting' / 'Started' state
)

// consumeCount keeps track of the total number of consumed messages
var consumeCount = 0
var goRoutinesOffset = 0

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
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Read Config
	cfg := &Config{
		Brokers:               []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		KafkaMaxBytes:         50 * 1024 * 1024,
		KafkaVersion:          "1.0.2",
		KafkaParallelMessages: 3,
		ConsumedTopic:         "myTopic",
		ConsumedGroup:         log.Namespace,
		// WaitForConsumerReady:    false,
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

	goRoutinesOffset = runtime.NumGoroutine()

	// Create ConsumerGroup with channels and config
	cgChannels := kafka.CreateConsumerGroupChannels(cfg.KafkaParallelMessages)
	cgConfig := &kafka.ConsumerGroupConfig{KafkaVersion: &cfg.KafkaVersion}
	cg, err := kafka.NewConsumerGroup(ctx, cfg.Brokers, cfg.ConsumedTopic, cfg.ConsumedGroup, cgChannels, cgConfig)
	if err != nil {
		return nil, err
	}

	// go-routine to log errors from error channel
	cgChannels.LogErrors(ctx, "[KAFKA-TEST] ConsumerGroup error")
	goRoutinesOffset++

	// eventLoop to consumer messages from Upstream channel by sending them the workers
	go func() {
		for {
			<-time.After(ticker)
			log.Info(ctx, "[KAFKA-TEST] tick ", log.Data{
				// "goroutines": runtime.NumGoroutine() - goRoutinesOffset,
				"state": cg.State(),
			})
		}
	}()
	goRoutinesOffset++

	// workers to consume messages in parallel
	for w := 1; w <= cfg.KafkaParallelMessages; w++ {
		go consume(ctx, cfg, w, cgChannels.Upstream)
	}
	goRoutinesOffset += cfg.KafkaParallelMessages

	// start consuming now
	cg.Channels().Consume <- true

	// create loop to start-stop the consumer periodically according to pre-defined constants
	createStartStopLoop(ctx, cg)
	goRoutinesOffset++

	return cg, nil
}

// consume waits for messages to arrive to the upstream channel and consumes them, in an infinite loop
func consume(ctx context.Context, cfg *Config, id int, upstream chan kafka.Message) {
	log.Info(ctx, "starting consumer worker", log.Data{"worker_id": id})
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

// createStartStopLoop creates a loop that periodically sends true or false to the Consume channel
// - A 'Starting'/'Consuming' consumer is sent true for iterationsFromStartToStop times, then false
// - A 'Stopping'/'Stopped' consumer is sent false for iterationsFromStopToStart times, then true
func createStartStopLoop(ctx context.Context, cg *kafka.ConsumerGroup) {
	cnt := 0
	// consume start-stop loop
	go func() {
		for {
			select {
			case <-time.After(startStop):
				logData := log.Data{
					"goroutines": runtime.NumGoroutine() - goRoutinesOffset,
					"state":      cg.State(),
				}
				cnt++
				switch cg.State() {
				case kafka.Starting.String(), kafka.Consuming.String():
					if cnt >= iterationsFromStartToStop {
						log.Info(ctx, "[KAFKA-TEST] ++ STOP consuming", logData)
						cnt = 0
						cg.Stop()
					} else {
						log.Info(ctx, "[KAFKA-TEST] START consuming", logData)
						cg.Start()
					}
				case kafka.Stopping.String(), kafka.Stopped.String():
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
			case <-ctx.Done():
				return
			}
		}
	}()
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
