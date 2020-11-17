package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/log"
	"github.com/kelseyhightower/envconfig"
)

const serviceName = "kafka-example-consumer"

// Config is the kafka configuration for this example
type Config struct {
	Brokers                 []string      `envconfig:"KAFKA_ADDR"`
	KafkaMaxBytes           int           `envconfig:"KAFKA_MAX_BYTES"`
	KafkaVersion            string        `envconfig:"KAFKA_VERSION"`
	KafkaBatchSize          int           `envconfig:"KAFKA_BATCH_SIZE"`
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
		log.Event(ctx, "fatal runtime error", log.Error(err), log.FATAL)
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
		KafkaBatchSize:          10,
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
	log.Event(ctx, "os signal received", log.Data{"signal": sig}, log.INFO)
	return closeConsumerGroup(ctx, consumerGroup, cfg.GracefulShutdownTimeout)
}

func runConsumerGroup(ctx context.Context, cfg *Config) (*kafka.ConsumerGroup, error) {
	log.Event(ctx, "[KAFKA-TEST] Starting ConsumerGroup (messages sent to stdout)", log.INFO, log.Data{"config": cfg})
	kafka.SetMaxMessageSize(int32(cfg.KafkaMaxBytes))

	// Create ConsumerGroup with channels and config
	cgChannels := kafka.CreateConsumerGroupChannels(cfg.KafkaBatchSize)
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
			log.Event(ctx, "[KAFKA-TEST] Consumer could not be initialised at creation time. Waiting until we can initialise it.", log.WARN)
			waitForInitialised(ctx, cg.Channels())
		} else {
			log.Event(ctx, "[KAFKA-TEST] Consumer could not be initialised at creation time. Will be initialised later.", log.WARN)
			go waitForInitialised(ctx, cg.Channels())
		}
	}

	// eventLoop to consumer messages from Upstream channel by sending them the workers
	go func() {
		for {
			<-time.After(ticker)
			log.Event(ctx, "[KAFKA-TEST] tick", log.INFO)
		}
	}()

	// consume messages (main loop)
	go consume(ctx, cfg, cgChannels.Upstream)

	return cg, nil
}

// consume waits for messages to arrive to the upstream channel, appends them to a batch, and then, once the batch is full, processes the batch.
// Note that the messages are released straight away, but not committed until the batch has been successfully processed.
func consume(ctx context.Context, cfg *Config, upstream chan kafka.Message) {
	log.Event(ctx, "started consuming")
	var batch = []kafka.Message{}
	for {
		// get message from upstream channel
		consumedMessage, ok := <-upstream
		if !ok {
			break
		}
		consumeCount++
		logData := log.Data{"consumeCount": consumeCount, "messageOffset": consumedMessage.Offset()}
		log.Event(ctx, "[KAFKA-TEST] Received message", log.INFO, logData)

		// append message to batch
		batch = append(batch, consumedMessage)

		// if batch is full, process it
		if len(batch) == cfg.KafkaBatchSize {
			processBatch(ctx, cfg, logData, batch)
			batch = []kafka.Message{}
		}

		// release the message, so that the next one can be consumed
		consumedMessage.Release()
	}
}

func processBatch(ctx context.Context, cfg *Config, logData log.Data, batch []kafka.Message) {
	// Offsets of messages that are part of the batch
	batchOffsets := []int64{}
	for _, msg := range batch {
		batchOffsets = append(batchOffsets, msg.Offset())
	}
	logData["batch_offsets"] = batchOffsets

	// log before sleep
	log.Event(ctx, "processing batch", log.INFO, logData)

	// Allows us to dictate the process for shutting down and how fast we consume messages in this example app, (should not be used in applications)
	sleepIfRequired(ctx, cfg, logData)

	// log after sleep
	log.Event(ctx, "batch processed", log.INFO, logData)

	// commit after successfully
	commitBatch(batch)
}

// commitBatch marks all messages as consumed, and commits the last one (which will commit all offsets,
// which might be different among partitions)
func commitBatch(batch []kafka.Message) {
	for i, msg := range batch {
		if i < len(batch)-1 {
			msg.Mark()
		} else {
			msg.Commit()
		}
	}
}

func closeConsumerGroup(ctx context.Context, cg *kafka.ConsumerGroup, gracefulShutdownTimeout time.Duration) error {
	log.Event(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": gracefulShutdownTimeout}, log.INFO)
	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)

	// track shutown gracefully closes up
	var hasShutdownError bool

	// background graceful shutdown
	go func() {
		defer cancel()
		log.Event(ctx, "[KAFKA-TEST] Closing kafka consumerGroup", log.INFO)
		if err := cg.Close(ctx); err != nil {
			hasShutdownError = true
		}
		log.Event(ctx, "[KAFKA-TEST] Closed kafka consumerGroup", log.INFO)
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		log.Event(ctx, "[KAFKA-TEST] graceful shutdown timed out", log.WARN, log.Error(ctx.Err()))
		return ctx.Err()
	}

	if hasShutdownError {
		err := errors.New("failed to shutdown gracefully")
		log.Event(ctx, "failed to shutdown gracefully ", log.ERROR, log.Error(err))
		return err
	}

	log.Event(ctx, "graceful shutdown was successful", log.INFO)
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

	log.Event(ctx, "[KAFKA-TEST] Message consumed", log.INFO, logData)
	if sleep > time.Duration(0) {
		time.Sleep(sleep)
		log.Event(ctx, "[KAFKA-TEST] done sleeping", log.INFO)
	}
}

// waitForInitialised blocks until the consumer is initialised or closed
func waitForInitialised(ctx context.Context, cgChannels *kafka.ConsumerGroupChannels) {
	select {
	case <-cgChannels.Ready:
		log.Event(ctx, "[KAFKA-TEST] Consumer is now initialised.", log.WARN)
	case <-cgChannels.Closer:
		log.Event(ctx, "[KAFKA-TEST] Consumer is being closed.", log.WARN)
	}
}
