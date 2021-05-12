package main

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/log"
	"github.com/kelseyhightower/envconfig"
)

const serviceName = "kafka-example-producer"

// Config is the kafka configuration for this example
type Config struct {
	Brokers                 []string      `envconfig:"KAFKA_ADDR"`
	KafkaMaxBytes           int           `envconfig:"KAFKA_MAX_BYTES"`
	KafkaVersion            string        `envconfig:"KAFKA_VERSION"`
	ProducedTopic           string        `envconfig:"KAFKA_PRODUCED_TOPIC"`
	WaitForProducerReady    bool          `envconfig:"KAFKA_WAIT_PRODUCER_READY"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Chomp                   bool          `envconfig:"CHOMP_MSG"`
	kafka.SecurityConfig
}

// period of time between tickers
const ticker = 3 * time.Second

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
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		sig := <-signals
		log.Event(ctx, "os signal received", log.Data{"signal": sig}, log.WARN)
		cancel()
	}()

	// Read Config
	cfg := &Config{
		Brokers:                 []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		KafkaMaxBytes:           50 * 1024 * 1024,
		KafkaVersion:            "1.0.2",
		ProducedTopic:           "myTopic",
		WaitForProducerReady:    true,
		GracefulShutdownTimeout: 5 * time.Second,
		Chomp:                   false,
	}
	if err := envconfig.Process("", cfg); err != nil {
		return err
	}

	// run kafka Producer
	producer, err := runProducer(ctx, cfg)
	if err != nil {
		return err
	}

	// blocks until context is done
	<-ctx.Done()
	return closeProducer(ctx, producer, cfg.GracefulShutdownTimeout)
}

func getProducerConfig(cfg *Config) *kafka.ProducerConfig {
	return &kafka.ProducerConfig{
		MaxMessageBytes: &cfg.KafkaMaxBytes,
		KafkaVersion:    &cfg.KafkaVersion,
		SecurityConfig:  cfg.SecurityConfig,
	}
}

func runProducer(ctx context.Context, cfg *Config) (*kafka.Producer, error) {
	stdinChannel := make(chan string)

	log.Event(ctx, "[KAFKA-TEST] Starting Producer (stdin sent to producer)", log.INFO, log.Data{"config": cfg})
	kafka.SetMaxMessageSize(int32(cfg.KafkaMaxBytes))

	// Create Producer with channels and config
	pChannels := kafka.CreateProducerChannels()
	pConfig := getProducerConfig(cfg)
	producer, err := kafka.NewProducer(ctx, cfg.Brokers, cfg.ProducedTopic, pChannels, pConfig)
	if err != nil {
		return nil, err
	}

	// go-routine to log errors from error channel
	pChannels.LogErrors(ctx, "[KAFKA-TEST] Producer error")

	// Producer not initialised at creation time. It will retry to initialise later, we may want to block until it is initialised
	if !producer.IsInitialised() {
		if cfg.WaitForProducerReady {
			log.Event(ctx, "[KAFKA-TEST] Producer could not be initialised at creation time. Waiting until we can initialise it.", log.WARN)
			waitForInitialised(ctx, pChannels)
		} else {
			log.Event(ctx, "[KAFKA-TEST] Producer could not be initialised at creation time. Will be initialised later.", log.WARN)
		}
	}

	// Create loop-control channel and context
	eventLoopContext, eventLoopCancel := context.WithCancel(ctx)
	eventLoopDone := make(chan bool)

	// stdin reader loop
	go func(ch chan string) {
		reader := bufio.NewReader(os.Stdin)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			if cfg.Chomp {
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
		defer close(eventLoopDone)
		for {
			select {

			case <-time.After(ticker):
				log.Event(ctx, "[KAFKA-TEST] tick", log.INFO)

			case <-eventLoopContext.Done():
				log.Event(ctx, "[KAFKA-TEST] Event loop context done", log.INFO, log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return

			case stdinLine := <-stdinChannel:
				// Used for this example to write messages to kafka consumer topic (should not be needed in applications)
				pChannels.Output <- []byte(stdinLine)
				log.Event(ctx, "[KAFKA-TEST] Message output", log.INFO, log.Data{"messageSent": stdinLine, "messageChars": []byte(stdinLine)})
			}
		}
	}()
	return producer, nil
}

func closeProducer(ctx context.Context, producer *kafka.Producer, gracefulShutdownTimeout time.Duration) error {
	log.Event(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": gracefulShutdownTimeout}, log.INFO)
	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)

	// track shutown gracefully closes up
	var hasShutdownError bool

	// background graceful shutdown
	go func() {
		defer cancel()
		log.Event(ctx, "[KAFKA-TEST] Closing kafka producer", log.INFO)
		if err := producer.Close(ctx); err != nil {
			hasShutdownError = true
		}
		log.Event(ctx, "[KAFKA-TEST] Closed kafka producer", log.INFO)
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

// waitForInitialised blocks until the producer is initialised or closed
func waitForInitialised(ctx context.Context, pChannels *kafka.ProducerChannels) {
	select {
	case <-pChannels.Ready:
		log.Event(ctx, "[KAFKA-TEST] Producer is now initialised.", log.WARN)
	case <-pChannels.Closer:
		log.Event(ctx, "[KAFKA-TEST] Producer is being closed.", log.WARN)
	case <-ctx.Done():
		log.Event(ctx, "[KAFKA-TEST] Producer context done.", log.WARN)
	}
}
